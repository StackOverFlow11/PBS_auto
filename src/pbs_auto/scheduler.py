"""Submission engine — core scheduling loop.

Runs inside the daemon (or `--foreground`) and drives poll → submit →
flush iterations until all tasks reach a terminal state or a shutdown
signal is received. All state mutations go through the BatchStore
context manager so that crash-recovery sentinels are always in a
consistent state.

No Rich / display imports here — `run_dry_run` lives in `cli.py`.
"""

from __future__ import annotations

import logging
import signal
import time
from datetime import datetime

from pbs_auto.batch_store import BatchStore, recover_sentinels
from pbs_auto.config import AppConfig, ServerConfig
from pbs_auto.models import BatchState, Task, TaskStatus
from pbs_auto.pbs import PBSClient


log = logging.getLogger(__name__)


# Retryable qsub error patterns (matched case-insensitively).
RETRYABLE_PATTERNS = [
    "would exceed",
    "resource busy",
    "try again",
    "temporarily unavailable",
]


def _is_retryable_error(error_msg: str) -> bool:
    """Check if a qsub error is transient and should be retried."""
    lower = error_msg.lower()
    return any(p in lower for p in RETRYABLE_PATTERNS)


class Scheduler:
    """Manages the submission loop: poll → submit → flush → sleep."""

    def __init__(
        self,
        state: BatchState,
        config: AppConfig,
        server: ServerConfig,
        pbs: PBSClient,
        store: BatchStore,
        dry_run: bool = False,
    ):
        self.state = state
        self.config = config
        self.server = server
        self.pbs = pbs
        self.store = store
        self.dry_run = dry_run
        self._shutdown = False
        self._recovery_pending = False
        self._original_sigint = None
        self._original_sigterm = None

    def run(self) -> None:
        """Run the main loop until all tasks are terminal or shutdown."""
        self._install_signal_handlers()
        try:
            self._main_loop()
        finally:
            self._restore_signal_handlers()

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def _main_loop(self) -> None:
        try:
            while not self._shutdown:
                self._poll_status()

                if self._recovery_pending:
                    try:
                        if recover_sentinels(self.state, self.pbs):
                            self._recovery_pending = False
                    except Exception as e:  # noqa: BLE001
                        log.warning("recover_sentinels retry failed: %s", e)

                if self._shutdown:
                    break

                self._submit_pending()

                self.store.maybe_debounced_flush()

                if self._all_done_fast():
                    break

                self._sleep(self.config.poll_interval)
        finally:
            # Each step independent — failures do not block the next.
            try:
                self.store.force_flush()
            except Exception as e:  # noqa: BLE001
                log.error("final force_flush failed: %s", e)

    # ------------------------------------------------------------------
    # Poll
    # ------------------------------------------------------------------

    def _poll_status(self) -> None:
        """Query PBS and update task states based on actual job status."""
        if self.dry_run:
            return

        try:
            self.pbs.invalidate_cache()
            pbs_jobs = self.pbs.query_user_jobs(force=True)
        except RuntimeError as e:
            log.warning("PBS query failed, skipping poll cycle: %s", e)
            return

        # Iterate task dict in insertion order, filtered by the active
        # index. This gives deterministic iteration AND O(1) filtering.
        active = self.state._active_set
        for dir_key, task in list(self.state.tasks.items()):
            if dir_key not in active:
                continue
            if task.job_id is None:
                continue

            job_id_short = task.job_id.split(".")[0]
            pbs_job = pbs_jobs.get(job_id_short)

            if pbs_job is None:
                with self.store.mutate(task=task, flush="debounced"):
                    self._handle_job_disappeared(task)
            elif pbs_job.state == "R":
                if task.status != TaskStatus.RUNNING:
                    with self.store.mutate(task=task, flush="debounced"):
                        task.status = TaskStatus.RUNNING
                        task.start_time = datetime.now().isoformat()
            elif pbs_job.state == "Q":
                if task.status != TaskStatus.QUEUED:
                    with self.store.mutate(task=task, flush="debounced"):
                        task.status = TaskStatus.QUEUED

    def _handle_job_disappeared(self, task: Task) -> None:
        """Task's PBS job is gone — decide COMPLETED vs WARNING."""
        task.end_time = datetime.now().isoformat()

        if task.status == TaskStatus.SUBMITTED:
            task.status = TaskStatus.WARNING
            task.error_message = "Job disappeared before entering running state"
            return

        if task.start_time:
            try:
                start = datetime.fromisoformat(task.start_time)
                end = datetime.fromisoformat(task.end_time)
            except ValueError:
                start = end = None
            if start is not None and end is not None:
                elapsed = (end - start).total_seconds()
                if elapsed < self.config.early_exit_threshold:
                    task.status = TaskStatus.WARNING
                    task.error_message = (
                        f"Job ran for only {elapsed:.0f}s "
                        f"(threshold: {self.config.early_exit_threshold}s)"
                    )
                    return

        task.status = TaskStatus.COMPLETED

    # ------------------------------------------------------------------
    # Submit
    # ------------------------------------------------------------------

    def _submit_pending(self) -> None:
        """Submit pending tasks within resource limits."""
        if self.dry_run:
            return

        # Deterministic iteration: walk state.tasks in insertion order
        # and filter by the pending index. _submit_task mutates the set,
        # so we materialize via list() to avoid "set changed during
        # iteration" errors.
        pending = self.state._pending_set
        for dir_key, task in list(self.state.tasks.items()):
            if self._shutdown:
                break
            if dir_key not in pending:
                continue
            if task.status != TaskStatus.PENDING:
                continue

            running_cores, queued_cores = self._get_resource_usage()

            if running_cores + task.cores > self.server.max_running_cores:
                continue
            if queued_cores + task.cores > self.server.max_queued_cores:
                continue

            proceed = self._submit_task(task)

            # Inner shutdown check — reliability 3.3
            if self._shutdown:
                break

            if not proceed:
                # Retryable qsub error (e.g. "would exceed ncpus")
                # stop this round entirely; next poll will retry.
                break

            if self.config.submit_delay > 0:
                self._sleep(self.config.submit_delay)
                if self._shutdown:
                    break

    def _submit_task(self, task: Task) -> bool:
        """Submit one task via qsub, wrapped in a mutation context.

        Returns True if submission succeeded or hit a permanent
        failure (caller can proceed to next task). Returns False
        on retryable errors (caller should `break` this round).
        """
        with self.store.mutate(task=task, flush="eager"):
            try:
                job_id = self.pbs.submit(task)
            except FileNotFoundError as e:
                task.status = TaskStatus.FAILED
                task.error_message = str(e)
                return True
            except (RuntimeError, OSError) as e:
                msg = str(e)
                if _is_retryable_error(msg):
                    task.error_message = f"Retryable: {msg}"
                    return False
                task.status = TaskStatus.FAILED
                task.error_message = msg
                return True
            task.job_id = job_id
            task.status = TaskStatus.SUBMITTED
            task.submit_time = datetime.now().isoformat()
            task.error_message = None

        try:
            self.pbs.invalidate_cache()
        except Exception:  # noqa: BLE001
            pass
        return True

    def _get_resource_usage(self) -> tuple[int, int]:
        """Return (running_cores, queued_cores) across ALL user jobs.

        If the PBS query fails we conservatively report max usage so
        no new submissions are attempted this cycle.
        """
        try:
            pbs_jobs = self.pbs.query_user_jobs()
        except RuntimeError:
            return self.server.max_running_cores, self.server.max_queued_cores

        running_cores = 0
        queued_cores = 0
        for job in pbs_jobs.values():
            if job.state == "R":
                running_cores += job.cores
            elif job.state == "Q":
                queued_cores += job.cores
        return running_cores, queued_cores

    # ------------------------------------------------------------------
    # Loop helpers
    # ------------------------------------------------------------------

    def _all_done_fast(self) -> bool:
        """Check if all tasks are terminal using the derived indexes."""
        return not self.state._pending_set and not self.state._active_set

    def _sleep(self, seconds: float) -> None:
        """Interruptible sleep."""
        end = time.monotonic() + seconds
        while time.monotonic() < end and not self._shutdown:
            time.sleep(min(0.5, max(0.0, end - time.monotonic())))

    # ------------------------------------------------------------------
    # Signal handling — flag-only
    # ------------------------------------------------------------------

    def _install_signal_handlers(self) -> None:
        """Install flag-only SIGINT + SIGTERM handlers.

        The handler simply sets `_shutdown = True` and returns —
        no signal-unsafe operations. The main loop checks the flag
        and exits via the normal `try/finally` path.
        """
        self._original_sigint = signal.getsignal(signal.SIGINT)
        self._original_sigterm = signal.getsignal(signal.SIGTERM)

        def handler(signum, frame):  # noqa: ARG001
            self._shutdown = True

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

    def _restore_signal_handlers(self) -> None:
        if self._original_sigint is not None:
            signal.signal(signal.SIGINT, self._original_sigint)
        if self._original_sigterm is not None:
            signal.signal(signal.SIGTERM, self._original_sigterm)
