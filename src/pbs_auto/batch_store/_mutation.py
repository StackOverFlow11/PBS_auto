"""BatchStore: single save entry + mutation context manager.

All state mutations go through `BatchStore.mutate(task, flush=...)`,
which creates a sentinel file (if a task is provided) before the
mutation body runs and removes the sentinel after a clean exit.
The mutation context NEVER raises — every step is wrapped in
try/except log-and-continue to guarantee the main loop keeps running.

Flush policy:
    - `flush="eager"` → `_flush_now` is called inside `__exit__`
    - `flush="debounced"` → `_dirty = True`; `maybe_debounced_flush`
      is called from the main loop end-of-iteration
    - A `FAILED` task is always promoted to eager flush (persisting
      permanent failures immediately is critical for operator trust)

Sentinel lifecycle contract:
    - `__enter__`: create sentinel with body "PENDING"
    - Body: normal exit → sentinel MUST be unconditionally removed
                          (even if `task.status` is still PENDING due
                          to a retryable error — the next loop will
                          create a fresh sentinel)
    - Body: exception → sentinel is LEFT so that recovery can see
                        something was in flight
"""

from __future__ import annotations

import logging
import time
from typing import Literal

from pbs_auto.batch_store._sentinels import (
    create_sentinel,
    remove_sentinel,
    update_sentinel_job_id,
)
from pbs_auto.models import ACTIVE_STATUSES, BatchState, Task, TaskStatus


log = logging.getLogger(__name__)


FlushPolicy = Literal["eager", "debounced"]


class BatchStore:
    """Single owner of batch state mutation + persistence.

    Construct once per daemon process AFTER `recover_sentinels` has
    attached any salvageable jobs. Three documented bypass call sites
    write state without going through BatchStore (all at bootstrap /
    recovery boundaries):
        1. Parent `--fresh` init save (fork happens after)
        2. `recover_sentinels` attach (BatchStore not yet constructed)
        3. `run_dry_run` (read-only, never saves)
    """

    # Minimum interval between debounced flushes, in seconds.
    _min_debounce_interval = 1.0

    def __init__(self, state: BatchState):
        self.state = state
        self._dirty = False
        self._last_flush_mono = time.monotonic()

    # ------------------------------------------------------------------
    # Mutation entry point
    # ------------------------------------------------------------------

    def mutate(
        self,
        task: Task | None = None,
        flush: FlushPolicy = "debounced",
    ) -> _MutationContext:
        """Return a context manager wrapping a single state mutation.

        Usage:
            with store.mutate(task=t, flush="eager") as m:
                t.status = TaskStatus.SUBMITTED
                t.job_id = job_id
        """
        return _MutationContext(self, task=task, flush=flush)

    # ------------------------------------------------------------------
    # Flushing
    # ------------------------------------------------------------------

    def maybe_debounced_flush(self) -> None:
        """Flush if dirty and ≥1s since last flush. Called end-of-iter."""
        if not self._dirty:
            return
        if (time.monotonic() - self._last_flush_mono) < self._min_debounce_interval:
            return
        self._flush_now(write_summary=True)

    def force_flush(self) -> None:
        """Force an immediate flush — used at shutdown."""
        self._flush_now(write_summary=True)

    def _flush_now(self, write_summary: bool) -> None:
        # Local import avoids a top-level circular state ↔ batch_store.
        from pbs_auto.state import save_state

        try:
            save_state(self.state, write_summary=write_summary)
        except OSError as e:
            log.error("BatchStore._flush_now: save_state failed: %s", e)
            # Keep _dirty so the next flush attempt retries.
            return
        self._dirty = False
        self._last_flush_mono = time.monotonic()

    # ------------------------------------------------------------------
    # Index maintenance
    # ------------------------------------------------------------------

    def _update_indexes(self, task: Task) -> None:
        """Incrementally update _pending_set and _active_set."""
        key = task.directory
        pending = self.state._pending_set
        active = self.state._active_set

        if task.status == TaskStatus.PENDING:
            pending.add(key)
            active.discard(key)
        elif task.status in ACTIVE_STATUSES:
            pending.discard(key)
            active.add(key)
        else:
            # Terminal state
            pending.discard(key)
            active.discard(key)


class _MutationContext:
    """Context manager for a single task mutation.

    `__exit__` is guaranteed to never raise — all filesystem operations
    (sentinel update, index update, flush, sentinel removal) are wrapped
    individually so that a failure in one step does not block the next.
    """

    def __init__(
        self,
        store: BatchStore,
        task: Task | None,
        flush: FlushPolicy,
    ):
        self.store = store
        self.task = task
        self.flush: FlushPolicy = flush
        self.sentinel_path = None

    def __enter__(self) -> _MutationContext:
        if self.task is not None:
            try:
                self.sentinel_path = create_sentinel(
                    self.store.state.batch_id, self.task
                )
            except OSError as e:
                log.error(
                    "_MutationContext: create_sentinel failed for %s: %s",
                    self.task.directory,
                    e,
                )
                # We continue anyway — sentinel loss only weakens
                # crash recovery, not the happy path.
                self.sentinel_path = None
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        # Exception path: leave sentinel so recovery can inspect it.
        if exc_type is not None:
            log.error(
                "_MutationContext body raised: %s",
                exc,
                exc_info=(exc_type, exc, tb),
            )
            return False  # Re-raise

        # Step 1: sync sentinel content with the real job_id if set
        if self.sentinel_path is not None and self.task is not None:
            if self.task.job_id:
                try:
                    update_sentinel_job_id(self.sentinel_path, self.task.job_id)
                except OSError as e:
                    log.warning(
                        "_MutationContext: update_sentinel_job_id failed: %s",
                        e,
                    )

        # Step 2: update indexes
        if self.task is not None:
            try:
                self.store._update_indexes(self.task)
            except Exception as e:  # noqa: BLE001
                log.warning("_MutationContext: index update failed: %s", e)

        # Step 3: flush (promote FAILED to eager — Bug B)
        eff_flush: FlushPolicy = self.flush
        if self.task is not None and self.task.status == TaskStatus.FAILED:
            eff_flush = "eager"

        try:
            if eff_flush == "eager":
                # Eager saves skip summary writes — next debounced flush
                # will catch up summary.json for list-batches readers.
                self.store._flush_now(write_summary=False)
            else:
                self.store._dirty = True
        except Exception as e:  # noqa: BLE001
            log.error("_MutationContext: flush failed: %s", e)

        # Step 4: unconditionally remove the sentinel on clean exit
        # (Bug A — the original design conditionally removed which
        # could leave stale sentinels after retryable-PENDING cycles).
        if self.sentinel_path is not None:
            try:
                remove_sentinel(self.sentinel_path)
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "_MutationContext: remove_sentinel failed: %s", e
                )

        return False  # Don't swallow exceptions (there weren't any)


__all__ = ["BatchStore", "_MutationContext"]
