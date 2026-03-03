"""Submission engine - core scheduling loop."""

from __future__ import annotations

import signal
import time
from datetime import datetime
from pathlib import Path

from pbs_auto.config import AppConfig, ServerConfig
from pbs_auto.display import Display
from pbs_auto.models import BatchState, Task, TaskStatus
from pbs_auto.pbs import PBSClient
from pbs_auto.state import save_state


class Scheduler:
    """Manages the submission loop: poll → submit → persist → display."""

    def __init__(
        self,
        state: BatchState,
        config: AppConfig,
        server: ServerConfig,
        pbs: PBSClient,
        display: Display,
        dry_run: bool = False,
    ):
        self.state = state
        self.config = config
        self.server = server
        self.pbs = pbs
        self.display = display
        self.dry_run = dry_run
        self._shutdown = False
        self._force_quit = False
        self._original_sigint = None

    def run(self) -> None:
        """Run the main scheduling loop until all tasks are done or interrupted."""
        self._install_signal_handler()
        try:
            self._main_loop()
        finally:
            self._restore_signal_handler()

    def _main_loop(self) -> None:
        """Core loop: poll → submit → persist → display → wait."""
        self.display.start()
        try:
            while not self._shutdown:
                # 1. Poll PBS for status updates
                self._poll_status()

                # 2. Submit pending tasks if resources allow
                self._submit_pending()

                # 3. Persist state
                if not self.dry_run:
                    save_state(self.state)

                # 4. Update display
                self.display.refresh(self.state, self.server)

                # 5. Check if all tasks are done
                if self._all_done():
                    break

                # 6. Interruptible sleep
                self._sleep(self.config.poll_interval)
        finally:
            self.display.stop()
            if not self.dry_run:
                save_state(self.state)

    def _poll_status(self) -> None:
        """Query PBS and update task states based on actual job status."""
        if self.dry_run:
            return

        try:
            self.pbs.invalidate_cache()
            pbs_jobs = self.pbs.query_user_jobs(force=True)
        except RuntimeError:
            # If PBS query fails, skip this cycle
            return

        for task in self.state.tasks.values():
            if task.job_id is None:
                continue
            if task.status in (
                TaskStatus.COMPLETED,
                TaskStatus.WARNING,
                TaskStatus.FAILED,
                TaskStatus.SKIPPED,
                TaskStatus.PENDING,
            ):
                continue

            job_id_short = task.job_id.split(".")[0]
            pbs_job = pbs_jobs.get(job_id_short)

            if pbs_job is None:
                # Job disappeared from PBS
                self._handle_job_disappeared(task)
            elif pbs_job.state == "R":
                if task.status != TaskStatus.RUNNING:
                    task.status = TaskStatus.RUNNING
                    task.start_time = datetime.now().isoformat()
            elif pbs_job.state == "Q":
                task.status = TaskStatus.QUEUED

    def _handle_job_disappeared(self, task: Task) -> None:
        """Handle a job that is no longer in PBS."""
        task.end_time = datetime.now().isoformat()

        if task.status == TaskStatus.SUBMITTED:
            # Never saw it running, too quick — warning
            task.status = TaskStatus.WARNING
            task.error_message = "Job disappeared before entering running state"
            return

        if task.start_time:
            start = datetime.fromisoformat(task.start_time)
            end = datetime.fromisoformat(task.end_time)
            elapsed = (end - start).total_seconds()
            if elapsed < self.config.early_exit_threshold:
                task.status = TaskStatus.WARNING
                task.error_message = (
                    f"Job ran for only {elapsed:.0f}s "
                    f"(threshold: {self.config.early_exit_threshold}s)"
                )
                return

        task.status = TaskStatus.COMPLETED

    def _submit_pending(self) -> None:
        """Try to submit pending tasks within resource limits."""
        if self.dry_run:
            return

        for task in self.state.tasks.values():
            if self._shutdown:
                break
            if task.status != TaskStatus.PENDING:
                continue

            # Check resource limits from PBS (real-time)
            running_cores, queued_cores = self._get_resource_usage()

            if running_cores + task.cores > self.server.max_running_cores:
                continue
            if queued_cores + task.cores > self.server.max_queued_cores:
                continue

            self._submit_task(task)

            # Delay between submissions
            if self.config.submit_delay > 0:
                self._sleep(self.config.submit_delay)

    def _submit_task(self, task: Task) -> None:
        """Submit a single task via qsub."""
        try:
            job_id = self.pbs.submit(task)
            task.job_id = job_id
            task.status = TaskStatus.SUBMITTED
            task.submit_time = datetime.now().isoformat()
            # Invalidate cache since we changed the queue
            self.pbs.invalidate_cache()
        except (RuntimeError, FileNotFoundError, OSError) as e:
            task.status = TaskStatus.FAILED
            task.error_message = str(e)

    def _get_resource_usage(self) -> tuple[int, int]:
        """Get current resource usage from PBS.

        Returns (running_cores, queued_cores) including ALL user jobs,
        not just ones managed by this tool.
        """
        try:
            pbs_jobs = self.pbs.query_user_jobs()
        except RuntimeError:
            # If query fails, report max to prevent new submissions
            return self.server.max_running_cores, self.server.max_queued_cores

        running_cores = 0
        queued_cores = 0
        for job in pbs_jobs.values():
            if job.state == "R":
                running_cores += job.cores
            elif job.state == "Q":
                queued_cores += job.cores

        return running_cores, queued_cores

    def _all_done(self) -> bool:
        """Check if all tasks are in a terminal state."""
        terminal = {
            TaskStatus.COMPLETED,
            TaskStatus.WARNING,
            TaskStatus.FAILED,
            TaskStatus.SKIPPED,
        }
        return all(t.status in terminal for t in self.state.tasks.values())

    def _sleep(self, seconds: float) -> None:
        """Interruptible sleep."""
        end = time.monotonic() + seconds
        while time.monotonic() < end and not self._shutdown:
            time.sleep(min(0.5, end - time.monotonic()))

    def _install_signal_handler(self) -> None:
        """Install SIGINT handler for graceful shutdown."""
        self._original_sigint = signal.getsignal(signal.SIGINT)

        def handler(signum, frame):
            if self._shutdown:
                # Second Ctrl+C → force quit
                self._force_quit = True
                if self._original_sigint and callable(self._original_sigint):
                    self._original_sigint(signum, frame)
                raise KeyboardInterrupt
            self._shutdown = True

        signal.signal(signal.SIGINT, handler)

    def _restore_signal_handler(self) -> None:
        """Restore original SIGINT handler."""
        if self._original_sigint is not None:
            signal.signal(signal.SIGINT, self._original_sigint)


def run_dry_run(state: BatchState, server: ServerConfig) -> None:
    """Display what would be submitted without actually submitting."""
    from rich.console import Console
    from rich.table import Table

    console = Console()

    console.print()
    console.print(f"[bold]Dry Run - Server: {server.name}[/bold]")
    console.print(
        f"Max Running: {server.max_running_cores} cores | "
        f"Max Queued: {server.max_queued_cores} cores"
    )
    console.print()

    table = Table(title="Tasks to Submit")
    table.add_column("#", style="dim", width=4)
    table.add_column("Name", style="cyan")
    table.add_column("Directory")
    table.add_column("Cores", justify="right", style="green")
    table.add_column("Queue", style="blue")
    table.add_column("Status", style="yellow")
    table.add_column("Note", style="dim")

    total_cores = 0
    pending_count = 0
    skipped_count = 0

    for i, task in enumerate(state.tasks.values(), 1):
        note = task.error_message or ""
        status_style = {
            TaskStatus.PENDING: "green",
            TaskStatus.SKIPPED: "yellow",
            TaskStatus.COMPLETED: "dim",
            TaskStatus.WARNING: "red",
            TaskStatus.FAILED: "red",
        }.get(task.status, "white")

        table.add_row(
            str(i),
            task.name,
            task.directory,
            str(task.cores),
            task.queue or "-",
            f"[{status_style}]{task.status.value}[/{status_style}]",
            note,
        )
        if task.status == TaskStatus.PENDING:
            total_cores += task.cores
            pending_count += 1
        elif task.status == TaskStatus.SKIPPED:
            skipped_count += 1

    console.print(table)
    console.print()
    console.print(
        f"[bold]Summary:[/bold] {pending_count} tasks to submit "
        f"({total_cores} total cores), {skipped_count} skipped"
    )
