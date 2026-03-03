"""Rich Live CLI interface for task monitoring."""

from __future__ import annotations

from datetime import datetime

from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress_bar import ProgressBar
from rich.table import Table
from rich.text import Text

from pbs_auto.config import ServerConfig
from pbs_auto.models import BatchState, TaskStatus


STATUS_STYLES = {
    TaskStatus.PENDING: "dim",
    TaskStatus.SUBMITTED: "blue",
    TaskStatus.QUEUED: "yellow",
    TaskStatus.RUNNING: "green bold",
    TaskStatus.COMPLETED: "green",
    TaskStatus.WARNING: "red bold",
    TaskStatus.FAILED: "red",
    TaskStatus.SKIPPED: "dim yellow",
}


class Display:
    """Rich Live display for monitoring batch submission progress."""

    def __init__(self, start_time: datetime | None = None):
        self.console = Console()
        self.start_time = start_time or datetime.now()
        self._live: Live | None = None

    def start(self) -> None:
        self._live = Live(
            self._build_empty(),
            console=self.console,
            refresh_per_second=2,
            transient=False,
        )
        self._live.start()

    def stop(self) -> None:
        if self._live:
            self._live.stop()
            self._live = None

    def refresh(self, state: BatchState, server: ServerConfig) -> None:
        if self._live is None:
            return
        renderable = self._build_layout(state, server)
        self._live.update(renderable)

    def _build_empty(self) -> Text:
        return Text("Initializing...", style="dim")

    def _build_layout(
        self, state: BatchState, server: ServerConfig
    ) -> Panel:
        """Build the complete display layout."""
        counts = self._count_statuses(state)
        running_cores, queued_cores = self._count_cores(state)

        parts = []

        # Resource usage bars
        parts.append(self._build_resource_section(
            running_cores, queued_cores, server
        ))

        # Status summary
        parts.append(self._build_status_summary(counts, state))

        # Active tasks table
        active_table = self._build_active_table(state)
        if active_table is not None:
            parts.append(active_table)

        # Timing info
        parts.append(self._build_timing())

        return Panel(
            Group(*parts),
            title=f"[bold]PBS Auto-Submit — {server.name}[/bold]",
            border_style="blue",
        )

    def _build_resource_section(
        self,
        running_cores: int,
        queued_cores: int,
        server: ServerConfig,
    ) -> Table:
        table = Table.grid(padding=(0, 1))
        table.add_column(width=20)
        table.add_column(width=40)
        table.add_column(width=15)

        # Running cores bar
        r_pct = min(running_cores / server.max_running_cores, 1.0) if server.max_running_cores > 0 else 0
        r_bar = ProgressBar(total=server.max_running_cores, completed=running_cores, width=30)
        r_style = "green" if r_pct < 0.8 else "yellow" if r_pct < 1.0 else "red"
        table.add_row(
            Text(f"Running Cores:", style=r_style),
            r_bar,
            Text(f"{running_cores}/{server.max_running_cores}", style=r_style),
        )

        # Queued cores bar
        q_pct = min(queued_cores / server.max_queued_cores, 1.0) if server.max_queued_cores > 0 else 0
        q_bar = ProgressBar(total=server.max_queued_cores, completed=queued_cores, width=30)
        q_style = "green" if q_pct < 0.8 else "yellow" if q_pct < 1.0 else "red"
        table.add_row(
            Text(f"Queued Cores:", style=q_style),
            q_bar,
            Text(f"{queued_cores}/{server.max_queued_cores}", style=q_style),
        )

        return table

    def _build_status_summary(
        self, counts: dict[TaskStatus, int], state: BatchState
    ) -> Text:
        total = len(state.tasks)
        parts = [
            f"Total: {total}",
            f"Pending: {counts.get(TaskStatus.PENDING, 0)}",
            f"Submitted: {counts.get(TaskStatus.SUBMITTED, 0)}",
            f"Queued: {counts.get(TaskStatus.QUEUED, 0)}",
            f"Running: {counts.get(TaskStatus.RUNNING, 0)}",
            f"Completed: {counts.get(TaskStatus.COMPLETED, 0)}",
            f"Warning: {counts.get(TaskStatus.WARNING, 0)}",
            f"Failed: {counts.get(TaskStatus.FAILED, 0)}",
            f"Skipped: {counts.get(TaskStatus.SKIPPED, 0)}",
        ]
        return Text(" | ".join(parts))

    def _build_active_table(self, state: BatchState) -> Table | None:
        """Build table showing non-terminal tasks."""
        active_statuses = {
            TaskStatus.SUBMITTED,
            TaskStatus.QUEUED,
            TaskStatus.RUNNING,
        }
        active_tasks = [
            t for t in state.tasks.values() if t.status in active_statuses
        ]

        if not active_tasks:
            # Show recently completed/warning tasks instead
            terminal_tasks = [
                t for t in state.tasks.values()
                if t.status in (TaskStatus.COMPLETED, TaskStatus.WARNING, TaskStatus.FAILED)
                and t.end_time
            ]
            terminal_tasks.sort(key=lambda t: t.end_time or "", reverse=True)
            active_tasks = terminal_tasks[:5]
            if not active_tasks:
                return None

        table = Table(title="Active Tasks", expand=True, show_edge=False)
        table.add_column("Name", style="cyan", width=20)
        table.add_column("Cores", justify="right", width=6)
        table.add_column("Status", width=12)
        table.add_column("Job ID", width=15)
        table.add_column("Elapsed", width=12)

        for task in active_tasks:
            style = STATUS_STYLES.get(task.status, "white")
            elapsed = self._calc_elapsed(task)
            table.add_row(
                task.name,
                str(task.cores),
                Text(task.status.value, style=style),
                task.job_id or "-",
                elapsed,
            )

        return table

    def _build_timing(self) -> Text:
        elapsed = datetime.now() - self.start_time
        h, remainder = divmod(int(elapsed.total_seconds()), 3600)
        m, s = divmod(remainder, 60)
        now_str = datetime.now().strftime("%H:%M:%S")
        return Text(
            f"Elapsed: {h:02d}:{m:02d}:{s:02d} | Last update: {now_str}",
            style="dim",
        )

    def _count_statuses(self, state: BatchState) -> dict[TaskStatus, int]:
        counts: dict[TaskStatus, int] = {}
        for task in state.tasks.values():
            counts[task.status] = counts.get(task.status, 0) + 1
        return counts

    def _count_cores(self, state: BatchState) -> tuple[int, int]:
        """Count cores for running and queued tasks (internal tracking only)."""
        running = 0
        queued = 0
        for task in state.tasks.values():
            if task.status == TaskStatus.RUNNING:
                running += task.cores
            elif task.status in (TaskStatus.QUEUED, TaskStatus.SUBMITTED):
                queued += task.cores
        return running, queued

    def _calc_elapsed(self, task) -> str:
        """Calculate elapsed time string for a task."""
        if task.status == TaskStatus.RUNNING and task.start_time:
            start = datetime.fromisoformat(task.start_time)
            delta = datetime.now() - start
        elif task.start_time and task.end_time:
            start = datetime.fromisoformat(task.start_time)
            end = datetime.fromisoformat(task.end_time)
            delta = end - start
        elif task.submit_time:
            start = datetime.fromisoformat(task.submit_time)
            delta = datetime.now() - start
        else:
            return "-"

        total_seconds = int(delta.total_seconds())
        h, remainder = divmod(total_seconds, 3600)
        m, s = divmod(remainder, 60)
        if h > 0:
            return f"{h}h{m:02d}m{s:02d}s"
        elif m > 0:
            return f"{m}m{s:02d}s"
        else:
            return f"{s}s"
