"""Click CLI command definitions."""

from __future__ import annotations

from pathlib import Path

import click
from rich.console import Console

from pbs_auto import __version__


@click.group()
@click.version_option(version=__version__, prog_name="pbs-auto")
def cli():
    """PBS Auto-Submit Tool - Bulk PBS job submission and monitoring."""
    pass


@cli.command()
@click.argument("root_dir", type=click.Path(exists=True, file_okay=False))
@click.option("--server", default=None, help="Server profile name from config")
@click.option("--config", "config_path", default=None, help="Path to config file")
@click.option("--dry-run", is_flag=True, help="Show plan without submitting")
@click.option("--fresh", is_flag=True, help="Discard saved state and start fresh")
@click.option("--script-name", default=None, help="PBS script filename (default: script.sh)")
def submit(root_dir, server, config_path, dry_run, fresh, script_name):
    """Scan directory and submit PBS tasks."""
    from pbs_auto.config import load_config
    from pbs_auto.display import Display
    from pbs_auto.models import BatchState, TaskStatus
    from pbs_auto.pbs import PBSClient
    from pbs_auto.scanner import scan_directory
    from pbs_auto.scheduler import Scheduler, run_dry_run
    from pbs_auto.state import (
        generate_batch_id,
        load_state,
        reconcile_tasks,
        save_state,
    )

    console = Console()

    # Load config
    try:
        config = load_config(config_path)
    except (FileNotFoundError, Exception) as e:
        console.print(f"[red]Config error: {e}[/red]")
        raise SystemExit(1)

    # Override script name if provided
    if script_name:
        config.script_name = script_name

    # Get server config
    server_name = server or config.server
    try:
        server_config = config.get_server(server_name)
    except ValueError as e:
        console.print(f"[red]{e}[/red]")
        raise SystemExit(1)

    root = Path(root_dir).resolve()

    # Scan directory
    console.print(f"Scanning [cyan]{root}[/cyan] for tasks...")
    tasks = scan_directory(root, config.script_name)
    if not tasks:
        console.print("[yellow]No task directories found.[/yellow]")
        return

    pending = [t for t in tasks if t.status == TaskStatus.PENDING]
    skipped = [t for t in tasks if t.status == TaskStatus.SKIPPED]
    console.print(
        f"Found [green]{len(pending)}[/green] tasks, "
        f"[yellow]{len(skipped)}[/yellow] skipped"
    )

    # Load or create state
    batch_id = generate_batch_id(str(root))

    if fresh:
        state = None
    else:
        state = load_state(batch_id)

    if state is not None:
        console.print(f"[blue]Resuming batch {batch_id[:8]}...[/blue]")
        state = reconcile_tasks(state, tasks)
    else:
        state = BatchState(
            batch_id=batch_id,
            root_directory=str(root),
            server_profile=server_name,
        )
        state.tasks = {t.name: t for t in tasks}

    # Dry run
    if dry_run:
        run_dry_run(state, server_config)
        return

    # Real submission
    pbs = PBSClient(server_config)
    display = Display()

    console.print(
        f"Starting submission on [bold]{server_config.name}[/bold] "
        f"(max R={server_config.max_running_cores}, "
        f"max Q={server_config.max_queued_cores} cores)"
    )
    console.print("Press Ctrl+C to gracefully stop.\n")

    scheduler = Scheduler(
        state=state,
        config=config,
        server=server_config,
        pbs=pbs,
        display=display,
        dry_run=False,
    )

    try:
        scheduler.run()
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted. State saved.[/yellow]")

    # Final summary
    _print_summary(console, state)


@cli.command()
@click.argument("root_dir", type=click.Path(exists=True, file_okay=False))
@click.option("--config", "config_path", default=None, help="Path to config file")
def status(root_dir, config_path):
    """Show status of a batch submission."""
    from pbs_auto.models import TaskStatus
    from pbs_auto.state import generate_batch_id, load_state

    console = Console()
    root = Path(root_dir).resolve()
    batch_id = generate_batch_id(str(root))

    state = load_state(batch_id)
    if state is None:
        console.print("[yellow]No saved state found for this directory.[/yellow]")
        return

    _print_summary(console, state)


@cli.command()
def init():
    """Create default configuration file."""
    from pbs_auto.config import init_config

    console = Console()
    try:
        path = init_config()
        console.print(f"[green]Config file created: {path}[/green]")
        console.print("Edit it to match your server configuration.")
    except FileExistsError as e:
        console.print(f"[yellow]{e}[/yellow]")


@cli.command("list-batches")
def list_batches():
    """List all saved batch states."""
    from rich.table import Table

    from pbs_auto.state import list_batches as _list_batches

    console = Console()
    batches = _list_batches()

    if not batches:
        console.print("[yellow]No saved batches found.[/yellow]")
        return

    table = Table(title="Saved Batches")
    table.add_column("Batch ID", style="cyan", width=10)
    table.add_column("Root Directory")
    table.add_column("Server", width=10)
    table.add_column("Tasks", justify="right", width=6)
    table.add_column("Status Summary")
    table.add_column("Updated", width=20)

    for b in batches:
        sc = b["status_counts"]
        parts = [f"{k}:{v}" for k, v in sc.items()]
        table.add_row(
            b["batch_id"][:8],
            b["root_directory"],
            b["server_profile"],
            str(b["total_tasks"]),
            " ".join(parts),
            b["updated_at"][:19],
        )

    console.print(table)


def _print_summary(console: Console, state) -> None:
    """Print a summary of the batch state."""
    from rich.table import Table

    from pbs_auto.models import TaskStatus

    console.print()
    console.print(f"[bold]Batch:[/bold] {state.batch_id[:8]}")
    console.print(f"[bold]Root:[/bold]  {state.root_directory}")
    console.print(f"[bold]Server:[/bold] {state.server_profile}")

    counts: dict[str, int] = {}
    for task in state.tasks.values():
        key = task.status.value
        counts[key] = counts.get(key, 0) + 1

    console.print()
    for status_name, count in counts.items():
        console.print(f"  {status_name}: {count}")

    # Show warning/failed tasks
    problem_tasks = [
        t for t in state.tasks.values()
        if t.status in (TaskStatus.WARNING, TaskStatus.FAILED)
    ]
    if problem_tasks:
        console.print()
        table = Table(title="Warning/Failed Tasks")
        table.add_column("Name", style="cyan")
        table.add_column("Status")
        table.add_column("Error")
        for t in problem_tasks:
            style = "red" if t.status == TaskStatus.FAILED else "yellow"
            table.add_row(
                t.name,
                f"[{style}]{t.status.value}[/{style}]",
                t.error_message or "",
            )
        console.print(table)
