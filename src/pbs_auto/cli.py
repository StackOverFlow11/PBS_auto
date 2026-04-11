"""Click CLI command definitions.

Commands:
    - submit   Scan directories and submit jobs (default: daemon mode)
    - status   Show a one-shot snapshot of a batch (reads summary.json)
    - stop     Send SIGTERM to a running daemon
    - logs     Tail the daemon log file
    - list-batches   List all saved batches
    - init     Write an example config.toml
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import click
from rich.console import Console

from pbs_auto import __version__


# ---------------------------------------------------------------------------
# CLI group
# ---------------------------------------------------------------------------


@click.group()
@click.version_option(version=__version__, prog_name="pbs-auto")
def cli():
    """PBS Auto-Submit Tool — bulk PBS job submission and monitoring."""
    pass


# ---------------------------------------------------------------------------
# submit
# ---------------------------------------------------------------------------


@cli.command()
@click.argument(
    "root_dirs",
    nargs=-1,
    type=click.Path(exists=True, file_okay=False),
)
@click.option(
    "--name",
    default=None,
    help="Batch name (required when multiple root dirs are given).",
)
@click.option(
    "--from-list",
    "from_list",
    type=click.Path(exists=True, dir_okay=False),
    default=None,
    help="Read root dirs from a file (one per line, # comments).",
)
@click.option("--server", default=None, help="Server profile name from config")
@click.option("--config", "config_path", default=None, help="Path to config file")
@click.option("--dry-run", is_flag=True, help="Show plan without submitting")
@click.option("--foreground", is_flag=True, help="Run in foreground (no fork)")
@click.option("--fresh", is_flag=True, help="Discard saved state and start fresh")
@click.option("--script-name", default=None, help="PBS script filename")
@click.option("--queue", "cli_queue", default=None, help="Force queue for all tasks")
@click.option("--no-queue-validation", is_flag=True, help="Skip queue compliance checks")
@click.option(
    "--skip-if-exists",
    "skip_if_exists_cli",
    multiple=True,
    metavar="GLOB",
    help=(
        "Glob pattern (relative to each task dir); if any matches an "
        "existing file, the task is marked SKIPPED. Can be repeated. "
        "Unioned with config.toml [defaults].skip_if_exists."
    ),
)
def submit(
    root_dirs,
    name,
    from_list,
    server,
    config_path,
    dry_run,
    foreground,
    fresh,
    script_name,
    cli_queue,
    no_queue_validation,
    skip_if_exists_cli,
):
    """Scan ROOT_DIRS and submit PBS tasks (daemonizes by default)."""
    from pbs_auto.batch_store import (
        BatchStore,
        cleanup_stale_artifacts,
        ensure_batch_dir,
        recover_sentinels,
    )
    from pbs_auto.config import load_config
    from pbs_auto.models import BatchState, TaskStatus
    from pbs_auto.pbs import PBSClient
    from pbs_auto.scanner import scan_directory
    from pbs_auto.scheduler import Scheduler
    from pbs_auto.state import (
        generate_batch_id,
        load_state,
        reconcile_tasks,
        save_state,
        validate_identifier,
    )

    console = Console()

    # --- Parse root dirs from args + --from-list ---
    roots: list[str] = [str(Path(r).resolve()) for r in root_dirs]
    if from_list:
        roots.extend(_read_roots_from_list(Path(from_list)))
    if not roots:
        console.print("[red]At least one root directory is required.[/red]")
        raise SystemExit(2)

    # Deduplicate while preserving order
    seen: set[str] = set()
    unique_roots: list[str] = []
    for r in roots:
        if r not in seen:
            seen.add(r)
            unique_roots.append(r)
    roots = unique_roots

    if len(roots) > 1 and not name:
        console.print(
            "[red]Multiple root directories require --name to identify the batch.[/red]"
        )
        raise SystemExit(2)

    if name:
        try:
            validate_identifier(name)
        except ValueError as e:
            console.print(f"[red]{e}[/red]")
            raise SystemExit(2)

    # --- Config ---
    try:
        config = load_config(config_path)
    except Exception as e:
        console.print(f"[red]Config error: {e}[/red]")
        raise SystemExit(1)

    if script_name:
        config.script_name = script_name

    server_name = server or config.server
    try:
        server_config = config.get_server(server_name)
    except ValueError as e:
        console.print(f"[red]{e}[/red]")
        raise SystemExit(1)

    # --- Resolve skip_if_exists (config defaults + CLI, unioned) ---
    skip_patterns: list[str] = list(config.skip_if_exists)
    for p in skip_if_exists_cli:
        if p not in skip_patterns:
            skip_patterns.append(p)

    # --- Scan directories ---
    console.print(f"Scanning {len(roots)} root dir(s)...")
    if skip_patterns:
        console.print(
            f"Skip patterns: [dim]{', '.join(skip_patterns)}[/dim]"
        )
    tasks = []
    for root in roots:
        tasks.extend(
            scan_directory(
                Path(root), config.script_name, skip_if_exists=skip_patterns
            )
        )
    if not tasks:
        console.print("[yellow]No task directories found.[/yellow]")
        return

    pending = [t for t in tasks if t.status == TaskStatus.PENDING]
    skipped = [t for t in tasks if t.status == TaskStatus.SKIPPED]
    console.print(
        f"Found [green]{len(pending)}[/green] tasks, "
        f"[yellow]{len(skipped)}[/yellow] skipped"
    )

    # --- Queue validation / assignment ---
    if not no_queue_validation and server_config.queues:
        from pbs_auto.queue import validate_and_assign_queues

        tasks, invalid = validate_and_assign_queues(
            tasks, server_config.queues, cli_queue=cli_queue
        )

        if invalid:
            _warn_non_compliant(console, invalid)
            if not click.confirm(
                "Continue submitting non-compliant tasks?", default=False
            ):
                for task, _errors in invalid:
                    task.status = TaskStatus.SKIPPED
                    task.error_message = "Skipped: queue compliance check failed"
                pending = [t for t in tasks if t.status == TaskStatus.PENDING]
                skipped = [t for t in tasks if t.status == TaskStatus.SKIPPED]
                console.print(
                    f"[yellow]Non-compliant tasks skipped. "
                    f"{len(pending)} tasks remaining.[/yellow]"
                )
    elif cli_queue and not no_queue_validation:
        for task in tasks:
            if task.status == TaskStatus.PENDING:
                task.queue = cli_queue

    # --- Load / create / reconcile state ---
    batch_id = generate_batch_id(roots, name=name)
    state = None if fresh else load_state(batch_id)
    if state is not None:
        console.print(f"[blue]Resuming batch {batch_id[:8]}...[/blue]")
        state = reconcile_tasks(state, tasks)
    else:
        state = BatchState(
            batch_id=batch_id,
            root_directories=roots,
            server_profile=server_name,
            name=name,
        )
        state.tasks = {t.directory: t for t in tasks}
        state.rebuild_indexes()

    # --- Dry run ---
    if dry_run:
        run_dry_run(state, server_config)
        return

    # Persist initial state (Parent --fresh init bypass — documented).
    ensure_batch_dir(batch_id)
    save_state(state)

    # --- Daemonize (unless --foreground) ---
    if foreground:
        console.print(
            f"Running in [yellow]foreground[/yellow] for batch "
            f"[cyan]{batch_id}[/cyan] (press Ctrl+C to stop)."
        )
        _run_main(state, config, server_config, batch_id, foreground=True)
        return

    # Daemon path
    from pbs_auto import daemon as daemon_mod

    # Check for an existing live daemon before forking.
    if daemon_mod.is_daemon_alive(batch_id):
        console.print(
            f"[red]A daemon is already running for batch {batch_id}.[/red]\n"
            f"Use 'pbs-auto status {name or batch_id}' to inspect or "
            f"'pbs-auto stop {name or batch_id}' to terminate it."
        )
        raise SystemExit(1)

    console.print(
        f"Starting daemon for batch [cyan]{batch_id}[/cyan]"
        + (f" ({name})" if name else "")
    )

    # Fork + run
    try:
        daemon_mod.daemonize(batch_id)
    except OSError as e:
        console.print(f"[red]Failed to daemonize: {e}[/red]")
        raise SystemExit(1)

    # Grandchild from here on — no more console output to parent terminal.
    _run_daemon_main(state, config, server_config, batch_id)


def _run_daemon_main(state, config, server_config, batch_id: str) -> None:
    """Entry point running inside the forked grandchild."""
    import logging

    from pbs_auto import daemon as daemon_mod
    from pbs_auto.batch_store import (
        BatchStore,
        cleanup_stale_artifacts,
        get_batch_dir,
        recover_sentinels,
    )
    from pbs_auto.pbs import PBSClient
    from pbs_auto.scheduler import Scheduler
    from pbs_auto.state import load_state, save_state

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    log = logging.getLogger("pbs_auto.daemon")

    batch_dir = get_batch_dir(batch_id)

    try:
        daemon_mod._verify_lockf_works(batch_dir)
    except RuntimeError as e:
        log.error("%s", e)
        sys.exit(2)

    try:
        lock_fd = daemon_mod.acquire_lock(batch_id)
    except RuntimeError as e:
        log.error("%s", e)
        sys.exit(1)

    daemon_mod.write_pid_file(batch_id)

    # Re-load state from disk (daemon is now authoritative).
    reloaded = load_state(batch_id)
    if reloaded is not None:
        state = reloaded

    # Cleanup + recovery + pbs client
    cleanup_stale_artifacts(batch_id, state)

    pbs = PBSClient(server_config, batch_id=batch_id)
    recovery_ok = True
    try:
        recover_sentinels(state, pbs)
    except Exception as e:  # noqa: BLE001
        log.warning("recover_sentinels during bootstrap failed: %s", e)
        recovery_ok = False
    pbs.invalidate_cache()

    store = BatchStore(state)
    scheduler = Scheduler(state, config, server_config, pbs, store)
    if not recovery_ok:
        scheduler._recovery_pending = True

    try:
        scheduler.run()
    finally:
        try:
            os.close(lock_fd)
        except OSError:
            pass
        try:
            daemon_mod.get_pid_path(batch_id).unlink(missing_ok=True)
        except Exception:  # noqa: BLE001
            pass

    sys.exit(0)


def _run_main(state, config, server_config, batch_id: str, foreground: bool) -> None:
    """Foreground runner: acquires the batch lock in this process."""
    from pbs_auto import daemon as daemon_mod
    from pbs_auto.batch_store import (
        BatchStore,
        cleanup_stale_artifacts,
        get_batch_dir,
        recover_sentinels,
    )
    from pbs_auto.pbs import PBSClient
    from pbs_auto.scheduler import Scheduler

    console = Console()

    batch_dir = get_batch_dir(batch_id)
    try:
        daemon_mod._verify_lockf_works(batch_dir)
    except RuntimeError as e:
        console.print(f"[red]{e}[/red]")
        raise SystemExit(2)

    try:
        lock_fd = daemon_mod.acquire_lock(batch_id)
    except RuntimeError as e:
        console.print(f"[red]{e}[/red]")
        raise SystemExit(1)

    daemon_mod.write_pid_file(batch_id)

    cleanup_stale_artifacts(batch_id, state)

    pbs = PBSClient(server_config, batch_id=batch_id)
    try:
        recover_sentinels(state, pbs)
    except Exception:  # noqa: BLE001
        pass
    pbs.invalidate_cache()

    store = BatchStore(state)
    scheduler = Scheduler(state, config, server_config, pbs, store)

    try:
        scheduler.run()
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted. State saved.[/yellow]")
    finally:
        try:
            os.close(lock_fd)
        except OSError:
            pass
        try:
            daemon_mod.get_pid_path(batch_id).unlink(missing_ok=True)
        except Exception:  # noqa: BLE001
            pass

    _print_summary(console, state)


# ---------------------------------------------------------------------------
# status / stop / logs / list-batches / init
# ---------------------------------------------------------------------------


@cli.command()
@click.argument("identifier")
@click.option("--detail", is_flag=True, help="Load full state.json (slower)")
def status(identifier, detail):
    """Show a snapshot of a batch by name or batch_id."""
    from pbs_auto import daemon as daemon_mod
    from pbs_auto.batch_store import get_batch_dir, get_summary_path
    from pbs_auto.state import load_state, resolve_batch_identifier

    console = Console()
    try:
        batch_id = resolve_batch_identifier(identifier)
    except ValueError as e:
        console.print(f"[red]{e}[/red]")
        raise SystemExit(1)

    alive = daemon_mod.is_daemon_alive(batch_id)
    daemon_label = (
        "[green]running[/green]" if alive else "[dim]stopped[/dim]"
    )

    if detail:
        state = load_state(batch_id)
        if state is None:
            console.print(f"[red]No state for {batch_id}[/red]")
            raise SystemExit(1)
        _print_summary(console, state, daemon_label=daemon_label)
        return

    # Fast path: read summary.json
    summary_path = get_summary_path(batch_id)
    if not summary_path.exists():
        state = load_state(batch_id)
        if state is None:
            console.print(f"[red]No state for {batch_id}[/red]")
            raise SystemExit(1)
        _print_summary(console, state, daemon_label=daemon_label)
        return

    import json

    try:
        with open(summary_path) as f:
            summary = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        console.print(f"[red]Failed to read summary.json: {e}[/red]")
        raise SystemExit(1)

    _print_summary_from_dict(console, summary, daemon_label=daemon_label)


@cli.command()
@click.argument("identifier")
@click.option("--timeout", type=float, default=60.0)
def stop(identifier, timeout):
    """Stop a running daemon (SIGTERM + wait)."""
    from pbs_auto import daemon as daemon_mod
    from pbs_auto.state import resolve_batch_identifier

    console = Console()
    try:
        batch_id = resolve_batch_identifier(identifier)
    except ValueError as e:
        console.print(f"[red]{e}[/red]")
        raise SystemExit(1)

    if not daemon_mod.is_daemon_alive(batch_id):
        console.print(
            f"[yellow]No running daemon found for {batch_id}[/yellow]"
        )
        return

    ok = daemon_mod.stop_daemon(batch_id, timeout=timeout)
    if ok:
        console.print(f"[green]Daemon for {batch_id} stopped.[/green]")
    else:
        console.print(
            f"[red]Daemon for {batch_id} did not exit within {timeout}s.[/red]"
        )
        raise SystemExit(1)


@cli.command()
@click.argument("identifier")
@click.option("--tail", "tail_n", type=int, default=None)
@click.option("-f", "follow", is_flag=True, help="Follow the log (tail -f)")
def logs(identifier, tail_n, follow):
    """Print or follow the daemon log."""
    import subprocess

    from pbs_auto.batch_store import get_log_path
    from pbs_auto.config import DEFAULT_STATE_DIR
    from pbs_auto.state import resolve_batch_identifier

    console = Console()
    try:
        batch_id = resolve_batch_identifier(identifier)
    except ValueError as e:
        console.print(f"[red]{e}[/red]")
        raise SystemExit(1)

    log_path = get_log_path(batch_id).resolve()
    state_root = DEFAULT_STATE_DIR.resolve()
    try:
        log_path.relative_to(state_root)
    except ValueError:
        console.print(f"[red]Log path escaped state dir: {log_path}[/red]")
        raise SystemExit(1)

    if not log_path.exists():
        console.print(f"[yellow]No log file at {log_path}[/yellow]")
        return

    if follow:
        cmd = ["tail", "-f", "--", str(log_path)]
        if tail_n is not None:
            cmd = ["tail", f"-n{tail_n}", "-f", "--", str(log_path)]
        try:
            subprocess.call(cmd)
        except KeyboardInterrupt:
            pass
        return

    if tail_n is not None:
        try:
            subprocess.call(["tail", f"-n{tail_n}", "--", str(log_path)])
        except FileNotFoundError:
            click.echo(log_path.read_text())
    else:
        click.echo(log_path.read_text())


@cli.command("list-batches")
def list_batches():
    """List all saved batches (reads summary.json for speed)."""
    from rich.table import Table

    from pbs_auto import daemon as daemon_mod
    from pbs_auto.state import list_batches as _list_batches

    console = Console()
    batches = _list_batches()

    if not batches:
        console.print("[yellow]No saved batches found.[/yellow]")
        return

    table = Table(title="Saved Batches")
    table.add_column("Batch ID", style="cyan", width=10)
    table.add_column("Name", style="magenta", width=16)
    table.add_column("Daemon", width=8)
    table.add_column("Roots")
    table.add_column("Server", width=10)
    table.add_column("Tasks", justify="right", width=6)
    table.add_column("Status Summary")
    table.add_column("Updated", width=20)

    for b in batches:
        alive = daemon_mod.is_daemon_alive(b["batch_id"])
        sc = b.get("status_counts", {})
        parts = [f"{k}:{v}" for k, v in sc.items()]
        roots_display = ", ".join(b.get("root_directories", []))
        table.add_row(
            b["batch_id"][:8],
            b.get("name") or "-",
            "[green]running[/green]" if alive else "[dim]stopped[/dim]",
            roots_display[:40],
            b.get("server_profile", "?"),
            str(b.get("total_tasks", "?")),
            " ".join(parts),
            (b.get("updated_at") or "?")[:19],
        )

    console.print(table)


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


# ---------------------------------------------------------------------------
# run_dry_run (moved from scheduler.py)
# ---------------------------------------------------------------------------


def run_dry_run(state, server_config) -> None:
    """Print a read-only plan table without touching BatchStore or PBS."""
    from rich.table import Table

    from pbs_auto.models import TaskStatus

    console = Console()
    console.print()
    console.print(f"[bold]Dry Run - Server: {server_config.name}[/bold]")
    console.print(
        f"Max Running: {server_config.max_running_cores} cores | "
        f"Max Queued: {server_config.max_queued_cores} cores"
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _read_roots_from_list(path: Path) -> list[str]:
    """Parse a --from-list file: one path per line, '#' comments."""
    if path.stat().st_size > 10 * 1024 * 1024:
        raise click.ClickException(f"--from-list file too large: {path}")

    roots: list[str] = []
    with open(path) as f:
        for lineno, raw in enumerate(f, 1):
            if lineno > 10_000:
                raise click.ClickException(
                    f"--from-list file has more than 10,000 lines"
                )
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if len(line) > 4096:
                raise click.ClickException(
                    f"--from-list line {lineno} exceeds 4 KiB"
                )
            if line in ("/", "/tmp", "/var"):
                raise click.ClickException(
                    f"--from-list line {lineno} refuses path: {line}"
                )
            expanded = os.path.expanduser(line)
            resolved = str(Path(expanded).resolve())
            roots.append(resolved)
    return roots


def _warn_non_compliant(console: Console, invalid) -> None:
    from rich.table import Table

    console.print()
    warn_table = Table(title="[yellow]Queue Compliance Warnings[/yellow]")
    warn_table.add_column("Task", style="cyan")
    warn_table.add_column("Cores", justify="right")
    warn_table.add_column("Queue", style="blue")
    warn_table.add_column("Issue", style="red")
    for task, errors in invalid:
        warn_table.add_row(
            task.name,
            str(task.cores),
            task.queue or "-",
            "; ".join(errors),
        )
    console.print(warn_table)
    console.print()


def _print_summary(console: Console, state, daemon_label: str | None = None) -> None:
    """Print a snapshot summary for the full BatchState."""
    from rich.table import Table

    from pbs_auto.models import TaskStatus

    console.print()
    console.print(f"[bold]Batch:[/bold] {state.batch_id[:8]}")
    if getattr(state, "name", None):
        console.print(f"[bold]Name:[/bold]  {state.name}")
    roots_display = ", ".join(state.root_directories)
    console.print(f"[bold]Roots:[/bold] {roots_display}")
    console.print(f"[bold]Server:[/bold] {state.server_profile}")
    if daemon_label is not None:
        console.print(f"[bold]Daemon:[/bold] {daemon_label}")

    counts: dict[str, int] = {}
    for task in state.tasks.values():
        key = task.status.value
        counts[key] = counts.get(key, 0) + 1

    console.print()
    for status_name, count in counts.items():
        console.print(f"  {status_name}: {count}")

    problem_tasks = [
        t
        for t in state.tasks.values()
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


def _print_summary_from_dict(
    console: Console, summary: dict, daemon_label: str | None = None
) -> None:
    """Print a quick summary from summary.json contents."""
    console.print()
    console.print(f"[bold]Batch:[/bold] {summary.get('batch_id', '?')[:8]}")
    if summary.get("name"):
        console.print(f"[bold]Name:[/bold]  {summary['name']}")
    roots = summary.get("root_directories", [])
    console.print(f"[bold]Roots:[/bold] {', '.join(roots)}")
    console.print(f"[bold]Server:[/bold] {summary.get('server_profile', '?')}")
    if daemon_label is not None:
        console.print(f"[bold]Daemon:[/bold] {daemon_label}")
    console.print(f"[bold]Updated:[/bold] {summary.get('updated_at', '?')}")
    console.print(
        f"[bold]Total tasks:[/bold] {summary.get('total_tasks', '?')}"
    )

    console.print()
    counts = summary.get("status_counts", {})
    for status_name, count in counts.items():
        console.print(f"  {status_name}: {count}")
