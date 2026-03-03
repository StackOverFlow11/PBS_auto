"""Queue validation and auto-selection."""

from __future__ import annotations

from pbs_auto.config import QueueConfig
from pbs_auto.models import Task


def validate_task_for_queue(
    task: Task, queue_config: QueueConfig
) -> list[str]:
    """Validate a single task against queue rules.

    Returns a list of error messages (empty means compliant).
    Checks: cores within allowed range/set, nodes within max_nodes.
    """
    errors: list[str] = []

    if queue_config.allowed_cores is not None:
        if task.cores not in queue_config.allowed_cores:
            errors.append(
                f"cores={task.cores} not in allowed values "
                f"{queue_config.allowed_cores} for queue '{queue_config.name}'"
            )
    else:
        if task.cores < queue_config.min_cores:
            errors.append(
                f"cores={task.cores} below minimum {queue_config.min_cores} "
                f"for queue '{queue_config.name}'"
            )
        if task.cores > queue_config.max_cores:
            errors.append(
                f"cores={task.cores} exceeds maximum {queue_config.max_cores} "
                f"for queue '{queue_config.name}'"
            )

    if queue_config.max_nodes != -1 and task.nodes > queue_config.max_nodes:
        errors.append(
            f"nodes={task.nodes} exceeds maximum {queue_config.max_nodes} "
            f"for queue '{queue_config.name}'"
        )

    return errors


def select_queue(
    task: Task,
    queues: dict[str, QueueConfig],
    walltime_seconds: int | None = None,
) -> str | None:
    """Auto-select the best queue for a task.

    Algorithm: filter compliant queues -> filter by walltime -> pick the one
    with shortest max_walltime (prefer tighter fit).
    Returns queue name or None if no queue fits.
    """
    candidates: list[QueueConfig] = []

    for qc in queues.values():
        errors = validate_task_for_queue(task, qc)
        if errors:
            continue

        if walltime_seconds is not None:
            max_seconds = qc.max_walltime_hours * 3600
            if walltime_seconds > max_seconds:
                continue

        candidates.append(qc)

    if not candidates:
        return None

    # Sort by max_walltime ascending (prefer shortest/tightest queue)
    candidates.sort(key=lambda q: q.max_walltime_hours)
    return candidates[0].name


def validate_and_assign_queues(
    tasks: list[Task],
    queues: dict[str, QueueConfig],
    cli_queue: str | None = None,
) -> tuple[list[Task], list[tuple[Task, list[str]]]]:
    """Validate and assign queues to a batch of tasks.

    Priority: cli_queue > script #PBS -q > auto-select.
    Non-compliant tasks are NOT skipped, but flagged for the CLI layer
    to ask the user.

    Returns (all_tasks, list_of_(task, errors) for non-compliant tasks).
    """
    invalid: list[tuple[Task, list[str]]] = []

    if not queues:
        return tasks, invalid

    for task in tasks:
        from pbs_auto.models import TaskStatus

        if task.status != TaskStatus.PENDING:
            continue

        # Determine target queue (priority: CLI > script > auto)
        if cli_queue:
            task.queue = cli_queue
        elif task.queue is None:
            task.queue = select_queue(task, queues)

        # Validate against target queue
        if task.queue and task.queue in queues:
            errors = validate_task_for_queue(task, queues[task.queue])
            if errors:
                invalid.append((task, errors))

    return tasks, invalid
