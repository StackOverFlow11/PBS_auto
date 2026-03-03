"""Directory scanning and PBS script resource parsing."""

from __future__ import annotations

import re
from pathlib import Path

from pbs_auto.models import Task, TaskStatus

# Match #PBS -l nodes=X:ppn=Y (with optional spaces)
PBS_RESOURCE_RE = re.compile(
    r"^\s*#PBS\s+-l\s+nodes\s*=\s*(\d+)\s*:\s*ppn\s*=\s*(\d+)",
    re.MULTILINE,
)


def natural_sort_key(name: str) -> list[int | str]:
    """Sort key for natural ordering: 1, 2, 10 instead of 1, 10, 2."""
    parts: list[int | str] = []
    for text in re.split(r"(\d+)", name):
        if text.isdigit():
            parts.append(int(text))
        else:
            parts.append(text.lower())
    return parts


def parse_cores_from_script(script_path: Path) -> int | None:
    """Parse core count from PBS script's #PBS -l nodes=X:ppn=Y directive.

    Returns nodes * ppn, or None if parsing fails.
    """
    try:
        content = script_path.read_text()
    except OSError:
        return None

    match = PBS_RESOURCE_RE.search(content)
    if not match:
        return None

    nodes = int(match.group(1))
    ppn = int(match.group(2))
    return nodes * ppn


def scan_directory(
    root: Path, script_name: str = "script.sh"
) -> list[Task]:
    """Scan root directory for task subdirectories.

    Each immediate subdirectory containing script_name is treated as a task.
    Returns tasks sorted in natural order by directory name.
    """
    root = root.resolve()
    if not root.is_dir():
        raise FileNotFoundError(f"Root directory not found: {root}")

    tasks: list[Task] = []

    subdirs = [d for d in root.iterdir() if d.is_dir()]
    subdirs.sort(key=lambda d: natural_sort_key(d.name))

    for subdir in subdirs:
        script_path = subdir / script_name
        task = Task(
            name=subdir.name,
            directory=str(subdir),
            script_name=script_name,
        )

        if not script_path.exists():
            task.status = TaskStatus.SKIPPED
            task.error_message = f"Script '{script_name}' not found"
            tasks.append(task)
            continue

        cores = parse_cores_from_script(script_path)
        if cores is None:
            task.status = TaskStatus.SKIPPED
            task.error_message = (
                f"Cannot parse resource request from '{script_name}'"
            )
            tasks.append(task)
            continue

        task.cores = cores
        tasks.append(task)

    return tasks
