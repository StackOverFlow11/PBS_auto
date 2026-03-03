"""Directory scanning and PBS script resource parsing."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from pbs_auto.models import Task, TaskStatus

# Match #PBS -l nodes=X:ppn=Y (with optional spaces)
PBS_RESOURCE_RE = re.compile(
    r"^\s*#PBS\s+-l\s+nodes\s*=\s*(\d+)\s*:\s*ppn\s*=\s*(\d+)",
    re.MULTILINE,
)

# Match #PBS -q <queue_name>
PBS_QUEUE_RE = re.compile(
    r"^\s*#PBS\s+-q\s+(\S+)",
    re.MULTILINE,
)

# Match #PBS -l walltime=HH:MM:SS
PBS_WALLTIME_RE = re.compile(
    r"^\s*#PBS\s+-l\s+walltime\s*=\s*(\d+):(\d+):(\d+)",
    re.MULTILINE,
)


@dataclass
class ScriptResources:
    """Parsed resource information from a PBS script."""

    nodes: int = 0
    ppn: int = 0
    cores: int = 0
    queue: str | None = None
    walltime_seconds: int | None = None


def natural_sort_key(name: str) -> list[int | str]:
    """Sort key for natural ordering: 1, 2, 10 instead of 1, 10, 2."""
    parts: list[int | str] = []
    for text in re.split(r"(\d+)", name):
        if text.isdigit():
            parts.append(int(text))
        else:
            parts.append(text.lower())
    return parts


def parse_script_resources(script_path: Path) -> ScriptResources | None:
    """Parse all PBS resource directives from a script.

    Returns ScriptResources with parsed values, or None if the script
    cannot be read or has no nodes/ppn resource line.
    """
    try:
        content = script_path.read_text()
    except OSError:
        return None

    res_match = PBS_RESOURCE_RE.search(content)
    if not res_match:
        return None

    nodes = int(res_match.group(1))
    ppn = int(res_match.group(2))

    queue_match = PBS_QUEUE_RE.search(content)
    queue = queue_match.group(1) if queue_match else None

    wt_match = PBS_WALLTIME_RE.search(content)
    walltime_seconds = None
    if wt_match:
        h, m, s = int(wt_match.group(1)), int(wt_match.group(2)), int(wt_match.group(3))
        walltime_seconds = h * 3600 + m * 60 + s

    return ScriptResources(
        nodes=nodes,
        ppn=ppn,
        cores=nodes * ppn,
        queue=queue,
        walltime_seconds=walltime_seconds,
    )


def parse_cores_from_script(script_path: Path) -> int | None:
    """Parse core count from PBS script's #PBS -l nodes=X:ppn=Y directive.

    Returns nodes * ppn, or None if parsing fails.
    Thin wrapper around parse_script_resources() for backward compatibility.
    """
    resources = parse_script_resources(script_path)
    if resources is None:
        return None
    return resources.cores


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

        resources = parse_script_resources(script_path)
        if resources is None:
            task.status = TaskStatus.SKIPPED
            task.error_message = (
                f"Cannot parse resource request from '{script_name}'"
            )
            tasks.append(task)
            continue

        task.cores = resources.cores
        task.nodes = resources.nodes
        task.queue = resources.queue
        tasks.append(task)

    return tasks
