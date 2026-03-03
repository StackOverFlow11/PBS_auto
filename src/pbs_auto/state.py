"""JSON state persistence with atomic writes."""

from __future__ import annotations

import hashlib
import json
import tempfile
from datetime import datetime
from pathlib import Path

from pbs_auto.config import DEFAULT_STATE_DIR
from pbs_auto.models import BatchState, Task, TaskStatus


def generate_batch_id(root_directory: str) -> str:
    """Generate a deterministic batch ID from root directory path."""
    normalized = str(Path(root_directory).resolve())
    return hashlib.sha256(normalized.encode()).hexdigest()[:16]


def get_state_path(batch_id: str) -> Path:
    """Get the file path for a batch state file."""
    return DEFAULT_STATE_DIR / f"{batch_id}.json"


def save_state(state: BatchState) -> None:
    """Save batch state to JSON file with atomic write."""
    state.updated_at = datetime.now().isoformat()
    state_path = get_state_path(state.batch_id)
    state_path.parent.mkdir(parents=True, exist_ok=True)

    data = json.dumps(state.to_dict(), indent=2, ensure_ascii=False)

    # Atomic write: write to temp file then rename
    fd, tmp_path = tempfile.mkstemp(
        dir=state_path.parent, suffix=".tmp"
    )
    try:
        with open(fd, "w") as f:
            f.write(data)
        Path(tmp_path).replace(state_path)
    except Exception:
        Path(tmp_path).unlink(missing_ok=True)
        raise


def load_state(batch_id: str) -> BatchState | None:
    """Load batch state from JSON file. Returns None if not found."""
    state_path = get_state_path(batch_id)
    if not state_path.exists():
        return None

    with open(state_path) as f:
        data = json.load(f)

    return BatchState.from_dict(data)


def reconcile_tasks(
    saved: BatchState, scanned: list[Task]
) -> BatchState:
    """Merge scanned tasks with saved state for resume support.

    - COMPLETED/WARNING/FAILED/SKIPPED tasks: keep saved state
    - RUNNING/QUEUED tasks: keep saved state (scheduler will re-check PBS)
    - SUBMITTED tasks: reset to PENDING (need to re-verify)
    - PENDING tasks: keep as PENDING
    - New tasks not in saved state: add as PENDING
    """
    for task in scanned:
        if task.name in saved.tasks:
            existing = saved.tasks[task.name]
            if existing.status == TaskStatus.SUBMITTED:
                # Reset SUBMITTED to PENDING since we can't verify
                # the submission happened without re-checking
                existing.status = TaskStatus.PENDING
                existing.job_id = None
                existing.submit_time = None
            # For all other states, keep the saved state
            # Update fields in case script changed
            existing.cores = task.cores
            existing.directory = task.directory
            existing.nodes = task.nodes
            existing.queue = task.queue
        else:
            saved.tasks[task.name] = task

    return saved


def list_batches() -> list[dict]:
    """List all saved batch state files with summary info."""
    if not DEFAULT_STATE_DIR.exists():
        return []

    batches = []
    for state_file in sorted(DEFAULT_STATE_DIR.glob("*.json")):
        try:
            with open(state_file) as f:
                data = json.load(f)
            tasks = data.get("tasks", {})
            status_counts: dict[str, int] = {}
            for t in tasks.values():
                s = t.get("status", "unknown")
                status_counts[s] = status_counts.get(s, 0) + 1
            batches.append({
                "batch_id": data.get("batch_id", state_file.stem),
                "root_directory": data.get("root_directory", "?"),
                "server_profile": data.get("server_profile", "?"),
                "created_at": data.get("created_at", "?"),
                "updated_at": data.get("updated_at", "?"),
                "total_tasks": len(tasks),
                "status_counts": status_counts,
            })
        except (json.JSONDecodeError, OSError):
            continue

    return batches
