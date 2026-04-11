"""Data models for PBS Auto-Submit Tool."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


SCHEMA_VERSION = 2


class TaskStatus(enum.Enum):
    """Task lifecycle states."""

    PENDING = "pending"
    SUBMITTED = "submitted"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    WARNING = "warning"
    FAILED = "failed"
    SKIPPED = "skipped"


TERMINAL_STATUSES = frozenset(
    {
        TaskStatus.COMPLETED,
        TaskStatus.WARNING,
        TaskStatus.FAILED,
        TaskStatus.SKIPPED,
    }
)

ACTIVE_STATUSES = frozenset(
    {TaskStatus.SUBMITTED, TaskStatus.QUEUED, TaskStatus.RUNNING}
)


@dataclass
class Task:
    """A single PBS job task."""

    name: str
    directory: str
    cores: int = 0
    status: TaskStatus = TaskStatus.PENDING
    job_id: str | None = None
    submit_time: str | None = None
    start_time: str | None = None
    end_time: str | None = None
    error_message: str | None = None
    script_name: str = "script.sh"
    queue: str | None = None
    nodes: int = 0

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "directory": self.directory,
            "cores": self.cores,
            "status": self.status.value,
            "job_id": self.job_id,
            "submit_time": self.submit_time,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "error_message": self.error_message,
            "script_name": self.script_name,
            "queue": self.queue,
            "nodes": self.nodes,
        }

    @classmethod
    def from_dict(cls, data: dict) -> Task:
        data = dict(data)
        data["status"] = TaskStatus(data["status"])
        data.setdefault("queue", None)
        data.setdefault("nodes", 0)
        return cls(**data)


@dataclass
class PBSJobInfo:
    """Information about a PBS job from qstat/q output."""

    job_id: str
    name: str
    state: str  # "R", "Q", "E", etc.
    cores: int
    elapsed: str = ""
    queue: str = ""


@dataclass
class BatchState:
    """State of a batch submission session.

    Tasks are keyed by absolute directory path (not name) to support
    multi-workdir batches where different parent directories may
    contain subdirectories with identical names.
    """

    batch_id: str
    root_directories: list[str] = field(default_factory=list)
    server_profile: str = ""
    name: str | None = None
    version: int = SCHEMA_VERSION
    created_at: str = ""
    updated_at: str = ""
    tasks: dict[str, Task] = field(default_factory=dict)

    # Derived indexes — rebuilt on load / after reconcile.
    # Not serialized; field(repr=False) keeps dataclass repr clean.
    _pending_set: set[str] = field(
        default_factory=set, repr=False, compare=False
    )
    _active_set: set[str] = field(
        default_factory=set, repr=False, compare=False
    )

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now().isoformat()
        if not self.updated_at:
            self.updated_at = self.created_at

    def to_dict(self) -> dict:
        return {
            "batch_id": self.batch_id,
            "root_directories": list(self.root_directories),
            "server_profile": self.server_profile,
            "name": self.name,
            "version": self.version,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "tasks": {k: v.to_dict() for k, v in self.tasks.items()},
        }

    @classmethod
    def from_dict(cls, data: dict) -> BatchState:
        """Construct from a post-migration dict (version=2 schema).

        Legacy (version=1) dicts must be migrated via
        `state._migrate_on_load` before reaching this method.
        """
        data = dict(data)
        if data.get("version") != SCHEMA_VERSION:
            raise ValueError(
                f"BatchState.from_dict expects version={SCHEMA_VERSION} "
                f"schema; got version={data.get('version')}. "
                f"Call state._migrate_on_load first."
            )
        tasks_data = data.pop("tasks", {})
        state = cls(
            batch_id=data["batch_id"],
            root_directories=list(data.get("root_directories", [])),
            server_profile=data.get("server_profile", ""),
            name=data.get("name"),
            version=data.get("version", SCHEMA_VERSION),
            created_at=data.get("created_at", ""),
            updated_at=data.get("updated_at", ""),
        )
        state.tasks = {k: Task.from_dict(v) for k, v in tasks_data.items()}
        state.rebuild_indexes()
        return state

    def rebuild_indexes(self) -> None:
        """Rebuild _pending_set and _active_set from current tasks.

        Called after from_dict() and after reconcile_tasks(). O(N)
        in task count — for 15k tasks this is ~1–2 ms, for 100k ~10 ms.
        Incremental updates during mutation happen in BatchStore;
        this is the only full-rebuild entry point.
        """
        pending = set()
        active = set()
        for key, task in self.tasks.items():
            if task.status == TaskStatus.PENDING:
                pending.add(key)
            elif task.status in ACTIVE_STATUSES:
                active.add(key)
        self._pending_set = pending
        self._active_set = active

    def source_root_for(self, task: Task) -> str | None:
        """Return the longest matching root directory for a task.

        Used to attribute a task back to its originating parent directory
        in multi-workdir batches. Returns None if no root matches
        (shouldn't happen in well-formed states).
        """
        task_path = Path(task.directory).resolve()
        best: str | None = None
        best_len = -1
        for root in self.root_directories:
            try:
                root_path = Path(root).resolve()
            except (OSError, ValueError):
                continue
            try:
                task_path.relative_to(root_path)
            except ValueError:
                continue
            if len(str(root_path)) > best_len:
                best = str(root_path)
                best_len = len(str(root_path))
        return best
