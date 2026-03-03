"""Data models for PBS Auto-Submit Tool."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime


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
        }

    @classmethod
    def from_dict(cls, data: dict) -> Task:
        data = dict(data)
        data["status"] = TaskStatus(data["status"])
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
    """State of a batch submission session."""

    batch_id: str
    root_directory: str
    server_profile: str
    created_at: str = ""
    updated_at: str = ""
    tasks: dict[str, Task] = field(default_factory=dict)

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now().isoformat()
        if not self.updated_at:
            self.updated_at = self.created_at

    def to_dict(self) -> dict:
        return {
            "batch_id": self.batch_id,
            "root_directory": self.root_directory,
            "server_profile": self.server_profile,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "tasks": {k: v.to_dict() for k, v in self.tasks.items()},
        }

    @classmethod
    def from_dict(cls, data: dict) -> BatchState:
        data = dict(data)
        tasks_data = data.pop("tasks", {})
        state = cls(**data)
        state.tasks = {k: Task.from_dict(v) for k, v in tasks_data.items()}
        return state
