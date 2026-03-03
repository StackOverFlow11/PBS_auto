"""Shared test fixtures."""

from __future__ import annotations

import pytest
from pathlib import Path

from pbs_auto.config import AppConfig, ServerConfig
from pbs_auto.models import BatchState, Task, TaskStatus


@pytest.fixture
def server_config():
    return ServerConfig(
        name="Test Server",
        max_running_cores=240,
        max_queued_cores=192,
        core_granularity=24,
    )


@pytest.fixture
def app_config(server_config):
    return AppConfig(
        server="test",
        servers={"test": server_config},
    )


@pytest.fixture
def sample_tasks():
    return [
        Task(name="1", directory="/tmp/tasks/1", cores=48, status=TaskStatus.PENDING),
        Task(name="2", directory="/tmp/tasks/2", cores=48, status=TaskStatus.PENDING),
        Task(name="3", directory="/tmp/tasks/3", cores=96, status=TaskStatus.PENDING),
    ]


@pytest.fixture
def sample_batch(sample_tasks):
    state = BatchState(
        batch_id="test123",
        root_directory="/tmp/tasks",
        server_profile="test",
    )
    state.tasks = {t.name: t for t in sample_tasks}
    return state


@pytest.fixture
def workdir(tmp_path):
    """Create a temporary work directory with task subdirectories."""
    for i in range(1, 6):
        task_dir = tmp_path / str(i)
        task_dir.mkdir()
        script = task_dir / "script.sh"
        script.write_text(
            "#!/bin/bash\n"
            "#PBS -q medium\n"
            f"#PBS -l nodes=1:ppn={24 * ((i % 2) + 1)}\n"
            "#PBS -j oe\n"
            "#PBS -V\n"
            "echo hello\n"
        )
    return tmp_path
