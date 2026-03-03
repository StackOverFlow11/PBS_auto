"""Tests for queue validation and auto-selection."""

from __future__ import annotations

import pytest

from pbs_auto.config import QueueConfig
from pbs_auto.models import Task, TaskStatus
from pbs_auto.queue import (
    select_queue,
    validate_and_assign_queues,
    validate_task_for_queue,
)


# --- Fixtures ---

@pytest.fixture
def server1_queues():
    return {
        "debug": QueueConfig(
            name="debug", max_cores=24, max_nodes=1, max_walltime_hours=0.5,
        ),
        "short": QueueConfig(
            name="short", max_cores=48, max_nodes=1, max_walltime_hours=168,
        ),
        "medium": QueueConfig(
            name="medium", max_cores=96, min_cores=24,
            allowed_cores=[24, 48, 72, 96], max_nodes=1, max_walltime_hours=240,
        ),
        "long": QueueConfig(
            name="long", max_cores=192, min_cores=48,
            allowed_cores=[48, 96, 144, 192], max_nodes=-1, max_walltime_hours=360,
        ),
    }


# --- validate_task_for_queue ---

class TestValidateTaskForQueue:
    def test_compliant_range_queue(self):
        qc = QueueConfig(name="short", max_cores=48, max_nodes=1)
        task = Task(name="t", directory="/tmp/t", cores=24, nodes=1)
        assert validate_task_for_queue(task, qc) == []

    def test_compliant_allowed_list(self):
        qc = QueueConfig(
            name="medium", max_cores=96, min_cores=24,
            allowed_cores=[24, 48, 72, 96], max_nodes=1,
        )
        task = Task(name="t", directory="/tmp/t", cores=48, nodes=1)
        assert validate_task_for_queue(task, qc) == []

    def test_cores_not_in_allowed_list(self):
        qc = QueueConfig(
            name="medium", max_cores=96, min_cores=24,
            allowed_cores=[24, 48, 72, 96], max_nodes=1,
        )
        task = Task(name="t", directory="/tmp/t", cores=36, nodes=1)
        errors = validate_task_for_queue(task, qc)
        assert len(errors) == 1
        assert "not in allowed" in errors[0]

    def test_cores_below_minimum(self):
        qc = QueueConfig(name="medium", max_cores=96, min_cores=24, max_nodes=1)
        task = Task(name="t", directory="/tmp/t", cores=12, nodes=1)
        errors = validate_task_for_queue(task, qc)
        assert len(errors) == 1
        assert "below minimum" in errors[0]

    def test_cores_above_maximum(self):
        qc = QueueConfig(name="short", max_cores=48, max_nodes=1)
        task = Task(name="t", directory="/tmp/t", cores=96, nodes=1)
        errors = validate_task_for_queue(task, qc)
        assert len(errors) == 1
        assert "exceeds maximum" in errors[0]

    def test_nodes_exceed_max(self):
        qc = QueueConfig(name="short", max_cores=48, max_nodes=1)
        task = Task(name="t", directory="/tmp/t", cores=48, nodes=2)
        errors = validate_task_for_queue(task, qc)
        assert len(errors) == 1
        assert "nodes=" in errors[0]

    def test_unlimited_nodes(self):
        qc = QueueConfig(
            name="long", max_cores=192, min_cores=48,
            allowed_cores=[48, 96, 144, 192], max_nodes=-1,
        )
        task = Task(name="t", directory="/tmp/t", cores=192, nodes=4)
        assert validate_task_for_queue(task, qc) == []

    def test_multiple_errors(self):
        qc = QueueConfig(
            name="medium", max_cores=96, min_cores=24,
            allowed_cores=[24, 48, 72, 96], max_nodes=1,
        )
        task = Task(name="t", directory="/tmp/t", cores=36, nodes=2)
        errors = validate_task_for_queue(task, qc)
        assert len(errors) == 2


# --- select_queue ---

class TestSelectQueue:
    def test_auto_select_prefers_shortest_walltime(self, server1_queues):
        task = Task(name="t", directory="/tmp/t", cores=24, nodes=1)
        # 24 cores fits debug, short, medium — should pick debug (shortest walltime)
        result = select_queue(task, server1_queues)
        assert result == "debug"

    def test_auto_select_with_walltime_filter(self, server1_queues):
        task = Task(name="t", directory="/tmp/t", cores=24, nodes=1)
        # 2 hours walltime rules out debug (0.5h max)
        result = select_queue(task, server1_queues, walltime_seconds=7200)
        assert result == "short"

    def test_auto_select_48_cores(self, server1_queues):
        task = Task(name="t", directory="/tmp/t", cores=48, nodes=1)
        # 48 fits short (range), medium (allowed), long (allowed)
        # debug max=24, so excluded
        # shortest walltime among valid: short (168h)
        result = select_queue(task, server1_queues)
        assert result == "short"

    def test_auto_select_96_cores(self, server1_queues):
        task = Task(name="t", directory="/tmp/t", cores=96, nodes=1)
        # 96 fits medium (allowed) and long (allowed)
        # short max=48 excluded, debug max=24 excluded
        result = select_queue(task, server1_queues)
        assert result == "medium"

    def test_auto_select_192_cores(self, server1_queues):
        task = Task(name="t", directory="/tmp/t", cores=192, nodes=4)
        # Only long allows 192 cores and nodes > 1
        result = select_queue(task, server1_queues)
        assert result == "long"

    def test_no_matching_queue(self, server1_queues):
        task = Task(name="t", directory="/tmp/t", cores=300, nodes=1)
        result = select_queue(task, server1_queues)
        assert result is None

    def test_empty_queues(self):
        task = Task(name="t", directory="/tmp/t", cores=48, nodes=1)
        assert select_queue(task, {}) is None


# --- validate_and_assign_queues ---

class TestValidateAndAssignQueues:
    def test_cli_queue_overrides_script(self, server1_queues):
        tasks = [
            Task(name="t1", directory="/tmp/t1", cores=48, nodes=1, queue="short"),
        ]
        all_tasks, invalid = validate_and_assign_queues(
            tasks, server1_queues, cli_queue="medium"
        )
        assert all_tasks[0].queue == "medium"
        assert invalid == []

    def test_script_queue_preserved(self, server1_queues):
        tasks = [
            Task(name="t1", directory="/tmp/t1", cores=48, nodes=1, queue="medium"),
        ]
        all_tasks, invalid = validate_and_assign_queues(tasks, server1_queues)
        assert all_tasks[0].queue == "medium"
        assert invalid == []

    def test_auto_select_when_no_queue(self, server1_queues):
        tasks = [
            Task(name="t1", directory="/tmp/t1", cores=96, nodes=1),
        ]
        all_tasks, invalid = validate_and_assign_queues(tasks, server1_queues)
        assert all_tasks[0].queue == "medium"
        assert invalid == []

    def test_non_compliant_flagged(self, server1_queues):
        tasks = [
            Task(name="t1", directory="/tmp/t1", cores=36, nodes=1, queue="medium"),
        ]
        all_tasks, invalid = validate_and_assign_queues(tasks, server1_queues)
        assert len(invalid) == 1
        assert invalid[0][0].name == "t1"
        assert any("not in allowed" in e for e in invalid[0][1])

    def test_skipped_tasks_ignored(self, server1_queues):
        tasks = [
            Task(name="t1", directory="/tmp/t1", cores=36, nodes=1,
                 status=TaskStatus.SKIPPED),
        ]
        all_tasks, invalid = validate_and_assign_queues(tasks, server1_queues)
        assert invalid == []

    def test_empty_queues_no_validation(self):
        tasks = [
            Task(name="t1", directory="/tmp/t1", cores=48, nodes=1),
        ]
        all_tasks, invalid = validate_and_assign_queues(tasks, {})
        assert invalid == []
        assert all_tasks[0].queue is None
