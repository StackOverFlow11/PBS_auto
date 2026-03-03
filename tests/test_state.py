"""Tests for state persistence."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pbs_auto.models import BatchState, Task, TaskStatus
from pbs_auto.state import (
    generate_batch_id,
    get_state_path,
    save_state,
    load_state,
    reconcile_tasks,
    list_batches,
)


class TestBatchId:
    def test_deterministic(self):
        id1 = generate_batch_id("/tmp/tasks")
        id2 = generate_batch_id("/tmp/tasks")
        assert id1 == id2

    def test_different_paths(self):
        id1 = generate_batch_id("/tmp/tasks1")
        id2 = generate_batch_id("/tmp/tasks2")
        assert id1 != id2

    def test_length(self):
        bid = generate_batch_id("/tmp/tasks")
        assert len(bid) == 16


class TestSaveLoad:
    def test_roundtrip(self, tmp_path, monkeypatch):
        monkeypatch.setattr("pbs_auto.state.DEFAULT_STATE_DIR", tmp_path)

        state = BatchState(
            batch_id="test123",
            root_directory="/tmp/tasks",
            server_profile="server1",
        )
        state.tasks["t1"] = Task(
            name="t1", directory="/tmp/tasks/1", cores=48,
            status=TaskStatus.RUNNING, job_id="12345.mgr",
        )

        save_state(state)
        loaded = load_state("test123")

        assert loaded is not None
        assert loaded.batch_id == "test123"
        assert loaded.root_directory == "/tmp/tasks"
        assert "t1" in loaded.tasks
        assert loaded.tasks["t1"].status == TaskStatus.RUNNING
        assert loaded.tasks["t1"].job_id == "12345.mgr"
        assert loaded.tasks["t1"].cores == 48

    def test_load_nonexistent(self, tmp_path, monkeypatch):
        monkeypatch.setattr("pbs_auto.state.DEFAULT_STATE_DIR", tmp_path)
        assert load_state("nonexistent") is None

    def test_save_creates_directory(self, tmp_path, monkeypatch):
        state_dir = tmp_path / "nested" / "dir"
        monkeypatch.setattr("pbs_auto.state.DEFAULT_STATE_DIR", state_dir)

        state = BatchState(
            batch_id="test",
            root_directory="/tmp",
            server_profile="s1",
        )
        save_state(state)
        assert (state_dir / "test.json").exists()

    def test_save_is_valid_json(self, tmp_path, monkeypatch):
        monkeypatch.setattr("pbs_auto.state.DEFAULT_STATE_DIR", tmp_path)

        state = BatchState(
            batch_id="test",
            root_directory="/tmp",
            server_profile="s1",
        )
        state.tasks["1"] = Task(name="1", directory="/tmp/1", cores=24)
        save_state(state)

        with open(tmp_path / "test.json") as f:
            data = json.load(f)
        assert data["batch_id"] == "test"
        assert "1" in data["tasks"]


class TestReconcile:
    def test_new_tasks_added(self):
        saved = BatchState(
            batch_id="b1", root_directory="/tmp", server_profile="s1"
        )
        saved.tasks["1"] = Task(
            name="1", directory="/tmp/1", cores=48,
            status=TaskStatus.COMPLETED,
        )
        scanned = [
            Task(name="1", directory="/tmp/1", cores=48),
            Task(name="2", directory="/tmp/2", cores=48),
        ]
        result = reconcile_tasks(saved, scanned)
        assert "1" in result.tasks
        assert "2" in result.tasks
        assert result.tasks["1"].status == TaskStatus.COMPLETED
        assert result.tasks["2"].status == TaskStatus.PENDING

    def test_submitted_reset_to_pending(self):
        saved = BatchState(
            batch_id="b1", root_directory="/tmp", server_profile="s1"
        )
        saved.tasks["1"] = Task(
            name="1", directory="/tmp/1", cores=48,
            status=TaskStatus.SUBMITTED, job_id="123",
        )
        scanned = [Task(name="1", directory="/tmp/1", cores=48)]
        result = reconcile_tasks(saved, scanned)
        assert result.tasks["1"].status == TaskStatus.PENDING
        assert result.tasks["1"].job_id is None

    def test_completed_preserved(self):
        saved = BatchState(
            batch_id="b1", root_directory="/tmp", server_profile="s1"
        )
        saved.tasks["1"] = Task(
            name="1", directory="/tmp/1", cores=48,
            status=TaskStatus.COMPLETED,
        )
        scanned = [Task(name="1", directory="/tmp/1", cores=96)]
        result = reconcile_tasks(saved, scanned)
        assert result.tasks["1"].status == TaskStatus.COMPLETED
        # Cores updated from rescan
        assert result.tasks["1"].cores == 96

    def test_running_preserved(self):
        saved = BatchState(
            batch_id="b1", root_directory="/tmp", server_profile="s1"
        )
        saved.tasks["1"] = Task(
            name="1", directory="/tmp/1", cores=48,
            status=TaskStatus.RUNNING, job_id="456",
        )
        scanned = [Task(name="1", directory="/tmp/1", cores=48)]
        result = reconcile_tasks(saved, scanned)
        assert result.tasks["1"].status == TaskStatus.RUNNING
        assert result.tasks["1"].job_id == "456"


class TestListBatches:
    def test_list_empty(self, tmp_path, monkeypatch):
        monkeypatch.setattr("pbs_auto.state.DEFAULT_STATE_DIR", tmp_path)
        assert list_batches() == []

    def test_list_with_batches(self, tmp_path, monkeypatch):
        monkeypatch.setattr("pbs_auto.state.DEFAULT_STATE_DIR", tmp_path)

        state = BatchState(
            batch_id="batch1",
            root_directory="/tmp/tasks",
            server_profile="server1",
        )
        state.tasks["1"] = Task(
            name="1", directory="/tmp/tasks/1", cores=48,
            status=TaskStatus.COMPLETED,
        )
        save_state(state)

        batches = list_batches()
        assert len(batches) == 1
        assert batches[0]["batch_id"] == "batch1"
        assert batches[0]["total_tasks"] == 1
