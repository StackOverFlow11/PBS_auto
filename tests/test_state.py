"""Tests for state persistence (JSON load/save + migration + reconcile)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pbs_auto.models import BatchState, Task, TaskStatus
from pbs_auto.state import (
    MAX_EAGER_SAVE_LATENCY_MS,
    _migrate_on_load,
    generate_batch_id,
    list_batches,
    load_state,
    reconcile_tasks,
    resolve_batch_identifier,
    save_state,
    validate_identifier,
)


@pytest.fixture(autouse=True)
def _isolate_state_dir(tmp_path, monkeypatch):
    """Redirect DEFAULT_STATE_DIR to tmp_path for all tests in this module."""
    monkeypatch.setattr("pbs_auto.state.DEFAULT_STATE_DIR", tmp_path)
    monkeypatch.setattr("pbs_auto.batch_store._paths.DEFAULT_STATE_DIR", tmp_path)
    monkeypatch.setattr("pbs_auto.config.DEFAULT_STATE_DIR", tmp_path)
    return tmp_path


class TestBatchId:
    def test_deterministic_from_path(self):
        id1 = generate_batch_id("/tmp/tasks")
        id2 = generate_batch_id("/tmp/tasks")
        assert id1 == id2

    def test_different_paths_differ(self):
        id1 = generate_batch_id("/tmp/tasks1")
        id2 = generate_batch_id("/tmp/tasks2")
        assert id1 != id2

    def test_length(self):
        bid = generate_batch_id("/tmp/tasks")
        assert len(bid) == 16

    def test_multi_root_sorted(self):
        """Order of roots must not affect the generated ID."""
        id1 = generate_batch_id(["/tmp/a", "/tmp/b"])
        id2 = generate_batch_id(["/tmp/b", "/tmp/a"])
        assert id1 == id2

    def test_name_overrides_roots(self):
        id1 = generate_batch_id(["/tmp/a"], name="test")
        id2 = generate_batch_id(["/tmp/b"], name="test")
        assert id1 == id2  # Name-derived, roots ignored

    def test_name_vs_path_differ(self):
        id_path = generate_batch_id("/tmp/test")
        id_name = generate_batch_id(["/tmp/test"], name="test")
        assert id_path != id_name


class TestSaveLoad:
    def test_roundtrip(self, tmp_path):
        state = BatchState(
            batch_id="test123",
            root_directories=["/tmp/tasks"],
            server_profile="server1",
            name="demo",
        )
        state.tasks["/tmp/tasks/1"] = Task(
            name="1",
            directory="/tmp/tasks/1",
            cores=48,
            status=TaskStatus.RUNNING,
            job_id="12345.mgr",
        )
        state.rebuild_indexes()

        save_state(state)
        loaded = load_state("test123")

        assert loaded is not None
        assert loaded.batch_id == "test123"
        assert loaded.root_directories == ["/tmp/tasks"]
        assert loaded.name == "demo"
        assert loaded.version == 2
        assert "/tmp/tasks/1" in loaded.tasks
        assert loaded.tasks["/tmp/tasks/1"].status == TaskStatus.RUNNING
        assert loaded.tasks["/tmp/tasks/1"].job_id == "12345.mgr"
        # Indexes are rebuilt on load
        assert "/tmp/tasks/1" in loaded._active_set

    def test_load_nonexistent(self, tmp_path):
        assert load_state("nonexistent") is None

    def test_save_creates_batch_dir(self, tmp_path):
        state = BatchState(
            batch_id="test",
            root_directories=["/tmp"],
            server_profile="s1",
        )
        save_state(state)
        assert (tmp_path / "test" / "state.json").exists()
        assert (tmp_path / "test" / "summary.json").exists()

    def test_compact_json_format(self, tmp_path):
        """save_state must use compact separators (no indent)."""
        state = BatchState(
            batch_id="compact",
            root_directories=["/tmp"],
            server_profile="s1",
        )
        state.tasks["/tmp/1"] = Task(name="1", directory="/tmp/1", cores=24)
        save_state(state)
        content = (tmp_path / "compact" / "state.json").read_text()
        # Compact JSON has no ": " or ", " whitespace between separators
        assert '":' in content
        assert '": ' not in content

    def test_save_without_summary(self, tmp_path):
        state = BatchState(
            batch_id="noSummary",
            root_directories=["/tmp"],
            server_profile="s1",
        )
        save_state(state, write_summary=False)
        assert (tmp_path / "noSummary" / "state.json").exists()
        assert not (tmp_path / "noSummary" / "summary.json").exists()

    def test_file_permissions_0o600(self, tmp_path):
        state = BatchState(
            batch_id="perms",
            root_directories=["/tmp"],
            server_profile="s1",
        )
        save_state(state)
        state_path = tmp_path / "perms" / "state.json"
        mode = state_path.stat().st_mode & 0o777
        assert mode == 0o600

    def test_save_latency_under_cap(self, tmp_path):
        """15k tasks must serialize within MAX_EAGER_SAVE_LATENCY_MS."""
        import time

        state = BatchState(
            batch_id="big",
            root_directories=["/tmp/big"],
            server_profile="s1",
        )
        for i in range(15000):
            d = f"/tmp/big/frame_{i:06d}"
            state.tasks[d] = Task(
                name=f"frame_{i:06d}",
                directory=d,
                cores=48,
                status=TaskStatus.PENDING,
            )
        state.rebuild_indexes()

        start = time.monotonic()
        save_state(state)
        elapsed_ms = (time.monotonic() - start) * 1000
        assert elapsed_ms < MAX_EAGER_SAVE_LATENCY_MS, (
            f"save_state took {elapsed_ms:.0f}ms for 15k tasks; "
            f"cap is {MAX_EAGER_SAVE_LATENCY_MS}ms"
        )


class TestReconcile:
    def test_new_tasks_added(self):
        saved = BatchState(
            batch_id="b1", root_directories=["/tmp"], server_profile="s1"
        )
        saved.tasks["/tmp/1"] = Task(
            name="1",
            directory="/tmp/1",
            cores=48,
            status=TaskStatus.COMPLETED,
        )
        scanned = [
            Task(name="1", directory="/tmp/1", cores=48),
            Task(name="2", directory="/tmp/2", cores=48),
        ]
        result = reconcile_tasks(saved, scanned)
        assert "/tmp/1" in result.tasks
        assert "/tmp/2" in result.tasks
        assert result.tasks["/tmp/1"].status == TaskStatus.COMPLETED
        assert result.tasks["/tmp/2"].status == TaskStatus.PENDING

    def test_submitted_with_job_id_kept(self):
        """Bug fix: SUBMITTED with job_id must NOT be reset to PENDING.

        The old behavior reset unconditionally, causing daemon restarts
        to re-submit jobs that PBS already accepted.
        """
        saved = BatchState(
            batch_id="b1", root_directories=["/tmp"], server_profile="s1"
        )
        saved.tasks["/tmp/1"] = Task(
            name="1",
            directory="/tmp/1",
            cores=48,
            status=TaskStatus.SUBMITTED,
            job_id="12345.mgr",
            submit_time="2026-04-11T00:00:00",
        )
        scanned = [Task(name="1", directory="/tmp/1", cores=48)]
        result = reconcile_tasks(saved, scanned)
        assert result.tasks["/tmp/1"].status == TaskStatus.SUBMITTED
        assert result.tasks["/tmp/1"].job_id == "12345.mgr"

    def test_submitted_without_job_id_reset(self):
        """Defensive: SUBMITTED without job_id resets (shouldn't happen)."""
        saved = BatchState(
            batch_id="b1", root_directories=["/tmp"], server_profile="s1"
        )
        saved.tasks["/tmp/1"] = Task(
            name="1",
            directory="/tmp/1",
            cores=48,
            status=TaskStatus.SUBMITTED,
            job_id=None,
        )
        scanned = [Task(name="1", directory="/tmp/1", cores=48)]
        result = reconcile_tasks(saved, scanned)
        assert result.tasks["/tmp/1"].status == TaskStatus.PENDING

    def test_completed_preserved_and_cores_refreshed(self):
        saved = BatchState(
            batch_id="b1", root_directories=["/tmp"], server_profile="s1"
        )
        saved.tasks["/tmp/1"] = Task(
            name="1",
            directory="/tmp/1",
            cores=48,
            status=TaskStatus.COMPLETED,
        )
        scanned = [Task(name="1", directory="/tmp/1", cores=96)]
        result = reconcile_tasks(saved, scanned)
        assert result.tasks["/tmp/1"].status == TaskStatus.COMPLETED
        assert result.tasks["/tmp/1"].cores == 96  # Rescan refresh

    def test_running_preserved(self):
        saved = BatchState(
            batch_id="b1", root_directories=["/tmp"], server_profile="s1"
        )
        saved.tasks["/tmp/1"] = Task(
            name="1",
            directory="/tmp/1",
            cores=48,
            status=TaskStatus.RUNNING,
            job_id="456",
        )
        scanned = [Task(name="1", directory="/tmp/1", cores=48)]
        result = reconcile_tasks(saved, scanned)
        assert result.tasks["/tmp/1"].status == TaskStatus.RUNNING
        assert result.tasks["/tmp/1"].job_id == "456"

    def test_reconcile_rebuilds_indexes(self):
        saved = BatchState(
            batch_id="b1", root_directories=["/tmp"], server_profile="s1"
        )
        saved.tasks["/tmp/1"] = Task(
            name="1",
            directory="/tmp/1",
            cores=48,
            status=TaskStatus.RUNNING,
            job_id="x",
        )
        scanned = [
            Task(name="1", directory="/tmp/1", cores=48),
            Task(name="2", directory="/tmp/2", cores=48),  # New → PENDING
        ]
        result = reconcile_tasks(saved, scanned)
        assert "/tmp/1" in result._active_set
        assert "/tmp/2" in result._pending_set
        assert "/tmp/1" not in result._pending_set


class TestMigration:
    def test_migrate_v1_root_directory_to_list(self):
        raw = {
            "batch_id": "old",
            "root_directory": "/tmp/old",
            "server_profile": "s1",
            "created_at": "2025-01-01T00:00:00",
            "updated_at": "2025-01-01T00:00:00",
            "tasks": {},
        }
        migrated = _migrate_on_load(raw)
        assert migrated["version"] == 2
        assert migrated["root_directories"] == ["/tmp/old"]
        assert "root_directory" not in migrated
        assert migrated["name"] is None

    def test_migrate_rekeys_tasks_by_directory(self):
        raw = {
            "batch_id": "old",
            "root_directory": "/tmp",
            "server_profile": "s1",
            "created_at": "2025",
            "updated_at": "2025",
            "tasks": {
                "t1": {
                    "name": "t1",
                    "directory": "/tmp/t1",
                    "cores": 24,
                    "status": "pending",
                    "job_id": None,
                    "submit_time": None,
                    "start_time": None,
                    "end_time": None,
                    "error_message": None,
                    "script_name": "script.sh",
                }
            },
        }
        migrated = _migrate_on_load(raw)
        assert "/tmp/t1" in migrated["tasks"]
        assert "t1" not in migrated["tasks"]

    def test_migrate_idempotent(self):
        raw = {
            "batch_id": "b",
            "root_directories": ["/tmp"],
            "server_profile": "s1",
            "name": None,
            "version": 2,
            "created_at": "2026",
            "updated_at": "2026",
            "tasks": {},
        }
        migrated = _migrate_on_load(raw)
        assert migrated == raw

    def test_load_legacy_flat_file(self, tmp_path):
        """Legacy <batch_id>.json files should auto-migrate to
        <batch_id>/state.json on load."""
        old_data = {
            "batch_id": "legacy",
            "root_directory": "/tmp/legacy",
            "server_profile": "s1",
            "created_at": "2025",
            "updated_at": "2025",
            "tasks": {
                "t1": {
                    "name": "t1",
                    "directory": "/tmp/legacy/t1",
                    "cores": 48,
                    "status": "completed",
                    "job_id": "999",
                    "submit_time": None,
                    "start_time": None,
                    "end_time": None,
                    "error_message": None,
                    "script_name": "script.sh",
                }
            },
        }
        (tmp_path / "legacy.json").write_text(json.dumps(old_data))

        loaded = load_state("legacy")
        assert loaded is not None
        assert loaded.root_directories == ["/tmp/legacy"]
        assert loaded.version == 2
        assert "/tmp/legacy/t1" in loaded.tasks
        # Legacy file moved into new layout
        assert (tmp_path / "legacy" / "state.json").exists()
        assert not (tmp_path / "legacy.json").exists()


class TestIdentifierValidation:
    def test_valid_names(self):
        for name in ["test", "mlip_round1", "job-2026", "abc.123", "a" * 64]:
            validate_identifier(name)  # Should not raise

    def test_rejects_path_separators(self):
        for bad in ["a/b", "a\\b", "../foo"]:
            with pytest.raises(ValueError):
                validate_identifier(bad)

    def test_rejects_null_byte(self):
        with pytest.raises(ValueError):
            validate_identifier("name\x00")

    def test_rejects_leading_dash(self):
        with pytest.raises(ValueError):
            validate_identifier("-rm")

    def test_rejects_reserved_names(self):
        for bad in ["daemon.pid", "state.json", "submitting", "."]:
            with pytest.raises(ValueError):
                validate_identifier(bad)

    def test_rejects_too_long(self):
        with pytest.raises(ValueError):
            validate_identifier("a" * 65)

    def test_rejects_empty(self):
        with pytest.raises(ValueError):
            validate_identifier("")


class TestResolveBatchIdentifier:
    def _make_batch(self, tmp_path, batch_id: str, name: str | None):
        state = BatchState(
            batch_id=batch_id,
            root_directories=["/tmp/x"],
            server_profile="s1",
            name=name,
        )
        save_state(state)

    def test_exact_batch_id(self, tmp_path):
        self._make_batch(tmp_path, "abcdef1234567890", None)
        assert resolve_batch_identifier("abcdef1234567890") == "abcdef1234567890"

    def test_prefix_match(self, tmp_path):
        self._make_batch(tmp_path, "abc12345deadbeef", None)
        assert resolve_batch_identifier("abc12345") == "abc12345deadbeef"

    def test_name_match(self, tmp_path):
        self._make_batch(tmp_path, "abc123ffbeef0000", "mlip_round1")
        assert resolve_batch_identifier("mlip_round1") == "abc123ffbeef0000"

    def test_ambiguous_prefix_rejected(self, tmp_path):
        self._make_batch(tmp_path, "abcdefgh00000000", None)
        self._make_batch(tmp_path, "abcdefij11111111", None)
        with pytest.raises(ValueError, match="Ambiguous"):
            resolve_batch_identifier("abcdef")

    def test_unknown_raises(self, tmp_path):
        with pytest.raises(ValueError, match="No batch found"):
            resolve_batch_identifier("nope")


class TestListBatches:
    def test_list_empty(self, tmp_path):
        assert list_batches() == []

    def test_list_with_batches(self, tmp_path):
        state = BatchState(
            batch_id="batch1",
            root_directories=["/tmp/tasks"],
            server_profile="server1",
            name="b1_name",
        )
        state.tasks["/tmp/tasks/1"] = Task(
            name="1",
            directory="/tmp/tasks/1",
            cores=48,
            status=TaskStatus.COMPLETED,
        )
        save_state(state)

        batches = list_batches()
        assert len(batches) == 1
        assert batches[0]["batch_id"] == "batch1"
        assert batches[0]["total_tasks"] == 1
        assert batches[0]["status_counts"]["completed"] == 1
        assert batches[0]["name"] == "b1_name"

    def test_list_fallback_to_state_when_summary_missing(self, tmp_path):
        state = BatchState(
            batch_id="nosum",
            root_directories=["/tmp"],
            server_profile="s1",
        )
        state.tasks["/tmp/1"] = Task(
            name="1", directory="/tmp/1", cores=48,
            status=TaskStatus.RUNNING,
        )
        save_state(state)
        # Remove summary to exercise the fallback path
        (tmp_path / "nosum" / "summary.json").unlink()

        batches = list_batches()
        assert len(batches) == 1
        assert batches[0]["batch_id"] == "nosum"
        assert batches[0]["status_counts"]["running"] == 1
