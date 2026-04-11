"""Tests for batch_store package: paths, sentinels, mutation, rotation."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pytest

from pbs_auto.batch_store import (
    BatchStore,
    MAX_LOG_SIZE,
    cleanup_stale_artifacts,
    create_sentinel,
    ensure_batch_dir,
    get_batch_dir,
    get_log_path,
    get_sentinel_dir,
    get_state_path,
    iter_sentinels,
    maybe_rotate_log_on_startup,
    migrate_layout,
    recover_sentinels,
    remove_sentinel,
    sentinel_key,
    sentinel_path,
    update_sentinel_job_id,
)
from pbs_auto.batch_store._sentinels import read_sentinel
from pbs_auto.models import BatchState, PBSJobInfo, Task, TaskStatus


@pytest.fixture(autouse=True)
def _isolate_state_dir(tmp_path, monkeypatch):
    monkeypatch.setattr("pbs_auto.state.DEFAULT_STATE_DIR", tmp_path)
    monkeypatch.setattr("pbs_auto.batch_store._paths.DEFAULT_STATE_DIR", tmp_path)
    monkeypatch.setattr("pbs_auto.config.DEFAULT_STATE_DIR", tmp_path)
    return tmp_path


# ---------------------------------------------------------------------------
# Paths + ensure_batch_dir
# ---------------------------------------------------------------------------


class TestPaths:
    def test_get_batch_dir(self, tmp_path):
        assert get_batch_dir("abc") == tmp_path / "abc"

    def test_ensure_batch_dir_creates_tree(self, tmp_path):
        ensure_batch_dir("b1")
        assert (tmp_path / "b1").is_dir()
        assert (tmp_path / "b1" / "submitting").is_dir()
        mode = (tmp_path / "b1").stat().st_mode & 0o777
        assert mode == 0o700


class TestMigrateLayout:
    def test_no_legacy_no_new_is_noop(self, tmp_path):
        migrate_layout("nothing")  # Should not raise
        assert not (tmp_path / "nothing").exists()

    def test_legacy_flat_file_moved(self, tmp_path):
        legacy = tmp_path / "legacy.json"
        legacy.write_text('{"batch_id":"legacy"}')
        migrate_layout("legacy")
        assert (tmp_path / "legacy" / "state.json").exists()
        assert not legacy.exists()

    def test_already_migrated_noop(self, tmp_path):
        (tmp_path / "done").mkdir()
        (tmp_path / "done" / "state.json").write_text('{"x":1}')
        original_content = (tmp_path / "done" / "state.json").read_text()
        migrate_layout("done")
        assert (tmp_path / "done" / "state.json").read_text() == original_content

    def test_symlink_refused(self, tmp_path):
        target = tmp_path / "target.json"
        target.write_text("{}")
        link = tmp_path / "evil.json"
        link.symlink_to(target)
        with pytest.raises(RuntimeError, match="symlink"):
            migrate_layout("evil")


# ---------------------------------------------------------------------------
# Sentinels
# ---------------------------------------------------------------------------


class TestSentinels:
    def _task(self, directory="/tmp/task/01"):
        return Task(
            name="01",
            directory=directory,
            cores=48,
            status=TaskStatus.PENDING,
        )

    def test_sentinel_key_deterministic(self):
        k1 = sentinel_key("/tmp/a")
        k2 = sentinel_key("/tmp/a")
        assert k1 == k2
        assert len(k1) == 24

    def test_create_then_read(self, tmp_path):
        ensure_batch_dir("b")
        task = self._task()
        path = create_sentinel("b", task)
        parsed = read_sentinel(path)
        assert parsed is not None
        assert parsed.directory == task.directory
        assert parsed.job_id == "PENDING"

    def test_sentinel_sharded_path(self, tmp_path):
        ensure_batch_dir("b")
        task = self._task()
        path = create_sentinel("b", task)
        key = sentinel_key(task.directory)
        assert path.parent.name == key[:2]
        assert path.name == key

    def test_sentinel_file_permissions(self, tmp_path):
        ensure_batch_dir("b")
        task = self._task()
        path = create_sentinel("b", task)
        mode = path.stat().st_mode & 0o777
        assert mode == 0o600

    def test_update_job_id_rewrites_line2(self, tmp_path):
        ensure_batch_dir("b")
        task = self._task()
        path = create_sentinel("b", task)
        update_sentinel_job_id(path, "12345.mgr")
        parsed = read_sentinel(path)
        assert parsed is not None
        assert parsed.job_id == "12345.mgr"
        assert parsed.directory == task.directory

    def test_remove_sentinel_idempotent(self, tmp_path):
        ensure_batch_dir("b")
        task = self._task()
        path = create_sentinel("b", task)
        remove_sentinel(path)
        remove_sentinel(path)  # Should not raise
        assert not path.exists()

    def test_iter_skips_tmp_files(self, tmp_path):
        ensure_batch_dir("b")
        task = self._task()
        path = create_sentinel("b", task)
        tmp = path.parent / "xxxxx.tmp"
        tmp.write_text("garbage")
        names = [n for n, _ in iter_sentinels("b")]
        assert path.name in names
        assert "xxxxx.tmp" not in names

    def test_read_sentinel_rejects_malformed(self, tmp_path):
        ensure_batch_dir("b")
        sentinel_root = get_sentinel_dir("b")
        shard = sentinel_root / "ff"
        shard.mkdir(parents=True)
        bad = shard / ("ff" * 12)
        bad.write_text("only-one-line\n")
        assert read_sentinel(bad) is None


# ---------------------------------------------------------------------------
# cleanup_stale_artifacts
# ---------------------------------------------------------------------------


class TestCleanup:
    def test_removes_stale_tempfiles(self, tmp_path):
        ensure_batch_dir("b")
        stale = get_batch_dir("b") / "state.json.stale.tmp"
        stale.write_text("x")
        # Age it past the threshold
        old_mtime = time.time() - 300
        os.utime(stale, (old_mtime, old_mtime))

        state = BatchState(
            batch_id="b", root_directories=["/tmp"], server_profile="s"
        )
        cleanup_stale_artifacts("b", state, max_tmp_age_s=60)
        assert not stale.exists()

    def test_removes_orphan_sentinel_not_in_state(self, tmp_path):
        ensure_batch_dir("b")
        task = Task(
            name="t1",
            directory="/tmp/t1",
            cores=48,
            status=TaskStatus.PENDING,
        )
        path = create_sentinel("b", task)
        # State has NO task matching this sentinel's key
        state = BatchState(
            batch_id="b", root_directories=["/tmp"], server_profile="s"
        )
        cleanup_stale_artifacts("b", state)
        assert not path.exists()


# ---------------------------------------------------------------------------
# recover_sentinels
# ---------------------------------------------------------------------------


class FakePBSClient:
    def __init__(self, jobs=None):
        self.jobs = jobs or {}

    def query_user_jobs(self, force=False):
        return dict(self.jobs)

    def invalidate_cache(self):
        pass


class TestRecoverSentinels:
    def _setup(self, tmp_path):
        ensure_batch_dir("rec")
        task = Task(
            name="t1",
            directory="/tmp/rec/t1",
            cores=48,
            status=TaskStatus.PENDING,
        )
        state = BatchState(
            batch_id="rec",
            root_directories=["/tmp/rec"],
            server_profile="s",
        )
        state.tasks[task.directory] = task
        state.rebuild_indexes()
        return state, task

    def test_pending_sentinel_unlinked(self, tmp_path):
        state, task = self._setup(tmp_path)
        path = create_sentinel("rec", task)  # body = "PENDING"

        pbs = FakePBSClient()
        changed = recover_sentinels(state, pbs)
        assert changed is False
        assert not path.exists()
        assert task.status == TaskStatus.PENDING

    def test_sentinel_with_active_job_attaches(self, tmp_path):
        state, task = self._setup(tmp_path)
        path = create_sentinel("rec", task)
        update_sentinel_job_id(path, "12345")

        pbs = FakePBSClient(
            {"12345": PBSJobInfo(job_id="12345", name="pa_rec", state="Q", cores=48)}
        )
        changed = recover_sentinels(state, pbs)
        assert changed is True
        assert task.status == TaskStatus.SUBMITTED
        assert task.job_id == "12345"
        assert not path.exists()

    def test_sentinel_with_dead_job_drops(self, tmp_path):
        state, task = self._setup(tmp_path)
        path = create_sentinel("rec", task)
        update_sentinel_job_id(path, "99999")

        pbs = FakePBSClient()  # Job NOT present — truly dead
        changed = recover_sentinels(state, pbs)
        assert changed is False
        assert task.status == TaskStatus.PENDING  # Unchanged
        assert not path.exists()

    def test_pbs_query_failure_returns_false(self, tmp_path):
        state, task = self._setup(tmp_path)
        create_sentinel("rec", task)

        class BrokenPBS:
            def query_user_jobs(self, force=False):
                raise RuntimeError("qstat failed")

            def invalidate_cache(self):
                pass

        assert recover_sentinels(state, BrokenPBS()) is False

    def test_forged_sentinel_rejected(self, tmp_path):
        """A sentinel with mismatched filename-vs-content hash is dropped."""
        state, task = self._setup(tmp_path)
        # Compute the legit path + key for the real task
        real_path = sentinel_path("rec", task.directory)
        real_path.parent.mkdir(parents=True, exist_ok=True)
        # Write a body pointing at a DIFFERENT directory than the filename claims
        from datetime import datetime

        forged_content = (
            f"/tmp/evil/elsewhere\n11111\n{datetime.now().isoformat()}\n"
        )
        real_path.write_text(forged_content)

        pbs = FakePBSClient(
            {"11111": PBSJobInfo(job_id="11111", name="pa_rec", state="R", cores=48)}
        )
        changed = recover_sentinels(state, pbs)
        assert changed is False  # Forgery rejected
        assert task.status == TaskStatus.PENDING


# ---------------------------------------------------------------------------
# BatchStore / _MutationContext
# ---------------------------------------------------------------------------


class TestBatchStore:
    def _state(self, tmp_path):
        ensure_batch_dir("bs")
        t = Task(
            name="t1",
            directory="/tmp/bs/t1",
            cores=48,
            status=TaskStatus.PENDING,
        )
        state = BatchState(
            batch_id="bs",
            root_directories=["/tmp/bs"],
            server_profile="s",
        )
        state.tasks[t.directory] = t
        state.rebuild_indexes()
        return state, t

    def test_eager_flush_persists_immediately(self, tmp_path):
        state, t = self._state(tmp_path)
        store = BatchStore(state)
        with store.mutate(task=t, flush="eager"):
            t.status = TaskStatus.SUBMITTED
            t.job_id = "999"
        assert get_state_path("bs").exists()
        # Indexes updated
        assert t.directory in state._active_set
        assert t.directory not in state._pending_set

    def test_debounced_flush_sets_dirty(self, tmp_path):
        state, t = self._state(tmp_path)
        store = BatchStore(state)
        with store.mutate(task=t, flush="debounced"):
            t.status = TaskStatus.RUNNING
        assert store._dirty is True

    def test_debounced_flush_respects_min_interval(self, tmp_path):
        state, t = self._state(tmp_path)
        store = BatchStore(state)
        # Mark dirty WITHOUT actually calling maybe_debounced_flush;
        # the interval check should prevent flushing too soon.
        store._dirty = True
        store._last_flush_mono = time.monotonic()  # Just flushed
        store.maybe_debounced_flush()
        assert store._dirty is True  # Did not flush

    def test_failed_task_promotes_to_eager(self, tmp_path):
        state, t = self._state(tmp_path)
        store = BatchStore(state)
        with store.mutate(task=t, flush="debounced"):
            t.status = TaskStatus.FAILED
            t.error_message = "oops"
        # FAILED promotion means we flushed eagerly, so dirty is False.
        assert store._dirty is False

    def test_clean_exit_removes_sentinel(self, tmp_path):
        state, t = self._state(tmp_path)
        store = BatchStore(state)
        with store.mutate(task=t, flush="eager") as m:
            t.status = TaskStatus.SUBMITTED
            t.job_id = "123"
            assert m.sentinel_path is not None
            assert m.sentinel_path.exists()
        # Sentinel must be cleaned up after clean exit.
        names = [n for n, _ in iter_sentinels("bs")]
        assert not names

    def test_exception_preserves_sentinel(self, tmp_path):
        state, t = self._state(tmp_path)
        store = BatchStore(state)
        saved_path = None
        with pytest.raises(RuntimeError):
            with store.mutate(task=t, flush="eager") as m:
                saved_path = m.sentinel_path
                raise RuntimeError("boom")
        # Sentinel should still be on disk for recovery.
        assert saved_path is not None
        assert saved_path.exists()

    def test_force_flush_writes_state(self, tmp_path):
        state, t = self._state(tmp_path)
        store = BatchStore(state)
        store._dirty = True
        store.force_flush()
        assert not store._dirty
        assert get_state_path("bs").exists()


# ---------------------------------------------------------------------------
# Log rotation
# ---------------------------------------------------------------------------


class TestLogRotation:
    def test_rotate_on_startup_oversize(self, tmp_path):
        ensure_batch_dir("rot")
        log_path = get_log_path("rot")
        # Create a fake log larger than MAX_LOG_SIZE (use sparse write).
        with open(log_path, "wb") as f:
            f.seek(MAX_LOG_SIZE + 1)
            f.write(b"x")

        maybe_rotate_log_on_startup("rot")
        assert not log_path.exists()
        gz_files = list(log_path.parent.glob("daemon.log.*.gz"))
        assert gz_files, "Expected a .gz archive after rotation"
