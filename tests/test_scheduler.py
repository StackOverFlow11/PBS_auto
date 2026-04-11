"""Tests for scheduler logic."""

from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

from pbs_auto.batch_store import BatchStore
from pbs_auto.config import AppConfig, ServerConfig
from pbs_auto.models import BatchState, PBSJobInfo, Task, TaskStatus
from pbs_auto.scheduler import Scheduler


class FakePBSClient:
    """Fake PBS client for testing scheduler logic."""

    def __init__(self):
        self.jobs: dict[str, PBSJobInfo] = {}
        self.submitted: list[str] = []
        self._submit_counter = 1000

    def submit(self, task: Task) -> str:
        job_id = str(self._submit_counter)
        self._submit_counter += 1
        self.submitted.append(task.name)
        self.jobs[job_id] = PBSJobInfo(
            job_id=job_id, name=task.name, state="Q", cores=task.cores
        )
        return job_id

    def query_user_jobs(self, force=False) -> dict[str, PBSJobInfo]:
        return dict(self.jobs)

    def invalidate_cache(self):
        pass


@pytest.fixture(autouse=True)
def _isolate_state_dir(tmp_path, monkeypatch):
    """Redirect DEFAULT_STATE_DIR to tmp_path for every scheduler test."""
    monkeypatch.setattr("pbs_auto.state.DEFAULT_STATE_DIR", tmp_path)
    monkeypatch.setattr("pbs_auto.batch_store._paths.DEFAULT_STATE_DIR", tmp_path)
    monkeypatch.setattr("pbs_auto.config.DEFAULT_STATE_DIR", tmp_path)
    return tmp_path


def _make_scheduler(tasks, pbs_client=None, early_exit=30, submit_delay=0):
    server = ServerConfig(
        name="Test",
        max_running_cores=240,
        max_queued_cores=192,
    )
    config = AppConfig(
        server="test",
        servers={"test": server},
        submit_delay=submit_delay,
        poll_interval=1,
        early_exit_threshold=early_exit,
    )
    state = BatchState(
        batch_id="test",
        root_directories=["/tmp"],
        server_profile="test",
    )
    state.tasks = {t.directory: t for t in tasks}
    state.rebuild_indexes()

    pbs = pbs_client or FakePBSClient()
    store = BatchStore(state)
    return Scheduler(state, config, server, pbs, store)


class TestSchedulerResourceCheck:
    def test_get_resource_usage_empty(self):
        scheduler = _make_scheduler([])
        r, q = scheduler._get_resource_usage()
        assert r == 0
        assert q == 0

    def test_get_resource_usage_with_jobs(self):
        pbs = FakePBSClient()
        pbs.jobs["1"] = PBSJobInfo(job_id="1", name="a", state="R", cores=96)
        pbs.jobs["2"] = PBSJobInfo(job_id="2", name="b", state="Q", cores=48)

        scheduler = _make_scheduler([], pbs)
        r, q = scheduler._get_resource_usage()
        assert r == 96
        assert q == 48

    def test_all_done_empty(self):
        scheduler = _make_scheduler([])
        assert scheduler._all_done_fast()

    def test_all_done_with_pending(self):
        tasks = [Task(name="1", directory="/tmp/1", cores=24)]
        scheduler = _make_scheduler(tasks)
        assert not scheduler._all_done_fast()

    def test_all_done_with_completed(self):
        tasks = [
            Task(name="1", directory="/tmp/1", cores=24, status=TaskStatus.COMPLETED)
        ]
        scheduler = _make_scheduler(tasks)
        assert scheduler._all_done_fast()


class TestHandleJobDisappeared:
    def test_submitted_disappears_is_warning(self):
        scheduler = _make_scheduler([])
        task = Task(
            name="1",
            directory="/tmp/1",
            cores=24,
            status=TaskStatus.SUBMITTED,
            job_id="123",
            submit_time=datetime.now().isoformat(),
        )
        scheduler._handle_job_disappeared(task)
        assert task.status == TaskStatus.WARNING

    def test_short_run_is_warning(self):
        scheduler = _make_scheduler([])
        now = datetime.now()
        task = Task(
            name="1",
            directory="/tmp/1",
            cores=24,
            status=TaskStatus.RUNNING,
            job_id="123",
            start_time=(now - timedelta(seconds=10)).isoformat(),
        )
        scheduler._handle_job_disappeared(task)
        assert task.status == TaskStatus.WARNING
        assert "10s" in task.error_message

    def test_long_run_is_completed(self):
        scheduler = _make_scheduler([])
        now = datetime.now()
        task = Task(
            name="1",
            directory="/tmp/1",
            cores=24,
            status=TaskStatus.RUNNING,
            job_id="123",
            start_time=(now - timedelta(hours=1)).isoformat(),
        )
        scheduler._handle_job_disappeared(task)
        assert task.status == TaskStatus.COMPLETED


class TestSubmitTaskRetry:
    """Tests for retryable vs permanent qsub error handling."""

    def test_retryable_error_stays_pending(self):
        pbs = FakePBSClient()
        pbs.submit = MagicMock(
            side_effect=RuntimeError(
                "qsub failed: qsub: would exceed user shaofl's limit on resource ncpus in complex"
            )
        )
        task = Task(
            name="t1", directory="/tmp/1", cores=24, status=TaskStatus.PENDING
        )
        scheduler = _make_scheduler([task], pbs)

        result = scheduler._submit_task(task)

        assert task.status == TaskStatus.PENDING
        assert "Retryable" in task.error_message
        assert result is False

    def test_permanent_error_becomes_failed(self):
        pbs = FakePBSClient()
        pbs.submit = MagicMock(
            side_effect=RuntimeError("qsub failed: invalid queue specified")
        )
        task = Task(
            name="t1", directory="/tmp/1", cores=24, status=TaskStatus.PENDING
        )
        scheduler = _make_scheduler([task], pbs)

        result = scheduler._submit_task(task)

        assert task.status == TaskStatus.FAILED
        assert "invalid queue" in task.error_message
        assert result is True

    def test_script_not_found_becomes_failed(self):
        pbs = FakePBSClient()
        pbs.submit = MagicMock(
            side_effect=FileNotFoundError("Script not found: /tmp/1/script.sh")
        )
        task = Task(
            name="t1", directory="/tmp/1", cores=24, status=TaskStatus.PENDING
        )
        scheduler = _make_scheduler([task], pbs)

        result = scheduler._submit_task(task)

        assert task.status == TaskStatus.FAILED
        assert "Script not found" in task.error_message
        assert result is True

    def test_retryable_error_stops_batch(self):
        """Retryable error stops submitting remaining tasks in this round."""
        pbs = FakePBSClient()
        call_count = 0

        def failing_submit(task):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise RuntimeError("qsub failed: would exceed ncpus limit")
            return pbs._real_submit(task)

        pbs._real_submit = pbs.submit
        pbs.submit = failing_submit

        tasks = [
            Task(name="t1", directory="/tmp/1", cores=24, status=TaskStatus.PENDING),
            Task(name="t2", directory="/tmp/2", cores=24, status=TaskStatus.PENDING),
            Task(name="t3", directory="/tmp/3", cores=24, status=TaskStatus.PENDING),
        ]
        scheduler = _make_scheduler(tasks, pbs)
        scheduler._submit_pending()

        # t1 submitted, t2 retryable fail → break, t3 never attempted
        assert tasks[0].status == TaskStatus.SUBMITTED
        assert tasks[1].status == TaskStatus.PENDING
        assert tasks[2].status == TaskStatus.PENDING
        assert call_count == 2

    def test_retryable_error_cleared_on_success(self):
        task = Task(
            name="t1",
            directory="/tmp/1",
            cores=24,
            status=TaskStatus.PENDING,
            error_message="Retryable: qsub failed: would exceed ncpus",
        )
        pbs = FakePBSClient()
        scheduler = _make_scheduler([task], pbs)

        result = scheduler._submit_task(task)

        assert task.status == TaskStatus.SUBMITTED
        assert task.error_message is None
        assert result is True


class TestPollStatus:
    def test_running_job_detected(self):
        pbs = FakePBSClient()
        pbs.jobs["123"] = PBSJobInfo(
            job_id="123", name="t1", state="R", cores=48
        )

        task = Task(
            name="t1",
            directory="/tmp/1",
            cores=48,
            status=TaskStatus.SUBMITTED,
            job_id="123",
        )
        scheduler = _make_scheduler([task], pbs)
        scheduler._poll_status()

        assert task.status == TaskStatus.RUNNING
        assert task.start_time is not None

    def test_queued_job_detected(self):
        pbs = FakePBSClient()
        pbs.jobs["123"] = PBSJobInfo(
            job_id="123", name="t1", state="Q", cores=48
        )

        task = Task(
            name="t1",
            directory="/tmp/1",
            cores=48,
            status=TaskStatus.SUBMITTED,
            job_id="123",
        )
        scheduler = _make_scheduler([task], pbs)
        scheduler._poll_status()

        assert task.status == TaskStatus.QUEUED

    def test_disappeared_job_handled(self):
        """A task whose job vanishes from PBS transitions to COMPLETED."""
        pbs = FakePBSClient()  # No jobs in PBS
        task = Task(
            name="t1",
            directory="/tmp/1",
            cores=48,
            status=TaskStatus.RUNNING,
            job_id="999",
            start_time=(datetime.now() - timedelta(hours=1)).isoformat(),
        )
        scheduler = _make_scheduler([task], pbs)
        scheduler._poll_status()

        assert task.status == TaskStatus.COMPLETED


class TestIndexMaintenance:
    def test_submit_moves_task_from_pending_to_active(self):
        task = Task(
            name="t1", directory="/tmp/1", cores=24, status=TaskStatus.PENDING
        )
        scheduler = _make_scheduler([task])
        assert "/tmp/1" in scheduler.state._pending_set

        scheduler._submit_task(task)

        assert "/tmp/1" not in scheduler.state._pending_set
        assert "/tmp/1" in scheduler.state._active_set

    def test_failed_task_removed_from_both_sets(self):
        pbs = FakePBSClient()
        pbs.submit = MagicMock(
            side_effect=RuntimeError("qsub failed: invalid queue")
        )
        task = Task(
            name="t1", directory="/tmp/1", cores=24, status=TaskStatus.PENDING
        )
        scheduler = _make_scheduler([task], pbs)

        scheduler._submit_task(task)

        assert task.status == TaskStatus.FAILED
        assert "/tmp/1" not in scheduler.state._pending_set
        assert "/tmp/1" not in scheduler.state._active_set
