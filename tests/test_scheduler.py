"""Tests for scheduler logic."""

from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

from pbs_auto.config import AppConfig, ServerConfig
from pbs_auto.display import Display
from pbs_auto.models import BatchState, PBSJobInfo, Task, TaskStatus
from pbs_auto.pbs import PBSClient
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


class FakeDisplay:
    """Fake display for testing."""

    def start(self): pass
    def stop(self): pass
    def refresh(self, state, server): pass


class TestSchedulerResourceCheck:
    def _make_scheduler(self, tasks, pbs_client=None):
        server = ServerConfig(
            name="Test",
            max_running_cores=240,
            max_queued_cores=192,
        )
        config = AppConfig(
            server="test",
            servers={"test": server},
            submit_delay=0,
            poll_interval=1,
        )
        state = BatchState(
            batch_id="test",
            root_directory="/tmp",
            server_profile="test",
        )
        state.tasks = {t.name: t for t in tasks}

        pbs = pbs_client or FakePBSClient()
        display = FakeDisplay()
        return Scheduler(state, config, server, pbs, display)

    def test_get_resource_usage_empty(self):
        scheduler = self._make_scheduler([])
        pbs = scheduler.pbs
        r, q = scheduler._get_resource_usage()
        assert r == 0
        assert q == 0

    def test_get_resource_usage_with_jobs(self):
        pbs = FakePBSClient()
        pbs.jobs["1"] = PBSJobInfo(job_id="1", name="a", state="R", cores=96)
        pbs.jobs["2"] = PBSJobInfo(job_id="2", name="b", state="Q", cores=48)

        scheduler = self._make_scheduler([], pbs)
        r, q = scheduler._get_resource_usage()
        assert r == 96
        assert q == 48

    def test_all_done_empty(self):
        scheduler = self._make_scheduler([])
        assert scheduler._all_done()

    def test_all_done_with_pending(self):
        tasks = [Task(name="1", directory="/tmp/1", cores=24)]
        scheduler = self._make_scheduler(tasks)
        assert not scheduler._all_done()

    def test_all_done_with_completed(self):
        tasks = [Task(name="1", directory="/tmp/1", cores=24, status=TaskStatus.COMPLETED)]
        scheduler = self._make_scheduler(tasks)
        assert scheduler._all_done()


class TestHandleJobDisappeared:
    def _make_scheduler(self):
        server = ServerConfig(name="Test", max_running_cores=240, max_queued_cores=192)
        config = AppConfig(
            server="test", servers={"test": server},
            early_exit_threshold=30,
        )
        state = BatchState(batch_id="test", root_directory="/tmp", server_profile="test")
        return Scheduler(state, config, server, FakePBSClient(), FakeDisplay())

    def test_submitted_disappears_is_warning(self):
        scheduler = self._make_scheduler()
        task = Task(name="1", directory="/tmp/1", cores=24,
                    status=TaskStatus.SUBMITTED, job_id="123",
                    submit_time=datetime.now().isoformat())
        scheduler._handle_job_disappeared(task)
        assert task.status == TaskStatus.WARNING

    def test_short_run_is_warning(self):
        scheduler = self._make_scheduler()
        now = datetime.now()
        task = Task(name="1", directory="/tmp/1", cores=24,
                    status=TaskStatus.RUNNING, job_id="123",
                    start_time=(now - timedelta(seconds=10)).isoformat())
        scheduler._handle_job_disappeared(task)
        assert task.status == TaskStatus.WARNING
        assert "10s" in task.error_message

    def test_long_run_is_completed(self):
        scheduler = self._make_scheduler()
        now = datetime.now()
        task = Task(name="1", directory="/tmp/1", cores=24,
                    status=TaskStatus.RUNNING, job_id="123",
                    start_time=(now - timedelta(hours=1)).isoformat())
        scheduler._handle_job_disappeared(task)
        assert task.status == TaskStatus.COMPLETED


class TestPollStatus:
    def test_running_job_detected(self):
        pbs = FakePBSClient()
        pbs.jobs["123"] = PBSJobInfo(
            job_id="123", name="t1", state="R", cores=48
        )

        server = ServerConfig(name="Test", max_running_cores=240, max_queued_cores=192)
        config = AppConfig(server="test", servers={"test": server})
        state = BatchState(batch_id="test", root_directory="/tmp", server_profile="test")
        task = Task(name="t1", directory="/tmp/1", cores=48,
                    status=TaskStatus.SUBMITTED, job_id="123")
        state.tasks["t1"] = task

        scheduler = Scheduler(state, config, server, pbs, FakeDisplay())
        scheduler._poll_status()

        assert task.status == TaskStatus.RUNNING
        assert task.start_time is not None

    def test_queued_job_detected(self):
        pbs = FakePBSClient()
        pbs.jobs["123"] = PBSJobInfo(
            job_id="123", name="t1", state="Q", cores=48
        )

        server = ServerConfig(name="Test", max_running_cores=240, max_queued_cores=192)
        config = AppConfig(server="test", servers={"test": server})
        state = BatchState(batch_id="test", root_directory="/tmp", server_profile="test")
        task = Task(name="t1", directory="/tmp/1", cores=48,
                    status=TaskStatus.SUBMITTED, job_id="123")
        state.tasks["t1"] = task

        scheduler = Scheduler(state, config, server, pbs, FakeDisplay())
        scheduler._poll_status()

        assert task.status == TaskStatus.QUEUED
