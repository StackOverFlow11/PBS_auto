"""Tests for scanner module."""

from __future__ import annotations

from pathlib import Path

import pytest

from pbs_auto.models import TaskStatus
from pbs_auto.scanner import (
    natural_sort_key,
    parse_cores_from_script,
    parse_script_resources,
    scan_directory,
)


class TestNaturalSort:
    def test_numeric_sort(self):
        names = ["10", "2", "1", "20", "3"]
        sorted_names = sorted(names, key=natural_sort_key)
        assert sorted_names == ["1", "2", "3", "10", "20"]

    def test_mixed_sort(self):
        names = ["task10", "task2", "task1"]
        sorted_names = sorted(names, key=natural_sort_key)
        assert sorted_names == ["task1", "task2", "task10"]


class TestParseCores:
    def test_basic_parse(self, tmp_path):
        script = tmp_path / "test.sh"
        script.write_text(
            "#!/bin/bash\n"
            "#PBS -q long\n"
            "#PBS -l nodes=1:ppn=96\n"
            "#PBS -j oe\n"
        )
        assert parse_cores_from_script(script) == 96

    def test_multi_node(self, tmp_path):
        script = tmp_path / "test.sh"
        script.write_text("#PBS -l nodes=4:ppn=24\n")
        assert parse_cores_from_script(script) == 96

    def test_with_spaces(self, tmp_path):
        script = tmp_path / "test.sh"
        script.write_text("#PBS -l nodes = 2 : ppn = 48\n")
        assert parse_cores_from_script(script) == 96

    def test_no_resource_line(self, tmp_path):
        script = tmp_path / "test.sh"
        script.write_text("#!/bin/bash\necho hello\n")
        assert parse_cores_from_script(script) is None

    def test_missing_file(self, tmp_path):
        script = tmp_path / "nonexistent.sh"
        assert parse_cores_from_script(script) is None


class TestParseScriptResources:
    def test_full_parse(self, tmp_path):
        script = tmp_path / "test.sh"
        script.write_text(
            "#!/bin/bash\n"
            "#PBS -q medium\n"
            "#PBS -l nodes=2:ppn=48\n"
            "#PBS -l walltime=120:00:00\n"
        )
        res = parse_script_resources(script)
        assert res is not None
        assert res.nodes == 2
        assert res.ppn == 48
        assert res.cores == 96
        assert res.queue == "medium"
        assert res.walltime_seconds == 120 * 3600

    def test_no_queue(self, tmp_path):
        script = tmp_path / "test.sh"
        script.write_text("#PBS -l nodes=1:ppn=24\n")
        res = parse_script_resources(script)
        assert res is not None
        assert res.queue is None
        assert res.cores == 24

    def test_no_walltime(self, tmp_path):
        script = tmp_path / "test.sh"
        script.write_text(
            "#PBS -q long\n"
            "#PBS -l nodes=1:ppn=96\n"
        )
        res = parse_script_resources(script)
        assert res is not None
        assert res.walltime_seconds is None
        assert res.queue == "long"

    def test_no_resource_line(self, tmp_path):
        script = tmp_path / "test.sh"
        script.write_text("#!/bin/bash\necho hello\n")
        assert parse_script_resources(script) is None

    def test_missing_file(self, tmp_path):
        script = tmp_path / "nonexistent.sh"
        assert parse_script_resources(script) is None

    def test_walltime_with_minutes_seconds(self, tmp_path):
        script = tmp_path / "test.sh"
        script.write_text(
            "#PBS -l nodes=1:ppn=24\n"
            "#PBS -l walltime=01:30:45\n"
        )
        res = parse_script_resources(script)
        assert res.walltime_seconds == 1 * 3600 + 30 * 60 + 45


class TestScanDirectory:
    def test_scan_basic(self, workdir):
        tasks = scan_directory(workdir)
        assert len(tasks) == 5
        # Natural sort order
        assert [t.name for t in tasks] == ["1", "2", "3", "4", "5"]
        # All should be PENDING with cores parsed
        for t in tasks:
            assert t.status == TaskStatus.PENDING
            assert t.cores > 0

    def test_scan_alternating_cores(self, workdir):
        tasks = scan_directory(workdir)
        # Odd dirs: ppn=48 (i%2+1=2, 1*48), Even dirs: ppn=24 (i%2+1=1, 1*24)
        # i=1: (1%2+1)=2 → ppn=48; i=2: (2%2+1)=1 → ppn=24
        assert tasks[0].cores == 48  # dir 1
        assert tasks[1].cores == 24  # dir 2
        assert tasks[2].cores == 48  # dir 3

    def test_scan_missing_script(self, tmp_path):
        task_dir = tmp_path / "task1"
        task_dir.mkdir()
        # No script.sh
        tasks = scan_directory(tmp_path)
        assert len(tasks) == 1
        assert tasks[0].status == TaskStatus.SKIPPED
        assert "not found" in tasks[0].error_message

    def test_scan_unparseable_script(self, tmp_path):
        task_dir = tmp_path / "task1"
        task_dir.mkdir()
        (task_dir / "script.sh").write_text("#!/bin/bash\necho hello\n")
        tasks = scan_directory(tmp_path)
        assert tasks[0].status == TaskStatus.SKIPPED
        assert "Cannot parse" in tasks[0].error_message

    def test_scan_nonexistent_dir(self):
        with pytest.raises(FileNotFoundError):
            scan_directory(Path("/nonexistent"))

    def test_scan_empty_dir(self, tmp_path):
        tasks = scan_directory(tmp_path)
        assert tasks == []

    def test_scan_custom_script_name(self, tmp_path):
        task_dir = tmp_path / "task1"
        task_dir.mkdir()
        (task_dir / "vasp.sh").write_text("#PBS -l nodes=1:ppn=24\n")
        tasks = scan_directory(tmp_path, script_name="vasp.sh")
        assert len(tasks) == 1
        assert tasks[0].cores == 24
        assert tasks[0].script_name == "vasp.sh"

    def test_scan_populates_queue_and_nodes(self, workdir):
        tasks = scan_directory(workdir)
        # The workdir fixture scripts have #PBS -q medium
        for t in tasks:
            assert t.queue == "medium"
            assert t.nodes == 1

    def test_scan_ignores_files_in_root(self, tmp_path):
        """Files (not dirs) in root should be ignored."""
        (tmp_path / "notes.txt").write_text("some notes")
        task_dir = tmp_path / "1"
        task_dir.mkdir()
        (task_dir / "script.sh").write_text("#PBS -l nodes=1:ppn=24\n")
        tasks = scan_directory(tmp_path)
        assert len(tasks) == 1
