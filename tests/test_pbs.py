"""Tests for PBS command parsing and PBSClient interaction."""

from __future__ import annotations

import os
import subprocess
import time
from unittest.mock import MagicMock, patch

import pytest

from pbs_auto.config import ServerConfig
from pbs_auto.models import PBSJobInfo, Task, TaskStatus
from pbs_auto.pbs import PBSClient, parse_qstat_output, parse_q_output


# ---------------------------------------------------------------------------
# Canned output fixtures
# ---------------------------------------------------------------------------

QSTAT_OUTPUT = """\
                                                            Req'd  Req'd   Elap
Job ID          Username Queue    Jobname    SessID NDS TSK Memory Time  S Time
--------------- -------- -------- ---------- ------ --- --- ------ ----- - -----
371824.mgr      shaofl   long     pzc_co2    12345   1  96    --  720:0 R 28:26
371866.mgr      shaofl   long     2k_pot     12346   1  96    --  720:0 R 25:09
371900.mgr      shaofl   medium   vasp_calc  12347   1  24    --  168:0 Q   --
"""

QSTAT_SINGLE = """\
                                                            Req'd  Req'd   Elap
Job ID          Username Queue    Jobname    SessID NDS TSK Memory Time  S Time
--------------- -------- -------- ---------- ------ --- --- ------ ----- - -----
371824.mgr      shaofl   long     test_job   12345   2  48    --  720:0 R 10:30
"""

QSTAT_MIXED_STATES = """\
                                                            Req'd  Req'd   Elap
Job ID          Username Queue    Jobname    SessID NDS TSK Memory Time  S Time
--------------- -------- -------- ---------- ------ --- --- ------ ----- - -----
400001.mgr      shaofl   long     job_run    11111   1  96    --  720:0 R 05:30
400002.mgr      shaofl   medium   job_queue  22222   1  48    --  240:0 Q   --
400003.mgr      shaofl   long     job_exit   33333   1  96    --  720:0 E 10:00
400004.mgr      shaofl   short    job_held   44444   1  24    --  168:0 H   --
"""

Q_OUTPUT = """\
----------------------------------------------------------------------------
////////////////////...(,,• ● •,,)/♥ [Hi..HuaXueXi..]2026-03-03 15:52:50
----------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 ID     Nodes   Owner   Name      Time_Used S Queue  N:T   Mem   Work Path                                                                                Start Time
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 371824 node05  shaofl  pzc_co2_d 28:26:50  R long   1:96  172G /share/home/chem-wangyg/shaofl/projects/03_CuAg_SAA/small/explicit/single-side/co2/1k/ad_ 03-02 11:24:16
                        esorb_sg                                stru/desorb
 371866 node06  shaofl  2k_pot    25:09:56  R long   1:96  208G /share/home/chem-wangyg/shaofl/projects/03_CuAg_SAA/small/explicit/double-side/00_initial 03-02 14:42:17
                                                                _stru/2K/initial_stru/potential
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
long:   node09: 96/96   node10: 96/96
medium: node17: 48/96   node19: 48/96
short:  node28: 24/96   node32: 12/96   node33: 24/96   node35: 12/96   node37: 36/96   node39: 12/96   node38: 60/96   node40: 32/96   node41: 96/96   node42: 96/96   mgr: 48/48
////////'⚫'////////...o(ง • ● • )ง [Bye bye]
 -h/--help for more information
"""

Q_EMPTY = """\
----------------------------------------------------------------------------
////////////////////...(,,• ● •,,)/♥ [Hi..HuaXueXi..]2026-03-03 15:52:50
----------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 ID     Nodes   Owner   Name      Time_Used S Queue  N:T   Mem   Work Path                                                                                Start Time
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
long:
medium:
short:
////////'⚫'////////...o(ง • ● • )ง [Bye bye]
"""

Q_MIXED_STATES = """\
----------------------------------------------------------------------------
////////////////////...(,,• ● •,,)/♥ [Hi..HuaXueXi..]2026-03-10 10:00:00
----------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 ID     Nodes   Owner   Name      Time_Used S Queue  N:T   Mem   Work Path                                                                                Start Time
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 500001 node01  shaofl  md_run    12:30:00  R long   1:96  200G /share/home/chem-wangyg/shaofl/projects/test1                                             03-10 00:00:00
 500002 --      shaofl  md_wait   00:00:00  Q medium 1:48  100G /share/home/chem-wangyg/shaofl/projects/test2                                             --
 500003 node03  shaofl  md_multi  06:15:30  R long   2:96  400G /share/home/chem-wangyg/shaofl/projects/test3                                             03-10 03:45:00
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
long:   node01: 96/96   node03: 96/96
medium:
short:
////////'⚫'////////...o(ง • ● • )ง [Bye bye]
"""


# ---------------------------------------------------------------------------
# parse_qstat_output tests
# ---------------------------------------------------------------------------

class TestParseQstat:
    def test_parse_multiple_jobs(self):
        jobs = parse_qstat_output(QSTAT_OUTPUT)
        assert len(jobs) == 3

        assert "371824" in jobs
        assert jobs["371824"].name == "pzc_co2"
        assert jobs["371824"].state == "R"
        assert jobs["371824"].cores == 96
        assert jobs["371824"].queue == "long"

        assert "371866" in jobs
        assert jobs["371866"].state == "R"
        assert jobs["371866"].cores == 96

        assert "371900" in jobs
        assert jobs["371900"].state == "Q"
        assert jobs["371900"].cores == 24

    def test_parse_single_job(self):
        jobs = parse_qstat_output(QSTAT_SINGLE)
        assert len(jobs) == 1
        assert jobs["371824"].cores == 48
        assert jobs["371824"].name == "test_job"

    def test_parse_empty(self):
        jobs = parse_qstat_output("")
        assert jobs == {}

    def test_parse_no_jobs(self):
        output = """\
                                                            Req'd  Req'd   Elap
Job ID          Username Queue    Jobname    SessID NDS TSK Memory Time  S Time
--------------- -------- -------- ---------- ------ --- --- ------ ----- - -----
"""
        jobs = parse_qstat_output(output)
        assert jobs == {}

    def test_parse_mixed_states(self):
        """Jobs with R, Q, E, H states are all parsed correctly."""
        jobs = parse_qstat_output(QSTAT_MIXED_STATES)
        assert len(jobs) == 4
        assert jobs["400001"].state == "R"
        assert jobs["400002"].state == "Q"
        assert jobs["400003"].state == "E"
        assert jobs["400004"].state == "H"

    def test_parse_elapsed_time_field(self):
        jobs = parse_qstat_output(QSTAT_OUTPUT)
        assert jobs["371824"].elapsed == "28:26"
        assert jobs["371900"].elapsed == "--"

    def test_parse_job_id_strips_suffix(self):
        """Job ID should have .mgr suffix removed."""
        jobs = parse_qstat_output(QSTAT_SINGLE)
        assert "371824" in jobs
        assert "371824.mgr" not in jobs

    def test_parse_whitespace_only(self):
        jobs = parse_qstat_output("   \n  \n  ")
        assert jobs == {}

    def test_parse_malformed_short_line(self):
        """Lines with fewer than 11 columns are skipped."""
        output = """\
                                                            Req'd  Req'd   Elap
Job ID          Username Queue    Jobname    SessID NDS TSK Memory Time  S Time
--------------- -------- -------- ---------- ------ --- --- ------ ----- - -----
371824.mgr      shaofl   long
"""
        jobs = parse_qstat_output(output)
        assert jobs == {}


# ---------------------------------------------------------------------------
# parse_q_output tests
# ---------------------------------------------------------------------------

class TestParseQ:
    def test_parse_running_jobs(self):
        jobs = parse_q_output(Q_OUTPUT)
        assert len(jobs) == 2

        assert "371824" in jobs
        assert jobs["371824"].name == "pzc_co2_d"
        assert jobs["371824"].state == "R"
        assert jobs["371824"].cores == 96
        assert jobs["371824"].queue == "long"

        assert "371866" in jobs
        assert jobs["371866"].name == "2k_pot"
        assert jobs["371866"].state == "R"
        assert jobs["371866"].cores == 96

    def test_parse_continuation_lines(self):
        """Continuation lines (wrapped paths) should be ignored."""
        jobs = parse_q_output(Q_OUTPUT)
        assert len(jobs) == 2

    def test_parse_empty_q(self):
        jobs = parse_q_output(Q_EMPTY)
        assert jobs == {}

    def test_parse_truly_empty(self):
        jobs = parse_q_output("")
        assert jobs == {}

    def test_parse_mixed_states(self):
        """Q output with R and Q states parsed correctly."""
        jobs = parse_q_output(Q_MIXED_STATES)
        assert len(jobs) == 3
        assert jobs["500001"].state == "R"
        assert jobs["500001"].cores == 96
        assert jobs["500002"].state == "Q"
        assert jobs["500002"].cores == 48
        assert jobs["500003"].state == "R"
        assert jobs["500003"].cores == 192  # 2 * 96

    def test_parse_multi_node_cores(self):
        """N:T field '2:96' should yield 192 cores."""
        jobs = parse_q_output(Q_MIXED_STATES)
        assert jobs["500003"].cores == 192

    def test_parse_elapsed_time(self):
        jobs = parse_q_output(Q_OUTPUT)
        assert jobs["371824"].elapsed == "28:26:50"
        assert jobs["371866"].elapsed == "25:09:56"

    def test_parse_queue_names(self):
        jobs = parse_q_output(Q_MIXED_STATES)
        assert jobs["500001"].queue == "long"
        assert jobs["500002"].queue == "medium"
        assert jobs["500003"].queue == "long"


# ---------------------------------------------------------------------------
# PBSClient tests
# ---------------------------------------------------------------------------

@pytest.fixture
def qstat_server_config():
    """ServerConfig using qstat command."""
    return ServerConfig(
        name="Test Server",
        status_command="qstat",
        status_args=["-au", "$USER"],
        max_running_cores=240,
        max_queued_cores=192,
    )


@pytest.fixture
def q_server_config():
    """ServerConfig using q command."""
    return ServerConfig(
        name="Test Server",
        status_command="q",
        status_args=["-u", "$USER"],
        max_running_cores=240,
        max_queued_cores=192,
    )


class TestPBSClientExpandArgs:
    def test_expand_user_variable(self, qstat_server_config):
        client = PBSClient(qstat_server_config)
        with patch.dict(os.environ, {"USER": "testuser"}):
            result = client._expand_args(["--user", "$USER", "--flag"])
        assert result == ["--user", "testuser", "--flag"]

    def test_expand_uses_logname_fallback(self, qstat_server_config):
        client = PBSClient(qstat_server_config)
        env = {"LOGNAME": "loguser"}
        with patch.dict(os.environ, env, clear=True):
            result = client._expand_args(["$USER"])
        assert result == ["loguser"]

    def test_expand_unknown_user_fallback(self, qstat_server_config):
        client = PBSClient(qstat_server_config)
        with patch.dict(os.environ, {}, clear=True):
            result = client._expand_args(["$USER"])
        assert result == ["unknown"]

    def test_expand_no_variables(self, qstat_server_config):
        client = PBSClient(qstat_server_config)
        result = client._expand_args(["--flag", "value"])
        assert result == ["--flag", "value"]


class TestPBSClientSubmit:
    def test_submit_success(self, qstat_server_config, tmp_path):
        """Successful qsub returns job_id."""
        task_dir = tmp_path / "task1"
        task_dir.mkdir()
        (task_dir / "script.sh").write_text("#!/bin/bash\necho hello\n")
        task = Task(name="task1", directory=str(task_dir), cores=24)

        client = PBSClient(qstat_server_config)
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "400100.mgr\n"
        mock_result.stderr = ""

        with patch("pbs_auto.pbs.subprocess.run", return_value=mock_result) as mock_run:
            job_id = client.submit(task)

        assert job_id == "400100.mgr"
        mock_run.assert_called_once()
        call_args = mock_run.call_args
        assert call_args[0][0] == ["qsub", "-N", "task1", "script.sh"]
        assert call_args[1]["cwd"] == str(task_dir)

    def test_submit_with_queue(self, qstat_server_config, tmp_path):
        """qsub includes -q flag when task has a queue."""
        task_dir = tmp_path / "task1"
        task_dir.mkdir()
        (task_dir / "script.sh").write_text("#!/bin/bash\necho hello\n")
        task = Task(name="task1", directory=str(task_dir), cores=48, queue="long")

        client = PBSClient(qstat_server_config)
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "400200.mgr\n"

        with patch("pbs_auto.pbs.subprocess.run", return_value=mock_result) as mock_run:
            job_id = client.submit(task)

        assert job_id == "400200.mgr"
        call_args = mock_run.call_args[0][0]
        assert call_args == ["qsub", "-q", "long", "-N", "task1", "script.sh"]

    def test_submit_script_not_found(self, qstat_server_config, tmp_path):
        """FileNotFoundError when script file does not exist."""
        task_dir = tmp_path / "task1"
        task_dir.mkdir()
        # No script.sh created
        task = Task(name="task1", directory=str(task_dir), cores=24)

        client = PBSClient(qstat_server_config)
        with pytest.raises(FileNotFoundError, match="Script not found"):
            client.submit(task)

    def test_submit_qsub_fails(self, qstat_server_config, tmp_path):
        """RuntimeError when qsub returns non-zero exit code."""
        task_dir = tmp_path / "task1"
        task_dir.mkdir()
        (task_dir / "script.sh").write_text("#!/bin/bash\n")
        task = Task(name="task1", directory=str(task_dir), cores=24)

        client = PBSClient(qstat_server_config)
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "qsub: queue not available\n"
        mock_result.stdout = ""

        with patch("pbs_auto.pbs.subprocess.run", return_value=mock_result):
            with pytest.raises(RuntimeError, match="qsub failed"):
                client.submit(task)

    def test_submit_empty_job_id(self, qstat_server_config, tmp_path):
        """RuntimeError when qsub returns empty job_id."""
        task_dir = tmp_path / "task1"
        task_dir.mkdir()
        (task_dir / "script.sh").write_text("#!/bin/bash\n")
        task = Task(name="task1", directory=str(task_dir), cores=24)

        client = PBSClient(qstat_server_config)
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "   \n"
        mock_result.stderr = ""

        with patch("pbs_auto.pbs.subprocess.run", return_value=mock_result):
            with pytest.raises(RuntimeError, match="empty job ID"):
                client.submit(task)


class TestPBSClientQueryQstat:
    def test_query_returns_parsed_jobs(self, qstat_server_config):
        """query_user_jobs delegates to qstat parser."""
        client = PBSClient(qstat_server_config)
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = QSTAT_OUTPUT

        with patch("pbs_auto.pbs.subprocess.run", return_value=mock_result):
            jobs = client.query_user_jobs(force=True)

        assert len(jobs) == 3
        assert "371824" in jobs

    def test_query_nonzero_returns_empty(self, qstat_server_config):
        """qstat non-zero exit (no jobs) returns empty dict."""
        client = PBSClient(qstat_server_config)
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""

        with patch("pbs_auto.pbs.subprocess.run", return_value=mock_result):
            jobs = client.query_user_jobs(force=True)

        assert jobs == {}

    def test_query_timeout_raises(self, qstat_server_config):
        """Timeout raises RuntimeError."""
        client = PBSClient(qstat_server_config)

        with patch("pbs_auto.pbs.subprocess.run", side_effect=subprocess.TimeoutExpired("qstat", 30)):
            with pytest.raises(RuntimeError, match="Failed to run"):
                client.query_user_jobs(force=True)

    def test_query_command_not_found_raises(self, qstat_server_config):
        """Missing command raises RuntimeError."""
        client = PBSClient(qstat_server_config)

        with patch("pbs_auto.pbs.subprocess.run", side_effect=FileNotFoundError("qstat")):
            with pytest.raises(RuntimeError, match="Failed to run"):
                client.query_user_jobs(force=True)


class TestPBSClientQueryQ:
    def test_query_q_returns_parsed_jobs(self, q_server_config):
        """query_user_jobs delegates to q parser when status_command is q."""
        client = PBSClient(q_server_config)
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = Q_OUTPUT

        with patch("pbs_auto.pbs.subprocess.run", return_value=mock_result):
            jobs = client.query_user_jobs(force=True)

        assert len(jobs) == 2
        assert "371824" in jobs
        assert "371866" in jobs

    def test_query_q_empty(self, q_server_config):
        """Empty q output returns empty dict."""
        client = PBSClient(q_server_config)
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = Q_EMPTY

        with patch("pbs_auto.pbs.subprocess.run", return_value=mock_result):
            jobs = client.query_user_jobs(force=True)

        assert jobs == {}


class TestPBSClientCache:
    def test_cache_returns_same_result(self, qstat_server_config):
        """Second call within TTL returns cached result without subprocess."""
        client = PBSClient(qstat_server_config)
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = QSTAT_SINGLE

        with patch("pbs_auto.pbs.subprocess.run", return_value=mock_result) as mock_run:
            jobs1 = client.query_user_jobs(force=True)
            jobs2 = client.query_user_jobs()

        assert mock_run.call_count == 1
        assert jobs1 is jobs2

    def test_force_bypasses_cache(self, qstat_server_config):
        """force=True always calls subprocess."""
        client = PBSClient(qstat_server_config)
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = QSTAT_SINGLE

        with patch("pbs_auto.pbs.subprocess.run", return_value=mock_result) as mock_run:
            client.query_user_jobs(force=True)
            client.query_user_jobs(force=True)

        assert mock_run.call_count == 2

    def test_invalidate_cache_forces_refresh(self, qstat_server_config):
        """invalidate_cache causes next query to call subprocess."""
        client = PBSClient(qstat_server_config)
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = QSTAT_SINGLE

        with patch("pbs_auto.pbs.subprocess.run", return_value=mock_result) as mock_run:
            client.query_user_jobs(force=True)
            client.invalidate_cache()
            client.query_user_jobs()

        assert mock_run.call_count == 2

    def test_cache_expired_refreshes(self, qstat_server_config):
        """Expired cache triggers new subprocess call."""
        client = PBSClient(qstat_server_config)
        client._cache_ttl = 0.01  # 10ms TTL for testing

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = QSTAT_SINGLE

        with patch("pbs_auto.pbs.subprocess.run", return_value=mock_result) as mock_run:
            client.query_user_jobs(force=True)
            time.sleep(0.02)
            client.query_user_jobs()

        assert mock_run.call_count == 2
