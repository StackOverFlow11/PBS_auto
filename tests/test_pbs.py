"""Tests for PBS command parsing."""

from __future__ import annotations

import pytest

from pbs_auto.pbs import parse_qstat_output, parse_q_output


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
        # Should not count continuation lines as separate jobs
        assert len(jobs) == 2

    def test_parse_empty_q(self):
        jobs = parse_q_output(Q_EMPTY)
        assert jobs == {}

    def test_parse_truly_empty(self):
        jobs = parse_q_output("")
        assert jobs == {}
