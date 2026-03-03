"""PBS command interaction and output parsing."""

from __future__ import annotations

import os
import re
import subprocess
import time
from pathlib import Path

from pbs_auto.config import ServerConfig
from pbs_auto.models import PBSJobInfo, Task


class PBSClient:
    """Interface for PBS commands (qsub, qstat, q)."""

    def __init__(self, server_config: ServerConfig):
        self.config = server_config
        self._cache: dict[str, PBSJobInfo] | None = None
        self._cache_time: float = 0
        self._cache_ttl: float = 5.0  # seconds

    def submit(self, task: Task) -> str:
        """Submit a job via qsub. Returns job_id on success, raises on failure."""
        script_path = Path(task.directory) / task.script_name
        if not script_path.exists():
            raise FileNotFoundError(f"Script not found: {script_path}")

        result = subprocess.run(
            ["qsub", "-N", task.name, task.script_name],
            cwd=task.directory,
            capture_output=True,
            text=True,
            timeout=30,
        )

        if result.returncode != 0:
            error = result.stderr.strip() or result.stdout.strip()
            raise RuntimeError(f"qsub failed: {error}")

        job_id = result.stdout.strip()
        if not job_id:
            raise RuntimeError("qsub returned empty job ID")

        return job_id

    def query_user_jobs(self, force: bool = False) -> dict[str, PBSJobInfo]:
        """Query PBS for current user's jobs. Results cached for 5 seconds."""
        now = time.monotonic()
        if not force and self._cache is not None and (now - self._cache_time) < self._cache_ttl:
            return self._cache

        cmd = self.config.status_command
        args = self._expand_args(self.config.status_args)

        if cmd == "q":
            jobs = self._query_q(cmd, args)
        else:
            jobs = self._query_qstat(cmd, args)

        self._cache = jobs
        self._cache_time = now
        return jobs

    def invalidate_cache(self) -> None:
        """Force next query to fetch fresh data."""
        self._cache = None

    def _expand_args(self, args: list[str]) -> list[str]:
        """Expand $USER in command arguments."""
        user = os.environ.get("USER", os.environ.get("LOGNAME", "unknown"))
        return [a.replace("$USER", user) for a in args]

    def _query_qstat(self, cmd: str, args: list[str]) -> dict[str, PBSJobInfo]:
        """Parse qstat -au $USER output.

        Expected format:
                                                                    Req'd  Req'd   Elap
        Job ID          Username Queue    Jobname    SessID NDS TSK Memory Time  S Time
        --------------- -------- -------- ---------- ------ --- --- ------ ----- - -----
        371824.mgr      shaofl   long     pzc_co2_d  12345   1  96    --  720:0 R 28:26
        """
        try:
            result = subprocess.run(
                [cmd] + args,
                capture_output=True,
                text=True,
                timeout=30,
            )
        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            raise RuntimeError(f"Failed to run {cmd}: {e}") from e

        if result.returncode != 0:
            # qstat returns non-zero when no jobs, which is fine
            return {}

        return parse_qstat_output(result.stdout)

    def _query_q(self, cmd: str, args: list[str]) -> dict[str, PBSJobInfo]:
        """Parse q -u $USER output.

        Expected format:
        ID     Nodes   Owner   Name      Time_Used S Queue  N:T   Mem   ...
        371824 node05  shaofl  pzc_co2_d 28:26:50  R long   1:96  172G  ...
        """
        try:
            result = subprocess.run(
                [cmd] + args,
                capture_output=True,
                text=True,
                timeout=30,
            )
        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            raise RuntimeError(f"Failed to run {cmd}: {e}") from e

        if result.returncode != 0:
            return {}

        return parse_q_output(result.stdout)


def parse_qstat_output(output: str) -> dict[str, PBSJobInfo]:
    """Parse qstat -au $USER output into job info dict.

    Handles the standard PBS qstat table format. Job ID keys are
    normalized to just the numeric part (without .hostname suffix).
    """
    jobs: dict[str, PBSJobInfo] = {}

    lines = output.strip().splitlines()
    # Find the data lines (after the dashed separator)
    data_started = False
    for line in lines:
        if re.match(r"^-{10,}", line.strip()):
            data_started = True
            continue
        if not data_started:
            continue
        if not line.strip():
            continue

        parts = line.split()
        if len(parts) < 11:
            continue

        # Format: JobID Username Queue Jobname SessID NDS TSK Memory Time S ElapTime
        raw_job_id = parts[0]
        job_id = raw_job_id.split(".")[0]  # Strip .mgr suffix
        job_name = parts[3]
        state = parts[9]

        try:
            cores = int(parts[6])
        except (ValueError, IndexError):
            cores = 0

        elapsed = parts[10] if len(parts) > 10 else ""
        queue = parts[2]

        jobs[job_id] = PBSJobInfo(
            job_id=job_id,
            name=job_name,
            state=state,
            cores=cores,
            elapsed=elapsed,
            queue=queue,
        )

    return jobs


def parse_q_output(output: str) -> dict[str, PBSJobInfo]:
    """Parse q -u $USER output into job info dict.

    Handles the custom q command format with N:T column for node:task count.
    Handles continuation lines (lines starting with whitespace that continue
    the previous job's data).

    The q output has decoration lines (with //) before the data table.
    We look for the header line containing "ID" to locate the data section.
    """
    jobs: dict[str, PBSJobInfo] = {}

    lines = output.strip().splitlines()

    # Find the header line containing column names
    header_idx = -1
    for i, line in enumerate(lines):
        stripped = line.strip()
        if "ID" in stripped and "Owner" in stripped and "Queue" in stripped:
            header_idx = i
            break

    if header_idx < 0:
        return jobs

    # Data starts after the dash separator following the header
    data_started = False
    for line in lines[header_idx + 1:]:
        stripped = line.strip()

        if re.match(r"^-{10,}", stripped):
            if not data_started:
                data_started = True
                continue
            else:
                # End-of-data separator
                break
        if not data_started:
            continue
        if not stripped:
            continue

        # Skip footer lines (queue summary)
        if re.match(r"^(long|medium|short):", stripped):
            break

        # Data line: first token must be a digit (job ID)
        # Continuation lines have non-numeric first tokens
        parts = line.split()
        if len(parts) < 8:
            continue
        if not parts[0].isdigit():
            continue

        # Format: ID Nodes Owner Name Time_Used S Queue N:T Mem ...
        job_id = parts[0]
        job_name = parts[3]
        time_used = parts[4]
        state = parts[5]
        queue = parts[6]
        nt_field = parts[7]  # e.g. "1:96"

        # Parse N:T → cores = N * T
        cores = 0
        nt_match = re.match(r"(\d+):(\d+)", nt_field)
        if nt_match:
            cores = int(nt_match.group(1)) * int(nt_match.group(2))

        jobs[job_id] = PBSJobInfo(
            job_id=job_id,
            name=job_name,
            state=state,
            cores=cores,
            elapsed=time_used,
            queue=queue,
        )

    return jobs
