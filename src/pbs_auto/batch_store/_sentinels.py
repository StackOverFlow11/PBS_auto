"""Crash-recovery sentinels with content-authoritative job_id lookup.

A sentinel file records "a qsub is in flight (or just committed) for
this task". The sentinel's filename is derived from the task directory
(sha256 prefix) and its body is a strict 3-line ASCII format:

    <absolute_task_directory>
    <job_id or "PENDING">
    <iso_timestamp>

The file is written atomically with `tempfile.mkstemp` + `os.fsync` +
`os.replace`. It is created with body `PENDING` before `qsub` is
invoked. On a successful `qsub` we atomically rewrite line 2 with the
real `job_id`. On clean exit the sentinel is unconditionally removed.

Recovery after a crash reads each remaining sentinel and does a direct
`pbs_jobs[job_id]` lookup (no name matching — that was unreliable due
to PBS job name truncation). A filename-vs-content hash cross-check
prevents forged sentinels from being trusted.

Crash-safety windows:
    - qsub failed before rewrite → body is "PENDING" → discard
    - qsub succeeded but rewrite failed → orphan PBS job, WARN scan
      reports for manual qdel
    - rewrite succeeded but save_state failed → recovery attaches
      job_id → SUBMITTED
    - save_state succeeded but sentinel cleanup failed → recovery
      sees terminal/active status → drops orphan sentinel
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from pbs_auto.batch_store._paths import (
    FILE_MODE,
    get_sentinel_dir,
)
from pbs_auto.models import BatchState, TaskStatus


log = logging.getLogger(__name__)


# Sentinel filename hash: first 24 hex chars of sha256(task_dir).
# 96 bits is plenty to avoid collisions among ~100k tasks.
_SENTINEL_KEY_LEN = 24

# Strict job_id regex: numeric ID optionally followed by .server suffix.
# Matches "12345", "12345.mgr", "12345.chem-hpc-master".
_JOB_ID_RE = re.compile(r"^(PENDING|[0-9]+(\.[A-Za-z0-9._-]+)?)$")

# ISO 8601 timestamp regex (relaxed — python's datetime.fromisoformat works).
_ISO_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(\+\d{2}:\d{2}|Z)?$"
)

_MAX_DIR_LEN = 4096
_MAX_TMP_AGE_S = 60


@dataclass
class SentinelContent:
    directory: str
    job_id: str  # "PENDING" or real job id
    timestamp: str


def sentinel_key(task_dir: str) -> str:
    """Derive the sentinel filename from a task directory path."""
    return hashlib.sha256(task_dir.encode()).hexdigest()[:_SENTINEL_KEY_LEN]


def sentinel_path(batch_id: str, task_dir: str) -> Path:
    """Return the sharded sentinel path: submitting/<xx>/<key>."""
    key = sentinel_key(task_dir)
    return get_sentinel_dir(batch_id) / key[:2] / key


def _atomic_write_sentinel(path: Path, content: str) -> None:
    """Write sentinel file atomically with 0o600 permissions."""
    path.parent.mkdir(parents=True, exist_ok=True)
    # Tighten directory permissions
    try:
        os.chmod(path.parent, 0o700)
        os.chmod(path.parent.parent, 0o700)
    except OSError:
        pass

    fd, tmp_str = tempfile.mkstemp(
        dir=path.parent,
        prefix=path.name + ".",
        suffix=".tmp",
    )
    tmp_path = Path(tmp_str)
    try:
        try:
            os.fchmod(fd, FILE_MODE)
        except OSError:
            pass
        with os.fdopen(fd, "w") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.replace(str(tmp_path), str(path))
    except Exception:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def _format_content(directory: str, job_id: str) -> str:
    return f"{directory}\n{job_id}\n{datetime.now().isoformat()}\n"


def create_sentinel(batch_id: str, task) -> Path:
    """Create a sentinel for `task` with body `PENDING`.

    Called BEFORE `qsub` in `_MutationContext`. Returns the sentinel
    path for the caller to rewrite or remove later.
    """
    path = sentinel_path(batch_id, task.directory)
    content = _format_content(task.directory, "PENDING")
    _atomic_write_sentinel(path, content)
    return path


def update_sentinel_job_id(path: Path, job_id: str) -> None:
    """Atomically rewrite line 2 of the sentinel with the real job_id.

    Called immediately after a successful `qsub`. Line 1 (directory)
    and line 3 (timestamp — refreshed to now) are preserved.
    """
    if not path.exists():
        # Sentinel somehow missing — don't create a phantom.
        log.warning("update_sentinel_job_id: %s missing", path)
        return

    try:
        existing = path.read_text()
    except OSError as e:
        log.warning("update_sentinel_job_id: read %s failed: %s", path, e)
        return

    lines = existing.splitlines()
    if len(lines) < 1:
        log.warning("update_sentinel_job_id: %s malformed", path)
        return

    directory = lines[0]
    new_content = _format_content(directory, job_id)
    _atomic_write_sentinel(path, new_content)


def remove_sentinel(path: Path | None) -> None:
    """Delete a sentinel file. Silent on missing."""
    if path is None:
        return
    try:
        path.unlink(missing_ok=True)
    except OSError as e:
        log.warning("remove_sentinel: %s failed: %s", path, e)


def read_sentinel(path: Path) -> SentinelContent | None:
    """Parse a sentinel strictly. Returns None on any malformation."""
    try:
        raw = path.read_text()
    except OSError:
        return None

    lines = raw.splitlines()
    if len(lines) != 3:
        return None

    directory, job_id, ts = lines
    if len(directory) > _MAX_DIR_LEN or not directory:
        return None
    if not _JOB_ID_RE.match(job_id):
        return None
    if not _ISO_RE.match(ts):
        return None

    return SentinelContent(directory=directory, job_id=job_id, timestamp=ts)


def iter_sentinels(batch_id: str):
    """Yield (filename, full_path) for each sentinel file in the batch.

    Skips `*.tmp` partial writes. Paths are the sharded layout
    `<batch>/submitting/<xx>/<key>`.
    """
    sentinel_root = get_sentinel_dir(batch_id)
    if not sentinel_root.exists():
        return
    for shard in sorted(sentinel_root.iterdir()):
        if not shard.is_dir():
            continue
        for entry in sorted(shard.iterdir()):
            if not entry.is_file():
                continue
            if entry.suffix == ".tmp":
                continue
            yield entry.name, entry


def cleanup_stale_artifacts(
    batch_id: str,
    state: BatchState,
    max_tmp_age_s: int = _MAX_TMP_AGE_S,
) -> None:
    """Remove stale tempfiles and orphan sentinel files.

    Called once at daemon startup, before `recover_sentinels`.
    """
    from pbs_auto.batch_store._paths import (
        get_batch_dir,
        get_sentinel_dir,
    )

    batch_dir = get_batch_dir(batch_id)
    if not batch_dir.exists():
        return

    now = time.time()

    # Stale tempfiles at batch root
    for tmp in batch_dir.glob("*.tmp"):
        try:
            if (now - tmp.stat().st_mtime) > max_tmp_age_s:
                tmp.unlink()
        except OSError as e:
            log.debug("cleanup: %s: %s", tmp, e)

    # Stale tempfiles in sentinel shards
    sentinel_root = get_sentinel_dir(batch_id)
    if sentinel_root.exists():
        for tmp in sentinel_root.glob("*/*.tmp"):
            try:
                if (now - tmp.stat().st_mtime) > max_tmp_age_s:
                    tmp.unlink()
            except OSError as e:
                log.debug("cleanup: %s: %s", tmp, e)

    # Orphan sentinel files: sentinel_key not referencing any PENDING task
    valid_keys = {
        sentinel_key(t.directory)
        for t in state.tasks.values()
        if t.status == TaskStatus.PENDING
    }
    for filename, path in list(iter_sentinels(batch_id)):
        if filename not in valid_keys:
            try:
                path.unlink()
                log.info("cleanup: removed orphan sentinel %s", filename)
            except OSError as e:
                log.debug("cleanup: orphan %s: %s", filename, e)


def recover_sentinels(state: BatchState, pbs) -> bool:
    """Walk surviving sentinels and attach job IDs from PBS.

    Returns True if any task state was changed (and save_state
    should be called). Returns False if nothing changed OR if PBS
    query failed — caller (`scheduler`) sets `_recovery_pending`
    in the failure case.

    Algorithm (see plan §_sentinels.py):
        1. Build reverse index key → Task from state.tasks
        2. Query PBS for current user jobs
        3. For each sentinel:
           - task missing or not PENDING → unlink
           - malformed content → unlink
           - filename hash != content hash → anti-forgery fail, unlink
           - body is "PENDING" (qsub never committed) → unlink
           - body has real job_id, PBS confirms → attach as SUBMITTED
        4. Orphan PBS job scan: any active job with name starting
           `pa_<batch_id[:6]>` whose job_id isn't referenced → WARN
           (user must qdel manually; no auto-qdel policy).
    """
    from pbs_auto.state import save_state

    changed = False

    try:
        pbs_jobs = pbs.query_user_jobs(force=True)
    except (RuntimeError, OSError) as e:
        log.warning("recover_sentinels: PBS query failed: %s", e)
        return False

    rev = {sentinel_key(t.directory): t for t in state.tasks.values()}

    for filename, path in list(iter_sentinels(state.batch_id)):
        task = rev.get(filename)
        if task is None or task.status != TaskStatus.PENDING:
            # Stale — task gone or already progressed past PENDING
            remove_sentinel(path)
            continue

        parsed = read_sentinel(path)
        if parsed is None:
            log.warning("recover_sentinels: malformed %s, discarding", path)
            remove_sentinel(path)
            continue

        # Anti-forgery: filename hash must match content directory hash
        if sentinel_key(parsed.directory) != filename:
            log.warning(
                "recover_sentinels: hash mismatch on %s (filename vs body)",
                path,
            )
            remove_sentinel(path)
            continue

        if parsed.job_id == "PENDING":
            # qsub never returned — safe to re-queue for submission
            remove_sentinel(path)
            continue

        # Look up PBS job by id
        short_id = parsed.job_id.split(".")[0]
        pbs_job = pbs_jobs.get(short_id)
        if pbs_job is not None and pbs_job.state in {"Q", "R", "H", "W"}:
            task.job_id = parsed.job_id
            task.status = TaskStatus.SUBMITTED
            task.submit_time = datetime.now().isoformat()
            task.error_message = "Recovered from crash mid-submit"
            changed = True
            log.info(
                "recover_sentinels: attached job_id=%s to %s",
                parsed.job_id,
                task.directory,
            )

        remove_sentinel(path)

    # Orphan PBS job scan (WARN only, user decides)
    expected_prefix = f"pa_{state.batch_id[:6]}"
    referenced_ids = {
        t.job_id.split(".")[0]
        for t in state.tasks.values()
        if t.job_id
    }
    for job_id_short, job in pbs_jobs.items():
        if not job.name.startswith(expected_prefix):
            continue
        if job_id_short in referenced_ids:
            continue
        log.warning(
            "orphan PBS job detected: job_id=%s name=%s state=%s "
            "(no sentinel or task reference — consider manual qdel)",
            job.job_id,
            job.name,
            job.state,
        )

    if changed:
        state.rebuild_indexes()
        try:
            save_state(state, write_summary=True)
        except OSError as e:
            log.error("recover_sentinels: save_state failed: %s", e)
            return False

    try:
        pbs.invalidate_cache()
    except Exception:
        pass

    return changed


__all__ = [
    "sentinel_key",
    "sentinel_path",
    "create_sentinel",
    "update_sentinel_job_id",
    "remove_sentinel",
    "read_sentinel",
    "iter_sentinels",
    "cleanup_stale_artifacts",
    "recover_sentinels",
    "SentinelContent",
]
