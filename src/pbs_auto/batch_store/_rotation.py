"""Daemon log rotation (size + age based, daemon-only trigger).

Two entry points:
    - `maybe_rotate_log_on_startup(batch_id)`: called before the log
      fd is opened, so it's safe to rename + gzip the old file.
    - `maybe_rotate_log_in_loop(batch_id, fd)`: called periodically
      from the main loop; if the current fd's file exceeds the size
      limit we rename, re-open a fresh log, and `dup2` it to stdout/
      stderr (fds 1 and 2). Returns the new fd.

Rotation is ONLY triggered from within the daemon process. `status`
and `list-batches` must NOT trigger it — external readers should
never mutate daemon-owned files.
"""

from __future__ import annotations

import gzip
import logging
import os
import shutil
import time
from datetime import datetime
from pathlib import Path

from pbs_auto.batch_store._paths import FILE_MODE, get_log_path


log = logging.getLogger(__name__)


MAX_LOG_SIZE = 50 * 1024 * 1024  # 50 MB
MAX_LOG_AGE_DAYS = 365


def _archive_name(log_path: Path, year: int) -> Path:
    """Pick a non-colliding archive name `daemon.log.<YYYY>-<N>.gz`."""
    parent = log_path.parent
    for n in range(10000):
        candidate = parent / f"{log_path.name}.{year}-{n}.gz"
        if not candidate.exists():
            return candidate
    raise RuntimeError(
        f"Could not find a free archive name for {log_path} in {parent}"
    )


def _archive_file(src: Path) -> Path | None:
    """Rename + gzip `src` in place. Returns archive path or None."""
    if not src.exists():
        return None
    try:
        stat = src.stat()
    except OSError:
        return None

    year = datetime.fromtimestamp(stat.st_mtime).year
    gz_path = _archive_name(src, year)

    # Rename first so the daemon's next open creates a fresh file.
    staging = src.parent / f"{src.name}.rotating"
    try:
        os.rename(str(src), str(staging))
    except OSError as e:
        log.warning("_archive_file: rename %s failed: %s", src, e)
        return None

    try:
        with open(staging, "rb") as src_f, gzip.open(gz_path, "xb") as dst_f:
            shutil.copyfileobj(src_f, dst_f)
        try:
            os.chmod(gz_path, FILE_MODE)
        except OSError:
            pass
        staging.unlink()
    except OSError as e:
        log.warning("_archive_file: gzip %s failed: %s", gz_path, e)
        # Try to put the file back so nothing is lost.
        try:
            os.rename(str(staging), str(src))
        except OSError:
            pass
        return None

    log.info("Archived %s → %s", src, gz_path)
    return gz_path


def maybe_rotate_log_on_startup(batch_id: str) -> None:
    """Rotate log before daemon opens its fd.

    Rotates if either:
        - size > MAX_LOG_SIZE
        - mtime age > MAX_LOG_AGE_DAYS
    """
    log_path = get_log_path(batch_id)
    if not log_path.exists():
        return

    try:
        stat = log_path.stat()
    except OSError:
        return

    now = time.time()
    age_days = (now - stat.st_mtime) / 86400
    if stat.st_size >= MAX_LOG_SIZE or age_days > MAX_LOG_AGE_DAYS:
        _archive_file(log_path)


def maybe_rotate_log_in_loop(batch_id: str, fd: int) -> int:
    """Check the current log fd's file size and rotate if needed.

    Called from the daemon main loop every ~60 iterations. Returns
    the (possibly new) fd that should be used going forward. If no
    rotation happens, the same `fd` is returned.

    The rotation sequence:
        1. `fstat(fd)` to get current size
        2. If below threshold, return `fd` unchanged
        3. Rename + gzip the log file (creates fresh empty path)
        4. Open a new log fd with `os.open`
        5. `dup2` new fd onto stdout/stderr (fds 1 and 2)
        6. Close the original fd
        7. Return the new fd
    """
    log_path = get_log_path(batch_id)
    try:
        st = os.fstat(fd)
    except OSError as e:
        log.warning("maybe_rotate_log_in_loop: fstat failed: %s", e)
        return fd

    if st.st_size < MAX_LOG_SIZE:
        return fd

    archived = _archive_file(log_path)
    if archived is None:
        # Rotation failed — keep writing to the current fd so we
        # don't lose future output.
        return fd

    # Open a fresh log file for the new fd.
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
    try:
        new_fd = os.open(str(log_path), flags, FILE_MODE)
    except OSError as e:
        log.warning("maybe_rotate_log_in_loop: open new log failed: %s", e)
        return fd

    try:
        os.dup2(new_fd, 1)
        os.dup2(new_fd, 2)
    except OSError as e:
        log.warning("maybe_rotate_log_in_loop: dup2 failed: %s", e)
        try:
            os.close(new_fd)
        except OSError:
            pass
        return fd

    try:
        os.close(fd)
    except OSError:
        pass

    return new_fd


__all__ = [
    "MAX_LOG_SIZE",
    "MAX_LOG_AGE_DAYS",
    "maybe_rotate_log_on_startup",
    "maybe_rotate_log_in_loop",
]
