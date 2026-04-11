"""Daemon process management: double-fork + lockf + PID files.

Provides:
    - `daemonize(batch_id)`: double-fork + setsid + umask(0o077) +
      closerange + log fd redirection. Only the grandchild returns.
    - `_verify_lockf_works(batch_dir)`: startup self-test that bails
      if `rpc.lockd` is unavailable (NFSv3 without lockd is unsafe).
    - `acquire_lock(batch_id)`: `fcntl.lockf` exclusive lock on the
      batch-specific lock file. The returned fd MUST be kept alive
      for the daemon's lifetime (kernel releases it on process death).
    - `write_pid_file(batch_id)`: atomic PID file write with boot
      timestamp for stale-detection.
    - `is_daemon_alive(batch_id)`: PID file + `/proc/<pid>/stat` +
      cmdline check.
    - `stop_daemon(batch_id, timeout)`: SIGTERM + poll until dead.
"""

from __future__ import annotations

import errno
import fcntl
import logging
import os
import resource
import signal
import sys
import time
from datetime import datetime
from pathlib import Path

from pbs_auto.batch_store import (
    FILE_MODE,
    ensure_batch_dir,
    get_batch_dir,
    get_lock_path,
    get_log_path,
    get_pid_path,
    maybe_rotate_log_on_startup,
)


log = logging.getLogger(__name__)


DEFAULT_STOP_TIMEOUT = 60.0


# ---------------------------------------------------------------------------
# /proc helpers
# ---------------------------------------------------------------------------


def _read_proc_starttime(pid: int) -> int | None:
    """Return the process's btime_ticks field from /proc/<pid>/stat.

    The comm field (field 2) can contain arbitrary characters including
    whitespace and parentheses, so we skip past the last ')' before
    splitting the remaining fields. After the comm field we want the
    22nd field overall (0-indexed 21), which corresponds to index 19
    in the post-comm split.
    """
    try:
        with open(f"/proc/{pid}/stat") as f:
            raw = f.read()
    except OSError:
        return None

    idx = raw.rfind(")")
    if idx < 0:
        return None
    # Fields after the comm field, space-separated. The 22nd overall
    # field is index 21; the 3rd field (state) is index 2; after comm
    # we have fields 3..N, so the 22nd overall is at index (22 - 3) = 19.
    tail_parts = raw[idx + 1 :].split()
    if len(tail_parts) < 20:
        return None
    try:
        return int(tail_parts[19])
    except ValueError:
        return None


def _read_proc_cmdline(pid: int) -> str:
    """Return the full cmdline of `pid`, or empty string on failure."""
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            raw = f.read()
    except OSError:
        return ""
    return raw.replace(b"\x00", b" ").decode(errors="replace")


# ---------------------------------------------------------------------------
# Lock self-test + acquire
# ---------------------------------------------------------------------------


def _verify_lockf_works(batch_dir: Path) -> None:
    """Smoke-test fcntl.lockf by acquiring + releasing on a scratch file.

    Raises `RuntimeError` on any failure — NFSv3 without `rpc.lockd`
    will error here and the daemon must refuse to start.
    """
    scratch = batch_dir / ".locktest"
    try:
        fd = os.open(
            str(scratch),
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            FILE_MODE,
        )
    except OSError as e:
        # A leftover .locktest from a previous failed run: attempt cleanup.
        if e.errno == errno.EEXIST:
            try:
                scratch.unlink()
            except OSError:
                pass
            try:
                fd = os.open(
                    str(scratch),
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    FILE_MODE,
                )
            except OSError as e2:
                raise RuntimeError(
                    f"_verify_lockf_works: cannot create {scratch}: {e2}"
                ) from e2
        else:
            raise RuntimeError(
                f"_verify_lockf_works: cannot create {scratch}: {e}"
            ) from e

    try:
        try:
            fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.lockf(fd, fcntl.LOCK_UN)
        except OSError as e:
            raise RuntimeError(
                "fcntl.lockf is not available on this filesystem — "
                "NFSv3 requires rpc.lockd / nfslock service to be "
                f"running. Original error: {e}"
            ) from e
    finally:
        try:
            os.close(fd)
        except OSError:
            pass
        try:
            scratch.unlink()
        except OSError:
            pass


def acquire_lock(batch_id: str) -> int:
    """Acquire an exclusive advisory lock on the batch's daemon.lock.

    Returns the open file descriptor. The caller MUST keep this fd
    open for the daemon's lifetime — the kernel releases the lock
    automatically when the process dies. If another process already
    holds the lock we raise `RuntimeError`.
    """
    lock_path = get_lock_path(batch_id)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(
        str(lock_path),
        os.O_WRONLY | os.O_CREAT,
        FILE_MODE,
    )
    try:
        fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as e:
        try:
            os.close(fd)
        except OSError:
            pass
        raise RuntimeError(
            f"Another pbs-auto daemon owns batch {batch_id}: {e}"
        ) from e
    return fd


# ---------------------------------------------------------------------------
# PID file
# ---------------------------------------------------------------------------


def write_pid_file(batch_id: str) -> None:
    """Write the daemon PID file atomically.

    Format: "<pid> <btime_ticks> <iso_timestamp>\\n". The btime
    field lets `is_daemon_alive` disambiguate from a PID that the
    kernel has recycled for a different process.
    """
    pid = os.getpid()
    btime = _read_proc_starttime(pid)
    iso = datetime.now().isoformat()
    content = f"{pid} {btime if btime is not None else '?'} {iso}\n"

    pid_path = get_pid_path(batch_id)
    pid_path.parent.mkdir(parents=True, exist_ok=True)
    # Clobber any stale file (caller is already holding the batch lock).
    try:
        pid_path.unlink()
    except FileNotFoundError:
        pass
    fd = os.open(
        str(pid_path),
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        FILE_MODE,
    )
    try:
        os.write(fd, content.encode())
    finally:
        os.close(fd)


def read_pid_file(batch_id: str) -> tuple[int, int | None, str] | None:
    """Return (pid, btime, iso) from daemon.pid, or None if missing."""
    pid_path = get_pid_path(batch_id)
    try:
        raw = pid_path.read_text().strip()
    except OSError:
        return None
    parts = raw.split(None, 2)
    if len(parts) < 1:
        return None
    try:
        pid = int(parts[0])
    except ValueError:
        return None
    btime: int | None
    try:
        btime = int(parts[1]) if len(parts) > 1 and parts[1] != "?" else None
    except ValueError:
        btime = None
    iso = parts[2] if len(parts) > 2 else ""
    return pid, btime, iso


def is_daemon_alive(batch_id: str) -> bool:
    """Best-effort check: PID file exists + process alive + cmdline matches."""
    info = read_pid_file(batch_id)
    if info is None:
        return False
    pid, btime, _ = info
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    current_btime = _read_proc_starttime(pid)
    if btime is not None and current_btime is not None and btime != current_btime:
        return False
    cmdline = _read_proc_cmdline(pid)
    if "pbs-auto" not in cmdline and "pbs_auto" not in cmdline:
        return False
    return True


# ---------------------------------------------------------------------------
# Stop
# ---------------------------------------------------------------------------


def stop_daemon(batch_id: str, timeout: float = DEFAULT_STOP_TIMEOUT) -> bool:
    """Send SIGTERM to the daemon and wait for it to exit.

    Returns True on clean stop, False on timeout or error.
    """
    info = read_pid_file(batch_id)
    if info is None:
        log.warning("stop_daemon: no PID file for %s", batch_id)
        return False

    pid, btime, _ = info
    current_btime = _read_proc_starttime(pid)
    if btime is not None and current_btime != btime:
        log.warning(
            "stop_daemon: PID %s boot time mismatch (stale PID file)", pid
        )
        # Clean up the stale PID file.
        try:
            get_pid_path(batch_id).unlink()
        except OSError:
            pass
        return False

    cmdline = _read_proc_cmdline(pid)
    if "pbs-auto" not in cmdline and "pbs_auto" not in cmdline:
        log.warning("stop_daemon: PID %s cmdline mismatch: %r", pid, cmdline)
        return False

    try:
        os.kill(pid, signal.SIGTERM)
    except OSError as e:
        log.warning("stop_daemon: kill %s failed: %s", pid, e)
        return False

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except OSError:
            return True
        time.sleep(0.5)

    log.warning("stop_daemon: PID %s did not exit within %.0fs", pid, timeout)
    return False


# ---------------------------------------------------------------------------
# Daemonize
# ---------------------------------------------------------------------------


def _max_fd() -> int:
    try:
        soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
        if 0 < soft < 1 << 20:
            return soft
    except (OSError, ValueError):
        pass
    return 1024


def daemonize(batch_id: str) -> int:
    """Double-fork into the background. Only the grandchild returns.

    Returns the log file descriptor (caller keeps this for log rotation).
    Must be called BEFORE `acquire_lock` + `write_pid_file` so the
    grandchild is the one holding them.
    """
    ensure_batch_dir(batch_id)

    # Flush stdio to avoid duplicated output after fork.
    try:
        sys.stdout.flush()
        sys.stderr.flush()
    except Exception:
        pass

    # First fork
    pid = os.fork()
    if pid > 0:
        os._exit(0)

    os.setsid()

    # Second fork — prevents reacquiring a controlling terminal.
    pid = os.fork()
    if pid > 0:
        os._exit(0)

    # Grandchild from here on.
    os.chdir("/")
    os.umask(0o077)

    # Close inherited fds (skip 0/1/2 — we re-dup them below).
    max_fd = _max_fd()
    try:
        os.closerange(3, max_fd)
    except OSError:
        pass

    maybe_rotate_log_on_startup(batch_id)

    log_path = get_log_path(batch_id)
    log_fd = os.open(
        str(log_path),
        os.O_WRONLY | os.O_CREAT | os.O_APPEND,
        FILE_MODE,
    )

    # stdin ← /dev/null; stdout/stderr ← log
    null_fd = os.open("/dev/null", os.O_RDONLY)
    os.dup2(null_fd, 0)
    os.close(null_fd)
    os.dup2(log_fd, 1)
    os.dup2(log_fd, 2)

    return log_fd


__all__ = [
    "daemonize",
    "_verify_lockf_works",
    "acquire_lock",
    "write_pid_file",
    "read_pid_file",
    "is_daemon_alive",
    "stop_daemon",
    "_read_proc_starttime",
    "_read_proc_cmdline",
    "DEFAULT_STOP_TIMEOUT",
]
