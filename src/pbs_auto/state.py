"""JSON serialization + migration + reconcile + identifier resolution.

This module owns batch state loading/saving, schema migration from
legacy formats, the reconcile logic for resume support, and
name/batch_id resolution. Disk layout (paths, permissions, sentinels,
rotation, mutation) lives in `batch_store/`.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import tempfile
from datetime import datetime
from pathlib import Path

from pbs_auto.batch_store import (
    FILE_MODE,
    ensure_batch_dir,
    get_batch_dir,
    get_log_path,
    get_pid_path,
    get_state_path,
    get_summary_path,
    migrate_layout,
)
from pbs_auto.config import DEFAULT_STATE_DIR
from pbs_auto.models import (
    ACTIVE_STATUSES,
    SCHEMA_VERSION,
    BatchState,
    Task,
    TaskStatus,
)


log = logging.getLogger(__name__)


# Maximum acceptable save_state latency before orjson is recommended.
# Documented as a constant so tests can assert against it.
MAX_EAGER_SAVE_LATENCY_MS = 500


# Identifier charset for --name and resolve_batch_identifier.
# Hex batch_ids (16 chars) and user-chosen names must satisfy this.
_IDENT_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")

# Reserved names that conflict with on-disk artifacts.
_RESERVED_NAMES = frozenset(
    {
        ".",
        "..",
        "daemon.pid",
        "daemon.lock",
        "daemon.log",
        "state.json",
        "summary.json",
        "submitting",
        ".orig",
        ".tmp",
    }
)


# ---------------------------------------------------------------------------
# Batch ID / identifier helpers
# ---------------------------------------------------------------------------


def generate_batch_id(
    roots: str | list[str], name: str | None = None
) -> str:
    """Generate a deterministic batch ID.

    - If `name` is provided, use `sha256(name)[:16]`.
    - Otherwise hash the sorted resolved root directories.

    Legacy callers that pass a single string for `roots` are supported
    for backward compatibility.
    """
    if name:
        return hashlib.sha256(name.encode()).hexdigest()[:16]

    if isinstance(roots, str):
        roots = [roots]
    if not roots:
        raise ValueError("generate_batch_id requires at least one root")

    normalized = sorted(str(Path(r).resolve()) for r in roots)
    return hashlib.sha256("\n".join(normalized).encode()).hexdigest()[:16]


def validate_identifier(identifier: str) -> None:
    """Raise ValueError if identifier is unsafe.

    Rejects path separators, NUL bytes, leading dashes, reserved
    filenames, and anything outside the charset.
    """
    if not isinstance(identifier, str):
        raise ValueError("identifier must be a string")
    if identifier in _RESERVED_NAMES:
        raise ValueError(f"'{identifier}' is a reserved name")
    if identifier.startswith("-"):
        raise ValueError(
            f"identifier may not start with '-': {identifier!r}"
        )
    if "\x00" in identifier or "/" in identifier or "\\" in identifier:
        raise ValueError(
            f"identifier contains forbidden characters: {identifier!r}"
        )
    if not _IDENT_RE.match(identifier):
        raise ValueError(
            f"identifier must match {_IDENT_RE.pattern}: {identifier!r}"
        )


def resolve_batch_identifier(identifier: str) -> str:
    """Resolve a user-provided identifier to a concrete batch_id.

    Tries in order:
        1. Exact match as batch_id (directory exists)
        2. Prefix match (unique)
        3. Name match — scan state.json / summary.json for `name`

    Raises ValueError with a friendly message on failure.
    """
    validate_identifier(identifier)

    if not DEFAULT_STATE_DIR.exists():
        raise ValueError(f"No batches found under {DEFAULT_STATE_DIR}")

    # 1. Exact batch_id
    direct = get_batch_dir(identifier)
    if direct.is_dir() and (direct / "state.json").exists():
        return identifier

    # Gather candidate batch dirs (new layout only).
    candidates: list[str] = []
    for child in DEFAULT_STATE_DIR.iterdir():
        if child.is_dir() and (child / "state.json").exists():
            candidates.append(child.name)

    # 2. Prefix match
    prefix_hits = [c for c in candidates if c.startswith(identifier)]
    if len(prefix_hits) == 1:
        return prefix_hits[0]
    if len(prefix_hits) > 1:
        raise ValueError(
            f"Ambiguous prefix '{identifier}' matches: "
            f"{', '.join(sorted(prefix_hits))}"
        )

    # 3. Name match — scan summary.json first, fall back to state.json.
    for candidate in candidates:
        for fname in ("summary.json", "state.json"):
            path = get_batch_dir(candidate) / fname
            if not path.exists():
                continue
            try:
                with open(path) as f:
                    data = json.load(f)
            except (OSError, json.JSONDecodeError):
                continue
            if data.get("name") == identifier:
                return candidate
            break  # summary present — don't check state

    raise ValueError(f"No batch found matching '{identifier}'")


# ---------------------------------------------------------------------------
# Schema migration
# ---------------------------------------------------------------------------


def _migrate_on_load(raw: dict) -> dict:
    """Migrate a raw JSON dict to the current schema (version=2).

    Idempotent. Version=1 dicts are recognized by the presence of
    `root_directory` (singular) and/or the absence of a `version` field.
    """
    if raw.get("version") == SCHEMA_VERSION:
        return raw

    # version=1 (implicit) → version=2
    data = dict(raw)
    if "root_directories" not in data:
        legacy_root = data.pop("root_directory", None)
        data["root_directories"] = [legacy_root] if legacy_root else []

    # Rekey tasks dict from task.name to task.directory.
    tasks = data.get("tasks", {})
    rekeyed: dict = {}
    for key, tdata in tasks.items():
        directory = tdata.get("directory") or key
        tdata = dict(tdata)
        tdata.setdefault("queue", None)
        tdata.setdefault("nodes", 0)
        rekeyed[directory] = tdata
    data["tasks"] = rekeyed

    data.setdefault("name", None)
    data["version"] = SCHEMA_VERSION
    return data


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def _atomic_write(path: Path, content: str) -> None:
    """Write `content` to `path` atomically with fsync + replace.

    Creates a tempfile in the same directory, writes + fsyncs, then
    renames. Leaves behind tempfiles on failure for cleanup_stale
    to pick up.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
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


def _build_summary(state: BatchState) -> dict:
    """Build a lightweight summary for summary.json.

    Contains per-status counts and basic metadata — enough for
    `list-batches` and `status` snapshot views without loading the
    full task dict.
    """
    counts: dict[str, int] = {}
    for task in state.tasks.values():
        key = task.status.value
        counts[key] = counts.get(key, 0) + 1

    return {
        "batch_id": state.batch_id,
        "name": state.name,
        "version": state.version,
        "root_directories": list(state.root_directories),
        "server_profile": state.server_profile,
        "created_at": state.created_at,
        "updated_at": state.updated_at,
        "total_tasks": len(state.tasks),
        "status_counts": counts,
    }


def save_state(state: BatchState, write_summary: bool = True) -> None:
    """Persist batch state to disk atomically.

    - `state.json` is the authoritative source of truth.
    - `summary.json` is a lightweight (≤1 flush stale) cache used by
      `list-batches` and `status`. Written BEFORE `state.json` when
      `write_summary=True`; a crash between the two writes leaves
      summary temporarily ahead, which is the documented contract.
    - `write_summary=False` is used for eager saves during rapid
      qsub bursts; the next debounced flush will catch up summary.
    """
    state.updated_at = datetime.now().isoformat()
    ensure_batch_dir(state.batch_id)

    content = json.dumps(
        state.to_dict(),
        separators=(",", ":"),
        ensure_ascii=False,
    )

    if write_summary:
        summary_content = json.dumps(
            _build_summary(state),
            separators=(",", ":"),
            ensure_ascii=False,
        )
        try:
            _atomic_write(get_summary_path(state.batch_id), summary_content)
        except OSError as e:
            log.warning("Failed to write summary.json: %s", e)

    _atomic_write(get_state_path(state.batch_id), content)


def load_state(batch_id: str) -> BatchState | None:
    """Load batch state from disk, migrating legacy layouts/schemas.

    Returns None if no state exists for this batch_id.
    """
    # First attempt layout migration (legacy <batch_id>.json → dir).
    try:
        migrate_layout(batch_id)
    except RuntimeError as e:
        log.error("Layout migration failed: %s", e)
        return None

    state_path = get_state_path(batch_id)
    if not state_path.exists():
        return None

    with open(state_path) as f:
        raw = json.load(f)

    migrated = _migrate_on_load(raw)
    return BatchState.from_dict(migrated)


def reconcile_tasks(
    saved: BatchState, scanned: list[Task]
) -> BatchState:
    """Merge scanned tasks with saved state for resume support.

    - Tasks are matched by absolute directory path (not name).
    - COMPLETED/WARNING/FAILED/SKIPPED: keep saved state as-is.
    - RUNNING/QUEUED: keep saved state (scheduler re-checks PBS).
    - SUBMITTED **with job_id**: KEEP (poll will re-verify in PBS).
      This is the bug fix — the old behavior reset to PENDING
      unconditionally, which caused duplicate submissions after
      daemon restart.
    - SUBMITTED **without job_id**: reset to PENDING (defensive;
      shouldn't happen with event-driven save + sentinels but
      serves as a belt-and-suspenders fallback).
    - PENDING: keep as PENDING.
    - New tasks not in saved state: add as PENDING.

    After reconcile, `saved.rebuild_indexes()` is called so
    `_pending_set` / `_active_set` reflect the merged state.
    """
    for task in scanned:
        key = task.directory
        if key in saved.tasks:
            existing = saved.tasks[key]
            if existing.status == TaskStatus.SUBMITTED and not existing.job_id:
                # Defensive reset — no job_id means qsub never
                # committed a record we can trust.
                existing.status = TaskStatus.PENDING
                existing.submit_time = None
            # Refresh mutable fields from the rescan in case the
            # script was edited between runs.
            existing.cores = task.cores
            existing.directory = task.directory
            existing.nodes = task.nodes
            existing.queue = task.queue
        else:
            saved.tasks[key] = task

    saved.rebuild_indexes()
    return saved


# ---------------------------------------------------------------------------
# Listing
# ---------------------------------------------------------------------------


def list_batches() -> list[dict]:
    """List all saved batches by reading summary.json.

    Falls back to state.json if summary.json is missing (e.g. the
    daemon died before ever flushing a summary).
    """
    if not DEFAULT_STATE_DIR.exists():
        return []

    batches = []
    for child in sorted(DEFAULT_STATE_DIR.iterdir()):
        if not child.is_dir():
            continue

        summary_path = child / "summary.json"
        state_path = child / "state.json"

        data = None
        if summary_path.exists():
            try:
                with open(summary_path) as f:
                    data = json.load(f)
            except (OSError, json.JSONDecodeError):
                data = None
        if data is None and state_path.exists():
            try:
                with open(state_path) as f:
                    raw = json.load(f)
                raw = _migrate_on_load(raw)
                data = {
                    "batch_id": raw.get("batch_id", child.name),
                    "name": raw.get("name"),
                    "version": raw.get("version", SCHEMA_VERSION),
                    "root_directories": raw.get("root_directories", []),
                    "server_profile": raw.get("server_profile", "?"),
                    "created_at": raw.get("created_at", "?"),
                    "updated_at": raw.get("updated_at", "?"),
                    "total_tasks": len(raw.get("tasks", {})),
                    "status_counts": _count_statuses(raw.get("tasks", {})),
                }
            except (OSError, json.JSONDecodeError):
                continue
        if data is None:
            continue

        # Backward-compat: UI code expects root_directory singular in
        # a couple of spots; expose the first root for convenience.
        roots = data.get("root_directories", [])
        data.setdefault("root_directory", roots[0] if roots else "?")
        batches.append(data)

    return batches


def _count_statuses(tasks: dict) -> dict[str, int]:
    counts: dict[str, int] = {}
    for tdata in tasks.values():
        s = tdata.get("status", "unknown")
        counts[s] = counts.get(s, 0) + 1
    return counts


__all__ = [
    "MAX_EAGER_SAVE_LATENCY_MS",
    "generate_batch_id",
    "validate_identifier",
    "resolve_batch_identifier",
    "save_state",
    "load_state",
    "reconcile_tasks",
    "list_batches",
    "get_state_path",  # re-exported from batch_store for backward compat
    "get_pid_path",
    "get_log_path",
]
