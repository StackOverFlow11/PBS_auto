"""Filesystem paths and layout for batch storage.

Owned by batch_store/. state.py owns JSON serialization + migration +
reconcile + identifier resolution; batch_store/ owns disk layout +
sentinels + rotation + mutation lifecycle.
"""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path

from pbs_auto.config import DEFAULT_STATE_DIR


log = logging.getLogger(__name__)


# Directory permissions — 0o700 (owner only).
DIR_MODE = 0o700
# File permissions — 0o600 (owner only).
FILE_MODE = 0o600


def get_batch_dir(batch_id: str) -> Path:
    """Return the per-batch directory path."""
    return DEFAULT_STATE_DIR / batch_id


def get_state_path(batch_id: str) -> Path:
    """Return the authoritative state.json path."""
    return get_batch_dir(batch_id) / "state.json"


def get_summary_path(batch_id: str) -> Path:
    """Return the summary.json (≤1 flush stale cache) path."""
    return get_batch_dir(batch_id) / "summary.json"


def get_pid_path(batch_id: str) -> Path:
    """Return the daemon PID file path."""
    return get_batch_dir(batch_id) / "daemon.pid"


def get_lock_path(batch_id: str) -> Path:
    """Return the daemon fcntl.lockf file path."""
    return get_batch_dir(batch_id) / "daemon.lock"


def get_log_path(batch_id: str) -> Path:
    """Return the daemon log file path."""
    return get_batch_dir(batch_id) / "daemon.log"


def get_sentinel_dir(batch_id: str) -> Path:
    """Return the submitting/ sentinel directory path."""
    return get_batch_dir(batch_id) / "submitting"


def ensure_batch_dir(batch_id: str) -> Path:
    """Create the batch directory tree with strict permissions.

    Audits existing directories and tightens permissions if laxer
    than 0o700. Called at daemon startup and parent bootstrap.
    """
    batch_dir = get_batch_dir(batch_id)
    sentinel_dir = get_sentinel_dir(batch_id)
    for d in (DEFAULT_STATE_DIR, batch_dir, sentinel_dir):
        d.mkdir(parents=True, exist_ok=True)
        try:
            current = d.stat().st_mode & 0o777
            if current != DIR_MODE:
                os.chmod(d, DIR_MODE)
        except OSError as e:
            log.warning("Could not chmod %s to 0o700: %s", d, e)
    return batch_dir


def migrate_layout(batch_id: str) -> None:
    """Migrate legacy flat state files to per-batch directory layout.

    Idempotent — safe to call on every load. Handles 5 branches:
        1. New <batch_id>/state.json exists → nothing to do
        2. Legacy <batch_id>.json is a symlink → refuse (security)
        3. Legacy <batch_id>.json exists + new dir missing → mkdir + move
        4. Legacy <batch_id>.json exists + new dir is empty → move
        5. Both legacy flat file AND new state.json exist → back up
           legacy as <batch_id>.json.orig and warn (keep new as truth)
    """
    legacy_path = DEFAULT_STATE_DIR / f"{batch_id}.json"
    new_state_path = get_state_path(batch_id)
    new_dir = get_batch_dir(batch_id)

    # Branch 1: already migrated
    if new_state_path.exists():
        if legacy_path.exists() and not legacy_path.is_symlink():
            # Branch 5: both exist — backup legacy
            orig = DEFAULT_STATE_DIR / f"{batch_id}.json.orig"
            log.warning(
                "Both %s and %s exist; backing up legacy to %s",
                legacy_path,
                new_state_path,
                orig,
            )
            try:
                shutil.move(str(legacy_path), str(orig))
            except OSError as e:
                log.error("Failed to back up legacy file: %s", e)
        return

    # No legacy file either → nothing to migrate
    if not legacy_path.exists():
        return

    # Branch 2: symlink → refuse
    if legacy_path.is_symlink():
        raise RuntimeError(
            f"Refusing to migrate symlink {legacy_path}; "
            f"resolve manually and retry."
        )

    # Branch 3 or 4: legacy file → move into new layout
    new_dir.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(new_dir, DIR_MODE)
    except OSError:
        pass

    # Check new dir is empty or only contains safe junk
    existing = [p for p in new_dir.iterdir() if p.name != ".DS_Store"]
    if existing:
        # Unexpected content — refuse rather than overwrite
        raise RuntimeError(
            f"Cannot migrate {legacy_path}: {new_dir} is not empty "
            f"(contains {[p.name for p in existing]})"
        )

    os.replace(str(legacy_path), str(new_state_path))
    try:
        os.chmod(new_state_path, FILE_MODE)
    except OSError:
        pass
    log.info("Migrated %s → %s", legacy_path, new_state_path)
