"""Batch storage: disk layout + sentinels + log rotation + mutation.

This package is the single authoritative owner of:
    - Batch directory layout (paths, permissions, migration)
    - Crash-recovery sentinel files
    - Daemon log rotation (size + age)
    - BatchStore / _MutationContext (single save entry point)

state.py owns JSON load/save + migration + reconcile + identifier
resolution. External consumers should import everything exposed here
via `from pbs_auto.batch_store import <name>`.
"""

from __future__ import annotations

from pbs_auto.batch_store._paths import (
    DIR_MODE,
    FILE_MODE,
    ensure_batch_dir,
    get_batch_dir,
    get_lock_path,
    get_log_path,
    get_pid_path,
    get_sentinel_dir,
    get_state_path,
    get_summary_path,
    migrate_layout,
)
from pbs_auto.batch_store._mutation import BatchStore
from pbs_auto.batch_store._rotation import (
    MAX_LOG_AGE_DAYS,
    MAX_LOG_SIZE,
    maybe_rotate_log_in_loop,
    maybe_rotate_log_on_startup,
)
from pbs_auto.batch_store._sentinels import (
    cleanup_stale_artifacts,
    create_sentinel,
    iter_sentinels,
    recover_sentinels,
    remove_sentinel,
    sentinel_key,
    sentinel_path,
    update_sentinel_job_id,
)


__all__ = [
    # Paths
    "get_batch_dir",
    "get_state_path",
    "get_summary_path",
    "get_pid_path",
    "get_lock_path",
    "get_log_path",
    "get_sentinel_dir",
    "ensure_batch_dir",
    "migrate_layout",
    "DIR_MODE",
    "FILE_MODE",
    # Sentinels (public)
    "create_sentinel",
    "update_sentinel_job_id",
    "remove_sentinel",
    "iter_sentinels",
    "cleanup_stale_artifacts",
    "recover_sentinels",
    # Sentinels (internal but useful for tests)
    "sentinel_key",
    "sentinel_path",
    # Rotation
    "maybe_rotate_log_on_startup",
    "maybe_rotate_log_in_loop",
    "MAX_LOG_SIZE",
    "MAX_LOG_AGE_DAYS",
    # Mutation
    "BatchStore",
]
