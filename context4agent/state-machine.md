# 任务状态机

## 状态转换图

```
                                ┌─────────────────────────────────────┐
                                │                                     │
scan_directory()                │  ┌────────────┐                     │
      │                         │  │  SKIPPED    │ ← 无script/解析失败│
      │                         │  └────────────┘                     │
      ▼                         │                                     │
┌──────────┐   qsub 成功   ┌───────────┐   60s后qstat    ┌─────────┐│
│ PENDING  │ ─────────────→│ SUBMITTED │ ───────────────→│ QUEUED  ││
└──────────┘               └───────────┘                 └─────────┘│
      │                         │                             │      │
      │ qsub 失败               │ 作业消失                     │ qstat│
      ▼                         │ (未见运行)                   │ → R  │
┌──────────┐                    │                             ▼      │
│ FAILED   │                    │                       ┌─────────┐ │
└──────────┘                    │                       │ RUNNING │ │
                                ▼                       └─────────┘ │
                          ┌──────────┐                      │       │
                          │ WARNING  │ ← 运行<30s           │       │
                          └──────────┘                      │ 作业  │
                                ▲                           │ 消失  │
                                │                           ▼       │
                                │ 运行<30s            ┌──────────┐  │
                                └─────────────────────│ 判定逻辑 │  │
                                                      └──────────┘  │
                                                           │        │
                                                           │ 运行≥30s
                                                           ▼        │
                                                     ┌──────────┐   │
                                                     │COMPLETED │   │
                                                     └──────────┘   │
                                                                    │
                                └────────────────────────────────────┘
```

## 终态 (Terminal States)

| 状态 | 含义 | 不可逆 |
|------|------|--------|
| COMPLETED | 正常完成 | 是 |
| WARNING | 异常完成（需要人工检查） | 是 |
| FAILED | 提交失败 | 是 |
| SKIPPED | 跳过（脚本问题） | 是 |

## 活跃态 (Active States)

| 状态 | 含义 | 下一步 |
|------|------|--------|
| PENDING | 等待提交 | 资源足够时提交 |
| SUBMITTED | 已提交，等待确认 | 60s 后查询 PBS |
| QUEUED | PBS 队列中 | 轮询直到 R 或消失 |
| RUNNING | 正在运行 | 轮询直到消失 |

## 状态转换触发条件

| 转换 | 条件 | 代码位置 |
|------|------|----------|
| PENDING → SUBMITTED | `qsub` 返回 job_id | `scheduler.py:_submit_task()` |
| PENDING → FAILED | `qsub` 不可重试错误（脚本不存在、无效队列等） | `scheduler.py:_submit_task()` |
| PENDING → PENDING | `qsub` 可重试错误（资源超限等），下一轮重试 | `scheduler.py:_submit_task()` |
| PENDING → SKIPPED | 扫描时发现问题 | `scanner.py:scan_directory()` |
| PENDING → SKIPPED | 队列合规检查失败且用户选择跳过 | `cli.py:submit()` |
| SUBMITTED → RUNNING | qstat 显示 state="R" | `scheduler.py:_poll_status()` |
| SUBMITTED → QUEUED | qstat 显示 state="Q" | `scheduler.py:_poll_status()` |
| SUBMITTED → WARNING | 作业从 PBS 中消失 | `scheduler.py:_handle_job_disappeared()` |
| QUEUED → RUNNING | qstat 显示 state="R" | `scheduler.py:_poll_status()` |
| RUNNING → COMPLETED | 作业消失且运行 >= 30s | `scheduler.py:_handle_job_disappeared()` |
| RUNNING → WARNING | 作业消失且运行 < 30s | `scheduler.py:_handle_job_disappeared()` |

## 恢复时的状态处理 (reconcile_tasks)

| 恢复前状态 | 恢复后状态 | 原因 |
|-----------|-----------|------|
| PENDING | PENDING | 继续等待提交 |
| SUBMITTED | → PENDING | 无法确认 qsub 是否成功，重新提交 |
| QUEUED | QUEUED | scheduler 会重新查询 PBS 确认 |
| RUNNING | RUNNING | scheduler 会重新查询 PBS 确认 |
| COMPLETED | COMPLETED | 已完成，不变 |
| WARNING | WARNING | 已标记，不变 |
| FAILED | FAILED | 已失败，不变 |
| SKIPPED | SKIPPED | 仍然跳过 |

## 可重试错误 (Retryable Errors)

`_submit_task()` 通过 `_is_retryable_error()` 判断 qsub 错误是否为临时性资源冲突。匹配以下关键字（不区分大小写）的错误保持 `PENDING`，下一轮重试：

- `would exceed` — PBS 资源限额超出（多实例并发提交时常见）
- `resource busy` — PBS 资源繁忙
- `try again` — 临时错误
- `temporarily unavailable` — 临时不可用

不匹配的错误（如无效队列、权限不足）标记为 `FAILED`。`FileNotFoundError`（脚本不存在）始终为永久错误。

当可重试错误发生时，`_submit_pending()` 会 `break` 停止本轮提交（资源已满，继续提交无意义）。

## early_exit_threshold

`config.early_exit_threshold` (默认 30 秒) 是判断作业是否异常退出的时间阈值。

判定逻辑在 `_handle_job_disappeared()` 中：
1. 如果 `task.status == SUBMITTED` 且作业消失 → WARNING（从未见到运行态）
2. 如果有 `start_time` 且 `now - start_time < threshold` → WARNING
3. 否则 → COMPLETED

## Crash Recovery Windows (`_submit_task`)

`_MutationContext` 配合 content-authoritative 哨兵文件覆盖 qsub 全生命周期的崩溃恢复。下表枚举 5 个崩溃点及其恢复行为：

| # | 崩溃位置 | 磁盘 state | sentinel 内容 | PBS 作业? | 恢复 |
|---|---|---|---|---|---|
| 1 | `create_sentinel` 前 | PENDING | - | no | 重跑 |
| 2 | sentinel 创建后 / qsub 前 | PENDING | `PENDING` | no | recover 读到 PENDING → unlink → 保持 PENDING → 下轮重新提交 |
| 3 | qsub 后 / `update_sentinel_job_id` 前 | PENDING | `PENDING` | **yes** | **可接受的极小窗口**（warm FS 亚毫秒，NFS 10–100 ms）：unlink → 下轮重 qsub → duplicate job。由 `recover_sentinels` 步骤 5 的 **orphan PBS job WARN 扫描**捕获（job name `pa_<batch_id[:6]>` 未被引用 → 用户手动 qdel） |
| 3' | `update_sentinel_job_id` 后 / save_state 前 | PENDING（陈旧） | `<real_job_id>` | yes | **3 → 3' 原子过渡**：`update_sentinel_job_id` 关闭窗口 3。recover 读 job_id → `pbs_jobs.get(id)` → state in {Q,R,H,W} → attach SUBMITTED |
| 4 | save_state 后 / `remove_sentinel` 前 | SUBMITTED + job_id | `<real_job_id>` | yes | recover 看到 task.status ≠ PENDING → 直接 unlink sentinel |
| 5 | `remove_sentinel` 后 / `invalidate_cache` 前 | SUBMITTED + job_id | - | yes | 下轮 poll 陈旧缓存 ≤5s，无害 |

### 反伪造机制

`recover_sentinels` 对每个 sentinel 做 filename-vs-content hash 交叉校验：`sha256(content_directory)[:24] == filename`。不匹配即拒绝并 unlink，防止伪造 sentinel 注入 job_id。

### 并发 submit race

`fcntl.lockf(<batch>/daemon.lock, LOCK_EX | LOCK_NB)` 是唯一权威互斥门：
- A grandchild：`_verify_lockf_works` → `acquire_lock` 成功 → PID (O_EXCL) → recover + 主循环
- B grandchild：`_verify_lockf_works` → `acquire_lock` 失败 → log + exit 1
- A SIGKILL → kernel 自动释放 lockf → 下次 submit 可进
- `recover_sentinels` 必须在 grandchild 持锁后执行（防止多实例同时恢复）

### NFS 要求

`fcntl.lockf` 依赖 NFSv3 的 `rpc.lockd` / `nfslock` 服务（CentOS 7 默认启用）。`_verify_lockf_works` 在 daemon 启动时做 O_EXCL 创建 → lockf → 释放的自检，失败明确报错退出。

### Shutdown contract

```
SIGTERM → flag-only handler 设 _shutdown = True → return
主循环完成当前 iter（_submit_pending 的 inner shutdown check 立即 yield）
finally:
  try: store.force_flush()                    # 最终快照 eager
  try: os.close(lock_fd)                      # kernel 自动释放 lockf
  try: get_pid_path(batch_id).unlink()
```

每一步 try/except log-and-continue，任一失败不阻断下一步。`_MutationContext.__exit__` 绝不抛异常，所有 FS 操作各自 try/except。`stop_daemon` timeout 默认 60s。
