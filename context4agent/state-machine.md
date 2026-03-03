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
| PENDING → FAILED | `qsub` 抛异常 | `scheduler.py:_submit_task()` |
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

## early_exit_threshold

`config.early_exit_threshold` (默认 30 秒) 是判断作业是否异常退出的时间阈值。

判定逻辑在 `_handle_job_disappeared()` 中：
1. 如果 `task.status == SUBMITTED` 且作业消失 → WARNING（从未见到运行态）
2. 如果有 `start_time` 且 `now - start_time < threshold` → WARNING
3. 否则 → COMPLETED
