# PBS Auto-Submit 项目架构

## 项目结构

```
PBS_auto/
├── pyproject.toml                    # 包配置 (setuptools, click/rich/tomli 依赖)
├── CLAUDE.md                         # AI agent 开发规范入口
├── context4agent/                    # AI agent 开发参考文档
│   ├── architecture.md               # 本文件 - 项目架构总览
│   ├── modules.md                    # 各模块详细说明
│   ├── queue-validation.md           # 队列验证模块说明
│   ├── state-machine.md              # 任务状态机 + crash recovery windows
│   └── testing.md                    # 测试策略与用例说明
├── docs/
│   └── usage.md                      # 面向用户的使用文档
├── examples/
│   └── config.toml                   # 示例配置文件
├── requirements/                     # 原始需求文档 (只读参考)
│   ├── requirments.md                # 主需求文档
│   ├── PBS.md                        # PBS 命令参考
│   ├── env.md                        # 服务器环境信息
│   ├── queue_specs.md                # 队列规格参考
│   ├── successfully_submitted.md     # q 命令输出格式样本
│   ├── scripts_template/             # PBS 脚本模板 (server1: 14个, server2: 9个)
│   └── wordir_example/               # 工作目录示例 (10 个任务子目录)
├── src/pbs_auto/                     # 源代码
│   ├── __init__.py                   # 版本号 (__version__)
│   ├── __main__.py                   # python -m pbs_auto 入口
│   ├── cli.py                        # Click CLI 命令定义 (含 run_dry_run)
│   ├── config.py                     # 配置加载与验证
│   ├── models.py                     # 数据模型 (Task, BatchState + 索引)
│   ├── state.py                      # JSON load/save + migration + reconcile + identifier
│   ├── batch_store/                  # 磁盘布局 + 哨兵 + 轮转 + mutation (package)
│   │   ├── __init__.py               # 公开 API facade + __all__
│   │   ├── _paths.py                 # 路径 + migrate_layout
│   │   ├── _sentinels.py             # 哨兵 CRUD + recover
│   │   ├── _rotation.py              # 日志轮转
│   │   └── _mutation.py              # BatchStore + _MutationContext
│   ├── scanner.py                    # 目录扫描 + 脚本解析
│   ├── queue.py                      # 队列验证与自动选择
│   ├── pbs.py                        # PBS 交互 (qsub/qstat/q)
│   ├── scheduler.py                  # 提交引擎 (主循环, 零 Rich)
│   └── daemon.py                     # 双 fork + fcntl.lockf + PID
└── tests/                            # 单元测试 (177 个)
    ├── conftest.py                   # 共享 fixtures
    ├── test_config.py
    ├── test_scanner.py
    ├── test_pbs.py
    ├── test_queue.py
    ├── test_scheduler.py
    ├── test_state.py                 # JSON/migration/reconcile/identifier
    └── test_batch_store.py           # paths/sentinels/mutation/rotation
```

## 模块依赖图

```
cli.py ──────────────────────────────┐
  │                                  │
  ├─→ config.py                      │
  ├─→ scanner.py ─→ models.py        │
  ├─→ queue.py ──→ models.py, config │
  ├─→ state.py ──→ models.py, config, batch_store
  ├─→ pbs.py ───→ models.py, config  │
  ├─→ batch_store/ ─→ models.py, config
  ├─→ daemon.py ──→ batch_store (paths + rotation)
  └─→ scheduler.py                   │
        ├─→ models.py                │
        ├─→ pbs.py                   │
        ├─→ batch_store (BatchStore + recover_sentinels)
        └─→ state.py (save_state via BatchStore._flush_now)
```

- `models.py` 是零依赖基础层
- `config.py` 仅依赖 stdlib + tomli
- `batch_store/` 是「磁盘布局 + 哨兵 + 轮转 + mutation」的单一权威，暴露 facade 给外部
- `state.py` 只负责 JSON load/save + migration + reconcile + identifier 解析
- `scheduler.py` 零 Rich 导入（`run_dry_run` 已迁到 `cli.py`）
- `daemon.py` 负责双 fork + PID + fcntl.lockf

## 核心数据流

```
用户执行 pbs-auto submit dir1 dir2 --name mlip_round1
         │
    Parent process (cli.py:submit):
      load_config → scan_directory × N → validate_queues → load_state + reconcile
      save_state (bootstrap bypass #1) → is_daemon_alive? → daemonize()
         │
    Grandchild (after double-fork):
      setsid + umask(0o077) + chdir("/") + closerange(3, nofile)
      maybe_rotate_log_on_startup → dup2 log_fd → stdin/dev/null
      _verify_lockf_works(batch_dir)               # 自检 rpc.lockd
      acquire_lock(batch_id)                       # fcntl.lockf EX|NB
      write_pid_file(batch_id)                     # O_EXCL + btime
      load_state(batch_id) → rebuild_indexes       # 权威 state
      cleanup_stale_artifacts                      # 废 tempfile + orphan sentinel
      recover_sentinels(state, pbs)                # 按 job_id 反查 (bypass #2)
      pbs.invalidate_cache()
      store = BatchStore(state)                    # 单一 mutation 入口
         │
    Main loop (scheduler.py):
      while not _shutdown:
          _poll_status()                           # 读 _active_set, mutate via store
          _recovery_pending retry?
          _submit_pending()                        # 读 _pending_set, mutate via store (eager)
          store.maybe_debounced_flush()
          _all_done_fast()? → break
          _sleep(poll_interval)
      finally: force_flush → close(lock_fd) → unlink PID
```

## 关键设计决策

### 1. 资源统计以 PBS 为准

`_get_resource_usage()` 每次调用 `qstat` 获取用户所有作业的实时核数，而非仅统计本工具管理的任务。查询失败时保守返回 `(max, max)` 防止误提交。

### 2. 两级 flush 策略

- **eager**：`_submit_task` 成功后立即 flush（捕获 job_id，`write_summary=False` 省 IO）
- **debounced**：`_poll_status` / `_handle_job_disappeared` 置 `_dirty=True`，主循环末尾 `maybe_debounced_flush` 统一落盘（最小间隔 1s）
- **FAILED 提升**：`_MutationContext` 检测到 task.status == FAILED 时强制 eager flush

### 3. 内容权威的 content-authoritative 哨兵

- 哨兵文件名：`submitting/<xx>/<sha256(dir)[:24]>`（256 路分片 + 文件名防伪）
- 内容严格 3 行：`<dir>\n<job_id|PENDING>\n<iso_ts>\n`
- `create_sentinel` 在 qsub 前写入 PENDING；`update_sentinel_job_id` 在 qsub 成功后原子重写 line 2；clean exit 时**无条件**移除
- 恢复：读内容 → 校验文件名 hash 匹配 → 按 job_id 直查 PBS → 命中则 attach 回 SUBMITTED
- 崩溃窗口 3（qsub 后 / update 前）通过 **orphan PBS job WARN 扫描**报告用户手动 qdel

### 4. 原子写入 + fsync

`state.save_state` 使用 `tempfile.mkstemp + fdopen + fsync + os.replace` 保证崩溃不坏数据。`write_summary=True` 时 **summary.json 先写，state.json 后写**；约定：summary 可能短暂比 state 新，list-batches 会短暂显示更新计数，下轮 debounced flush 追平。

### 5. 5 秒查询缓存

`PBSClient` 缓存 `query_user_jobs()` 结果 5 秒，避免同一轮询周期内重复 qstat。每次 poll 周期开始强制 `invalidate_cache()`。

### 6. 配置独立于项目目录

配置文件 `~/.config/pbs_auto/config.toml`，状态 `~/.local/share/pbs_auto/batches/<batch_id>/`，遵循 XDG 规范。新文件布局（每批一子目录）：

```
<batch_id>/                       0o700
    state.json          0o600     # 权威状态 (compact JSON, version=2)
    summary.json        0o600     # 快速列表缓存 (≤1 flush 陈旧)
    daemon.pid          0o600     # "PID btime_ticks ISO"
    daemon.lock         0o600     # fcntl.lockf by grandchild
    .locktest           0o600     # 临时自检文件
    daemon.log          0o600     # 50 MB 尺寸轮转
    daemon.log.2025-0.gz          # 历史归档
    submitting/         0o700     # 哨兵分片目录
        ab/             0o700
            abcd1234... 0o600     # 3 行内容
```

### 7. Batch ID

- 无 `--name`：`sha256("\n".join(sorted(resolve(root))))[:16]`
- 有 `--name`：`sha256(name)[:16]`
- 旧版 `<batch_id>.json` 扁平文件由 `migrate_layout` 幂等搬迁到 `<batch_id>/state.json`

### 8. Daemon 互斥 + 并发 submit race

`fcntl.lockf(<batch>/daemon.lock, LOCK_EX | LOCK_NB)` 是并发的唯一权威门。PID 文件只是 UX 辅助（`is_daemon_alive` 用 PID + btime + cmdline 三重校验，防止内核 PID 复用）。两实例 submit 同一 batch，后者在 `_verify_lockf_works` + `acquire_lock` 失败后 log 退出。

### 9. Flag-only 信号处理

SIGINT / SIGTERM handler 只做 `self._shutdown = True`，所有 FS 操作留给主循环的 `try/finally` 块。`_submit_pending` 内部有 **inner shutdown check**，每次 `_submit_task` 返回后检查一次。

### 10. NFSv3 要求

`fcntl.lockf` 依赖 NFSv3 的 `rpc.lockd` / `nfslock` 服务。CentOS 7 默认启用；缺失时 `_verify_lockf_works` 在 daemon 启动时报错退出。NFSv4 更佳但非必需。

## 第三方依赖

| 包 | 版本 | 用途 |
|---|---|---|
| click | >= 8.0 | CLI 框架 |
| rich | >= 13.0 | 终端 UI（Rich Table 一次性打印；**无 Live**） |
| tomli | >= 2.0 | TOML 解析 (Python 3.10 无内置 tomllib) |
| pytest | >= 7.0 | 测试 (dev 依赖) |

## 入口点

- CLI 命令: `pbs-auto` → `pbs_auto.cli:cli` (pyproject.toml `[project.scripts]`)
- 模块执行: `python -m pbs_auto` → `pbs_auto.__main__`
