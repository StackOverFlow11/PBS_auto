# 模块详细说明

## models.py — 数据模型

**文件**: `src/pbs_auto/models.py`
**依赖**: 无外部依赖（仅 stdlib）
**测试**: 通过 `test_state.py` 和 `test_scheduler.py` 间接测试

### 类型定义

#### `TaskStatus(Enum)`
8 个状态值：`PENDING`, `SUBMITTED`, `QUEUED`, `RUNNING`, `COMPLETED`, `WARNING`, `FAILED`, `SKIPPED`。
值为小写字符串，直接用于 JSON 序列化。

#### `Task(dataclass)`
单个 PBS 作业的完整描述。

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| name | str | (必填) | 任务名，即子目录名，也是 `qsub -N` 的值 |
| directory | str | (必填) | 任务目录绝对路径 |
| cores | int | 0 | 计算核数 (nodes * ppn) |
| status | TaskStatus | PENDING | 当前状态 |
| job_id | str \| None | None | PBS Job ID (如 "371824.mgr") |
| submit_time | str \| None | None | 提交时间 ISO 格式 |
| start_time | str \| None | None | 开始运行时间 |
| end_time | str \| None | None | 结束时间 |
| error_message | str \| None | None | 错误/警告信息 |
| script_name | str | "script.sh" | PBS 脚本文件名 |
| queue | str \| None | None | 目标队列名（来自 #PBS -q / 自动选择 / CLI --queue） |
| nodes | int | 0 | 原始节点数（用于队列合规验证） |

提供 `to_dict()` / `from_dict()` 用于 JSON 序列化。

#### `PBSJobInfo(dataclass)`
从 qstat/q 输出解析的 PBS 作业信息。

| 字段 | 类型 | 说明 |
|------|------|------|
| job_id | str | 纯数字 ID (已去除 .mgr 后缀) |
| name | str | 作业名 |
| state | str | PBS 状态码 ("R", "Q", "E" 等) |
| cores | int | 使用核数 |
| elapsed | str | 已运行时间 |
| queue | str | 队列名 (long/medium/short) |

#### `BatchState(dataclass)` — version 2 schema
一次批量提交会话的完整状态。**tasks dict 的 key 是 task.directory 绝对路径**（不是 task.name），支持多 workdir 下跨父目录同名子目录。

**派生索引**（`field(repr=False, compare=False)`，不序列化）：
- `_pending_set: set[str]` — PENDING task 的 directory 集合
- `_active_set: set[str]` — SUBMITTED/QUEUED/RUNNING task 的 directory 集合

调用 `rebuild_indexes()` 在 `from_dict` 后和 reconcile 后全量重建；`BatchStore._update_indexes()` 在 mutation context 内部做增量维护。

方法 `source_root_for(task)` 按最长前缀匹配 root_directories，用于多 workdir 任务溯源。

| 字段 | 类型 | 说明 |
|------|------|------|
| batch_id | str | 由 sorted(resolve(roots)) 或 name 的 SHA256 前 16 字符 |
| root_directories | list[str] | 工作目录绝对路径列表（多 workdir 支持） |
| server_profile | str | 使用的服务器配置名 |
| name | str \| None | 用户指定的 batch 名，多 root 时必填 |
| version | int | schema 版本（当前为 2） |
| created_at | str | 创建时间 ISO 格式 |
| updated_at | str | 最后更新时间 ISO 格式 |
| tasks | dict[str, Task] | 任务字典 (**key = task.directory 绝对路径**) |

---

## config.py — 配置系统

**文件**: `src/pbs_auto/config.py`
**依赖**: `tomli`
**测试**: `tests/test_config.py` (13 个用例)

### 关键常量

- `DEFAULT_CONFIG_PATH`: `~/.config/pbs_auto/config.toml`
- `DEFAULT_STATE_DIR`: `~/.local/share/pbs_auto/batches/`
- `DEFAULT_CONFIG_CONTENT`: 内置默认配置字符串

### 数据类

- `QueueConfig`: 单个 PBS 队列的资源规则（核心数范围/列表、节点限制、walltime 上限）
- `ServerConfig`: 单个服务器的资源限额和查询命令配置，含 `queues: dict[str, QueueConfig]`
- `AppConfig`: 全局配置，包含 defaults + servers 字典

### 关键函数

- `find_config_path(cli_path)`: 按优先级查找配置文件（CLI > 环境变量 > 默认路径）
- `load_config(cli_path)`: 加载并解析配置，无文件时返回内置默认
- `_parse_config(raw)`: 将 TOML dict 转为 AppConfig
- `init_config()`: 在默认路径创建配置文件模板

### 注意事项

- `$USER` 在 `status_args` 中是字面量，运行时由 `PBSClient._expand_args()` 展开
- `get_server()` 不存在时抛 `ValueError`，CLI 层捕获并友好提示

---

## scanner.py — 目录扫描

**文件**: `src/pbs_auto/scanner.py`
**依赖**: `models.py`
**测试**: `tests/test_scanner.py` (15 个用例)

### 核心正则

```python
PBS_RESOURCE_RE   # 匹配 #PBS -l nodes=X:ppn=Y
PBS_QUEUE_RE      # 匹配 #PBS -q <queue_name>
PBS_WALLTIME_RE   # 匹配 #PBS -l walltime=HH:MM:SS
```

### 数据类

#### `ScriptResources(dataclass)`

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| nodes | int | 0 | 节点数 |
| ppn | int | 0 | 每节点核数 |
| cores | int | 0 | nodes * ppn |
| queue | str \| None | None | #PBS -q 指定的队列名 |
| walltime_seconds | int \| None | None | walltime 总秒数 |

### 关键函数

- `natural_sort_key(name)`: 自然排序 key 函数 (1, 2, 10 而非 1, 10, 2)
- `parse_script_resources(script_path)`: 解析 PBS 脚本中所有资源指令，返回 ScriptResources 或 None
- `parse_cores_from_script(script_path)`: 向后兼容的薄包装器，返回 cores 或 None
- `scan_directory(root, script_name)`: 扫描一级子目录，返回 Task 列表（填充 queue、nodes 字段）
  - 仅扫描直接子目录（不递归）
  - 缺少脚本 → SKIPPED + error_message
  - 无法解析资源 → SKIPPED + error_message
  - 结果按自然顺序排序

---

## queue.py — 队列验证与自动选择

**文件**: `src/pbs_auto/queue.py`
**依赖**: `config.py` (QueueConfig), `models.py` (Task, TaskStatus)
**测试**: `tests/test_queue.py` (21 个用例)

详见 [queue-validation.md](queue-validation.md)。

### 关键函数

- `validate_task_for_queue(task, queue_config)`: 验证单任务合规性，返回错误列表
- `select_queue(task, queues, walltime_seconds)`: 自动选择最合适的队列
- `validate_and_assign_queues(tasks, queues, cli_queue)`: 批量验证与分配

---

## pbs.py — PBS 命令交互

**文件**: `src/pbs_auto/pbs.py`
**依赖**: `config.py`, `models.py`
**测试**: `tests/test_pbs.py` (8 个用例)

### PBSClient 类

```python
PBSClient(server_config: ServerConfig)
```

- `submit(task)`: 在 task.directory 下执行 `qsub [-q <queue>] -N <name> <script_name>`，返回 job_id
- `query_user_jobs(force=False)`: 查询用户作业，结果缓存 5 秒
- `invalidate_cache()`: 清除缓存

根据 `server_config.status_command` 选择 qstat 或 q 解析器。

### 模块级函数

#### `_is_retryable_error(error_msg)`

判断 qsub 错误是否为临时性资源冲突，匹配 `RETRYABLE_PATTERNS` 列表中的关键字（不区分大小写）。匹配时任务保持 PENDING 下一轮重试，不匹配时标记为 FAILED。

### 解析函数

#### `parse_qstat_output(output)`

解析 `qstat -au $USER` 标准表格输出。

格式：
```
Job ID          Username Queue    Jobname    SessID NDS TSK Memory Time  S Time
--------------- -------- -------- ---------- ------ --- --- ------ ----- - -----
371824.mgr      shaofl   long     pzc_co2    12345   1  96    --  720:0 R 28:26
```

- 以 `---` 分隔线定位数据区
- Job ID 去除 `.mgr` 后缀
- 核数从 TSK 列 (parts[6]) 读取

#### `parse_q_output(output)`

解析自定义 `q` 命令输出，需处理：
1. 开头的装饰行（含 `//` 和 emoji）
2. 列头行（含 "ID", "Owner", "Queue"）
3. 数据行（首 token 为纯数字）
4. 续行（上一行的路径换行，首 token 非数字）
5. 页脚（`long:`, `medium:`, `short:` 开头的节点汇总）

算法：先找到包含 "ID"+"Owner"+"Queue" 的列头行，再找其后第一个 `---` 分隔线作为数据起点，到下一个 `---` 分隔线或页脚时结束。数据行判定条件：`parts[0].isdigit()`。

核数从 N:T 列解析：`"1:96"` → `1 * 96 = 96`。

---

## state.py — JSON load/save + migration + reconcile + identifier

**文件**: `src/pbs_auto/state.py`
**依赖**: `config.py`, `models.py`, `batch_store`
**测试**: `tests/test_state.py` (38 个用例)

**所有权边界**：JSON 序列化、schema migration、reconcile 逻辑、identifier 解析。磁盘布局（路径、权限、哨兵、轮转、mutation）由 `batch_store/` 包拥有。

### 常量

- `MAX_EAGER_SAVE_LATENCY_MS = 500` — eager save 延迟上限（`test_batch_store.py` 断言 15k 任务在此之内；超过则推荐 orjson 落地）

### 关键函数

- `generate_batch_id(roots, name=None)`: 有 name → SHA256(name)[:16]；否则 SHA256("\n".join(sorted(resolve(r)))))[:16]。legacy 单字符串 root 仍支持
- `validate_identifier(identifier)`: 字符集 `[A-Za-z0-9._-]{1,64}`；拒 `/`、`\`、`\x00`、前导 `-`、reserved names (`.`, `..`, `daemon.pid`, `daemon.lock`, `daemon.log`, `state.json`, `summary.json`, `submitting`, `.orig`, `.tmp`)
- `resolve_batch_identifier(identifier)`: 解析顺序 → exact batch_id → prefix match → name match via summary.json/state.json
- `_migrate_on_load(raw)`: 幂等 v1→v2 升级，重命名 `root_directory` → `root_directories`，按 directory 重建 tasks dict key
- `save_state(state, write_summary=True)`:
  - `json.dumps(..., separators=(",",":"), ensure_ascii=False)` — compact，无 indent
  - `_atomic_write`: `tempfile.mkstemp` → `fchmod(0o600)` → `write` → `fsync` → `replace`
  - 顺序：summary.json 先写，state.json 后写（约定：summary 可比 state 新 ≤1 flush）
  - `write_summary=False` 用于 eager qsub 保存，降低 IO
- `load_state(batch_id)`: 调 `migrate_layout` 尝试搬迁 legacy flat file → 读 state.json → `_migrate_on_load` → `BatchState.from_dict` → `rebuild_indexes`
- `reconcile_tasks(saved, scanned)`: 按 `task.directory` 匹配；**bug fix**：SUBMITTED 有 job_id 保持不变（只有无 job_id 时才重置为 PENDING），结束后 `rebuild_indexes`
- `list_batches()`: 优先读 `<batch_id>/summary.json`，缺失降级读 state.json

---

## batch_store/ — 磁盘布局 + 哨兵 + 轮转 + mutation (NEW package)

**位置**: `src/pbs_auto/batch_store/`
**依赖**: `models.py`, `config.py`
**测试**: `tests/test_batch_store.py` (29 个用例)

**所有权边界**（与 state.py 对称）：磁盘布局、权限、哨兵 CRUD、日志轮转、mutation 生命周期。JSON load/save + migration + reconcile + identifier 由 `state.py` 拥有。

### `_paths.py`

路径函数：`get_batch_dir / get_state_path / get_summary_path / get_pid_path / get_lock_path / get_log_path / get_sentinel_dir`。`ensure_batch_dir` 在 daemon startup audit 目录权限（< 0o700 时 chmod 收紧）。

`migrate_layout(batch_id)` 幂等 5 分支：
1. `<id>/state.json` 存在 → noop
2. `<id>.json` symlink → 拒绝（RuntimeError）
3. `<id>.json` + 新目录不存在 → mkdir + `os.replace`
4. `<id>.json` + 新目录空 → `os.replace`
5. 两者都有 → 备份 legacy 为 `.orig` 并警告

### `_sentinels.py` (content-authoritative)

- `sentinel_key(dir) = sha256(dir)[:24]` — 96 bits 防碰撞
- `sentinel_path(batch_id, dir)` → `<batch>/submitting/<key[:2]>/<key>` (256 分片)
- 3 行 ASCII 严格内容：`<abs_dir>\n<job_id|PENDING>\n<iso_ts>\n`
- `create_sentinel(batch_id, task)`: `tempfile.mkstemp + fchmod(0o600) + fsync + replace`，内容 line 2 = `PENDING`
- `update_sentinel_job_id(path, job_id)`: 原子重写 line 2，保留 line 1，刷新 line 3
- `remove_sentinel(path)`: `unlink(missing_ok=True)`
- `read_sentinel(path)`: 严格 3 行解析，dir ≤4096，job_id 正则 `^(PENDING|[0-9]+(\.[A-Za-z0-9._-]+)?)$`，iso ts 正则；畸形返回 None
- `iter_sentinels(batch_id)`: 遍历分片目录，跳过 `*.tmp`
- `cleanup_stale_artifacts(batch_id, state, max_tmp_age_s=60)`: startup 唯一调用。删除 batch 根下的 tempfiles（age > max）+ 孤儿 sentinel（其 key 不在 state 任何 PENDING task 上）
- **`recover_sentinels(state, pbs) -> bool`**：启动时调用，返回 True 表示 state 已变更
  1. 建 reverse index `key → task`
  2. query PBS，失败 → return False（caller 设 `_recovery_pending`）
  3. 遍历 sentinels：
     - task 不存在或非 PENDING → unlink
     - 解析失败 → unlink
     - filename hash ≠ content dir hash → 防伪失败 unlink
     - body == "PENDING" → qsub 未返回 → unlink
     - body 有 job_id 且 PBS 命中 (state in {Q,R,H,W}) → attach 成 SUBMITTED
  4. Orphan PBS job 扫描：job name startswith `pa_<batch_id[:6]>` 且 id 未被引用 → log WARN 让用户手动 qdel
  5. 有 change 则 rebuild_indexes + save_state (bypass)

### `_rotation.py` (daemon-only)

- `MAX_LOG_SIZE = 50 MB`, `MAX_LOG_AGE_DAYS = 365`
- `maybe_rotate_log_on_startup(batch_id)`: 打开 log fd **前**，size 或 age 超阈则 archive
- `maybe_rotate_log_in_loop(batch_id, fd)`: 主循环定期调用；`fstat(fd)` → 超尺寸则 rename → `gzip.open(gz, "xb")` → `os.open` 新 fd → `dup2` 到 1/2 → `close(旧 fd)` → 返回新 fd
- 归档名 `daemon.log.<YYYY>-<N>.gz`，`xb` 模式 + 递增 N 避免同年冲突

### `_mutation.py` — BatchStore + _MutationContext

**`BatchStore(state)`**：per-process 唯一 mutation 入口。字段：`state`, `_dirty=False`, `_last_flush_mono=time.monotonic()`, `_min_debounce_interval=1.0`。

API：
- `mutate(task=None, flush="debounced") -> _MutationContext`
- `maybe_debounced_flush()`: dirty 且距上次 flush ≥ `_min_debounce_interval` 时才 `_flush_now(write_summary=True)`
- `force_flush()`: 立即 `_flush_now(write_summary=True)`
- `_flush_now(write_summary)`: `try: save_state except OSError: log + keep _dirty`；成功清 dirty
- `_update_indexes(task)`: 增量维护 `_pending_set` / `_active_set`

**`_MutationContext`**：绝不抛异常，每步 try/except log-and-continue。`__exit__` 流程：
1. 异常路径 → 留下 sentinel → return False
2. sentinel 内容同步：task.job_id 非空 → `update_sentinel_job_id`
3. 索引增量更新
4. flush 策略：`task.status == FAILED` → 强制 eager；eager → `_flush_now(write_summary=False)`；debounced → 仅置 `_dirty=True`
5. **无条件**删除 sentinel（clean exit 意味着 sentinel 职责完成；retryable-PENDING 下轮循环会重建）

---

## scheduler.py — 提交引擎（零 Rich）

**文件**: `src/pbs_auto/scheduler.py`
**依赖**: `batch_store`, `config`, `models`, `pbs`
**测试**: `tests/test_scheduler.py` (18 个用例)

### Scheduler 类

核心调度器。`run_dry_run` 已迁到 `cli.py`，scheduler 零 `from rich` 导入。

#### 构造参数

```python
Scheduler(state, config, server, pbs, store: BatchStore, dry_run=False)
```

#### 主循环 `_main_loop()`

```
try:
  while not _shutdown:
    _poll_status()                         # 读 _active_set, mutate via store (debounced)
    if _recovery_pending: recover_sentinels retry
    if _shutdown: break                    # inner check
    _submit_pending()                      # 读 _pending_set, mutate via store (eager)
    store.maybe_debounced_flush()          # 本轮合并 flush
    if _all_done_fast(): break
    _sleep(poll_interval)
finally:
  try: store.force_flush()                 # 最终快照
```

#### 信号处理

Flag-only：SIGINT / SIGTERM 只设 `_shutdown = True`，所有 FS 操作留给主循环 `try/finally`。

#### 状态转换 `_poll_status()` / `_handle_job_disappeared()`

不变；但所有 mutation 都包在 `with store.mutate(task=t, flush="debounced"):` 里。遍历 `self.state.tasks.items()` 并按 `_active_set` 过滤以保证确定性顺序。

#### 提交逻辑 `_submit_pending()`

遍历 `state.tasks` 按 `_pending_set` 过滤。每次 `_submit_task(task)` 返回后检查 `_shutdown`（inner check）。retryable error → `break`。

`_submit_task(task) -> bool` 使用 `with store.mutate(task=task, flush="eager"):` 包住整个 qsub 调用。FileNotFoundError / 非 retryable → FAILED；retryable → 保持 PENDING 返回 False；成功 → SUBMITTED + job_id + submit_time。

---

## daemon.py — 双 fork + fcntl.lockf + PID (NEW)

**文件**: `src/pbs_auto/daemon.py`
**依赖**: `batch_store` (paths + rotation)
**测试**: 验证脚本级 fork 不在单测中执行；helper 函数用 monkeypatch 的 /proc 和 fcntl stub 测试（待补）

### 关键函数

- `daemonize(batch_id)`: flush stdio → fork → setsid → fork → chdir("/") → `umask(0o077)` → `closerange(3, max_fd)` → `maybe_rotate_log_on_startup` → open log fd O_WRONLY|O_CREAT|O_APPEND 0o600 → stdin ← /dev/null, 1/2 ← log。只有 grandchild 返回 log_fd
- `_verify_lockf_works(batch_dir)`: 开 `.locktest` O_EXCL 0o600 → `lockf(EX|NB)` → `LOCK_UN` → close → unlink。任何 OSError 抛 RuntimeError「NFSv3 需 rpc.lockd」
- `acquire_lock(batch_id) -> int`: 开 `daemon.lock` O_WRONLY|O_CREAT 0o600 → `lockf(EX|NB)`；失败 raise RuntimeError（另一实例持锁）；成功返回 fd（**不关闭**，kernel 在进程死时自动释放）
- `write_pid_file(batch_id)`: `/proc/<pid>/stat` 读 btime_ticks → O_EXCL 0o600 写 `"PID btime ISO\n"`
- `_read_proc_starttime(pid)`: 解析 `/proc/<pid>/stat`，从最后一个 `)` 切片避开 `(comm)` 里的空格，字段索引 19（post-comm）= field 22（全局，1-based）= starttime
- `is_daemon_alive(batch_id)`: PID 文件 + `os.kill(pid, 0)` + btime 匹配 + cmdline 含 `pbs-auto`
- `stop_daemon(batch_id, timeout=60.0)`: 校验同进程 → SIGTERM → 0.5s 轮询等 PID 消失

当 `_submit_task()` 返回 `False`（可重试错误）时，`break` 停止本轮提交。

#### 提交单任务 `_submit_task(task) -> bool`

返回 `True` 表示成功或永久失败（继续下一个任务），`False` 表示可重试错误（停止本轮）。

错误分类：
- `FileNotFoundError` → FAILED（永久）
- `RuntimeError`/`OSError` + 匹配 `_is_retryable_error()` → 保持 PENDING（可重试）
- `RuntimeError`/`OSError` + 不匹配 → FAILED（永久）

成功时清除之前可能存在的 `error_message`。

#### SIGINT 处理

- 安装自定义信号处理器
- 第一次 → `_shutdown = True`，主循环自然退出
- 第二次 → 恢复原始处理器并 `raise KeyboardInterrupt`

---

## display.py — DELETED

本模块已删除。实时 Rich Live 面板不再维护，`status` 命令改用 summary.json 一次性快照。

---

## cli.py — 命令行入口（含 run_dry_run）

**文件**: `src/pbs_auto/cli.py`
**依赖**: 所有其他模块 (延迟导入)
**测试**: 通过端到端 dry-run 集成测试

### 命令

| 命令 | 函数 | 说明 |
|------|------|------|
| `submit [ROOT_DIRS]...` | `submit()` | 扫描 + 提交；默认双 fork daemon |
| `status <name\|batch_id>` | `status()` | 一次性快照（读 summary.json）|
| `stop <name\|batch_id>` | `stop()` | SIGTERM 并等待 daemon 退出 |
| `logs <name\|batch_id>` | `logs()` | cat / tail -f daemon.log |
| `list-batches` | `list_batches()` | 列出所有批次（读 summary.json，显示 daemon status） |
| `init` | `init()` | 创建默认配置 |

### submit 命令流程

1. 解析 ROOT_DIRS + `--from-list` → 去重合并
2. 多 root 时强制 `--name`；validate_identifier
3. `load_config()` + `get_server()`
4. `scan_directory()` × N → 合并 tasks
5. `validate_and_assign_queues()` → 队列验证与分配
6. `generate_batch_id(roots, name)` + `load_state()` → reconcile 或新建
7. `BatchState.tasks = {t.directory: t}` + `rebuild_indexes()`
8. `save_state()` 初始 bootstrap（documented bypass #1）
9. `--dry-run` → `run_dry_run()` 返回
10. `--foreground` → `_run_main()`（acquire_lock + scheduler.run 同进程）
11. 默认 → `is_daemon_alive?` 拒绝 → `daemonize()` → `_run_daemon_main()` 在 grandchild

### `_run_daemon_main` (grandchild entry)

1. `_verify_lockf_works(batch_dir)`
2. `acquire_lock(batch_id)` (`fcntl.lockf LOCK_EX|LOCK_NB`)
3. `write_pid_file(batch_id)` (O_EXCL + btime)
4. `load_state(batch_id)` — daemon 持锁后重读确保权威
5. `cleanup_stale_artifacts(batch_id, state)`
6. `PBSClient(server_config, batch_id=batch_id)` → `recover_sentinels(state, pbs)` (bypass #2)
7. `BatchStore(state)` + `Scheduler(state, config, server, pbs, store)`
8. `scheduler.run()`
9. `finally`: close(lock_fd), unlink PID file

### `run_dry_run(state, server_config)` (moved from scheduler.py)

迁入 `cli.py`。纯读 Rich Table 预览，不实例化 BatchStore、不调用 save_state、不调用 PBS（bypass #3 是零调用的 bypass）。

### 延迟导入

所有重量级模块在命令函数内部 `from ... import ...`，避免 `--help` 时加载 scheduler/daemon/batch_store。
