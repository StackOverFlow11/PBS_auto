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

#### `BatchState(dataclass)`
一次批量提交会话的完整状态。

| 字段 | 类型 | 说明 |
|------|------|------|
| batch_id | str | 由目录路径 SHA256 前 16 字符 |
| root_directory | str | 工作目录绝对路径 |
| server_profile | str | 使用的服务器配置名 |
| created_at | str | 创建时间 ISO 格式 |
| updated_at | str | 最后更新时间 ISO 格式 |
| tasks | dict[str, Task] | 任务字典 (key = task.name) |

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

## state.py — 持久化

**文件**: `src/pbs_auto/state.py`
**依赖**: `config.py`, `models.py`
**测试**: `tests/test_state.py` (12 个用例)

### 关键函数

- `generate_batch_id(root_directory)`: SHA256(resolve(path))[:16]
- `get_state_path(batch_id)`: `DEFAULT_STATE_DIR / f"{batch_id}.json"`
- `save_state(state)`: JSON 序列化 + 原子写入 (mkstemp → write → replace)
- `load_state(batch_id)`: 从 JSON 反序列化，不存在返回 None
- `reconcile_tasks(saved, scanned)`: 合并已保存状态和新扫描结果
  - COMPLETED/WARNING/FAILED/SKIPPED: 保持不变
  - RUNNING/QUEUED: 保持不变（scheduler 会重新检查 PBS）
  - SUBMITTED: 重置为 PENDING（无法验证 qsub 是否真的成功）
  - 新任务: 添加为 PENDING
  - 所有任务更新 cores、directory、nodes、queue（脚本可能被编辑）
- `list_batches()`: 扫描状态目录，返回所有批次的摘要信息

---

## scheduler.py — 提交引擎

**文件**: `src/pbs_auto/scheduler.py`
**依赖**: 所有其他模块
**测试**: `tests/test_scheduler.py` (10 个用例)

### Scheduler 类

核心调度器，管理完整的提交生命周期。

#### 构造参数

```python
Scheduler(state, config, server, pbs, display, dry_run=False)
```

#### 主循环 `_main_loop()`

```
while not shutdown:
    _poll_status()      # 查询 PBS，更新任务状态
    _submit_pending()   # 提交等待中的任务
    save_state()        # 持久化状态
    display.refresh()   # 更新显示
    if _all_done(): break
    _sleep(interval)    # 可中断的等待
```

#### 状态转换逻辑 `_poll_status()`

对每个有 job_id 的非终态任务：
- PBS 中有 job，state == "R" → RUNNING（记录 start_time）
- PBS 中有 job，state == "Q" → QUEUED
- PBS 中无 job → `_handle_job_disappeared()`

#### 消失处理 `_handle_job_disappeared(task)`

- 状态为 SUBMITTED（从未看到运行）→ WARNING
- 有 start_time 且运行 < early_exit_threshold → WARNING
- 否则 → COMPLETED

#### 资源检查 `_get_resource_usage()`

从 PBS 查询实时资源使用（非仅内部状态），返回 `(running_cores, queued_cores)`。
查询失败时返回 `(max_running, max_queued)` 防止误提交。

#### 提交逻辑 `_submit_pending()`

遍历 PENDING 任务，每个任务提交前检查：
- running_cores + task.cores <= max_running_cores
- queued_cores + task.cores <= max_queued_cores
- 不满足则 `continue`（跳过，等下个周期）

提交后调用 `invalidate_cache()` 因为队列状态已变化。

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

### `run_dry_run(state, server)`

独立函数，使用 Rich Table 显示计划但不执行。

---

## display.py — CLI 界面

**文件**: `src/pbs_auto/display.py`
**依赖**: `config.py`, `models.py`, `rich`
**测试**: 无直接测试（UI 层）

### Display 类

使用 `rich.live.Live` 实现实时刷新终端界面。

#### 布局组成

```
╭─ PBS Auto-Submit — Server Name ──────────────────────╮
│ Running Cores:  ████████░░░░░░░░░░░░  96/240         │
│ Queued Cores:   ██████░░░░░░░░░░░░░░  48/192         │
│                                                       │
│ Total: 10 | Pending: 3 | Running: 2 | Completed: 5  │
│                                                       │
│ Name     Cores  Queue   Status    Job ID    Elapsed   │
│ task_1      48  long    running   371824    1h23m45s  │
│ task_2      48  medium  queued    371825    5m30s     │
│                                                       │
│ Elapsed: 02:15:30 | Last update: 14:23:45            │
╰──────────────────────────────────────────────────────╯
```

#### 方法

- `start()`: 启动 Live 刷新
- `stop()`: 停止 Live 刷新
- `refresh(state, server)`: 构建并更新面板
- `_build_resource_section()`: 进度条，< 80% 绿 / < 100% 黄 / 100% 红
- `_build_status_summary()`: 各状态计数单行摘要
- `_build_active_table()`: 活跃任务表；无活跃任务时显示最近 5 个终态任务
- `_build_timing()`: 累计耗时 + 当前时间
- `_calc_elapsed()`: 根据任务状态计算合适的耗时

---

## cli.py — 命令行入口

**文件**: `src/pbs_auto/cli.py`
**依赖**: 所有其他模块 (延迟导入)
**测试**: 通过 CLI 集成测试

### 命令

| 命令 | 函数 | 说明 |
|------|------|------|
| `submit <root_dir>` | `submit()` | 主命令：扫描+提交+监控 |
| `status <root_dir>` | `status()` | 查看批次状态 |
| `init` | `init()` | 创建默认配置 |
| `list-batches` | `list_batches()` | 列出保存的批次 |

### submit 命令流程

1. `load_config()` → 加载配置
2. `scan_directory()` → 扫描任务（填充 queue/nodes）
3. `validate_and_assign_queues()` → 队列验证与分配（除非 `--no-queue-validation`）
   - 不合规任务：显示警告 → `click.confirm()` → 用户选跳过则标记 SKIPPED
4. `generate_batch_id()` + `load_state()` → 加载/创建状态
5. `reconcile_tasks()` → 合并新旧状态（恢复场景）
6. 如果 `--dry-run` → `run_dry_run()` 后返回
7. 创建 `PBSClient` + `Display` + `Scheduler`
8. `scheduler.run()` → 进入主循环
9. `_print_summary()` → 输出最终摘要

### 延迟导入

所有重量级模块在命令函数内部导入，避免 `--help` 时加载不必要的模块。
