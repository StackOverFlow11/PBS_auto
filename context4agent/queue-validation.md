# 队列验证模块说明

## 文件

`src/pbs_auto/queue.py`

## 依赖

- `config.py` (QueueConfig)
- `models.py` (Task, TaskStatus)

## 测试

`tests/test_queue.py`

## 核心函数

### `validate_task_for_queue(task, queue_config) -> list[str]`

验证单个任务是否符合队列规则。返回错误消息列表（空列表 = 合规）。

**检查逻辑：**
1. 如果 `allowed_cores` 非空：cores 必须在列表中
2. 如果 `allowed_cores` 为空/None：cores 必须在 `[min_cores, max_cores]` 范围内
3. `max_nodes` 始终检查（-1 表示无限制）

### `select_queue(task, queues, walltime_seconds=None) -> str | None`

自动选择最合适的队列。

**算法：**
1. 遍历所有队列，过滤掉不合规的
2. 如果有 walltime，进一步过滤掉 max_walltime 不够的
3. 按 `max_walltime_hours` 升序排列
4. 选择第一个（最紧凑的队列）

**设计决策：** 优先选择 walltime 限制最短的队列（即最"紧"的队列），因为较短的队列通常调度优先级更高。

### `validate_and_assign_queues(tasks, queues, cli_queue=None) -> tuple[list[Task], list[tuple[Task, list[str]]]]`

批量验证与分配队列。

**优先级链：**
1. `cli_queue`（CLI `--queue` 参数）→ 覆盖一切
2. `task.queue`（脚本 `#PBS -q`）→ 保留脚本中指定的
3. `select_queue()` 自动选择 → 没有指定时的默认行为

**返回值：**
- `all_tasks`: 所有任务（队列已分配）
- `invalid`: 不合规任务列表，每项为 `(task, errors)`

**重要：** 不合规任务不会被自动跳过，而是返回给 CLI 层由用户决定。

## 验证规则

### 两层验证机制

| 队列类型 | 验证方式 | 示例 |
|---------|---------|------|
| 范围型（debug/short） | `min_cores <= cores <= max_cores` | debug: 1-24 任意值 |
| 列表型（medium/long） | `cores in allowed_cores` | medium: [24, 48, 72, 96] |

两种类型都检查 `nodes <= max_nodes`。

### 数据流

```
scan_directory() → tasks (带 queue/nodes 字段)
        ↓
validate_and_assign_queues(tasks, queues, cli_queue)
        ↓
    ┌─ cli_queue? → 覆盖所有 task.queue
    ├─ task.queue 有值? → 保留
    └─ 无 queue → select_queue() 自动选
        ↓
    validate_task_for_queue() 验证
        ↓
    返回 (all_tasks, invalid_tasks)
        ↓
CLI 层: 显示警告 → click.confirm() → 跳过/继续
```
