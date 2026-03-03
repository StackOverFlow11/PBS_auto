# PBS Auto-Submit 使用文档

## 简介

PBS Auto-Submit (`pbs-auto`) 是一个 Python CLI 工具，用于在 PBS 集群环境下大批量自动提交第一性原理计算任务（CP2K、VASP 等）。它解决了手动逐个 `qsub` 上百个任务时需要反复检查资源限额的痛点，自动化整个流程：扫描目录 → 检查资源 → 提交任务 → 监控状态 → 持久化记录。

## 环境要求

- Python >= 3.10
- PBS 18.1.4（CentOS 7 环境）
- 无需 root 权限

## 安装

```bash
# 在项目目录下
pip install --user -e .

# 验证安装
pbs-auto --version
```

安装后可通过两种方式调用：
- `pbs-auto` — 直接命令
- `python -m pbs_auto` — 模块方式

## 快速开始

### 1. 初始化配置

```bash
pbs-auto init
```

这会在 `~/.config/pbs_auto/config.toml` 创建默认配置文件。根据实际服务器情况编辑配置。

### 2. 准备任务目录

任务目录结构：
```
workdir/
├── 1/
│   ├── script.sh    # PBS 提交脚本（必须包含 #PBS -l nodes=X:ppn=Y）
│   ├── cmd.inp      # 计算输入文件
│   └── init.xyz     # 初始结构文件
├── 2/
│   ├── script.sh
│   └── ...
├── 3/
│   └── ...
...
```

每个一级子目录即为一个独立任务，目录名即为 PBS 作业名。

### 3. 预览提交计划

```bash
pbs-auto submit ./workdir --dry-run
```

输出示例：
```
Scanning /path/to/workdir for tasks...
Found 10 tasks, 0 skipped

Dry Run - Server: Server 1
Max Running: 240 cores | Max Queued: 192 cores

┏━━━━┳━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━━━┳━━━━━━┓
┃ #  ┃ Name ┃ Directory    ┃ Cores ┃ Status  ┃ Note ┃
┡━━━━╇━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━━━╇━━━━━━┩
│ 1  │ 1    │ /path/to/1   │    48 │ pending │      │
│ 2  │ 2    │ /path/to/2   │    48 │ pending │      │
│ ...│ ...  │ ...          │   ... │ ...     │      │
└────┴──────┴──────────────┴───────┴─────────┴──────┘

Summary: 10 tasks to submit (480 total cores), 0 skipped
```

### 4. 实际提交

```bash
pbs-auto submit ./workdir
```

工具会启动实时监控界面，显示：
- 资源使用进度条（Running/Queued 核数及限额）
- 任务统计（各状态计数）
- 活跃任务列表（名称、核数、状态、Job ID、耗时）
- 累计耗时和最后更新时间

### 5. 中断与恢复

- **第一次 Ctrl+C**：优雅停止，完成当前操作后保存状态退出
- **第二次 Ctrl+C**：强制退出
- **恢复**：对同一目录再次运行 `pbs-auto submit ./workdir`，工具会自动加载上次状态，跳过已完成的任务，从断点继续

## 命令参考

### `pbs-auto submit <root_dir>`

扫描目录并提交 PBS 任务。

| 选项 | 说明 |
|------|------|
| `--server <name>` | 指定服务器配置名（对应 config.toml 中的 `[servers.xxx]`） |
| `--config <path>` | 指定配置文件路径 |
| `--dry-run` | 仅展示计划，不实际提交 |
| `--fresh` | 丢弃旧状态，重新开始 |
| `--script-name <name>` | 自定义 PBS 脚本文件名（默认 `script.sh`） |
| `--queue <name>` | 强制所有任务使用指定队列（覆盖脚本 `#PBS -q` 和自动选择） |
| `--no-queue-validation` | 跳过队列合规性检查 |

### `pbs-auto status <root_dir>`

查看指定目录的批次状态摘要。

### `pbs-auto init`

在默认位置创建配置文件模板。

### `pbs-auto list-batches`

列出所有保存的批次状态记录。

## 队列验证与自动选择

工具会根据配置文件中的队列规则自动验证和选择队列：

### 队列确定优先级

1. `--queue` CLI 参数 → 强制所有任务使用指定队列
2. 脚本中的 `#PBS -q` → 保留脚本指定的队列
3. 自动选择 → 根据核心数和 walltime 选择最紧凑的合适队列

### 合规性检查

提交前工具会检查每个任务是否符合目标队列的规则（核心数、节点数）。如果发现不合规任务：
- 显示警告表格（任务名、核心数、目标队列、不合规原因）
- 询问是否继续提交
- 选择"否"时，不合规任务被标记为 SKIPPED

使用 `--no-queue-validation` 可跳过所有检查。

### 使用示例

```bash
# 自动选择队列
pbs-auto submit ./workdir --dry-run

# 强制使用 long 队列
pbs-auto submit ./workdir --queue long

# 跳过队列检查
pbs-auto submit ./workdir --no-queue-validation
```

## 配置文件

### 查找顺序

1. `--config` CLI 参数
2. `$PBS_AUTO_CONFIG` 环境变量
3. `~/.config/pbs_auto/config.toml`（默认位置）
4. 若均不存在，使用内置默认值

### 配置项说明

```toml
[defaults]
server = "server1"           # 默认服务器配置名
script_name = "script.sh"   # PBS 脚本文件名
poll_interval = 15           # 状态轮询间隔（秒）
submit_delay = 2             # 连续提交间隔（秒）
post_submit_check_delay = 60 # 提交后首次检查延迟（秒）
early_exit_threshold = 30    # 运行时间低于此值判定为 warning（秒）

[servers.server1]
name = "Server 1"            # 显示名称
status_command = "qstat"     # 状态查询命令（"qstat" 或 "q"）
status_args = ["-au", "$USER"]  # 命令参数（$USER 会自动展开）
max_running_cores = 240      # Running 状态最大核数
max_queued_cores = 192       # Queued 状态最大核数
core_granularity = 24        # 核数粒度
```

### 多服务器配置

```toml
[servers.server1]
name = "Server 1"
max_running_cores = 240
max_queued_cores = 192

[servers.server2]
name = "Server 2"
max_running_cores = 480
max_queued_cores = 384
```

使用时指定：
```bash
pbs-auto submit ./workdir --server server2
```

## 状态文件

状态文件保存在 `~/.local/share/pbs_auto/batches/<batch_id>.json`，其中 `batch_id` 由工作目录的绝对路径哈希生成（同一目录总是产生相同 ID）。

状态文件为 JSON 格式，可人工查看和修改。

## 任务状态说明

| 状态 | 含义 |
|------|------|
| `pending` | 等待提交 |
| `submitted` | 已提交，等待 PBS 确认 |
| `queued` | PBS 队列中排队 |
| `running` | 正在运行 |
| `completed` | 正常完成 |
| `warning` | 异常完成（运行时间过短或未进入运行态即消失） |
| `failed` | 提交失败 |
| `skipped` | 跳过（缺少脚本或无法解析资源） |

## PBS 脚本要求

脚本中必须包含 PBS 资源声明以便工具解析核数：

```bash
#PBS -l nodes=1:ppn=48
```

工具会计算 `nodes * ppn` 作为该任务的核数。若无法解析，任务状态设为 `skipped`。

## 常见场景

### 使用 VASP 脚本

```bash
pbs-auto submit ./workdir --script-name vasp.sh
```

### 查看历史批次

```bash
pbs-auto list-batches
```

### 强制重新提交

```bash
pbs-auto submit ./workdir --fresh
```

### 使用环境变量指定配置

```bash
export PBS_AUTO_CONFIG=/path/to/my_config.toml
pbs-auto submit ./workdir
```
