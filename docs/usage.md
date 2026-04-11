# PBS Auto-Submit 使用文档

## 简介

PBS Auto-Submit (`pbs-auto`) 是一个 Python CLI 工具，用于在 PBS Pro 集群环境下大批量自动提交第一性原理计算任务（CP2K、VASP 等）。典型场景：一次提交 10,000–100,000 个单点计算（MLIP 训练集生成），让 daemon 在后台运行数天，配额自动节流，SSH 断开无影响，任意时刻崩溃可完美恢复。

核心能力：
- **Daemon 模式**（默认）—— 双 fork 到后台，断 SSH 不影响运行
- **多 workdir 批次** —— 一条命令提交多个父目录下的任务
- **事件驱动持久化 + 哨兵恢复** —— 任意时刻 SIGKILL 都能从磁盘完整恢复，不会重复提交已成功的任务
- **配额节流** —— 以 PBS `qstat` 为真相源，自动维持 running/queued 核数在配额之内
- **队列合规检查** —— 提交前校验 `#PBS -l nodes=...:ppn=...` 是否符合目标队列的 `allowed_cores`

## 环境要求

- Python ≥ 3.10
- PBS Pro（测试于 CentOS 7）
- **NFSv3 需启用 `rpc.lockd` / `nfslock` 服务**（daemon 用 `fcntl.lockf` 做跨进程互斥；启动时会自检，缺失直接报错退出）
- 无需 root 权限

## 安装

```bash
# 项目根目录
pip install --user -e ".[dev]"

# 推荐在 conda env 下安装
conda activate <env_name>
pip install -e ".[dev]"

# 验证
pbs-auto --version
python -m pytest tests/ -v     # 177 个测试
```

调用方式：
- `pbs-auto <subcommand>` — 直接命令
- `python -m pbs_auto <subcommand>` — 模块方式

## 快速开始

### 1. 初始化配置

```bash
pbs-auto init
```

在 `~/.config/pbs_auto/config.toml` 创建模板。编辑以匹配集群的队列、配额、状态命令。

### 2. 准备任务目录

单 workdir 结构（向后兼容）：
```
workdir/
├── task_001/
│   ├── script.sh      # PBS 脚本，必须含 #PBS -l nodes=X:ppn=Y
│   ├── sp.inp         # 计算输入
│   └── init.xyz       # 初始结构
├── task_002/
├── task_003/
└── ...
```

多 workdir 结构（新）—— 把不同父目录下的扁平子任务合到一个批次：
```
traj_051/
  sp_t0_i0/{script.sh, sp.inp, init.xyz}
  sp_t25_i25/...
  ...
traj_052/
  sp_t0_i0/...
  ...
```

每个**一级子目录**即为一个独立任务，`#PBS -N` 由 pbs-auto 自动赋值（见下方"PBS 脚本要求"）。

### 3. 预览（dry-run）

```bash
# 单 workdir
pbs-auto submit ./workdir --dry-run

# 多 workdir，必须给 --name
pbs-auto submit traj_051/ traj_052/ traj_053/ --name mlip_round1 --dry-run
```

输出一次性 Rich Table 显示计划，不启动 daemon、不调用 qsub、不写状态文件。

### 4. 实际提交（默认 daemon）

```bash
# 单 workdir（向后兼容，batch_id 由路径 hash 生成）
pbs-auto submit ./workdir

# 多 workdir（必须 --name）
pbs-auto submit traj_051/ traj_052/ ... traj_077/ --name mlip_round1_g51_77

# 从文件读 workdir 列表
pbs-auto submit --from-list trajs.txt --name mlip_round1_g51_77
```

默认行为：
1. 扫描 + 队列校验 + 初始化 state
2. **双 fork 到后台 daemon**，父进程打印 PID 后退出
3. daemon 在 `~/.local/share/pbs_auto/batches/<batch_id>/` 下写 `daemon.pid`、`daemon.log`、`daemon.lock`
4. 主循环: `qstat` poll → 提交 PENDING → 更新状态 → 按两级 flush 落盘

父进程返回后终端立即可用，daemon 在后台运行直到所有任务终态或接收 SIGTERM。

### 5. 监控与管理

```bash
# 快照状态（读 summary.json，≤1 flush stale，<100 ms）
pbs-auto status mlip_round1_g51_77

# 完整状态（读 state.json，权威但较慢）
pbs-auto status mlip_round1_g51_77 --detail

# 实时日志
pbs-auto logs mlip_round1_g51_77 -f
pbs-auto logs mlip_round1_g51_77 --tail 100

# 列出所有批次（含 daemon 状态列）
pbs-auto list-batches

# 优雅停止（SIGTERM + 等 60s）
pbs-auto stop mlip_round1_g51_77
```

`status`、`stop`、`logs` 都接受 **name**、**完整 batch_id** 或 **batch_id 唯一前缀** 三种定位方式。

### 6. 恢复（自动）

daemon 崩溃（SIGKILL / OOM / login node 清理）后：

```bash
# 同样的命令再跑一次 —— batch_id 由 --name 决定，状态从磁盘恢复
pbs-auto submit traj_051/ ... --name mlip_round1_g51_77
```

启动时 daemon 会：
1. `_verify_lockf_works` 自检（NFSv3 锁可用性）
2. `acquire_lock` 取 `fcntl.lockf` 独占锁
3. `load_state` 权威状态（已完成任务保持 COMPLETED，不重提）
4. `cleanup_stale_artifacts` 清理 tempfile + 孤儿哨兵
5. `recover_sentinels` —— 按 job_id 直查 PBS，命中则 attach 回 SUBMITTED；orphan job 扫描报告用户手动 qdel 的遗漏
6. 进入主循环继续

## 命令参考

### `pbs-auto submit [ROOT_DIRS]... [OPTIONS]`

扫描 ROOT_DIRS 并提交任务（默认 daemon 模式）。

| 选项 | 说明 |
|------|------|
| `--name NAME` | 批次名，多 workdir 时必填；指定后 `batch_id = sha256(name)[:16]` |
| `--from-list FILE` | 从文件读 root 列表（一行一个路径，`#` 注释，`~` 展开，≤10000 行，≤4 KiB/行，拒绝 `/`, `/tmp`, `/var`） |
| `--server NAME` | 服务器配置名（对应 `[servers.NAME]`） |
| `--config PATH` | 指定配置文件路径 |
| `--dry-run` | 仅展示计划，不启动 daemon，不 submit |
| `--foreground` | 前台运行（不 fork），Ctrl+C 可停，同样会 `acquire_lock` |
| `--fresh` | 丢弃已保存状态，从头开始 |
| `--script-name NAME` | PBS 脚本文件名（默认 `script.sh`） |
| `--queue NAME` | 强制所有任务使用指定队列 |
| `--no-queue-validation` | 跳过队列合规性检查 |

**batch_id 生成规则**：
- 有 `--name`：`sha256(name)[:16]`
- 无 `--name` 且单 root：`sha256(resolve(root))[:16]`（向后兼容）
- 无 `--name` 且多 root：报错要求必填

### `pbs-auto status <NAME|BATCH_ID>`

显示批次快照。默认读 `summary.json` 快速返回；`--detail` 读 `state.json` 显示完整状态 + 问题任务详情。`summary.json` 最多比 `state.json` 陈旧 1 次 flush（约 1-15 s）。

### `pbs-auto stop <NAME|BATCH_ID> [--timeout SEC]`

发送 SIGTERM 到 daemon 并等待优雅退出。默认超时 60 s。验证流程：读 PID 文件 → 校对 `/proc/<pid>/stat` 的 btime 防 PID 重用 → 校对 `/proc/<pid>/cmdline` 含 `pbs-auto` → SIGTERM → 轮询等待 PID 消失。

### `pbs-auto logs <NAME|BATCH_ID> [--tail N] [-f]`

打印或跟踪 daemon 日志文件。`-f` 使用 `tail -f` 子进程（含 `--` 防 argv 注入），`--tail N` 只显示最后 N 行。日志路径会做 `is_relative_to(DEFAULT_STATE_DIR)` 安全校验。

### `pbs-auto list-batches`

列出所有保存的批次，优先读每个批次的 `summary.json`（快）。输出列：

| 列 | 说明 |
|----|------|
| Batch ID | batch_id 前 8 字符 |
| Name | `--name` 指定的名字 |
| Daemon | `running` / `stopped` |
| Roots | root 目录列表（最多 40 字符） |
| Server | 服务器配置名 |
| Tasks | 总任务数 |
| Status Summary | 各状态计数 |
| Updated | 最后更新时间 |

### `pbs-auto init`

创建默认配置文件模板。已存在时拒绝覆盖。

## 队列验证与自动选择

工具会根据配置文件中的队列规则自动验证和选择：

### 队列确定优先级

1. `--queue` CLI 参数 → 覆盖所有任务
2. 脚本中的 `#PBS -q` → 保留脚本指定
3. 自动选择 → 根据核心数和 walltime 选最紧凑合适的队列

### 合规性检查

提交前校验每个任务是否符合目标队列规则（核心数是否在 `allowed_cores`，节点数是否 ≤ `max_nodes`）。不合规任务会弹出警告表格并询问是否继续；选"否"则标记为 SKIPPED。

使用 `--no-queue-validation` 跳过所有检查。

## 配置文件

### 查找顺序

1. `--config` CLI 参数
2. `$PBS_AUTO_CONFIG` 环境变量
3. `~/.config/pbs_auto/config.toml`
4. 都不存在时使用 `config.py` 内置默认

### 完整示例

```toml
[defaults]
server = "server1"
script_name = "script.sh"
poll_interval = 15           # 状态轮询间隔（秒）
submit_delay = 2             # 连续 qsub 间隔（秒）
post_submit_check_delay = 60 # 提交后首次 PBS 查询延迟（秒）
early_exit_threshold = 30    # 运行时间 < 此值 → WARNING（秒）

[servers.server1]
name = "Chemistry Department"
status_command = "qstat"
status_args = ["-au", "$USER"]
max_running_cores = 192      # 用户 running 核数上限（PBS 配额）
max_queued_cores = 192       # 用户 queued 核数上限
core_granularity = 24

[servers.server1.queues.debug]
max_cores = 24
max_nodes = 1
max_walltime_hours = 0.5

[servers.server1.queues.long]
max_cores = 192
min_cores = 48
allowed_cores = [48, 96, 144, 192]   # 列表型约束
max_nodes = -1
max_walltime_hours = 360

[servers.server2]
name = "Group Server"
max_running_cores = 240
max_queued_cores = 400
core_granularity = 24

[servers.server2.queues.long]
max_cores = 192
min_cores = 48
allowed_cores = [48, 96, 192]
max_nodes = -1
max_walltime_hours = 360
```

**重要**：`max_running_cores` 和 `max_queued_cores` 必须匹配 PBS 服务器的 **每用户** 配额。查询当前用户配额：

```bash
qmgr -c 'list server' | grep "u:$USER"
# 输出: max_run_res.ncpus = [u:shaofl=240]
#       max_queued_res.ncpus = [u:shaofl=400]
```

低估会让 daemon 过早停止提交，高估会让 PBS 频繁退回 `would exceed ncpus`（会触发 retryable fallback 但增加噪音）。

## 文件布局

### 状态目录

```
~/.local/share/pbs_auto/batches/    0o700
  <batch_id>/                       0o700
    state.json          0o600  权威状态 (compact JSON, version=2)
    summary.json        0o600  快速列表缓存 (≤1 flush 陈旧)
    daemon.pid          0o600  "PID btime_ticks ISO"
    daemon.lock         0o600  fcntl.lockf by grandchild
    daemon.log          0o600  50 MB 尺寸轮转, 1 年尺寸轮转
    daemon.log.2025-0.gz       历史归档
    submitting/         0o700  哨兵分片目录
      ab/               0o700
        abcd1234...     0o600  3 行哨兵内容
```

所有目录 0o700 / 文件 0o600 由 daemon 的 `umask(0o077)` 强制。外部用户无法读取。

### 向后兼容

旧版 `<batch_id>.json` 扁平文件由 `migrate_layout` 在首次 `load_state` 时幂等搬迁到 `<batch_id>/state.json`，无需手动操作。遇到 symlink 或已存在两者冲突时会备份为 `<batch_id>.json.orig` 并警告。

## 任务状态

| 状态 | 含义 |
|------|------|
| `pending` | 等待提交 |
| `submitted` | 已 qsub，等待进入队列（SUBMITTED → QUEUED 或 RUNNING） |
| `queued` | PBS 队列中排队 |
| `running` | 正在运行 |
| `completed` | 正常完成 |
| `warning` | 异常完成：SUBMITTED 直接消失，或运行时间 < `early_exit_threshold` |
| `failed` | qsub 永久失败（无效队列、脚本不存在等） |
| `skipped` | 扫描时发现问题（缺 script.sh / 无法解析 nodes:ppn / 队列合规检查失败） |

## PBS 脚本要求

### 必需

每个任务的 `script.sh`（或 `--script-name` 指定的文件）必须包含：

```bash
#PBS -l nodes=X:ppn=Y
```

工具会解析 `nodes * ppn` 作为任务核数。无法解析则任务状态设为 SKIPPED。

### 可选但建议

```bash
#PBS -q <queue>              # 指定队列；留空则自动选
#PBS -l walltime=HH:MM:SS    # walltime 用于自动队列选择
```

### Job Name

**用户脚本中的 `#PBS -N` 会被 pbs-auto 的 `qsub -N pa_<batch_id[:6]>` 覆盖**。恢复机制按 **job_id**（从哨兵内容读取）匹配 PBS 作业，不依赖 job name；`pa_<batch_id[:6]>` 仅是 orphan scan 的识别前缀。

## Crash Recovery 保证

所有状态转换（PENDING → SUBMITTED → ...）都包在 `BatchStore.mutate()` context manager 里，外加 qsub 前写 / qsub 后重写的哨兵文件，保证在任意点崩溃都能正确恢复：

| 崩溃位置 | 恢复行为 |
|---|---|
| 哨兵创建前 | 重跑 |
| 哨兵写入后 / qsub 前 | recovery 读 `PENDING` → unlink → 下轮重新提交 |
| qsub 返回后 / 哨兵 update 前 | **唯一小窗口**（亚毫秒到 ~100 ms on NFS）：recovery orphan scan 报告用户手动 qdel |
| update 后 / save 前 | recovery 读 `<real_job_id>` → qstat 命中 → attach 为 SUBMITTED |
| save 后 / sentinel remove 前 | recovery 看任务已 SUBMITTED → 仅清 sentinel |
| sentinel remove 后 | 无害，下轮 poll 正常 |

详见 `context4agent/state-machine.md#crash-recovery-windows`。

## 常见场景

### MLIP 训练集批量（典型）

```bash
# 准备 77 条轨迹各自含 ~100 个单点子目录
pbs-auto submit traj_001/ traj_002/ ... traj_077/ --name mlip_round1 --dry-run
pbs-auto submit traj_001/ traj_002/ ... traj_077/ --name mlip_round1
# 立即返回，去睡觉

# 第二天查进度
pbs-auto status mlip_round1
pbs-auto logs mlip_round1 --tail 50
```

### 使用 VASP 脚本

```bash
pbs-auto submit ./workdir --script-name vasp.sh
```

### 强制重新提交（丢弃旧状态）

```bash
pbs-auto submit ./workdir --fresh
```

### 前台运行（调试）

```bash
pbs-auto submit ./workdir --foreground
```

### 从文件读 workdir 列表

```bash
cat > trajs.txt <<EOF
# MLIP round 1
~/projects/mlip/traj_001
~/projects/mlip/traj_002
~/projects/mlip/traj_003
EOF
pbs-auto submit --from-list trajs.txt --name mlip_round1
```

### 通过环境变量指定配置

```bash
export PBS_AUTO_CONFIG=/path/to/my_config.toml
pbs-auto submit ./workdir
```

## 故障排查

### `fcntl.lockf is not available` / `NFSv3 requires rpc.lockd`

Daemon 启动时自检锁服务失败。联系集群管理员启用 `rpc.lockd` / `nfslock` 服务；或改用 NFSv4 mount 的目录作为状态目录（通过 `$XDG_DATA_HOME` 或修改源码 `DEFAULT_STATE_DIR`）。

### `Another pbs-auto daemon owns batch <id>`

同一 batch 已有 daemon 在跑。用 `pbs-auto list-batches` 确认，或 `pbs-auto stop <name>` 停掉旧 daemon 再重跑。

### 任务被 PBS 拒绝 `would exceed ncpus`

配置的 `max_running_cores` 高于实际用户配额。用 `qmgr -c 'list server' | grep "u:$USER"` 查真实配额并更新 config.toml。

### 大量 WARNING 状态

检查任务实际运行时间：`warning` 多半是运行 < `early_exit_threshold`（默认 30s），通常是 CP2K 输入错误或 POTCAR/BASIS 缺失导致秒退。看 `pbs-auto status <name> --detail` 里的 error_message。

### `recover_sentinels: orphan PBS job detected`

崩溃窗口 3 的提示：qsub 成功但哨兵 update 未完成。日志会报告 job_id，需要手动 `qdel <job_id>` 避免该任务重复运行（pbs-auto 会在下轮重新提交相同内容）。

## 参考资料

- **架构**：`context4agent/architecture.md`
- **模块细节**：`context4agent/modules.md`
- **状态机 + 恢复窗口**：`context4agent/state-machine.md`
- **队列验证**：`context4agent/queue-validation.md`
- **测试策略**：`context4agent/testing.md`
