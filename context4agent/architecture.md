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
│   ├── state-machine.md              # 任务状态机文档
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
│   ├── cli.py                        # Click CLI 命令定义
│   ├── config.py                     # 配置加载与验证
│   ├── models.py                     # 数据模型 (核心类型定义)
│   ├── state.py                      # JSON 持久化
│   ├── scanner.py                    # 目录扫描 + 脚本解析
│   ├── queue.py                     # 队列验证与自动选择
│   ├── pbs.py                        # PBS 交互 (qsub/qstat/q)
│   ├── scheduler.py                  # 提交引擎 (主循环)
│   └── display.py                    # Rich Live CLI 界面
└── tests/                            # 单元测试 (~89 个)
    ├── conftest.py                   # 共享 fixtures
    ├── test_config.py                # 配置测试 (13)
    ├── test_scanner.py               # 扫描器测试 (15)
    ├── test_pbs.py                   # PBS 解析测试 (8)
    ├── test_queue.py                 # 队列验证测试 (21)
    ├── test_scheduler.py             # 调度器测试 (10)
    └── test_state.py                 # 持久化测试 (12)
```

## 模块依赖图

```
cli.py ──────────────────────────┐
  │                              │
  ├─→ config.py                  │
  ├─→ scanner.py ─→ models.py   │
  ├─→ queue.py ──→ models.py    │
  │               → config.py   │
  ├─→ state.py ──→ models.py    │
  │               → config.py   │
  ├─→ pbs.py ───→ models.py    │
  │               → config.py   │
  ├─→ scheduler.py              │
  │     ├─→ config.py           │
  │     ├─→ models.py           │
  │     ├─→ pbs.py              │
  │     ├─→ display.py          │
  │     └─→ state.py            │
  └─→ display.py ─→ models.py  │
                    → config.py │
```

依赖方向：`models.py` 是零依赖的基础层，`config.py` 仅依赖标准库 + tomli，其余模块均依赖这两个基础模块。

## 核心数据流

```
用户执行 pbs-auto submit ./workdir
         │
    cli.py: 加载配置 → 扫描目录 → 队列验证/分配 → 加载/创建状态
         │
    scheduler.py: 进入主循环
         │
         ├── 1. _poll_status()
         │      PBSClient.query_user_jobs() → qstat/q → 解析输出
         │      对比内部状态与 PBS 实际状态 → 更新 Task.status
         │
         ├── 2. _submit_pending()
         │      遍历 PENDING 任务
         │      检查资源限额 (从 PBS 查询实时 R/Q 核数)
         │      PBSClient.submit(task) → qsub → 获取 job_id
         │
         ├── 3. save_state()
         │      BatchState → JSON → 原子写入文件
         │
         ├── 4. display.refresh()
         │      Rich Live 面板更新
         │
         ├── 5. _all_done()? → 退出循环
         │
         └── 6. _sleep(poll_interval)
                可被 SIGINT 中断
```

## 关键设计决策

### 1. 资源统计以 PBS 为准

`_get_resource_usage()` 每次调用 `qstat` 获取用户所有作业的实时核数，而非仅统计本工具管理的任务。因为用户可能通过其他途径提交了作业。

### 2. 原子写入

`state.py` 中的 `save_state()` 使用 `tempfile.mkstemp()` + `Path.replace()` 实现原子写入，防止中途断电导致状态文件损坏。

### 3. 5 秒查询缓存

`PBSClient` 内部缓存 `query_user_jobs()` 结果 5 秒，避免同一轮询周期内重复调用 qstat（`_poll_status` 和 `_submit_pending` 都需要查询）。每次轮询周期开始时调用 `invalidate_cache()` 强制刷新。

### 4. 配置独立于项目目录

配置文件在 `~/.config/pbs_auto/config.toml`，状态文件在 `~/.local/share/pbs_auto/batches/`，遵循 XDG Base Directory 规范，不污染项目目录。

### 5. Batch ID 由目录路径哈希生成

`generate_batch_id()` 对工作目录绝对路径做 SHA256 取前 16 字符，同一目录总是得到相同 ID，实现自动恢复。

### 6. SIGINT 双击退出

第一次 Ctrl+C 设置 `_shutdown` 标志，主循环会在当前操作完成后保存状态退出；第二次 Ctrl+C 触发 `KeyboardInterrupt` 强制退出。

## 第三方依赖

| 包 | 版本 | 用途 |
|---|---|---|
| click | >= 8.0 | CLI 框架 |
| rich | >= 13.0 | 终端 UI (Live, Table, Panel, ProgressBar) |
| tomli | >= 2.0 | TOML 解析 (Python 3.10 无内置 tomllib) |
| pytest | >= 7.0 | 测试 (dev 依赖) |

## 入口点

- CLI 命令: `pbs-auto` → `pbs_auto.cli:cli` (pyproject.toml `[project.scripts]`)
- 模块执行: `python -m pbs_auto` → `pbs_auto.__main__`
