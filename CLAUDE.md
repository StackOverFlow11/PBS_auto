# PBS Auto-Submit — AI Agent 开发规范

## 项目简介

PBS 集群批量任务自动提交工具。Python 3.10 + Click + Rich + tomli，运行在 CentOS 7 + PBS Pro 环境。支持 daemon 模式（断 SSH 不影响运行）、多 workdir 批次、事件驱动持久化 + 哨兵文件 crash 恢复。

## 快速命令

```bash
pip install -e ".[dev]"                                                  # 安装 (含 pytest)
python -m pytest tests/ -v                                               # 运行测试 (177 个)
pbs-auto submit ./requirements/wordir_example --dry-run                  # 单 workdir dry-run
pbs-auto submit traj_001 traj_002 --name mlip_round1 --foreground        # 多 workdir 前台
pbs-auto submit traj_001 traj_002 --name mlip_round1                     # 默认 daemon 后台
pbs-auto status mlip_round1                                              # 快照状态 (读 summary.json)
pbs-auto logs mlip_round1 -f                                             # 实时日志 (tail -f)
pbs-auto stop mlip_round1                                                # 优雅停止 daemon
pbs-auto list-batches                                                    # 列出所有批次
```

## 项目结构

```
src/pbs_auto/
  models.py                 → 数据模型 (TaskStatus, Task, PBSJobInfo, BatchState + 索引)
  config.py                 → 配置加载 (~/.config/pbs_auto/config.toml) + QueueConfig
  scanner.py                → 目录扫描 + #PBS -l/-q/walltime 解析
  queue.py                  → 队列验证与自动选择
  state.py                  → JSON 持久化 + migration + reconcile + identifier 解析
  batch_store/              → 磁盘布局 + 哨兵 + 日志轮转 + mutation (单一权威)
    __init__.py             →   公开 API facade + __all__
    _paths.py               →   路径常量 + migrate_layout 幂等 5 分支
    _sentinels.py           →   哨兵 CRUD + content-authoritative recover
    _rotation.py            →   startup + in-loop log 轮转
    _mutation.py            →   BatchStore + _MutationContext (never-raise)
  pbs.py                    → PBS 命令交互 + batch_id informational prefix
  scheduler.py              → 提交引擎主循环 (零 Rich, flag-only signals)
  daemon.py                 → 双 fork + fcntl.lockf + PID 文件 + stop
  cli.py                    → Click CLI (submit/status/stop/logs/list-batches/init)
tests/                      → 177 个单元测试 (pytest)
```

## 详细文档索引 (context4agent/)

| 文件 | 内容 |
|------|------|
| [architecture.md](context4agent/architecture.md) | 项目结构、模块依赖图、核心数据流、关键设计决策 |
| [modules.md](context4agent/modules.md) | 每个模块的类/函数/字段详细说明 |
| [queue-validation.md](context4agent/queue-validation.md) | queue.py 模块说明、验证规则、自动选择算法 |
| [state-machine.md](context4agent/state-machine.md) | 8 个 TaskStatus 的完整转换图 + crash recovery windows |
| [testing.md](context4agent/testing.md) | 测试策略、fixture 说明、各测试文件用例分类 |

## 关键设计约束

- **state.json 是权威真相，summary.json 是 ≤1 flush 陈旧的只读缓存**。`list-batches` 和 `status` 优先读 summary，`status --detail` 读 state
- **两级 flush**：qsub 成功 → eager flush；poll 转换 → debounced flush (≥1s 间隔)；FAILED 提升为 eager
- **哨兵内容 = 3 行 ASCII**：`<directory>\n<job_id|PENDING>\n<iso_ts>\n`，恢复时按 job_id 直查 PBS
- **并发互斥**：`fcntl.lockf(<batch>/daemon.lock, LOCK_EX|LOCK_NB)`（NFSv3 需 rpc.lockd 可用，daemon 启动时自检）
- **文件权限**：目录 0o700，文件 0o600，`umask(0o077)` 在 daemonize 中设置
- **Task dict key 是 directory 绝对路径**（不是 task.name），支持多 workdir 跨父目录同名

## 开发规范

### 代码风格

- Python 3.10 兼容（使用 `from __future__ import annotations`）
- dataclass 用于数据模型，提供 `to_dict()`/`from_dict()` 序列化
- CLI 命令中使用延迟导入避免启动时加载重量级模块
- 配置/状态路径遵循 XDG 规范 (~/.config/ 和 ~/.local/share/)

### 修改检查清单

1. 修改 `models.py` 中的字段 → 同步更新 `to_dict()`/`from_dict()` + 状态 JSON 兼容性
2. 修改状态转换逻辑 → 更新 `context4agent/state-machine.md`
3. 修改 PBS 解析 → 确保对应 `test_pbs.py` canned output 测试通过
4. 新增模块 → 在 `context4agent/modules.md` 添加说明，更新 `architecture.md` 依赖图
5. 新增 CLI 命令 → 更新 `docs/usage.md` 命令参考
6. 新增配置项 → 更新 `examples/config.toml` 和 `config.py:DEFAULT_CONFIG_CONTENT`

### 测试要求

- 所有修改必须通过 `python -m pytest tests/ -v` 全部通过
- PBS 命令解析使用固化字符串测试，不依赖真实环境
- 文件系统操作使用 pytest `tmp_path`
- 路径常量用 `monkeypatch` 覆盖

### context4agent 维护要求

**每次开发完成后必须**：
1. 检查 `context4agent/` 下的文档是否需要更新
2. 如有新模块/重大变更，补充对应的 context4agent 文档
3. 更新本文件 (CLAUDE.md) 中的「详细文档索引」表格，确保指向正确
4. 新增 context4agent 文件时在索引表格中添加对应条目

## 需求参考（只读）

- `requirements/requirments.md` — 原始需求文档
- `requirements/PBS.md` — PBS 命令参考
- `requirements/queue_specs.md` — 队列规格参考（server1/server2 的队列资源规则）
- `requirements/successfully_submitted.md` — q 命令输出格式样本
- `requirements/scripts_template/` — PBS 脚本模板
- `requirements/wordir_example/` — 工作目录结构示例
