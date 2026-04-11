# 测试策略与用例说明

## 概览

- 测试框架: pytest >= 7.0
- 测试数量: 177 个
- 运行命令: `python -m pytest tests/ -v`
- 所有测试均不依赖真实 PBS 环境，可在任何机器上运行

## 测试文件分布

| 文件 | 用例数 | 覆盖模块 |
|------|--------|----------|
| test_config.py | 13 | config.py |
| test_scanner.py | 15 | scanner.py |
| test_pbs.py | 36 | pbs.py (解析函数 + PBSClient) |
| test_queue.py | 21 | queue.py |
| test_scheduler.py | 18 | scheduler.py (含 mutation context + index 维护) |
| test_state.py | 38 | state.py (JSON/migration/reconcile/identifier) |
| test_batch_store.py | 29 | batch_store/ (paths/sentinels/mutation/rotation) |

## 测试文件边界（与源码对称）

- **test_state.py**：`state.py` 的 JSON 序列化、migration、reconcile、identifier 解析、list_batches
- **test_batch_store.py**：`batch_store/` 的路径、权限、哨兵 CRUD、mutation context、日志轮转
- **test_scheduler.py**：scheduler 主循环逻辑、mutation context 使用、flag-only signals、index 维护
- **test_daemon.py**（规划中）：PID 文件、/proc/stat 解析、lockf helper（不真正 fork）

## 共享 Fixtures (conftest.py)

| Fixture | 说明 |
|---------|------|
| `server_config` | 测试用 ServerConfig (240/192 cores)，含 short/medium/long 队列 |
| `app_config` | 包含 server_config 的 AppConfig |
| `sample_tasks` | 3 个 PENDING 状态的 Task |
| `sample_batch` | 包含 sample_tasks 的 BatchState，`root_directories=["/tmp/tasks"]` + `name="sample"`，tasks 按 directory 索引 |
| `workdir` | tmp_path 下创建 5 个任务子目录 (含 script.sh)，核数交替 24/48 |

`test_state.py` 和 `test_batch_store.py` 都用 module-level `_isolate_state_dir` autouse fixture 把 `DEFAULT_STATE_DIR` 重定向到 `tmp_path`，避免污染用户真实 state 目录。

## 测试策略

### 1. 配置测试 (test_config.py)

- **TestParseConfig**: 验证 TOML 解析 → AppConfig 转换
  - 默认配置完整性
  - 自定义值覆盖
  - 服务器配置不存在时报错
- **TestQueueConfigParsing**: 队列配置解析
  - 默认配置包含队列 / server1 队列值验证 / server2 队列验证
  - 无队列配置时默认空字典
- **TestLoadConfig**: 验证配置加载优先级
  - 显式路径加载
  - 不存在文件报错
  - 无配置时使用默认值
- **TestInitConfig**: 验证配置文件创建
  - 正常创建
  - 已存在时拒绝覆盖

### 2. 扫描器测试 (test_scanner.py)

- **TestNaturalSort**: 纯数字和混合排序
- **TestParseCores**: PBS 资源解析
  - `nodes=1:ppn=96` → 96
  - `nodes=4:ppn=24` → 96
  - 带空格格式
  - 无资源行 → None
  - 文件不存在 → None
- **TestParseScriptResources**: 完整脚本资源解析
  - 含 queue + walltime → 全部字段
  - 无 queue / 无 walltime / 无资源行 / 文件不存在
  - walltime 含分秒的精确解析
- **TestScanDirectory**: 目录扫描
  - 基本扫描 + 排序验证
  - 核数正确解析
  - 缺少脚本 → SKIPPED
  - 无法解析 → SKIPPED
  - 不存在目录 → FileNotFoundError
  - 空目录 → 空列表
  - 自定义脚本名
  - 扫描填充 queue 和 nodes 字段
  - 忽略根目录文件

### 3. PBS 解析测试 (test_pbs.py)

使用固化的命令输出字符串 (canned output) 测试解析器。

- **TestParseQstat**: qstat -au 输出解析
  - 多任务：3 个任务 (2R + 1Q)，验证 job_id/name/state/cores/queue
  - 单任务：验证多节点核数
  - 空输出 / 仅表头
- **TestParseQ**: q 命令输出解析
  - 带装饰行、emoji、续行的真实输出
  - 验证续行不计为独立作业
  - 空队列输出
  - 完全空输出

**关键测试数据**: `Q_OUTPUT` 常量来自 `requirements/successfully_submitted.md`，包含真实服务器输出格式。

### 4. 队列验证测试 (test_queue.py)

- **TestValidateTaskForQueue**: 单任务合规性验证
  - 范围型队列合规 / 列表型队列合规
  - 核心数不在允许列表 / 低于最小值 / 超过最大值
  - 节点数超限 / 无限制节点 / 多错误同时存在
- **TestSelectQueue**: 自动选择队列
  - 优先选最短 walltime / walltime 过滤
  - 48/96/192 核心各场景
  - 无匹配 / 空队列
- **TestValidateAndAssignQueues**: 批量验证与分配
  - CLI 队列覆盖脚本 / 脚本队列保留 / 自动选择
  - 不合规任务标记 / SKIPPED 任务忽略 / 空队列

### 5. 调度器测试 (test_scheduler.py)

使用 `FakePBSClient` 和 `FakeDisplay` 替代真实依赖。

#### FakePBSClient

```python
class FakePBSClient:
    jobs: dict[str, PBSJobInfo]  # 模拟 PBS 当前状态
    submitted: list[str]          # 记录提交历史
    _submit_counter: int          # 自增 job_id
```

- **TestSchedulerResourceCheck**: 资源统计逻辑
  - 空队列 → (0, 0)
  - 有 R 和 Q 作业 → 正确统计
  - _all_done 判定逻辑
- **TestHandleJobDisappeared**: 作业消失处理
  - SUBMITTED 消失 → WARNING
  - 短时间运行 → WARNING + 包含秒数的 error_message
  - 长时间运行 → COMPLETED
- **TestSubmitTaskRetry**: 可重试错误处理
  - `would exceed` 错误 → 任务保持 PENDING + error_message 含 "Retryable"
  - 非重试错误 → 任务变为 FAILED
  - `FileNotFoundError` → 任务变为 FAILED（永久）
  - 可重试错误 → 停止本轮提交（后续任务不再尝试）
  - 重试成功 → 清除之前的 error_message
- **TestPollStatus**: 轮询状态更新
  - PBS 报告 R → task 变为 RUNNING + 记录 start_time
  - PBS 报告 Q → task 变为 QUEUED

### 6. 持久化测试 (test_state.py)

所有测试使用 `monkeypatch` 重定向 `DEFAULT_STATE_DIR` 到 `tmp_path`。

- **TestBatchId**: ID 生成
  - 确定性 (同路径 → 同 ID)
  - 唯一性 (不同路径 → 不同 ID)
  - 长度为 16
- **TestSaveLoad**: 序列化/反序列化
  - 完整 roundtrip (所有字段)
  - 不存在返回 None
  - 自动创建目录
  - 输出为合法 JSON
- **TestReconcile**: 状态合并
  - 新任务添加
  - SUBMITTED 重置为 PENDING
  - COMPLETED 保持不变
  - RUNNING 保持不变（含 job_id）
- **TestBackwardCompat**: 向后兼容性
  - 旧 JSON 缺少 queue/nodes 字段仍可加载
  - reconcile 同步新字段 (queue/nodes)
- **TestListBatches**: 批次列表
  - 空目录
  - 有批次时返回摘要

## 测试模式说明

### Mock/Fake 策略

- **PBSClient**: 使用 FakePBSClient (in-memory dict)，不调用 subprocess
- **Display**: 使用 FakeDisplay (no-op)，不使用 Rich
- **文件系统**: 使用 pytest `tmp_path` fixture
- **配置路径**: 使用 `monkeypatch.setattr` 替换默认路径常量

### 未测试部分

- `display.py`: UI 渲染层，不做单元测试
- `cli.py`: 入口层，通过集成测试覆盖 (CLI dry-run)

## 添加新测试的规范

1. 新模块的测试文件放在 `tests/test_<module>.py`
2. 使用 `conftest.py` 中的共享 fixtures
3. 测试类以 `Test` 前缀分组，方法以 `test_` 前缀
4. PBS 输出使用固化字符串常量，不依赖真实命令
5. 文件系统操作使用 `tmp_path`，路径常量用 `monkeypatch` 覆盖
