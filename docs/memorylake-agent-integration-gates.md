# MemoryLake Agent 接线：方案 2 的原生保留与新读授权

状态：**用户已选择方案 2：普通完整对话、其他工具可用、保存到原会话。生产 Agent 接线尚未启用。** 不实施专用隔离图，不关闭或临时修改共享原生 callbacks、checkpoint、memory。人工 HTTP/UI 文件闭环不能代替 Agent 检索。

**另一个兼容性决定仍未回答**：旧共享 Agent 是经维护窗口转为 owner 专用，还是实施每用户 workspace/memory 隔离。已向用户询问；当前不能默认实施任一项。用户选择完整原生对话，并不等于授权旧共享实例的隐私迁移策略。未来共享 guard 也不能代替旧 viewer 的排空与隔离。

本记录只调查当前仓库及已锁定安装的 `octop-harness`，不解除 `GlobalProcessor` 的 `memorylake_production_lifecycle_unverified` 拒绝，不旁路 observer，也不修改依赖或 CLI。以下安装包路径均相对于 `.venv/lib/python3.12/site-packages/`，行号是调查时位置。

## 已确认的数据接收点

| 位置 | 实际接收内容 / 限制 |
| --- | --- |
| `src/octop/infra/gateway/process/processor.py:1175–1228` | `_begin_history`、trajectory 用户 query/上下文、`history_tracker.observe` 与 `flush_tracker`、trajectory chunk 都先于对外 yield；包含用户输入和工具输出，不能靠过滤最终响应阻止。 |
| 同文件 `:198–244, 1294–1319, 1774–1803` | 历史初始化、错误/未完成轮次持久化、结束后的 thread history projection 都是独立写入面。只禁正常路径会遗漏异常/finally 路径。 |
| 同文件 `:1211–1218` | `iter_with_session_auto_approve` 可再次调用 `resume_hitl`。现有 typed channel 禁 continuation 不等于 processor 已禁自动恢复。 |
| `octop_harness/agent.py:1309–1319`；`octop_harness/manager.py:180,207` | Langfuse callbacks 绑定共享 graph；`langfuse/langchain/CallbackHandler.py:884,1099,1313` 分别接收模型 messages、工具 output、模型 response。请求 `callbacks=[]` 不会清除已绑定回调。 |
| `octop_harness/middleware/memory.py:318–384,1129–1178` | MemoryService 在 model 后过滤为用户与最终 assistant，可保存 MemoryLake 衍生回答；原始 ToolMessage / tool-calling assistant 不直接进入该 capture。 |
| 同文件 `:886–928,1040–1058`；`octop_harness/config/__init__.py:660–663` | 默认 session JSONL 实测用户与最终回答。实际 writer 选择 first HumanMessage / last AIMessage，只写 content 等元数据，不序列化 ToolMessage/tool_calls；不能依据上游注释“full message stream”断言 JSONL 保存工具原文。已核配置仅 enabled/max_bytes/dir（及 memory_jsonl_enabled），未发现此 writer 的 include_tools 开关。工具详细记录另属可配置 trajectory/Langfuse 或 checkpoint/history，不应混称 session JSONL。 |
| `octop_harness/agent.py:1867–1917` | 默认复用 Memory checkpointer，否则创建 SQLite saver。graph 写入发生在 processor 收到结果之前；`ChatRequest` 没有 per-turn checkpointer 开关。 |
| 同文件 `:1814–1843` | `deepagents.create_deep_agent` 内建 summarization 还可能把历史交给 seed model/backend；只禁 checkpoint、memory、session 不足以证明所有接收面已隔离。 |
| `src/octop/infra/gateway/ws/ws_channel.py:174–225` | `turn.started` 及后续 chunk 发给 thread 订阅者。Hub registration nonce 防止同 ID 重注册误投，但不是每个订阅者的实时 SQL 授权。 |

`origin.py:117–144` 的 identity 检查覆盖存活、可信 socket source、agent/thread 与期限；SQL `owns_dashboard_thread` 在 `:318–330` 等 issue/consume 阶段执行，不能称为每次模型/发送前的 fresh SQL 检查。工具本身另有 binding/credential 授权。

现有 `middleware.py::dashboard_read_turn` 以 ContextVar 保持一次调用工具，finally 先关闭 identity 再 reset；编译 agent ID 不取自客户端 config。该机制并不能撤回已经交给 callback、checkpointer 或网络发送函数的数据。

## 纯内存证据

新增 `tests/unit/connectors/test_memorylake_agent_lifecycle_probe.py` 使用真实 LangGraph `StateGraph`、`InMemorySaver`、回调与 Harness `ChatRequest`，不调用模型/CLI/网络：

1. `ChatRequest(callbacks=[])` 输出配置没有 callbacks 字段。
2. 即便显式写入 `config['callbacks']=[]`，graph 既有回调仍收到合成 query。
3. checkpoint 仍保存合成工具结果。

```sh
UV_CACHE_DIR=/tmp/octop-uv-cache uv run --locked --offline pytest tests/unit/connectors/test_memorylake_agent_lifecycle_probe.py -q
```

实测 **1 passed**；该文件 Ruff 检查通过。它证明不能用空 callbacks 清掉共享图的接收者，不证明任何生产 Agent 接线已安全。未运行此前被禁止的全套测试或 `make all`。

## 已选方案 2：合法接纳后保留，新读取重新授权

1. **新读取边界**：MemoryLake 工具执行及返回结果前，验证可信 caller/thread、当前 binding/credential revision，结果有界、脱敏后才交给原生 Agent 图。只保存 thread/user 字符串、ToolMessage 或批准 HITL 不会生成新的 MemoryLake grant。
2. **已合法交付内容**：原生 checkpoint/history/trajectory、默认 JSONL 的用户/assistant、MemoryService 的最终衍生答案，以及已配置的 summary/offload/Langfuse，沿用各自原生会话 ACL、工作区/运维访问及保留配置。不能把它们全部说成同一 thread ACL，更不能承诺撤销已发送的外部 trace。
3. **撤销语义**：连接撤销阻止后续 MemoryLake 读取及尚未合法交付的工具结果；不会自动删历史、摘要、原生 memory、日志或已经发出的消息。删除这些副本是独立产品/运维政策，不在本轮自动执行。
4. **后续 turn / HITL 恢复**：原生线程 ACL 允许的调用可继续使用已保存上下文；若没有新的可信活跃 MemoryLake scope，新的 MemoryLake tool call 必须返回 unavailable。普通工具与原生恢复语义保留。当前 `MemoryLakeReadMiddleware` 的“scope不存在”路径支持此区分；出现失效 scope 时仍 fail closed，不能暗降级绕过正在进行的授权检查。
5. **观察者接纳点**：不试图在 processor 最后 yield 才撤回此前已进入 graph 的数据；需验证工具→原生图的接纳边界，以及真实框架各观察者的调用顺序。现有调查/小 probe 不是生产全链路已安全的证明。

## 原生 thread ACL 与 HITL 修复

- `api/routers/chat/history.py::_require_thread` 核对 agent access、thread.agent_id 和 thread.user_id。`chat/turn.py::resolve_thread_id` 对显式 thread 同样核 owner。
- 新发现并修复：`chat/routes.py::resume_hitl` 原先仅核 agent access 与可选 pending；无 pending 或过时 pending 时仍可能沿任意 thread_id 恢复，shared agent 的访问权限不等于别人的 thread 权限。
- 现已在 pending 解析、session policy 修改和 SSE 构造之前复用 `_require_thread(..., as_user=None)`。匹配 pending 不能绕过现态线程归属；不新增 admin 线程绕过。
- 该入口检查沿用原生 ACL，不声称已完成流持续期间的全局撤权机制；生产 MemoryLake 接线门禁仍保留。

## 新增恢复与 ACL 证据

`tests/unit/connectors/test_memorylake_native_resume_policy.py`（真实 LangChain graph、InMemorySaver、HumanInTheLoopMiddleware；合成 SQLite/Fernet、fake model/runner）：

- 可信 turn 的 ToolMessage 合法存入 checkpoint 后，禁用连接；后续无 scope turn 仍保留历史，但新的 MemoryLake 请求 unavailable，runner 计数不增加。
- 真正 HITL interrupt 后 `Command.resume` 批准，不恢复已关闭 identity；无 scope 的新读拒绝，runner 零调用。
- 不把 checkpoint 访问直接等同于 HTTP 已认证：这两项假设原生线程 ACL 已通过，HTTP ACL 由下述独立测试覆盖。

`tests/unit/api/test_memorylake_native_hitl_owner.py` 调用真实 `resume_hitl` 路由，真实临时 SQLite Agent/ThreadRepo、真实 HitlChannelCoordinator，外围执行器使用 fake：跨用户同 shared agent、其他 agent thread、缺失 thread × 无 pending / 匹配 pending，先得到 **6 个失败子项**；修复后全部拒绝，合法 owner 两种恢复正常。

```sh
UV_CACHE_DIR=/tmp/octop-uv-cache uv run --locked --offline pytest \
  tests/unit/api/test_memorylake_native_hitl_owner.py \
  tests/unit/api/test_chat_hitl_resume.py \
  tests/unit/api/test_hitl_resume_validation.py \
  tests/unit/connectors/test_memorylake_native_resume_policy.py -q
```

结果 **25 passed，8 subtests passed**；相关 Ruff、routes.py mypy 与 diff-check 通过。无真实模型、CLI、MemoryLake 服务、网络或全套测试执行。专用隔离 turn 仅为已放弃的前期候选，不作为本轮实施方向。

## 当前逐项接线状态（补充验证）

| 接收面 / 生命周期 | 已有证据 | 未闭环项 |
| --- | --- | --- |
| 普通工具与 MemoryLake 工具循环 | `test_memorylake_native_harness.py` 使用公开真实 HarnessAgent，native_echo 与 MemoryLake fake runner 同轮工作，原生 checkpoint/history 保留；无 scope 仍保留普通工具、不能从 config 伪造 MemoryLake 权限。 | 尚未注册到生产 AgentManager/GlobalProcessor；不是“用户现在可在 Agent 使用”的声明。 |
| 工具结果接纳 | 同测试的 `test_material_committed_before_next_model_and_cannot_share`、`test_material_admission_failure_never_delivers_document_to_model` 检查持久化标记先于后续模型接收，以及接纳失败不交付结果。 | 不能用该边界宣称发布、导出、peer 或所有原生数据传播已隔离。 |
| 原生 checkpoint / history | 真实 Harness 与 `test_memorylake_native_resume_policy.py` 验证已保存 ToolMessage 可留存，连接撤销不自动清除历史；新读仍需 fresh grant。 | 入口鉴权、未来生产全链路与所有恢复场景还需组合验证。 |
| HITL | `resume_hitl` 已复用线程 owner ACL；真实 HITL Command.resume 不会重建已失效的 MemoryLake scope。 | 不宣称所有长流/并发恢复已经持续重新授权；不解除生产 gate。 |
| MemoryService / session JSONL | MemoryService 保存最终衍生答案；默认 JSONL 实测用户/assistant，不直接存 ToolMessage。接收面与配置见上表。 | 结构化 memory 自动后台提取/atoms 不在下面显式 capture probe 的证明范围。 |
| 共享原生 memory | `test_memorylake_native_memory_sharing.py` 真实 Harness 得到 ML 辅助最终答案，显式调用公开 `MemoryService.capture_turn`，同 SQLite 重开后仍保存 marker；027 接纳标记现使新的 `set_shared` 拒绝，native shared-member 准入仍拒绝 viewer。 | 这是原生持久化与后续新增共享保护的证据，不是旧 viewer 已排空或支持共享的安全验证。自动 capture 桥在测试中显式执行，不能声称完整后台链已验证。 |
| 旧 viewer in-flight | 同文件 `test_inflight_shared_viewer_reads_new_owner_capture_after_unshare`：共享时已获准运行的 viewer 在取消共享后仍能通过同一 native memory 实例读到后来 owner capture 的 marker；viewer 没有继承 ML scope。 | `is_shared=False` 不会排空旧调用。维护转专用与每用户隔离仍待选择；不得默认仅切私有就安全。 |
| summary/offload、trajectory、Langfuse | 已定位实际接收时点与原生配置；方案 2 不临时关闭共享 callbacks。 | 不是已完成全 DLP/外部 trace 删除验证；仍按明确的原生保留/运维访问政策处理。 |
| WebSocket 最终交付 | 可信 connection ref / nonce 与 identity 生命周期已有离线切片。 | 不能撤回已发消息；不把 thread 订阅广播等同于逐接收者 fresh SQL 校验。 |
| publish / peer / export / partial PATCH | 是已识别的其他权限扩大/副本传播入口。 | 尚未端到端闭环，不能以单个 sharing guard 代替。 |

### 027 标记：只解决未来共享事务，不解决旧调用

`027_memorylake_agent_admissions.sql` / `.pg.sql` 仅存 agent_id、owner_user_id、首次接纳时间，无内容/密钥、无资源 FK。`MemoryLakeBindingRepo.admit_agent_material` 同事务核对当前私有 expert owner、dashboard thread、binding/credential revision 与凭据有效期，再提交标记；`AgentRepo.set_shared(True)` 与其使用同一 agent 行线性化顺序。已标记实例删除 binding 或同 ID 重建都不能因此恢复共享。

`agent_material_owner` 仅真缺行返回 None；缺表、SQL 失败、坏 owner 不当作无标记。SQLite 双连接正反交错已有测试，PostgreSQL 仅 recorded SQL 契约，不能声称 live PostgreSQL 并发通过。高水位幂等 schema repair 不代表能恢复从备份中丢失的既有标记。

以上保护是**接纳之后的未来 share**：不取消旧 viewer、不清其持有对象、不隔离已共享的 workspace/memory，也不自动阻止所有发布/复制路径。旧共享兼容性未决期间保持 fail closed，不接生产入口。

### Reader 与 factory 的共享资源要求

当前 `storage.py::StoredMemoryLakeReader`、`langchain.py` factory、`middleware.py::dashboard_read_turn` 强制使用 server-owned `DatabaseJobAdmission`；缺参或 None 拒绝，已删除按 reader 创建额度或走无界 executor 的 fallback。实际源码与 `test_memorylake_reader_admission.py`、`test_memorylake_langchain.py`、`test_memorylake_middleware.py` 包含缺参拒绝、同实例传递、取消后实际 worker 仍占额等检查。

协调者报告该 reader/factory 强制共享 DB 切片回归为 **99 tests / 101 subtests**；这是切片结果，不是全仓测试总数。本次仅更新文档，没有重新运行该组合，也不补造其精确命令。可独立核验的测试文件还包括：

- `tests/unit/connectors/test_memorylake_native_harness.py`
- `tests/unit/connectors/test_memorylake_native_memory_sharing.py`
- `tests/unit/connectors/test_memorylake_native_resume_policy.py`
- `tests/unit/connectors/test_memorylake_reader_admission.py`
- `tests/unit/db/test_memorylake_agent_admission.py`

本轮没有修改共享 graph/observer 配置、启用 Agent 入口、修改 CLI、执行真实服务或运行被拒绝的全套 `make all`。原生完整对话方案已选；旧共享 Agent 的兼容实现尚未选。
