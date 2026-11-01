"""Real graph composition, NOT production middleware or trusted turn authorization.

The original graph probes supply scope manually in a test ContextVar. The queue
composition additionally exercises accepted same-process origin delivery, but
issuance still uses manually trusted test inputs, not a registered auth endpoint.
It does not establish QueueFull acknowledgements, cross-worker delivery, or
HITL/peer/Ask/Plan lifecycle safety. No graph capabilities are placed in state or
configurable metadata; the middleware and consumer here are test-only shims.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
import unittest
from contextvars import ContextVar
from pathlib import Path
from unittest.mock import patch

from langchain.agents import create_agent
from langchain.agents.middleware import AgentMiddleware
from langchain_core.callbacks import AsyncCallbackHandler
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.memory import InMemorySaver
from octop_gateway.manager import ChannelManager
from octop_gateway.models import ChannelSubject, TextContent
from pydantic import Field
from tests.support.memorylake import offline_test_guard

from octop.infra.connectors.crypto import encrypt_credentials
from octop.infra.connectors.memorylake.admission import ReadAdmission
from octop.infra.connectors.memorylake.langchain import build_dashboard_read_tools
from octop.infra.connectors.memorylake.origin import (
    DashboardInbound,
    DashboardOriginError,
    DashboardOriginRegistry,
)
from octop.infra.connectors.memorylake.runner import ProcessResult
from octop.infra.connectors.memorylake.submission_storage import DatabaseJobAdmission
from octop.infra.db.migrate import run_migrations
from octop.infra.db.pool import SqlitePool
from octop.infra.db.repos.agents import AgentRepo
from octop.infra.db.repos.connectors import ConnectorRepo
from octop.infra.db.repos.memorylake import MemoryLakeBindingRepo
from octop.infra.db.repos.secrets import SecretRepo
from octop.infra.db.repos.threads import ThreadRepo
from octop.infra.db.repos.users import UserRepo
from octop.infra.gateway.ws.ws_channel import WS_CHANNEL_ID, WebSocketChannel
from octop.infra.gateway.ws.ws_hub import WebSocketHub

_SCOPE = ContextVar("test_only_memorylake_tools", default=())
_TOOL = "memorylake_search_documents"


class _Model(BaseChatModel):
    arguments: dict = Field(default_factory=lambda: {"query": "synthetic"})
    offered: tuple[str, ...] = ()
    observations: list = Field(default_factory=list)
    force_call: bool = False
    tool_name: str = _TOOL

    @property
    def _llm_type(self):
        return "offline-test-only"

    def bind_tools(self, tools, **kwargs):
        names = tuple(tool.name for tool in tools)
        return self.model_copy(update={"offered": names})

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        self.observations.append(self.offered)
        if isinstance(messages[-1], ToolMessage) or not (self.force_call or _TOOL in self.offered):
            message = AIMessage(content="done")
        else:
            message = AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": self.tool_name,
                        "args": self.arguments,
                        "id": "call-synthetic",
                        "type": "tool_call",
                    }
                ],
            )
        return ChatResult(generations=[ChatGeneration(message=message)])

    async def _agenerate(self, messages, stop=None, run_manager=None, **kwargs):
        return self._generate(messages, stop, run_manager, **kwargs)


class _DynamicTools(AgentMiddleware):
    """Test-only stateless override probe, not an authentication mechanism."""

    async def awrap_model_call(self, request, handler):
        return await handler(request.override(tools=[*request.tools, *_SCOPE.get()]))

    async def awrap_tool_call(self, request, handler):
        tool = next((tool for tool in _SCOPE.get() if tool.name == request.tool_call["name"]), None)
        if tool is None:
            return ToolMessage(
                content="test_scope_absent", tool_call_id=request.tool_call["id"], status="error"
            )
        return await handler(request.override(tool=tool))


@unittest.skipUnless(os.name == "posix", "private CLI sessions require POSIX")
class MemoryLakeLangGraphTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())
        root = self.enterContext(tempfile.TemporaryDirectory())
        self.pool = SqlitePool(Path(root) / "graph.db")
        self.addCleanup(self.pool.close)
        run_migrations(self.pool)
        self.owner = UserRepo(self.pool).create(username="owner", role="user")
        AgentRepo(self.pool).create(agent_id="agent", user_id=self.owner, name="agent")
        connectors = ConnectorRepo(self.pool)
        connectors.create(
            instance_id="connector",
            user_id=self.owner,
            kind="memorylake",
            display_name="ml",
            mcp_server_name="ml",
        )
        secrets = self.secrets = SecretRepo(self.pool)
        blob = encrypt_credentials(
            secrets, {"instance_id": "connector", "api_key": "DUMMY-GRAPH-KEY"}
        )
        connectors.upsert_credentials(instance_id="connector", blob=blob)
        bindings = self.bindings = MemoryLakeBindingRepo(self.pool)
        bindings.create(
            binding_id="binding",
            connector_id="connector",
            owner_user_id=self.owner,
            agent_id="agent",
            base_url="https://app.memorylake.ai/openapi/memorylake",
            workspace_id="workspace",
            project_ids_json='["project"]',
            enabled=True,
        )
        self.session = f"agent:dashboard:{self.owner}:dm"
        ThreadRepo(self.pool).insert(
            thread_id="thread",
            agent_id="agent",
            user_id=self.owner,
            channel_type="dashboard",
            session_key=self.session,
        )
        self.entered, self.release = asyncio.Event(), asyncio.Event()
        self.block = False
        self.after_run = None
        self.paths, self.calls = [], []
        self.context_after_exit = []
        outer = self

        class Runner:
            async def run(self, argv, *, env, cwd):
                outer.calls.append(tuple(argv))
                outer.paths.append(cwd)
                outer.entered.set()
                if outer.block:
                    await outer.release.wait()
                if outer.after_run:
                    outer.after_run()
                return ProcessResult(
                    0, b'{"documents":[{"document_id":"doc","project_id":"project"}]}', b""
                )

        self.enterContext(
            patch(
                "octop.infra.connectors.memorylake.read_service.MemoryLakeRunner",
                return_value=Runner(),
            )
        )
        self.config = {
            "configurable": {
                "user": str(self.owner),
                "source": "dashboard/octop-dashboard",
                "agent_id": "agent",
                "thread_id": "thread",
                "session_key": self.session,
            }
        }

    def message(self):
        return DashboardInbound(
            channel_id=WS_CHANNEL_ID,
            channel_type="dashboard",
            tenant_id="agent",
            channel_subject=ChannelSubject(subject_id=str(self.owner)),
            channel_session_id=self.session,
            content=[TextContent(text="synthetic")],
            metadata={"thread_id": "thread", "session_key": self.session},
        )

    async def asyncSetUp(self):
        self.now = 100.0
        self.registry = DashboardOriginRegistry(identities=self.bindings, clock=lambda: self.now)
        self.admission = ReadAdmission()
        self.database = DatabaseJobAdmission(max_jobs=8)
        self.addAsyncCleanup(self.database.aclose)
        self.identity = await self.new_identity()
        self.tools = self.build_tools(self.identity)

    async def new_identity(self):
        message = self.message()
        await self.registry.issue(
            message, caller_user_id=self.owner, agent_id="agent", thread_id="thread"
        )
        identity = await self.registry.consume(message)
        self.addCleanup(identity.close)
        return identity

    def build_tools(self, identity):
        return build_dashboard_read_tools(
            identity=identity,
            caller_user_id=identity.user_id,
            agent_id=identity.agent_id,
            thread_id=identity.thread_id,
            binding_id="binding",
            connector_id="connector",
            bindings=self.bindings,
            secrets=self.secrets,
            executable=Path(sys.executable),
            admission=self.admission,
            database=self.database,
        )

    def graph(self, model=None, checkpoint=None):
        model = model or _Model()
        return create_agent(
            model, tools=[], middleware=[_DynamicTools()], checkpointer=checkpoint
        ), model

    async def invoke(self, graph, *, scoped=True, config=None):
        token = _SCOPE.set(tuple(self.tools) if scoped else ())
        try:
            return await graph.ainvoke(
                {"messages": [HumanMessage(content="synthetic")]},
                self.config if config is None else config,
            )
        finally:
            _SCOPE.reset(token)
            self.context_after_exit.append(_SCOPE.get())

    async def test_dynamic_tool_real_node_success_and_checkpoint_has_no_secrets_or_scope(self):
        saver = InMemorySaver()
        graph, model = self.graph(checkpoint=saver)
        result = await self.invoke(graph)
        messages = [m for m in result["messages"] if isinstance(m, ToolMessage)]
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0].status, "success")
        self.assertEqual(
            json.loads(messages[0].content)["data"]["documents"][0]["document_id"], "doc"
        )
        self.assertEqual(len(self.calls), 1)
        self.assertIn(_TOOL, model.observations[0])
        # The real ToolNode has no globally registered MemoryLake tool.
        self.assertEqual(graph.get_graph().nodes["tools"].data.tools_by_name, {})
        checkpoints = [item async for item in saver.alist(self.config)]
        text = repr(checkpoints)
        for forbidden in (
            "DUMMY-GRAPH-KEY",
            "test_only_memorylake_tools",
            "ReadCredentials",
            "StructuredTool",
            "_ReadTool",
            "StoredMemoryLakeReader",
            "credential_blob",
        ):
            self.assertNotIn(forbidden, text)
        self.assertTrue(checkpoints)
        self.assertEqual(_SCOPE.get(), ())

    async def test_model_identity_and_config_arguments_cannot_override(self):
        for forbidden in ("as_user", "config", "api_key"):
            with self.subTest(forbidden=forbidden):
                graph, _ = self.graph(
                    _Model(
                        arguments={"query": "q", forbidden: {"user": "MODEL-SECRET-MUST-NOT-ECHO"}}
                    )
                )
                result = await self.invoke(graph)
                message = next(m for m in result["messages"] if isinstance(m, ToolMessage))
                self.assertEqual(message.status, "error")
                self.assertEqual(message.content, "memorylake_invalid_arguments")
                self.assertNotIn("MODEL-SECRET-MUST-NOT-ECHO", message.content)
        self.assertEqual(self.calls, [])

    async def test_unknown_write_names_cannot_resolve_to_tools(self):
        for name in ("memorylake_write", "memorylake_upload"):
            with self.subTest(name=name):
                graph, _ = self.graph(_Model(force_call=True, tool_name=name))
                result = await self.invoke(graph)
                message = next(m for m in result["messages"] if isinstance(m, ToolMessage))
                self.assertEqual((message.status, message.content), ("error", "test_scope_absent"))
        self.assertEqual(self.calls, [])

    async def test_changed_source_or_user_rejected(self):
        for field in ("user", "source"):
            with self.subTest(field=field):
                graph, _ = self.graph()
                config = {"configurable": {**self.config["configurable"], field: "forged"}}
                result = await self.invoke(graph, config=config)
                message = next(m for m in result["messages"] if isinstance(m, ToolMessage))
                self.assertEqual(
                    (message.status, message.content), ("error", "memorylake_read_unavailable")
                )
        self.assertEqual(self.calls, [])

    async def test_next_unscoped_invocation_has_no_tool_residue(self):
        graph, model = self.graph(_Model(force_call=True))
        await self.invoke(graph)
        result = await self.invoke(graph, scoped=False)
        self.assertEqual(len(self.calls), 1)
        message = next(m for m in result["messages"] if isinstance(m, ToolMessage))
        self.assertEqual(message.content, "test_scope_absent")
        self.assertNotIn(_TOOL, model.observations[-1])

    async def test_cancel_cleans_session_and_test_context(self):
        graph, _ = self.graph()
        self.block = True
        task = asyncio.create_task(self.invoke(graph))
        await asyncio.wait_for(self.entered.wait(), 2)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(self.paths)
        self.assertTrue(all(not path.exists() for path in self.paths))
        self.assertEqual(_SCOPE.get(), ())
        self.assertEqual(self.context_after_exit, [()])
        self.block = False
        await self.invoke(graph)
        self.assertEqual(len(self.calls), 2)

    async def test_accepted_real_queue_origin_to_graph_and_closed_inherited_claim(self):
        # Actual manager queue/parser; custom consumer shim, NOT the registered
        # dashboard processor. QueueFull/drop acknowledgement is not covered.
        channel = WebSocketChannel(object(), hub=WebSocketHub())
        manager = ChannelManager(
            {WS_CHANNEL_ID: channel},
            workers_per_channel=0,
            media_backend=object(),
            interrupt_message_predicate=lambda message: False,
        )
        await manager.start()
        graph, _ = self.graph()
        start = asyncio.Event()
        finish_child = asyncio.Event()
        claimed = []
        children = []

        async def consumer():
            self.assertEqual(_SCOPE.get(), ())
            start.set()
            queued = await manager._queues[WS_CHANNEL_ID].get()
            parsed = channel.parse_inbound(queued)
            identity = await self.registry.consume(parsed)
            claimed.append(identity)
            tools = self.build_tools(identity)
            token = _SCOPE.set(tuple(tools))
            try:

                async def inherited_child():
                    await finish_child.wait()
                    with self.assertRaises(DashboardOriginError):
                        identity.check_current(
                            agent_id=identity.agent_id, thread_id=identity.thread_id
                        )
                    inherited_tools = _SCOPE.get()
                    self.assertEqual(len(inherited_tools), 5)
                    return await inherited_tools[0].ainvoke({"query": "late"}, config=self.config)

                children.append(asyncio.create_task(inherited_child()))
                config = {
                    "configurable": {
                        "user": str(identity.user_id),
                        "source": "dashboard/octop-dashboard",
                        "agent_id": identity.agent_id,
                        "thread_id": identity.thread_id,
                        "session_key": f"{identity.agent_id}:dashboard:{identity.user_id}:dm",
                    }
                }
                result = await graph.ainvoke(
                    {"messages": [HumanMessage(content="synthetic")]}, config
                )
                return parsed, result
            finally:
                identity.close()
                _SCOPE.reset(token)
                self.assertEqual(_SCOPE.get(), ())
                manager._queues[WS_CHANNEL_ID].task_done()

        task = asyncio.create_task(consumer())  # Exists before origin issuance.
        try:
            await start.wait()
            self.assertEqual(_SCOPE.get(), ())  # No producer ambient authority.
            message = self.message()
            await self.registry.issue(
                message, caller_user_id=self.owner, agent_id="agent", thread_id="thread"
            )
            manager.enqueue(WS_CHANNEL_ID, message)
            parsed, result = await asyncio.wait_for(task, 3)
            self.assertIs(parsed, message)
            message_result = next(m for m in result["messages"] if isinstance(m, ToolMessage))
            self.assertEqual(message_result.status, "success")
            self.assertEqual(len(self.calls), 1)
            finish_child.set()
            self.assertEqual(await asyncio.wait_for(children[0], 2), "memorylake_read_unavailable")
            self.assertEqual(len(self.calls), 1)
            with self.assertRaises(DashboardOriginError):
                await self.registry.consume(message)
            with self.assertRaises(DashboardOriginError):
                claimed[0].check_current(agent_id="agent", thread_id="thread")
        finally:
            task.cancel()
            for child in children:
                child.cancel()
            await asyncio.gather(task, *children, return_exceptions=True)
            await manager.stop()

    async def test_identity_invalidation_during_cli_hides_result_before_tool_end(self):
        class Events(AsyncCallbackHandler):
            def __init__(self):
                self.outputs = []

            async def on_tool_start(self, serialized, input_str, **kwargs):
                self.outputs.append(("start", input_str))

            async def on_tool_end(self, output, **kwargs):
                self.outputs.append(("end", output))

            async def on_tool_error(self, error, **kwargs):
                self.outputs.append(("error", error))

        for action in ("close", "expire"):
            with self.subTest(action=action):
                identity = await self.new_identity()
                self.tools = self.build_tools(identity)
                if action == "close":
                    self.after_run = identity.close
                else:

                    def advance():
                        self.now += 60

                    self.after_run = advance
                events = Events()
                graph, _ = self.graph()
                result = await self.invoke(graph, config={**self.config, "callbacks": [events]})
                message = next(m for m in result["messages"] if isinstance(m, ToolMessage))
                self.assertEqual(
                    (message.status, message.content), ("error", "memorylake_read_unavailable")
                )
                self.assertEqual(events.outputs, [])
                self.assertNotIn("document_id", repr(events.outputs))
                self.assertNotIn("DUMMY-GRAPH-KEY", repr(events.outputs))
                self.assertNotIn("document_id", repr(result))
        self.assertEqual(len(self.calls), 2)

    async def test_scheduled_close_before_tool_return_omits_document_from_checkpoint(self):
        # Only the child-coroutine -> arun return scheduling boundary is tested.
        # This does not retract results legitimately returned before revocation,
        # nor establish graph/model callback or graph-wide persistence isolation.
        tool = next(tool for tool in self.tools if tool.name == _TOOL)
        original = tool.coroutine
        closed = []

        def close():
            self.identity.close()
            closed.append(True)

        async def scheduled(config: RunnableConfig, **arguments):
            result = await original(config=config, **arguments)
            asyncio.get_running_loop().call_soon(close)
            return result

        tool.coroutine = scheduled
        saver = InMemorySaver()
        graph, _ = self.graph(checkpoint=saver)
        result = await self.invoke(graph)
        self.assertEqual(closed, [True])
        self.assertEqual(len(self.calls), 1)
        message = next(m for m in result["messages"] if isinstance(m, ToolMessage))
        self.assertEqual(
            (message.status, message.content), ("error", "memorylake_read_unavailable")
        )
        checkpoints = [item async for item in saver.alist(self.config)]
        self.assertTrue(checkpoints)
        for output in (result, checkpoints):
            self.assertNotIn("document_id", repr(output))
            self.assertNotIn("DUMMY-GRAPH-KEY", repr(output))
