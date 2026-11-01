"""Dormant agent-bound middleware with real graph/SQLite, fake model/CLI only.

Origin issuance is manually trusted fixture input, not a registered ingress.
These tests do not establish observer isolation, independent HITL resume policy,
queue shutdown wiring, or permission to enable production tools.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
import time
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from langchain.agents import create_agent
from langchain.agents.middleware import AgentMiddleware, ModelRequest, ModelResponse
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.errors import GraphBubbleUp
from langgraph.prebuilt.tool_node import ToolCallRequest
from langgraph.types import Command
from octop_gateway.manager import ChannelManager
from octop_gateway.models import ChannelSubject, TextContent
from tests.support.memorylake import offline_test_guard
from tests.unit.connectors.test_memorylake_langgraph import _Model

from octop.infra.connectors.crypto import encrypt_credentials
from octop.infra.connectors.memorylake.admission import ReadAdmission
from octop.infra.connectors.memorylake.delivery import DashboardDelivery
from octop.infra.connectors.memorylake.middleware import (
    MemoryLakeReadMiddleware,
    dashboard_read_turn,
)
from octop.infra.connectors.memorylake.origin import (
    DashboardInbound,
    DashboardOriginError,
    DashboardOriginRegistry,
    DashboardTurnIdentity,
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
from octop.infra.gateway.ws.memorylake import MemoryLakeWebSocketChannel
from octop.infra.gateway.ws.ws_channel import WS_CHANNEL_ID
from octop.infra.gateway.ws.ws_hub import WebSocketHub

_NAME = "memorylake_search_documents"


@tool
def native_echo(text: str) -> str:
    """Synthetic native tool, unrelated to MemoryLake."""
    return text


@unittest.skipUnless(os.name == "posix", "private CLI sessions require POSIX")
class MemoryLakeMiddlewareTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())
        root = self.enterContext(tempfile.TemporaryDirectory())
        self.pool = SqlitePool(Path(root) / "middleware.db")
        self.addCleanup(self.pool.close)
        run_migrations(self.pool)
        self.bindings = MemoryLakeBindingRepo(self.pool)
        self.secrets = SecretRepo(self.pool)
        self.admission = ReadAdmission()
        self.database = DatabaseJobAdmission(max_jobs=8)
        self.addAsyncCleanup(self.database.aclose)
        self.calls = []
        self.paths = []
        self.runner_hook = None
        outer = self

        class Runner:
            async def run(self, arguments, *, env, cwd):
                outer.calls.append((tuple(arguments), env["MEMORYLAKE_API_KEY"]))
                outer.paths.append(cwd)
                if outer.runner_hook:
                    await outer.runner_hook()
                return ProcessResult(
                    0, b'{"documents":[{"document_id":"doc","project_id":"project"}]}', b""
                )

        self.enterContext(
            patch(
                "octop.infra.connectors.memorylake.read_service.MemoryLakeRunner",
                return_value=Runner(),
            )
        )

    async def asyncSetUp(self):
        self.now = 100.0
        self.registry = DashboardOriginRegistry(identities=self.bindings, clock=lambda: self.now)
        self.first = await self.seed("first")
        self.second = await self.seed("second")

    async def seed(self, label):
        user = UserRepo(self.pool).create(username=label, role="user")
        AgentRepo(self.pool).create(agent_id=label, user_id=user, name=label)
        connectors = ConnectorRepo(self.pool)
        connectors.create(
            instance_id=label,
            user_id=user,
            kind="memorylake",
            display_name=label,
            mcp_server_name=label,
        )
        connectors.upsert_credentials(
            instance_id=label,
            blob=encrypt_credentials(
                self.secrets, {"instance_id": label, "api_key": f"DUMMY-{label}-KEY"}
            ),
        )
        self.bindings.create(
            binding_id=label,
            connector_id=label,
            owner_user_id=user,
            agent_id=label,
            base_url="https://app.memorylake.ai/openapi/memorylake",
            workspace_id=label,
            project_ids_json='["project"]',
            enabled=True,
        )
        session = f"{label}:dashboard:{user}:dm"
        ThreadRepo(self.pool).insert(
            thread_id=label,
            agent_id=label,
            user_id=user,
            channel_type="dashboard",
            session_key=session,
        )
        message = DashboardInbound(
            channel_id="octop-dashboard",
            channel_type="dashboard",
            tenant_id=label,
            channel_subject=ChannelSubject(subject_id=str(user)),
            channel_session_id=session,
            content=[TextContent(text="synthetic")],
            metadata={"thread_id": label, "session_key": session},
        )
        await self.registry.issue(message, caller_user_id=user, agent_id=label, thread_id=label)
        identity = await self.registry.consume(message)
        self.addCleanup(identity.close)
        return {
            "identity": identity,
            "label": label,
            "user": user,
            "config": {
                "configurable": {
                    "user": str(user),
                    "agent_id": label,
                    "thread_id": label,
                    "session_key": session,
                    "source": "dashboard/octop-dashboard",
                }
            },
        }

    def turn(self, fixture=None, **overrides):
        fixture = fixture or self.first
        kwargs = {
            "identity": fixture["identity"],
            "binding_id": fixture["label"],
            "connector_id": fixture["label"],
            "bindings": self.bindings,
            "secrets": self.secrets,
            "executable": Path(sys.executable),
            "admission": self.admission,
            "database": self.database,
        }
        kwargs.update(overrides)
        return dashboard_read_turn(**kwargs)

    def graph(self, fixture=None, *, model=None, middleware=None, tools=None, saver=None):
        fixture = fixture or self.first
        model = model or _Model(force_call=True)
        graph = create_agent(
            model,
            tools=tools or [],
            middleware=[MemoryLakeReadMiddleware(agent_id=fixture["label"]), *(middleware or [])],
            checkpointer=saver,
        )
        return graph, model

    async def invoke(self, graph, fixture=None, *, config=None):
        fixture = fixture or self.first
        return await graph.ainvoke(
            {"messages": [HumanMessage(content="synthetic")]}, config or fixture["config"]
        )

    def assert_unavailable(self, result):
        message = next(m for m in result["messages"] if isinstance(m, ToolMessage))
        self.assertEqual(
            (message.status, message.content), ("error", "memorylake_read_unavailable")
        )

    async def test_real_graph_scope_dynamic_tool_and_checkpoint(self):
        saver = InMemorySaver()
        graph, model = self.graph(saver=saver)
        with self.turn():
            result = await self.invoke(graph)
        message = next(m for m in result["messages"] if isinstance(m, ToolMessage))
        self.assertEqual(message.status, "success")
        self.assertEqual(json.loads(message.content)["data"]["documents"][0]["document_id"], "doc")
        self.assertEqual(graph.get_graph().nodes["tools"].data.tools_by_name, {})
        self.assertIn(_NAME, model.observations[0])
        with self.assertRaises(DashboardOriginError):
            self.first["identity"].check_current(agent_id="first", thread_id="first")
        checkpoints = [item async for item in saver.alist(self.first["config"])]
        for secret in (
            "DUMMY-first-KEY",
            "DashboardTurnIdentity",
            "credential_blob",
            "_ReadTool",
            "additionalProperties",
        ):
            self.assertNotIn(secret, repr(checkpoints))

    async def test_config_cannot_grant_scope(self):
        graph, model = self.graph()
        result = await self.invoke(graph)
        self.assert_unavailable(result)
        self.assertFalse(
            any(name.startswith("memorylake_") for names in model.observations for name in names)
        )
        self.assertEqual(self.calls, [])

    async def test_mismatching_config_and_real_bound_peer_agent_are_denied(self):
        with self.turn():
            for field in ("user", "source", "agent_id", "thread_id"):
                with self.subTest(field=field):
                    graph, model = self.graph()
                    config = {
                        "configurable": {**self.first["config"]["configurable"], field: "forged"}
                    }
                    with self.assertRaises(ValueError):
                        await self.invoke(graph, config=config)
                    self.assertEqual(model.observations, [])
                    self.assertFalse(
                        any(
                            name.startswith("memorylake_")
                            for names in model.observations
                            for name in names
                        )
                    )
            # Config still names the ORIGINAL agent; middleware is bound to the
            # actual peer graph and must not trust that forged graph association.
            peer, model = self.graph(self.second)
            with self.assertRaises(ValueError):
                await self.invoke(peer, config=self.first["config"])
            self.assertEqual(model.observations, [])
            self.assertFalse(
                any(
                    name.startswith("memorylake_") for names in model.observations for name in names
                )
            )
        self.assertEqual(self.calls, [])

    async def test_nested_scope_and_unconsumed_identity_rejected(self):
        forged = DashboardTurnIdentity(
            self.first["user"], "first", "first", time.monotonic() + 30, time.monotonic
        )
        try:
            with self.assertRaises(ValueError), self.turn(identity=forged):
                self.fail("unconsumed identity accepted")
        finally:
            forged.close()
        with self.turn():
            with self.assertRaises(ValueError), self.turn(self.second):
                self.fail("nested scope accepted")
            self.first["identity"].check_current(agent_id="first", thread_id="first")

    async def test_exit_invalidates_inherited_child_scope(self):
        release = asyncio.Event()
        graph, _ = self.graph()

        async def child():
            await release.wait()
            return await self.invoke(graph)

        with self.turn():
            task = asyncio.create_task(child())
        release.set()
        with self.assertRaises(ValueError):
            await task
        self.assertEqual(self.calls, [])

    async def test_concurrent_turns_use_separate_identity_and_credentials(self):
        entered = 0
        both = asyncio.Event()

        async def meet():
            nonlocal entered
            entered += 1
            if entered == 2:
                both.set()
            await asyncio.wait_for(both.wait(), 2)

        self.runner_hook = meet

        async def run(fixture):
            graph, _ = self.graph(fixture)
            with self.turn(fixture):
                return await self.invoke(graph, fixture)

        results = await asyncio.gather(run(self.first), run(self.second))
        self.assertTrue(
            all(
                next(m for m in result["messages"] if isinstance(m, ToolMessage)).status
                == "success"
                for result in results
            )
        )
        self.assertEqual({key for _, key in self.calls}, {"DUMMY-first-KEY", "DUMMY-second-KEY"})
        for arguments, key in self.calls:
            workspace = arguments[arguments.index("--workspace") + 1]
            self.assertEqual(key, f"DUMMY-{workspace}-KEY")

    async def test_closed_during_model_await_does_not_deliver_output(self):
        identity = self.first["identity"]

        class CloseModel(AgentMiddleware):
            async def awrap_model_call(self, request, handler):
                result = await handler(request)
                identity.close()
                return result

        graph, _ = self.graph(middleware=[CloseModel()])
        with self.turn(), self.assertRaises(ValueError) as caught:
            await self.invoke(graph)
        self.assertNotIn("synthetic", str(caught.exception))
        self.assertEqual(self.calls, [])

    async def test_closed_after_tool_handler_await_does_not_deliver_old_result(self):
        identity = self.first["identity"]
        request = ToolCallRequest(
            tool_call={
                "name": _NAME,
                "args": {"query": "synthetic"},
                "id": "call-test",
                "type": "tool_call",
            },
            tool=None,
            state={},
            runtime=None,
        )

        async def handler(current):
            result = await current.tool.ainvoke(current.tool_call, config=self.first["config"])
            identity.close()
            return result

        with (
            self.turn(),
            patch(
                "octop.infra.connectors.memorylake.middleware.get_config",
                return_value=self.first["config"],
            ),
        ):
            result = await MemoryLakeReadMiddleware(agent_id="first").awrap_tool_call(
                request, handler
            )
        self.assertEqual((result.status, result.content), ("error", "memorylake_read_unavailable"))
        self.assertNotIn("document_id", repr(result))
        self.assertEqual(len(self.calls), 1)

    async def test_native_tool_remains_available_without_scope(self):
        graph, _ = self.graph(
            model=_Model(force_call=True, tool_name="native_echo", arguments={"text": "native-ok"}),
            tools=[native_echo],
        )
        result = await self.invoke(graph)
        message = next(m for m in result["messages"] if isinstance(m, ToolMessage))
        self.assertEqual((message.status, message.content), ("success", "native-ok"))
        self.assertEqual(self.calls, [])

    async def test_sync_graph_never_exposes_or_executes_memorylake(self):
        graph, model = self.graph()
        with self.turn():
            result = graph.invoke(
                {"messages": [HumanMessage(content="synthetic")]}, self.first["config"]
            )
        self.assert_unavailable(result)
        self.assertFalse(
            any(name.startswith("memorylake_") for names in model.observations for name in names)
        )
        self.assertEqual(self.calls, [])

    async def test_strips_stale_tool_and_function_schemas_without_scope(self):
        @tool("memorylake_stale")
        def stale() -> str:
            """Never execute an old caller closure."""
            raise AssertionError("stale tool executed")

        request = ModelRequest(
            model=_Model(),
            messages=[],
            tools=[
                native_echo,
                stale,
                {"type": "function", "function": {"name": "memorylake_hidden", "parameters": {}}},
                {"name": "memorylake_flat", "description": "stale schema"},
            ],
        )
        observed = []

        async def handler(current):
            observed.extend(current.tools)
            return ModelResponse(result=[AIMessage(content="native-only")])

        await MemoryLakeReadMiddleware(agent_id="first").awrap_model_call(request, handler)
        self.assertEqual(observed, [native_echo])

    async def test_real_external_graph_cancellation_closes_scope(self):
        entered = asyncio.Event()

        class WaitingModel(AgentMiddleware):
            async def awrap_model_call(self, request, handler):
                entered.set()
                await asyncio.Event().wait()
                return await handler(request)

        graph, _ = self.graph(middleware=[WaitingModel()])

        async def run():
            with self.turn():
                return await self.invoke(graph)

        task = asyncio.create_task(run())
        try:
            await asyncio.wait_for(entered.wait(), 2)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            with self.assertRaises(DashboardOriginError):
                self.first["identity"].check_current(agent_id="first", thread_id="first")
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self.assertEqual(self.calls, [])

    async def test_graph_bubble_up_propagates(self):
        class RaiseModel(AgentMiddleware):
            async def awrap_model_call(self, request, handler):
                raise GraphBubbleUp("synthetic control flow")

        graph, _ = self.graph(middleware=[RaiseModel()])
        with self.turn(), self.assertRaises(GraphBubbleUp):
            await self.invoke(graph)

    async def test_direct_hook_does_not_swallow_cancelled_error(self):
        async def handler(request):
            raise asyncio.CancelledError()

        request = ModelRequest(model=_Model(), messages=[])
        with self.assertRaises(asyncio.CancelledError):
            await MemoryLakeReadMiddleware(agent_id="first").awrap_model_call(request, handler)

    async def test_expired_scope_does_not_offer_or_execute_memorylake(self):
        graph, model = self.graph()
        with self.turn():
            self.now += 60
            with self.assertRaises(ValueError):
                await self.invoke(graph)
            self.assertEqual(model.observations, [])
        self.assertFalse(
            any(name.startswith("memorylake_") for names in model.observations for name in names)
        )
        self.assertEqual(self.calls, [])

    async def test_command_is_not_accepted_as_a_read_result(self):
        class CommandTool(AgentMiddleware):
            async def awrap_tool_call(self, request, handler):
                return Command(
                    update={"messages": [AIMessage(content="forbidden command payload")]}
                )

        graph, _ = self.graph(middleware=[CommandTool()])
        with self.turn():
            result = await self.invoke(graph)
        self.assert_unavailable(result)
        self.assertNotIn("forbidden command payload", repr(result))
        self.assertEqual(self.calls, [])

    async def test_valid_scope_replaces_stale_global_closure_and_rejects_unknown_write(self):
        @tool(_NAME)
        def stale(query: str) -> str:
            """Old caller closure must never execute."""
            raise AssertionError("stale global caller executed")

        with self.turn():
            graph, model = self.graph(tools=[stale])
            result = await self.invoke(graph)
            message = next(m for m in result["messages"] if isinstance(m, ToolMessage))
            self.assertEqual(message.status, "success")
            self.assertEqual(model.observations[0].count(_NAME), 1)
            unknown, _ = self.graph(model=_Model(force_call=True, tool_name="memorylake_upload"))
            self.assert_unavailable(await self.invoke(unknown))
        self.assertEqual(len(self.calls), 1)

    async def test_existing_invalid_scope_never_passes_old_tool_payload_to_model(self):
        sentinel = "SYNTHETIC-OLD-TOOL-DOCUMENT-NOT-FOR-MODEL"
        request = ModelRequest(
            model=_Model(), messages=[ToolMessage(content=sentinel, tool_call_id="old", name=_NAME)]
        )
        for mode in ("async", "sync"):
            for invalid in ("closed", "expired", "config"):
                with self.subTest(mode=mode, invalid=invalid):
                    fixture = await self.seed(f"{mode}-{invalid}")
                    calls = []
                    config = fixture["config"]
                    if invalid == "config":
                        config = {"configurable": {**config["configurable"], "user": "forged"}}

                    def handler(current, *, calls=calls):
                        calls.append(current.messages)
                        return ModelResponse(result=[AIMessage(content=sentinel)])

                    async def async_handler(current, *, handler=handler):
                        return handler(current)

                    with (
                        self.turn(fixture),
                        patch(
                            "octop.infra.connectors.memorylake.middleware.get_config",
                            return_value=config,
                        ),
                    ):
                        if invalid == "closed":
                            fixture["identity"].close()
                        elif invalid == "expired":
                            self.now += 60
                        middleware = MemoryLakeReadMiddleware(agent_id=fixture["label"])
                        with self.assertRaises(ValueError) as caught:
                            if mode == "async":
                                await middleware.awrap_model_call(request, async_handler)
                            else:
                                middleware.wrap_model_call(request, handler)
                        self.assertNotIn(sentinel, str(caught.exception))
                        self.assertEqual(calls, [])

    async def test_tool_entry_invalid_scope_returns_constant_without_handler(self):
        request = ToolCallRequest(
            tool_call={
                "name": _NAME,
                "args": {"query": "SYNTHETIC-PRIVATE-QUERY"},
                "id": "call-test",
                "type": "tool_call",
            },
            tool=None,
            state={},
            runtime=None,
        )
        calls = []

        async def handler(current):
            calls.append(current)
            return ToolMessage(content="forbidden", tool_call_id="call-test")

        with (
            self.turn(),
            patch(
                "octop.infra.connectors.memorylake.middleware.get_config",
                return_value=self.first["config"],
            ),
        ):
            self.first["identity"].close()
            result = await MemoryLakeReadMiddleware(agent_id="first").awrap_tool_call(
                request, handler
            )
        self.assertEqual((result.status, result.content), ("error", "memorylake_read_unavailable"))
        self.assertEqual(calls, [])

    async def test_runtime_modes_cannot_be_overridden_by_extra_read_tools(self):
        with self.turn():
            for mode in ("ask", "plan", "unknown"):
                with self.subTest(mode=mode):
                    graph, model = self.graph()
                    config = {
                        "configurable": {
                            **self.first["config"]["configurable"],
                            "conversation_mode": mode,
                            "extra_read_tools": [_NAME],
                        }
                    }
                    with self.assertRaises(ValueError):
                        await self.invoke(graph, config=config)
                    self.assertEqual(model.observations, [])
            for mode in (None, "craft"):
                with self.subTest(allowed=mode):
                    graph, _ = self.graph()
                    configurable = dict(self.first["config"]["configurable"])
                    if mode is not None:
                        configurable["conversation_mode"] = mode
                    result = await self.invoke(graph, config={"configurable": configurable})
                    message = next(m for m in result["messages"] if isinstance(m, ToolMessage))
                    self.assertEqual(message.status, "success")
            unknown, model = self.graph(
                model=_Model(force_call=True, tool_name="memorylake_upload")
            )
            config = {
                "configurable": {
                    **self.first["config"]["configurable"],
                    "conversation_mode": "craft",
                    "extra_read_tools": ["memorylake_upload"],
                }
            }
            self.assert_unavailable(await self.invoke(unknown, config=config))
            self.assertNotIn("memorylake_upload", model.observations[0])
        self.assertEqual(len(self.calls), 2)
        # An unscoped config hint is still not authority.
        graph, _ = self.graph()
        config = {
            "configurable": {**self.first["config"]["configurable"], "extra_read_tools": [_NAME]}
        }
        self.assert_unavailable(await self.invoke(graph, config=config))
        self.assertEqual(len(self.calls), 2)

    async def test_real_delivery_scope_middleware_graph_success(self):
        await self._delivery_graph(cancel=False)

    async def test_real_delivery_manager_stop_cancels_graph_and_cleans_scope(self):
        await self._delivery_graph(cancel=True)

    async def _delivery_graph(self, *, cancel):
        # Every boundary here is real except the trusted fixture issuer,
        # processor, model and CLI. Not JWT/GlobalProcessor enablement.
        # No test-only authority ContextVar or legacy dynamic middleware is used.
        self.first["identity"].close()
        delivery = DashboardDelivery(self.registry)
        entered, finished = asyncio.Event(), asyncio.Event()
        results, claims = [], []
        graph, _ = self.graph()
        self.assertEqual(graph.get_graph().nodes["tools"].data.tools_by_name, {})
        outer = self

        async def block_cli():
            entered.set()
            await asyncio.Event().wait()

        if cancel:
            self.runner_hook = block_cli

        class Processor:
            async def iter_turn_chunks(self, message):
                result = await outer.invoke(graph)
                results.append(result)
                yield {"type": "done"}

        @contextmanager
        def turn_scope(identity):
            claims.append(identity)
            try:
                with outer.turn(identity=identity):
                    yield
            finally:
                with self.assertRaises(DashboardOriginError):
                    identity.check_current(agent_id="first", thread_id="first")
                finished.set()

        manager = ChannelManager(
            {
                WS_CHANNEL_ID: MemoryLakeWebSocketChannel(
                    Processor(), hub=WebSocketHub(), delivery=delivery, turn_scope=turn_scope
                )
            },
            workers_per_channel=1,
            media_backend=object(),
        )
        await manager.start()
        try:
            config = self.first["config"]["configurable"]
            message = DashboardInbound(
                channel_id=WS_CHANNEL_ID,
                channel_type="dashboard",
                tenant_id="first",
                channel_subject=ChannelSubject(subject_id=str(self.first["user"])),
                channel_session_id=config["session_key"],
                content=[TextContent(text="synthetic")],
                metadata={
                    "thread_id": "first",
                    "session_key": config["session_key"],
                    "ws_connection_id": "fake-connection",
                },
            )
            receipt = await delivery.dispatch(
                message,
                caller_user_id=self.first["user"],
                agent_id="first",
                thread_id="first",
                enqueue=lambda msg: manager.enqueue(WS_CHANNEL_ID, msg),
                timeout_s=2,
            )
            self.assertEqual(receipt.status, "accepted")
            if cancel:
                await asyncio.wait_for(entered.wait(), 2)
                await manager.stop()
            await asyncio.wait_for(finished.wait(), 2)
            self.assertEqual(len(claims), 1)
            with self.assertRaises(DashboardOriginError):
                claims[0].check_current(agent_id="first", thread_id="first")
            self.assertEqual(len(self.calls), 1)
            self.assertTrue(self.paths)
            self.assertTrue(all(not path.exists() for path in self.paths))
            if cancel:
                self.assertEqual(results, [])
            else:
                self.assertEqual(len(results), 1)
                result = next(m for m in results[0]["messages"] if isinstance(m, ToolMessage))
                self.assertEqual(result.status, "success")
                self.assertEqual(
                    json.loads(result.content)["data"]["documents"][0]["document_id"], "doc"
                )
        finally:
            await manager.stop()
            delivery.close()

    async def test_turn_rejects_none_database_before_building_reader(self):
        with (
            patch(
                "octop.infra.connectors.memorylake.middleware.build_dashboard_read_tools"
            ) as build,
            self.assertRaisesRegex(ValueError, "shared_database_admission_required"),
            self.turn(database=None),
        ):
            self.fail("must reject")
        build.assert_not_called()
        self.assertEqual(self.calls, [])
