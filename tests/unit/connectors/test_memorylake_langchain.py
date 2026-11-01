"""Real LangChain/harness + SQLite/crypto; synthetic CLI results, no services."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from langchain_core.callbacks import AsyncCallbackHandler
from langchain_core.messages import ToolMessage
from langchain_core.runnables import RunnableConfig
from langgraph.errors import GraphBubbleUp, GraphInterrupt
from octop_gateway.models import ChannelSubject, TextContent
from octop_harness.request import ChatRequest
from tests.support.memorylake import offline_test_guard

from octop.infra.connectors.crypto import encrypt_credentials
from octop.infra.connectors.memorylake.admission import ReadAdmission
from octop.infra.connectors.memorylake.langchain import build_dashboard_read_tools
from octop.infra.connectors.memorylake.origin import DashboardInbound, DashboardOriginRegistry
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
from octop.infra.gateway.process.harness_request import build_harness_request


@unittest.skipUnless(os.name == "posix", "private CLI sessions require POSIX")
class MemoryLakeLangChainTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())
        self.temp = self.enterContext(tempfile.TemporaryDirectory())
        self.pool = SqlitePool(Path(self.temp) / "test.db")
        self.addCleanup(self.pool.close)
        run_migrations(self.pool)
        users = UserRepo(self.pool)
        self.owner = users.create(username="owner", role="user")
        self.other = users.create(username="other", role="user")
        AgentRepo(self.pool).create(agent_id="agent", user_id=self.owner, name="agent")
        self.connectors = ConnectorRepo(self.pool)
        self.connectors.create(
            instance_id="connector",
            user_id=self.owner,
            kind="memorylake",
            display_name="ML",
            mcp_server_name="ml",
        )
        self.secrets = SecretRepo(self.pool)
        self.store_key("dummy-first")
        self.bindings = MemoryLakeBindingRepo(self.pool)
        self.bindings.create(
            binding_id="binding",
            connector_id="connector",
            owner_user_id=self.owner,
            agent_id="agent",
            base_url="https://app.memorylake.ai/openapi/memorylake",
            workspace_id="workspace",
            project_ids_json='["project"]',
            actor_id="actor",
            enabled=True,
        )
        self.threads = ThreadRepo(self.pool)
        self.session_key = f"agent:dashboard:{self.owner}:dm"
        self.threads.insert(
            thread_id="thread",
            agent_id="agent",
            user_id=self.owner,
            channel_type="dashboard",
            session_key=self.session_key,
        )
        self.calls = []
        self.after_run = None
        self.admission = ReadAdmission()
        self.database = DatabaseJobAdmission(max_jobs=8)
        self.addAsyncCleanup(self.database.aclose)
        outer = self

        class FakeRunner:
            async def run(self, arguments, *, env, cwd):
                outer.calls.append((tuple(arguments), dict(env)))
                if outer.after_run:
                    outer.after_run()
                if "trace" in arguments:
                    body = {"trace": []}
                elif "--actors" in arguments:
                    body = {"facts": [{"id": "fact", "actor_id": "actor"}]}
                elif "fact" in arguments:
                    body = {"id": "fact", "actor_id": "actor", "text": "synthetic"}
                elif "get" in arguments:
                    body = {"id": "doc", "project_id": "project", "status": "okay"}
                else:
                    body = {"documents": [{"document_id": "doc", "project_id": "project"}]}
                return ProcessResult(0, json.dumps(body).encode(), b"")

        self.enterContext(
            patch(
                "octop.infra.connectors.memorylake.read_service.MemoryLakeRunner",
                return_value=FakeRunner(),
            )
        )
        request = build_harness_request(
            thread_id="thread",
            user_id=self.owner,
            source="dashboard/octop-dashboard",
            agent_id="agent",
            session_key=self.session_key,
            text="synthetic",
        )
        self.request = ChatRequest.coerce(request)
        self.config = self.request.to_runnable_config()

    async def asyncSetUp(self):
        self.now = 100.0
        registry = DashboardOriginRegistry(identities=self.bindings, clock=lambda: self.now)
        message = DashboardInbound(
            channel_id="octop-dashboard",
            channel_type="dashboard",
            tenant_id="agent",
            channel_subject=ChannelSubject(subject_id=str(self.owner)),
            channel_session_id=self.session_key,
            content=[TextContent(text="synthetic")],
            metadata={"thread_id": "thread", "session_key": self.session_key},
        )
        await registry.issue(
            message, caller_user_id=self.owner, agent_id="agent", thread_id="thread"
        )
        self.identity = await registry.consume(message)
        self.addCleanup(self.identity.close)
        self.tools = {tool.name: tool for tool in self.build_tools()}

    def build_tools(self, **overrides):
        kwargs = {
            "caller_user_id": self.owner,
            "agent_id": "agent",
            "thread_id": "thread",
            "binding_id": "binding",
            "connector_id": "connector",
            "bindings": self.bindings,
            "secrets": self.secrets,
            "executable": Path(sys.executable),
            "admission": self.admission,
            "database": self.database,
            "identity": self.identity,
        }
        kwargs.update(overrides)
        return build_dashboard_read_tools(**kwargs)

    def store_key(self, key):
        blob = encrypt_credentials(self.secrets, {"instance_id": "connector", "api_key": key})
        self.connectors.upsert_credentials(instance_id="connector", blob=blob, expires_at=None)

    def sql(self, statement, parameters=()):
        with self.pool.transaction() as conn:
            conn.execute(statement, parameters)

    async def invoke(self, arguments=None, *, config=None, name="memorylake_search_documents"):
        return await self.tools[name].ainvoke(
            arguments if arguments is not None else {"query": "synthetic"},
            config=self.config if config is None else config,
        )

    def test_only_five_read_schemas_and_no_runtime_identity_or_credentials(self):
        self.assertEqual(
            set(self.tools),
            {
                "memorylake_search_documents",
                "memorylake_search_facts",
                "memorylake_get_document",
                "memorylake_get_fact",
                "memorylake_trace_fact",
            },
        )
        for tool in self.tools.values():
            schema = tool.tool_call_schema.model_json_schema()
            self.assertIs(schema["additionalProperties"], False)
            for name in (
                "config",
                "user",
                "agent_id",
                "binding_id",
                "connector_id",
                "api_key",
                "source",
                "argv",
                "endpoint",
                "profile",
                "as_user",
            ):
                self.assertNotIn(name, schema["properties"])

    async def test_harness_config_real_tool_sql_crypto_and_sanitized_result_compose(self):
        result = json.loads(await self.invoke())
        self.assertIs(result["untrusted"], True)
        self.assertEqual(result["data"]["documents"][0]["document_id"], "doc")
        self.assertEqual(self.calls[0][1]["MEMORYLAKE_API_KEY"], "dummy-first")
        self.assertNotIn("dummy-first", json.dumps(result))

    async def test_closed_origin_during_read_is_checked_before_tool_end_callback(self):
        self.after_run = self.identity.close
        await self.assert_origin_change_hidden_from_callbacks()

    async def test_expired_origin_during_read_is_checked_before_tool_end_callback(self):
        self.after_run = lambda: setattr(self, "now", 161)
        await self.assert_origin_change_hidden_from_callbacks()

    async def test_scheduled_close_between_coroutine_and_arun_discards_result(self):
        tool = self.tools["memorylake_search_documents"]
        original = tool.coroutine
        observed = []

        async def late_close(config: RunnableConfig, **arguments):
            result = await original(config=config, **arguments)
            asyncio.get_running_loop().call_soon(self.identity.close)
            return result

        class Observe(AsyncCallbackHandler):
            async def on_tool_end(self, output, **kwargs):
                observed.append(output)

        tool.coroutine = late_close
        result = await self.invoke(config={**self.config, "callbacks": [Observe()]})
        self.assertEqual(result, "memorylake_read_unavailable")
        self.assertNotIn("document_id", str(observed))

    async def test_closed_origin_never_exposes_query_to_start_callback(self):
        observed = []

        class Observe(AsyncCallbackHandler):
            async def on_tool_start(self, serialized, input_str, **kwargs):
                observed.append((input_str, kwargs.get("inputs")))

        self.identity.close()
        result = await self.invoke(
            {"query": "private-query"}, config={**self.config, "callbacks": [Observe()]}
        )
        self.assertEqual(result, "memorylake_read_unavailable")
        self.assertEqual(observed, [])
        self.assertEqual(self.calls, [])

    async def test_tool_callbacks_disabled_or_closed_during_callback_result_discarded(self):
        entered, release = asyncio.Event(), asyncio.Event()
        observed = []

        class Blocking(AsyncCallbackHandler):
            async def on_tool_end(self, output, **kwargs):
                entered.set()
                await release.wait()
                observed.append(output)

        task = asyncio.create_task(self.invoke(config={**self.config, "callbacks": [Blocking()]}))
        started = asyncio.create_task(entered.wait())
        try:
            await asyncio.wait_for(
                asyncio.wait({task, started}, return_when=asyncio.FIRST_COMPLETED), 2
            )
            if entered.is_set():
                self.identity.close()
                release.set()
                self.assertEqual(await task, "memorylake_read_unavailable")
            else:
                # The restrictive policy emits no sensitive tool callback at all.
                self.assertIn("document_id", await task)
            self.assertEqual(observed, [])
        finally:
            release.set()
            started.cancel()
            task.cancel()
            await asyncio.gather(started, task, return_exceptions=True)

    async def test_origin_closed_while_waiting_for_admission_never_starts_second_cli(self):
        entered, release = asyncio.Event(), asyncio.Event()
        calls = []

        class Runner:
            async def run(self, *args, **kwargs):
                calls.append(1)
                entered.set()
                await release.wait()
                return ProcessResult(0, b'{"documents":[]}', b"")

        with patch(
            "octop.infra.connectors.memorylake.read_service.MemoryLakeRunner", return_value=Runner()
        ):
            tool = self.build_tools()[0]
        first = asyncio.create_task(tool.ainvoke({"query": "q"}, config=self.config))
        await asyncio.wait_for(entered.wait(), 2)
        second = asyncio.create_task(tool.ainvoke({"query": "q"}, config=self.config))
        try:
            async with asyncio.timeout(2):
                while self.admission._queued != 1:
                    await asyncio.sleep(0)
            self.identity.close()
        finally:
            release.set()
        results = await asyncio.gather(first, second)
        self.assertEqual(len(calls), 1)
        self.assertEqual(results, ["memorylake_read_unavailable"] * 2)

    async def assert_origin_change_hidden_from_callbacks(self):
        observed = []

        class Observe(AsyncCallbackHandler):
            async def on_tool_end(self, output, **kwargs):
                observed.append(output)

        result = await self.invoke(config={**self.config, "callbacks": [Observe()]})
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(observed, [])
        self.assertEqual(result, "memorylake_read_unavailable")

    async def test_success_has_no_sensitive_tool_callbacks_or_callback_manager(self):
        observed = []

        class Observe(AsyncCallbackHandler):
            async def on_tool_start(self, *args, **kwargs):
                observed.append((args, kwargs))

            async def on_tool_end(self, *args, **kwargs):
                observed.append((args, kwargs))

        tool = self.tools["memorylake_search_documents"]
        tool.callbacks = [Observe()]
        with patch(
            "langchain_core.callbacks.manager.AsyncCallbackManager.configure",
            side_effect=AssertionError("tool_must_not_configure_callbacks"),
        ) as manager:
            result = await self.invoke(config={**self.config, "callbacks": [Observe()]})
        self.assertIn("document_id", result)
        manager.assert_not_called()
        self.assertEqual(observed, [])

    def test_sync_invocation_fails_closed_before_any_callbacks(self):
        with (
            patch("langchain_core.callbacks.manager.CallbackManager.configure") as manager,
            self.assertRaisesRegex(Exception, "memorylake_async_required"),
        ):
            self.tools["memorylake_search_documents"].invoke({"query": "private-query"})
        manager.assert_not_called()
        self.assertEqual(self.calls, [])

    async def test_framework_control_signals_from_coroutine_propagate(self):
        tool = self.tools["memorylake_search_documents"]
        for signal in (GraphBubbleUp(), GraphInterrupt()):

            async def control_flow(config: RunnableConfig, _signal=signal, **arguments):
                raise _signal

            tool.coroutine = control_flow
            with self.subTest(signal=type(signal).__name__):
                with self.assertRaises(type(signal)) as caught:
                    await self.invoke()
                self.assertIs(caught.exception, signal)
        self.assertEqual(self.calls, [])

    async def test_framework_control_signals_are_not_masked_by_reader_wrapper(self):
        for signal in (GraphBubbleUp(), GraphInterrupt()):
            with (
                self.subTest(signal=type(signal).__name__),
                patch(
                    "octop.infra.connectors.memorylake.storage.StoredMemoryLakeReader.call",
                    side_effect=signal,
                ),
            ):
                with self.assertRaises(type(signal)) as caught:
                    await self.invoke()
                self.assertIs(caught.exception, signal)
        self.assertEqual(self.calls, [])

    async def test_native_tool_call_produces_tool_message(self):
        message = await self.invoke(
            {
                "type": "tool_call",
                "id": "call-test",
                "name": "memorylake_search_documents",
                "args": {"query": "q"},
            }
        )
        self.assertIsInstance(message, ToolMessage)
        self.assertEqual(message.tool_call_id, "call-test")
        self.assertEqual(json.loads(message.content)["operation"], "search_documents")

    async def test_model_cannot_override_identity_runtime_or_cli_options(self):
        for name in (
            "as_user",
            "user",
            "agent_id",
            "binding_id",
            "connector_id",
            "api_key",
            "config",
            "endpoint",
            "argv",
            "profile",
            "source",
            "workspace_id",
        ):
            with self.subTest(name=name):
                result = await self.invoke({"query": "q", name: "dummy-must-not-echo"})
                self.assertEqual(result, "memorylake_invalid_arguments")
        self.assertEqual(self.calls, [])

    async def test_missing_or_different_injected_context_fails_before_decrypt(self):
        with patch(
            "octop.infra.connectors.memorylake.credentials.decrypt_credentials_snapshot"
        ) as dec:
            self.assertEqual(await self.invoke(config={}), "memorylake_read_unavailable")
            for name, value in (
                ("user", str(self.other)),
                ("user", self.owner),
                ("thread_id", "other"),
                ("agent_id", "other"),
                ("session_key", "agent:dashboard:1:group"),
            ):
                config = {"configurable": {**self.config["configurable"], name: value}}
                with self.subTest(name=name, value=value):
                    self.assertEqual(
                        await self.invoke(config=config), "memorylake_read_unavailable"
                    )
            dec.assert_not_called()
        self.assertEqual(self.calls, [])

    async def test_im_owner_mapping_and_other_sources_never_authorize_read(self):
        for source in ("telegram/bot", "dashboard/other", "cli", "mcp", "cron", ""):
            config = {"configurable": {**self.config["configurable"], "source": source}}
            with self.subTest(source=source):
                self.assertEqual(await self.invoke(config=config), "memorylake_read_unavailable")
        self.assertEqual(self.calls, [])

    async def test_harness_configurable_override_does_not_change_trusted_caller(self):
        self.request.configurable["user"] = str(self.other)
        self.assertEqual(
            await self.invoke(config=self.request.to_runnable_config()),
            "memorylake_read_unavailable",
        )
        self.assertEqual(self.calls, [])

    async def test_current_thread_owner_channel_and_dm_are_checked(self):
        for field, bad, original in (
            ("user_id", self.other, self.owner),
            ("channel_type", "telegram", "dashboard"),
            ("session_key", "agent:dashboard:1:group", self.session_key),
        ):
            self.sql(f"UPDATE threads SET {field}=?", (bad,))
            with self.subTest(field=field):
                self.assertEqual(await self.invoke(), "memorylake_read_unavailable")
            self.sql(f"UPDATE threads SET {field}=?", (original,))
        self.assertEqual(self.calls, [])

    async def test_thread_deletion_after_remote_read_discards_result(self):
        self.after_run = lambda: self.sql("DELETE FROM threads")
        self.assertEqual(await self.invoke(), "memorylake_read_unavailable")
        self.assertEqual(len(self.calls), 1)

    async def test_no_separate_thread_read_after_final_binding_authorization(self):
        original = self.threads.get
        lookups = 0

        def get(thread_id):
            nonlocal lookups
            row = original(thread_id)
            lookups += 1
            if lookups == 2:
                # Reproduce the split-check window: the read service has already
                # finished its final binding check, but this await is outstanding.
                self.sql("UPDATE memorylake_bindings SET enabled=0, authorization_revision=2")
            return row

        with patch.object(self.threads, "get", side_effect=get):
            result = await self.invoke()
        if lookups:
            self.assertEqual(result, "memorylake_read_unavailable")
        self.assertEqual(lookups, 0, "thread must be part of the same authorization SQL")

    async def test_thread_revocation_during_decrypt_prevents_remote_launch(self):
        from octop.infra.connectors.crypto import decrypt_credentials_snapshot

        def decrypt(master, blob):
            result = decrypt_credentials_snapshot(master, blob)
            self.sql("DELETE FROM threads")
            return result

        with patch(
            "octop.infra.connectors.memorylake.credentials.decrypt_credentials_snapshot",
            side_effect=decrypt,
        ):
            self.assertEqual(await self.invoke(), "memorylake_read_unavailable")
        self.assertEqual(self.calls, [])

    async def test_binding_revoked_after_factory_creation_fails_closed(self):
        self.sql("UPDATE memorylake_bindings SET enabled=0, authorization_revision=2")
        self.assertEqual(await self.invoke(), "memorylake_read_unavailable")
        self.assertEqual(self.calls, [])

    async def test_shared_agent_does_not_enable_owner_credentials(self):
        self.sql("UPDATE agents SET is_shared=1")
        self.assertEqual(await self.invoke(), "memorylake_read_unavailable")
        self.assertEqual(self.calls, [])

    async def test_existing_tool_uses_current_key_not_factory_snapshot(self):
        await self.invoke()
        self.store_key("dummy-rotated")
        await self.invoke()
        self.assertEqual(
            [c[1]["MEMORYLAKE_API_KEY"] for c in self.calls], ["dummy-first", "dummy-rotated"]
        )

    async def test_distinct_turn_factories_share_connector_admission(self):
        admission = ReadAdmission(max_active=2, max_queued=2)
        entered, release = asyncio.Event(), asyncio.Event()
        active = peak = calls = 0

        class Runner:
            async def run(self, *args, **kwargs):
                nonlocal active, peak, calls
                active += 1
                calls += 1
                peak = max(peak, active)
                entered.set()
                try:
                    await release.wait()
                    return ProcessResult(0, b'{"documents":[]}', b"")
                finally:
                    active -= 1

        with patch(
            "octop.infra.connectors.memorylake.read_service.MemoryLakeRunner",
            side_effect=lambda *args: Runner(),
        ):
            turns = [self.build_tools(admission=admission)[0] for _ in range(2)]
        first = asyncio.create_task(turns[0].ainvoke({"query": "q"}, config=self.config))
        await entered.wait()
        second = asyncio.create_task(turns[1].ainvoke({"query": "q"}, config=self.config))
        try:
            await asyncio.sleep(0)
            self.assertEqual(calls, 1)
        finally:
            release.set()
        results = await asyncio.gather(first, second)
        self.assertTrue(all(json.loads(result)["untrusted"] for result in results))
        self.assertEqual(calls, 2)
        self.assertEqual(peak, 1)

    async def test_all_operations_use_real_argument_contract(self):
        cases = (
            ("search_documents", {"query": "q", "project_ids": ["project"], "top_k": 2}),
            ("search_facts", {"query": "q"}),
            ("get_document", {"project_id": "project", "document_id": "doc"}),
            ("get_fact", {"fact_id": "fact"}),
            ("trace_fact", {"fact_id": "fact"}),
        )
        for operation, arguments in cases:
            with self.subTest(operation=operation):
                result = json.loads(await self.invoke(arguments, name=f"memorylake_{operation}"))
                self.assertEqual(result["operation"], operation)
        self.assertEqual(len(self.calls), 5)

    async def test_strict_input_validation_rejects_coercions_empty_or_unbounded_arguments(self):
        for arguments in (
            {"query": 1},
            {"query": "q", "top_k": True},
            {"query": "q", "top_k": "1"},
            {"query": "q", "top_k": 51},
            {"query": ""},
            {"query": " "},
            {"query": "q\0"},
            {"query": "x" * 4097},
            {"query": "q", "project_ids": []},
        ):
            with self.subTest(arguments=arguments):
                self.assertEqual(await self.invoke(arguments), "memorylake_invalid_arguments")
        self.assertEqual(self.calls, [])

    async def test_project_scope_is_not_expanded_by_model(self):
        self.assertEqual(
            await self.invoke({"query": "q", "project_ids": ["other"]}),
            "memorylake_read_unavailable",
        )
        self.assertEqual(self.calls, [])

    async def test_thread_and_binding_authorization_io_is_off_event_loop(self):
        seen = []
        original = self.bindings.load_owned_active

        def get(*args, **kwargs):
            seen.append(threading.get_ident())
            self.assertEqual(kwargs["dashboard_thread_id"], "thread")
            self.assertEqual(kwargs["dashboard_session_key"], self.session_key)
            return original(*args, **kwargs)

        with patch.object(self.bindings, "load_owned_active", side_effect=get):
            await self.invoke()
        self.assertEqual(len(seen), 5)
        self.assertNotIn(threading.get_ident(), seen)

    async def test_cancel_is_not_swallowed_as_tool_error(self):
        with (
            patch(
                "octop.infra.connectors.memorylake.storage.StoredMemoryLakeReader.call",
                side_effect=asyncio.CancelledError,
            ),
            self.assertRaises(asyncio.CancelledError),
        ):
            await self.invoke()

    def test_factory_rejects_non_authenticated_identity_shapes(self):
        for overrides in (
            {"caller_user_id": True},
            {"caller_user_id": 0},
            {"caller_user_id": "1"},
            {"agent_id": "../agent"},
            {"thread_id": "bad\nthread"},
        ):
            with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                self.build_tools(**overrides)

    def test_factory_requires_explicit_shared_admission(self):
        with self.assertRaisesRegex(ValueError, "shared_read_admission_required"):
            self.build_tools(admission=None)

    def test_factory_requires_live_origin_identity(self):
        with self.assertRaisesRegex(ValueError, "dashboard_identity_required"):
            self.build_tools(identity=None)
        self.identity.close()
        with self.assertRaises(ValueError):
            self.build_tools()

    def test_factory_rejects_missing_shared_database_before_reader(self):
        with patch("octop.infra.connectors.memorylake.langchain.StoredMemoryLakeReader") as reader:
            with self.assertRaisesRegex(ValueError, "shared_database_admission_required"):
                self.build_tools(database=None)
            reader.assert_not_called()

    def test_factory_passes_same_shared_database_to_each_reader(self):
        from octop.infra.connectors.memorylake.storage import StoredMemoryLakeReader

        with patch(
            "octop.infra.connectors.memorylake.langchain.StoredMemoryLakeReader",
            wraps=StoredMemoryLakeReader,
        ) as reader:
            self.build_tools()
            self.build_tools()
        self.assertEqual(reader.call_count, 2)
        self.assertTrue(
            all(call.kwargs["database"] is self.database for call in reader.call_args_list)
        )
