"""Public HarnessAgent ordinary-tool loop; synthetic model/CLI and local fixtures.

No production AgentManager/processor registration. Origin issuance is trusted
fixture input, NOT authentication coverage. JSONL/checkpoint retention is kept;
structured-memory background extraction and external observers are not covered.
"""

import tempfile
import unittest
from pathlib import Path

from langchain_core.messages import AIMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langgraph.checkpoint.memory import InMemorySaver
from octop_harness import ChatRequest, HarnessAgent, HarnessAgentConfig
from octop_harness.llm.factory import ChatModelFactory
from pydantic import Field
from tests.unit.connectors import test_memorylake_middleware as fixtures
from tests.unit.connectors.test_memorylake_langgraph import _Model
from tests.unit.connectors.test_memorylake_middleware import native_echo

from octop.infra.connectors.memorylake.middleware import MemoryLakeReadMiddleware


class OrdinaryModel(_Model):
    received: list = Field(default_factory=list)
    before_model: object = None

    def bind_tools(self, tools, **kwargs):
        names = tuple(tool.name if hasattr(tool, "name") else tool["name"] for tool in tools)
        return self.model_copy(update={"offered": names})

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        if self.before_model is not None:
            self.before_model(messages)
        self.observations.append(self.offered)
        self.received.append(list(messages))
        if isinstance(messages[-1], ToolMessage):
            message = AIMessage(content="ordinary done")
        else:
            calls = [
                {"name": "native_echo", "args": {"text": "native-result"}, "id": "native-call"}
            ]
            if "memorylake_search_documents" in self.offered:
                calls.append(
                    {
                        "name": "memorylake_search_documents",
                        "args": {"query": "synthetic"},
                        "id": "ml-call",
                    }
                )
            message = AIMessage(content="", tool_calls=calls)
        return ChatResult(generations=[ChatGeneration(message=message)])


class OfflineFactory(ChatModelFactory):
    def __init__(self, model):
        super().__init__([])
        self.model = model

    def get_chat_model(self, model_ref, **kwargs):
        return self.model


class NativeHarnessTests(unittest.IsolatedAsyncioTestCase):
    # Reuse fixture construction only, not the bare-create_agent test methods.
    setUp = fixtures.MemoryLakeMiddlewareTests.setUp
    seed = fixtures.MemoryLakeMiddlewareTests.seed

    async def asyncSetUp(self):
        await fixtures.MemoryLakeMiddlewareTests.asyncSetUp(self)

    def turn(self, fixture=None, **overrides):
        return fixtures.MemoryLakeMiddlewareTests.turn(self, fixture, **overrides)

    def harness(self, *, agent_id="first", tool_search_mode="client"):
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        model = OrdinaryModel()
        agent = HarnessAgent(
            HarnessAgentConfig(
                workspace_dir=root,
                default_model="offline/synthetic",
                backend="filesystem",
                memory_enabled=False,
                session_log_enabled=True,
                checkpointer=InMemorySaver(),
                tools=[native_echo],
                middleware=[MemoryLakeReadMiddleware(agent_id=agent_id)],
                tool_search_mode=tool_search_mode,
            ),
            model_factory=OfflineFactory(model),
            agent_id=agent_id,
        )
        self.addAsyncCleanup(agent.aclose)
        return agent, model, root

    def request(self, **overrides):
        fixture = self.first
        return ChatRequest(
            **(
                {
                    "messages": "synthetic ordinary turn",
                    "thread_id": fixture["label"],
                    "agent_id": fixture["label"],
                    "user": str(fixture["user"]),
                    "source": "dashboard/octop-dashboard",
                    "configurable": {
                        "session_key": fixture["config"]["configurable"]["session_key"]
                    },
                }
                | overrides
            )
        )

    async def test_public_harness_keeps_native_tool_loop_with_scoped_memorylake(self):
        agent, model, root = self.harness()
        with self.turn():
            await agent.call(self.request())
        self.assertEqual(len(self.calls), 1)
        self.assertIn("native_echo", model.observations[0])
        self.assertIn("read_file", model.observations[0])
        self.assertIn("memorylake_search_documents", model.observations[0])
        messages = model.received[-1]
        tools = [message for message in messages if isinstance(message, ToolMessage)]
        self.assertTrue(any(message.content == "native-result" for message in tools))
        self.assertTrue(any("doc" in str(message.content) for message in tools))
        history = await agent.aget_history("first")
        self.assertIn("native-result", str(history))
        self.assertIn("document_id", str(history))
        logs = list(root.rglob("*.jsonl"))
        self.assertTrue(logs)
        self.assertIn("ordinary done", "".join(path.read_text() for path in logs))

    async def test_unscoped_public_harness_preserves_native_and_cannot_mint_from_config(self):
        agent, model, _ = self.harness()
        await agent.call(self.request(configurable={"as_user": "owner", "memorylake": True}))
        self.assertTrue(
            all(
                not any(name.startswith("memorylake_") for name in offered)
                for offered in model.observations
            )
        )
        self.assertTrue(any("native_echo" in offered for offered in model.observations))
        self.assertEqual(self.calls, [])

    async def test_foreign_compiled_agent_denies_before_model(self):
        agent, model, _ = self.harness(agent_id="second")
        with self.turn(), self.assertRaises(ValueError):
            await agent.call(self.request())
        self.assertEqual(model.observations, [])
        self.assertEqual(self.calls, [])

    async def test_revoked_result_is_not_passed_to_next_model(self):
        agent, model, _ = self.harness()

        async def revoke():
            with self.pool.transaction() as conn:
                conn.execute("UPDATE memorylake_bindings SET enabled=0 WHERE binding_id='first'")

        self.runner_hook = revoke
        with self.turn():
            await agent.call(self.request())
        tools = [message for message in model.received[-1] if isinstance(message, ToolMessage)]
        self.assertTrue(any(message.content == "native-result" for message in tools))
        self.assertFalse(any('"document_id": "doc"' in str(message.content) for message in tools))
        self.assertTrue(any("unavailable" in str(message.content) for message in tools))

    async def test_closed_turn_scope_stops_before_model_and_next_native_turn_has_no_schema(self):
        agent, model, _ = self.harness()
        with self.turn(), self.assertRaises(ValueError):
            self.first["identity"].close()
            await agent.call(self.request())
        self.assertEqual(model.observations, [])
        await agent.call(self.request())
        self.assertTrue(
            all(
                not any(name.startswith("memorylake_") for name in offered)
                for offered in model.observations
            )
        )
        self.assertEqual(self.calls, [])

    async def test_material_committed_before_next_model_and_cannot_share(self):
        from octop.infra.db.repos.agents import AgentRepo

        agent, model, _ = self.harness()
        checked = []

        def verify(messages):
            if any(isinstance(message, ToolMessage) for message in messages):
                with self.pool.connect() as conn:
                    count = conn.execute(
                        "SELECT COUNT(*) FROM memorylake_agent_admissions WHERE agent_id='first'"
                    ).fetchone()[0]
                self.assertEqual(count, 1)
                with self.assertRaises(ValueError):
                    AgentRepo(self.pool).set_shared("first", True)
                checked.append(True)

        model.before_model = verify
        with self.turn():
            await agent.call(self.request())
        self.assertTrue(checked)

    async def test_material_admission_failure_never_delivers_document_to_model(self):
        from unittest.mock import patch

        agent, model, _ = self.harness()
        with (
            self.turn(),
            patch.object(
                self.bindings,
                "admit_agent_material",
                side_effect=ValueError("private admission failure"),
            ),
        ):
            await agent.call(self.request())
        tools = [message for message in model.received[-1] if isinstance(message, ToolMessage)]
        self.assertFalse(any('"document_id": "doc"' in str(message.content) for message in tools))
        self.assertFalse(
            any("private admission failure" in str(message.content) for message in tools)
        )
        self.assertTrue(any("unavailable" in str(message.content) for message in tools))
