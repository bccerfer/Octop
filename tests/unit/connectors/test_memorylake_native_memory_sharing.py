"""Characterize native sharing after an ordinary MemoryLake-assisted answer.

Real HarnessAgent/model tool loops and SQLite MemoryService, fake model/CLI.
The public capture_turn bridge is invoked explicitly on the actual final answer:
this proves raw persistence/recall, NOT automatic background extraction or atoms.
No production gateway/authentication wiring or isolation guard is added here.
"""

import asyncio
import json
import re
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from langchain_core.messages import AIMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langgraph.checkpoint.memory import InMemorySaver
from octop_harness import ChatRequest, HarnessAgent, HarnessAgentConfig
from octop_harness.builtin.tools.memory_tools import build_memory_tools
from octop_harness.memory.store import close_memory_resources
from octop_memory import Memory, MemoryService
from tests.unit.connectors import test_memorylake_middleware as fixtures
from tests.unit.connectors.test_memorylake_native_harness import OfflineFactory, OrdinaryModel

from octop.infra.agents.teams.service import _user_may_use_member
from octop.infra.connectors.memorylake.middleware import MemoryLakeReadMiddleware
from octop.infra.connectors.memorylake.runner import ProcessResult
from octop.infra.db.repos.agents import AgentRepo

MARKER = "LANTERNQUARTZ"


class SharingModel(OrdinaryModel):
    viewer: bool = False

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        self.observations.append(self.offered)
        self.received.append(list(messages))
        last = messages[-1]
        if not isinstance(last, ToolMessage):
            name = "memory_search" if self.viewer else "memorylake_search_documents"
            message = AIMessage(
                content="", tool_calls=[{"name": name, "args": {"query": MARKER}, "id": name}]
            )
        elif self.viewer and last.name == "memory_search":
            path = re.search(r"raw/[^\s]+\.md", str(last.content))
            assert path is not None, last.content
            message = AIMessage(
                content="",
                tool_calls=[{"name": "memory_get", "args": {"path": path.group()}, "id": "get"}],
            )
        else:
            # Output is derived from the actual tool result, not seeded in the model.
            content = str(last.content)
            if not self.viewer:
                content = json.loads(content)["data"]["documents"][0]["document_summary"]
            message = AIMessage(content=content)
        return ChatResult(generations=[ChatGeneration(message=message)])


class NativeMemorySharingTests(unittest.IsolatedAsyncioTestCase):
    setUp = fixtures.MemoryLakeMiddlewareTests.setUp
    asyncSetUp = fixtures.MemoryLakeMiddlewareTests.asyncSetUp
    seed = fixtures.MemoryLakeMiddlewareTests.seed
    turn = fixtures.MemoryLakeMiddlewareTests.turn

    def harness(self, root, service, *, viewer=False):
        model = SharingModel(viewer=viewer)
        agent = HarnessAgent(
            HarnessAgentConfig(
                workspace_dir=root,
                default_model="offline/synthetic",
                backend="filesystem",
                memory_enabled=False,  # Explicit public capture bridge below, no extraction timers.
                session_log_enabled=True,
                checkpointer=InMemorySaver(),
                tools=build_memory_tools(service),
                middleware=[MemoryLakeReadMiddleware(agent_id="first")],
                tool_search_mode="client",
            ),
            model_factory=OfflineFactory(model),
            agent_id="first",
        )
        self.addAsyncCleanup(agent.aclose)
        return agent, model

    async def test_native_capture_persists_but_new_shared_viewer_admission_is_blocked(self):
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        db = root / "memory.sqlite"
        memory = Memory("agent_first", backend_config={"db_path": str(db)})
        self.addCleanup(close_memory_resources, memory)
        service = MemoryService(memory)
        owner, owner_model = self.harness(root, service)
        calls = []

        class Runner:
            async def run(self, arguments, **kwargs):
                calls.append(tuple(arguments))
                return ProcessResult(
                    0,
                    json.dumps(
                        {
                            "documents": [
                                {
                                    "document_id": "doc",
                                    "project_id": "project",
                                    "document_summary": f"The synthetic project marker is {MARKER} and delivery is next Friday.",
                                }
                            ]
                        }
                    ).encode(),
                    b"",
                )

        with patch(
            "octop.infra.connectors.memorylake.read_service.MemoryLakeRunner", return_value=Runner()
        ):
            with self.turn():
                await owner.call(
                    ChatRequest(
                        messages="Read the synthetic project summary.",
                        thread_id="first",
                        agent_id="first",
                        user=str(self.first["user"]),
                        source="dashboard/octop-dashboard",
                        configurable={
                            "session_key": self.first["config"]["configurable"]["session_key"]
                        },
                    )
                )
            history = await owner.aget_history("first")
            answers = [
                message
                for message in history
                if isinstance(message, AIMessage) and not message.tool_calls
            ]
            self.assertTrue(answers, history)
            answer = str(answers[-1].content)
            self.assertIn(MARKER, answer)
            self.assertEqual(len(calls), 1)
            self.assertIn("memorylake_search_documents", owner_model.observations[0])
            captured = service.capture_turn(
                assistant=answer,
                session_id="owner-session",
                thread_id="first",
                user_id=str(self.first["user"]),
            )
            self.assertEqual(captured["accepted"], 1)
            close_memory_resources(memory)
            reopened = Memory("agent_first", backend_config={"db_path": str(db)})
            self.addCleanup(close_memory_resources, reopened)
            persisted = MemoryService(reopened)
            self.assertIn(MARKER, str(persisted.search(MARKER)))
            agents = AgentRepo(self.pool)
            viewer_user = SimpleNamespace(id=self.second["user"], is_admin=False)
            self.assertFalse(_user_may_use_member(agents.get("first"), viewer_user))
            with self.assertRaisesRegex(ValueError, "^memorylake_material_not_shareable$"):
                agents.set_shared("first", True)
            self.assertFalse(agents.get("first").is_shared)
            self.assertFalse(_user_may_use_member(agents.get("first"), viewer_user))
            self.assertEqual(len(calls), 1)
            # Do not bypass the rejected sharing operation to invoke a viewer.
            # This establishes the repo/native member admission boundary, not
            # cancellation of a viewer already admitted before first ML use.

    async def test_inflight_shared_viewer_reads_new_owner_capture_after_unshare(self):
        """Pause native search in its executor, immediately before real SQL search.

        Shared-member admission is checked before starting the viewer Harness.
        Unshare does not drain that invocation. No MemoryLake scope is inherited
        by the viewer task, which was created before the owner scope exists.
        """
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        memory = Memory("agent_first", backend_config={"db_path": str(root / "memory.sqlite")})
        self.addCleanup(close_memory_resources, memory)
        service = MemoryService(memory)
        self.assertEqual(service.search(MARKER)["hits"], [])
        owner, _ = self.harness(root, service)
        viewer, viewer_model = self.harness(root, service, viewer=True)
        agents = AgentRepo(self.pool)
        viewer_user = SimpleNamespace(id=self.second["user"], is_admin=False)
        agents.set_shared("first", True)
        self.assertTrue(_user_may_use_member(agents.get("first"), viewer_user))
        entered, release, queried = threading.Event(), threading.Event(), threading.Event()
        actual_search = service.search

        def delayed_search(*args, **kwargs):
            entered.set()
            if not release.wait(10):
                raise RuntimeError("synthetic_search_wait_expired")
            result = actual_search(*args, **kwargs)
            queried.set()
            return result

        calls = []

        class Runner:
            async def run(self, arguments, **kwargs):
                calls.append(tuple(arguments))
                return ProcessResult(
                    0,
                    json.dumps(
                        {
                            "documents": [
                                {
                                    "document_id": "doc",
                                    "project_id": "project",
                                    "document_summary": f"The synthetic project marker is {MARKER} and delivery is next Friday.",
                                }
                            ]
                        }
                    ).encode(),
                    b"",
                )

        with (
            patch.object(service, "search", side_effect=delayed_search),
            patch(
                "octop.infra.connectors.memorylake.read_service.MemoryLakeRunner",
                return_value=Runner(),
            ),
        ):
            task = asyncio.create_task(
                viewer.call(
                    ChatRequest(
                        messages=f"Recall {MARKER} from native memory.",
                        thread_id="already-admitted-viewer",
                        agent_id="first",
                        user=str(viewer_user.id),
                        source="dashboard/octop-dashboard",
                    )
                )
            )
            try:
                self.assertTrue(await asyncio.to_thread(entered.wait, 5))
                self.assertFalse(queried.is_set())
                self.assertFalse(task.done())
                agents.set_shared("first", False)
                self.assertFalse(_user_may_use_member(agents.get("first"), viewer_user))
                # Current-private SQL authorizes the owner's FIRST MemoryLake read.
                with self.turn():
                    await owner.call(
                        ChatRequest(
                            messages="Read the synthetic project summary.",
                            thread_id="first",
                            agent_id="first",
                            user=str(self.first["user"]),
                            source="dashboard/octop-dashboard",
                            configurable={
                                "session_key": self.first["config"]["configurable"]["session_key"]
                            },
                        )
                    )
                history = await owner.aget_history("first")
                answer = str(
                    [m for m in history if isinstance(m, AIMessage) and not m.tool_calls][
                        -1
                    ].content
                )
                self.assertIn(MARKER, answer)
                self.assertEqual(
                    service.capture_turn(
                        assistant=answer,
                        session_id="owner-session",
                        thread_id="first",
                        user_id=str(self.first["user"]),
                    )["accepted"],
                    1,
                )
                self.assertEqual(len(calls), 1)
                self.assertFalse(queried.is_set())
            finally:
                release.set()
                # Await the actual invocation, not just the thread's Event signal.
                await asyncio.wait_for(asyncio.shield(task), timeout=10)
            self.assertTrue(queried.is_set())
            viewer_history = await viewer.aget_history("already-admitted-viewer")
            tools = [m for m in viewer_history if isinstance(m, ToolMessage)]
            self.assertEqual({m.name for m in tools}, {"memory_search", "memory_get"})
            self.assertTrue(all(MARKER in str(m.content) for m in tools))
            self.assertIn(MARKER, str(viewer_history[-1].content))
            self.assertEqual(len(calls), 1)
            self.assertTrue(
                all(
                    "memorylake_search_documents" not in names
                    for names in viewer_model.observations
                )
            )
