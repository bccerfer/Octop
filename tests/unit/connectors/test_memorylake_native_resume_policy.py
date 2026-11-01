"""Option 2 policy probes: native history survives; new ML reads need live scope.

Real graph/checkpointer/HITL and SQLite; fake model/runner. This does not authorize
production activation or prove HTTP thread ACLs and external sink configuration.
"""

import unittest

from langchain.agents import create_agent
from langchain.agents.middleware import HumanInTheLoopMiddleware
from langchain_core.messages import HumanMessage, ToolMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command
from tests.unit.connectors import test_memorylake_middleware as fixture
from tests.unit.connectors.test_memorylake_langgraph import _Model

from octop.infra.connectors.memorylake.middleware import MemoryLakeReadMiddleware


class NativeResumePolicyTests(unittest.IsolatedAsyncioTestCase):
    setUp = fixture.MemoryLakeMiddlewareTests.setUp
    asyncSetUp = fixture.MemoryLakeMiddlewareTests.asyncSetUp
    seed = fixture.MemoryLakeMiddlewareTests.seed
    turn = fixture.MemoryLakeMiddlewareTests.turn
    graph = fixture.MemoryLakeMiddlewareTests.graph
    invoke = fixture.MemoryLakeMiddlewareTests.invoke

    async def test_saved_tool_data_remains_but_next_turn_without_scope_cannot_read_again(self):
        saver = InMemorySaver()
        graph, model = self.graph(saver=saver)
        with self.turn():
            first = await self.invoke(graph)
        saved = next(m for m in first["messages"] if isinstance(m, ToolMessage))
        self.assertEqual(saved.status, "success")
        self.assertIn('"document_id": "doc"', saved.content)
        self.assertEqual(len(self.calls), 1)
        # Connection revocation forbids new reads; it is NOT deletion of prior
        # legally saved ToolMessages. This probe assumes native thread ACL passed.
        with self.pool.transaction() as conn:
            conn.execute("UPDATE memorylake_bindings SET enabled=0 WHERE binding_id='first'")
        second = await graph.ainvoke(
            {"messages": [HumanMessage(content="ask again without trusted capability")]},
            self.first["config"],
        )
        results = [m for m in second["messages"] if isinstance(m, ToolMessage)]
        self.assertTrue(any(m.status == "success" and m.content == saved.content for m in results))
        self.assertEqual(results[-1].content, "memorylake_read_unavailable")
        self.assertEqual(results[-1].status, "error")
        self.assertEqual(len(self.calls), 1)
        checkpoint = await saver.aget_tuple(self.first["config"])
        self.assertIn(
            saved.content, [m.content for m in checkpoint.checkpoint["channel_values"]["messages"]]
        )
        self.assertNotIn("DUMMY-first-KEY", repr(checkpoint))
        self.assertNotIn("memorylake_search_documents", model.observations[-1])

    async def test_actual_hitl_resume_cannot_reconstruct_expired_read_grant(self):
        saver = InMemorySaver()
        graph = create_agent(
            _Model(force_call=True),
            tools=[],
            middleware=[
                MemoryLakeReadMiddleware(agent_id="first"),
                HumanInTheLoopMiddleware(interrupt_on={"memorylake_search_documents": True}),
            ],
            checkpointer=saver,
        )
        with self.turn():
            paused = await self.invoke(graph)
        self.assertTrue(paused.get("__interrupt__"))
        self.assertEqual(self.calls, [])
        # Real HITL Command.resume resumes pending state, not a fabricated new
        # call. Config retains thread/user strings but carries no live grant.
        resumed = await graph.ainvoke(
            Command(resume={"decisions": [{"type": "approve"}]}), self.first["config"]
        )
        result = next(m for m in resumed["messages"] if isinstance(m, ToolMessage))
        self.assertEqual((result.status, result.content), ("error", "memorylake_read_unavailable"))
        self.assertEqual(self.calls, [])
