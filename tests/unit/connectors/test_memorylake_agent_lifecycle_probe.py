"""Characterize why shared graph observers cannot be disabled by callbacks=[].

Pure in-memory real LangGraph and locked Harness request; no model, service,
production gate removal or native graph mutation.
"""

import unittest
from typing import TypedDict

from langchain_core.callbacks import BaseCallbackHandler
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from octop_harness.request import ChatRequest
from tests.support.memorylake import offline_test_guard


class _State(TypedDict):
    query: str
    result: str


class AgentLifecycleProbeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())

    async def test_empty_request_callbacks_do_not_disable_bound_graph_or_checkpoint(self):
        observed = []

        class Observer(BaseCallbackHandler):
            def on_chain_start(self, serialized, inputs, **kwargs):
                observed.append(inputs)

        async def synthetic_node(state):
            return {"result": "synthetic private tool result"}

        builder = StateGraph(_State)
        builder.add_node("synthetic", synthetic_node)
        builder.add_edge(START, "synthetic")
        builder.add_edge("synthetic", END)
        saver = InMemorySaver()
        graph = builder.compile(checkpointer=saver).with_config(callbacks=[Observer()])
        request = ChatRequest(messages="synthetic query", thread_id="owned-thread", callbacks=[])
        config = request.to_runnable_config()
        self.assertNotIn("callbacks", config)
        # Even explicitly adding [] to RunnableConfig merges with bound handlers.
        config["callbacks"] = []
        result = await graph.ainvoke({"query": "synthetic query", "result": ""}, config)
        self.assertEqual(result["result"], "synthetic private tool result")
        self.assertTrue(
            any(
                isinstance(value, dict) and value.get("query") == "synthetic query"
                for value in observed
            )
        )
        saved = await saver.aget_tuple(config)
        self.assertEqual(
            saved.checkpoint["channel_values"]["result"], "synthetic private tool result"
        )
