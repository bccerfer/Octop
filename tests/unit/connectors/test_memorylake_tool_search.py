"""Installed harness ToolSearch contracts; no model, CLI, database, or keys."""

import unittest

from langchain.agents.middleware import ModelRequest
from octop_harness.middleware.tool_search import ToolSearchMiddleware
from tests.support.memorylake import offline_test_guard

from octop.infra.connectors.memorylake.langchain import _ReadTool, _SearchDocuments


class MemoryLakeToolSearchTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())
        self.calls = []

        async def synthetic(**kwargs):
            self.calls.append(kwargs)
            return "synthetic"

        self.tool = _ReadTool.from_function(
            name="memorylake_search_documents",
            description="Synthetic read schema contract.",
            coroutine=synthetic,
            args_schema=_SearchDocuments,
            infer_schema=False,
        )

    async def test_real_client_deferred_rejected_before_model_handler(self):
        middleware = ToolSearchMiddleware(deferred_tools=frozenset({self.tool.name}))
        handlers = []

        async def handler(request):
            handlers.append(request)
            # Actual model serialization would ask for this schema next.
            return [tool.tool_call_schema for tool in request.tools]

        request = ModelRequest(model=object(), messages=[], tools=[self.tool], state={})
        with self.assertRaisesRegex(ValueError, "^memorylake_deferred_tools_unsupported$"):
            await middleware.awrap_model_call(request, handler)
        self.assertEqual(handlers, [])
        self.assertEqual(self.calls, [])
        self.assertIs(self.tool.args_schema, _SearchDocuments)

    async def test_real_client_self_deferred_rejected(self):
        middleware = ToolSearchMiddleware(deferred_tools=frozenset())
        marked = self.tool.model_copy(update={"extras": {"defer_loading": True}})

        async def handler(request):
            self.fail("deferred tool reached model")

        request = ModelRequest(model=object(), messages=[], tools=[marked], state={})
        with self.assertRaisesRegex(ValueError, "^memorylake_deferred_tools_unsupported$"):
            await middleware.awrap_model_call(request, handler)
        self.assertEqual(self.calls, [])

    def test_metadata_copy_preserves_schema_and_private_context_check(self):
        def check(config):
            return None

        self.tool._check_context = check
        copied = self.tool.model_copy(update={"description": "new synthetic description"})
        self.assertIs(copied.args_schema, _SearchDocuments)
        self.assertIs(copied.tool_call_schema, _SearchDocuments)
        self.assertIs(copied._check_context, check)
        same = self.tool.model_copy(update={"args_schema": _SearchDocuments})
        self.assertIs(same.tool_call_schema, _SearchDocuments)
        self.assertEqual(self.calls, [])

    async def test_real_client_eager_keeps_original_read_schema(self):
        middleware = ToolSearchMiddleware(deferred_tools=frozenset())
        seen = []

        async def handler(request):
            seen.extend(request.tools)
            return None

        await middleware.awrap_model_call(
            ModelRequest(model=object(), messages=[], tools=[self.tool], state={}), handler
        )
        self.assertEqual(seen, [self.tool])
        self.assertIs(seen[0].tool_call_schema, _SearchDocuments)
        self.assertEqual(self.calls, [])
