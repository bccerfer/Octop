"""Invocation-time authorization with fake loaders and fake execution only."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from tests.support.memorylake import offline_test_guard

from octop.infra.connectors.memorylake.contract import CallerContext, ReadScope
from octop.infra.connectors.memorylake.read_service import (
    MemoryLakeReadError,
    MemoryLakeReadService,
    ReadConnection,
    ReadCredentials,
)
from octop.infra.connectors.memorylake.runner import ProcessResult


@unittest.skipUnless(os.name == "posix", "private CLI sessions require POSIX")
class MemoryLakeReadServiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.enterContext(offline_test_guard())
        self.scope = ReadScope(
            "connection-a", 11, "agent-a", "workspace-a", ("project-a",), "actor-a"
        )
        self.caller = CallerContext(11, "agent-a", 11, 1)
        self.connection = ReadConnection(
            self.scope, "https://app.memorylake.ai/openapi/memorylake", credential_revision=1
        )
        self.key = "dummy-api-key"
        self.calls = []
        self.events = []
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.block = False
        self.reply = ProcessResult(0, b'{"documents":[],"facts":[],"databases":[]}', b"")

        class FakeRunner:
            async def run(inner_self, arguments, *, env, cwd):
                self.calls.append((tuple(arguments), dict(env), cwd))
                self.entered.set()
                if self.block:
                    await self.release.wait()
                return self.reply

        async def connection():
            self.events.append("load-connection")
            return self.connection

        async def key():
            self.events.append("load-key")
            return ReadCredentials(self.key, self.connection.credential_revision)

        with patch(
            "octop.infra.connectors.memorylake.read_service.MemoryLakeRunner",
            return_value=FakeRunner(),
        ):
            self.service = MemoryLakeReadService(
                "connection-a",
                executable=Path(sys.executable),
                load_connection=connection,
                load_credentials=key,
            )

    async def call(self, **arguments):
        return await self.service.call(self.caller, "search_documents", {"query": "q", **arguments})

    async def test_valid_read_reloads_binding_and_cleans_private_session(self) -> None:
        response = await self.call()
        self.assertTrue(response["untrusted"])
        self.assertEqual(response["connector_id"], "connection-a")
        self.assertEqual(
            self.events, ["load-connection", "load-key", "load-connection", "load-connection"]
        )
        args, env, cwd = self.calls[0]
        self.assertEqual(args[:4], ("--profile", "octop", "--base-url", self.connection.base_url))
        self.assertEqual(args[-2:], ("--", "q"))
        self.assertEqual(env["MEMORYLAKE_API_KEY"], self.key)
        self.assertFalse(cwd.exists())

    async def test_denied_caller_never_loads_credentials_or_executes(self) -> None:
        self.caller = replace(self.caller, user_id=12)
        with self.assertRaises(ValueError):
            await self.call()
        self.assertEqual(self.events, ["load-connection"])
        self.assertEqual(self.calls, [])

    async def test_queued_call_rechecks_revocation_after_admission(self) -> None:
        self.block = True
        first = asyncio.create_task(self.call())
        await self.entered.wait()
        second = asyncio.create_task(self.call())
        await asyncio.sleep(0)
        self.connection = replace(self.connection, scope=replace(self.scope, enabled=False))
        self.release.set()
        with self.assertRaisesRegex(MemoryLakeReadError, "authorization_changed"):
            await first
        with self.assertRaises(ValueError):
            await second
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.events.count("load-key"), 1)

    async def test_scope_change_during_credential_load_is_rejected_before_execution(self) -> None:
        async def changed_key():
            self.connection = replace(
                self.connection, scope=replace(self.scope, authorization_revision=2)
            )
            return ReadCredentials(self.key, self.connection.credential_revision)

        self.service._load_credentials = changed_key
        with self.assertRaisesRegex(MemoryLakeReadError, "authorization_changed"):
            await self.call()
        self.assertEqual(self.calls, [])

    async def test_rotated_credentials_are_not_retained_in_closure(self) -> None:
        await self.call()
        self.key = "rotated-dummy-key"
        self.connection = replace(self.connection, credential_revision=2)
        await self.call()
        self.assertEqual(self.calls[0][1]["MEMORYLAKE_API_KEY"], "dummy-api-key")
        self.assertEqual(self.calls[1][1]["MEMORYLAKE_API_KEY"], "rotated-dummy-key")
        self.assertNotEqual(self.calls[0][2], self.calls[1][2])

    async def test_rotated_key_between_loads_is_rejected(self) -> None:
        async def racing_rotation():
            snapshot = ReadCredentials(self.key, 1)
            self.connection = replace(self.connection, credential_revision=2)
            return snapshot

        self.service._load_credentials = racing_rotation
        with self.assertRaisesRegex(MemoryLakeReadError, "authorization_changed"):
            await self.call()
        self.assertEqual(self.calls, [])

    async def test_credentials_snapshot_revision_must_match_binding(self) -> None:
        async def mismatched_credentials():
            return ReadCredentials(self.key, 2)

        self.service._load_credentials = mismatched_credentials
        with self.assertRaisesRegex(MemoryLakeReadError, "credentials_changed"):
            await self.call()
        self.assertEqual(self.calls, [])

    async def test_wrong_connection_and_unapproved_endpoint_fail_before_credentials(self) -> None:
        for connection in (
            replace(self.connection, scope=replace(self.scope, connector_id="connection-b")),
            replace(self.connection, base_url="https://attacker.invalid"),
            replace(self.connection, base_url="http://127.0.0.1"),
            replace(self.connection, base_url=self.connection.base_url + "?secret=key"),
        ):
            self.connection = connection
            with self.subTest(connection=connection), self.assertRaises(ValueError):
                await self.call()
        self.assertNotIn("load-key", self.events)
        self.assertEqual(self.calls, [])

    async def test_model_cannot_override_identity_or_endpoint(self) -> None:
        for key in ("as_user", "profile", "base_url", "workspace", "argv"):
            with self.subTest(key=key), self.assertRaises(ValueError):
                await self.call(**{key: "attacker"})
        self.assertNotIn("load-key", self.events)
        self.assertEqual(self.calls, [])

    async def test_failure_and_malformed_output_do_not_expose_upstream_body(self) -> None:
        for result in (
            ProcessResult(1, b"private stdout", b"private stderr"),
            ProcessResult(0, b"private stdout", b""),
            ProcessResult(0, b"\xff", b""),
        ):
            self.reply = result
            with (
                self.subTest(code=result.returncode),
                self.assertRaises(MemoryLakeReadError) as raised,
            ):
                await self.call()
            self.assertNotIn("private", str(raised.exception))

    async def test_api_key_is_redacted_from_successful_retrieval_text(self) -> None:
        self.reply = ProcessResult(
            0,
            json.dumps(
                {
                    "documents": [
                        {
                            "document_id": "doc-1",
                            "project_id": "project-a",
                            "document_summary": self.key,
                        }
                    ],
                    "facts": [],
                    "databases": [],
                }
            ).encode(),
            b"",
        )
        response = await self.call()
        self.assertNotIn(self.key, json.dumps(response))
        self.assertIn("[redacted]", json.dumps(response))

    async def test_json_escaped_key_echo_is_also_redacted(self) -> None:
        payload = json.dumps(
            {
                "documents": [
                    {
                        "document_id": "doc-1",
                        "document_summary": self.key,
                        "items": [{"type": "future", "extra": {self.key: self.key}}],
                    }
                ],
                "facts": [],
                "databases": [],
            }
        )
        escaped = "".join(f"\\u{ord(char):04x}" for char in self.key)
        self.reply = ProcessResult(0, payload.replace(self.key, escaped).encode(), b"")
        response = await self.call()
        self.assertNotIn(self.key, json.dumps(response))
        self.assertIn("[redacted]", json.dumps(response))

    async def test_whitespace_padded_key_is_normalized_before_redaction(self) -> None:
        self.key = "  dummy-api-key \n"
        self.reply = ProcessResult(
            0,
            b'{"documents":[{"document_id":"doc-1","document_summary":"dummy-api-key"}],"facts":[],"databases":[]}',
            b"",
        )
        response = await self.call()
        self.assertNotIn("dummy-api-key", json.dumps(response))
        self.assertEqual(self.calls[0][1]["MEMORYLAKE_API_KEY"], "dummy-api-key")

    async def test_out_of_scope_result_is_not_returned(self) -> None:
        self.reply = ProcessResult(
            0,
            b'{"documents":[{"document_id":"doc-1","project_id":"other"}],"facts":[],"databases":[]}',
            b"",
        )
        with self.assertRaisesRegex(MemoryLakeReadError, "invalid_read_response"):
            await self.call()

    async def test_other_read_operations_use_the_same_execution_boundary(self) -> None:
        for operation, arguments, payload in (
            ("search_facts", {"query": "q"}, {"facts": [{"id": "fact-1", "fact": "fixture"}]}),
            ("get_fact", {"fact_id": "fact-1"}, {"id": "fact-1", "fact": "fixture"}),
            ("trace_fact", {"fact_id": "fact-1"}, {"fact": {"id": "fact-1"}, "trace": []}),
            (
                "get_document",
                {"project_id": "project-a", "document_id": "doc-1"},
                {"id": "doc-1", "status": "okay"},
            ),
        ):
            with self.subTest(operation=operation):
                self.reply = ProcessResult(0, json.dumps(payload).encode(), b"")
                response = await self.service.call(self.caller, operation, arguments)
                self.assertEqual(response["operation"], operation)
                self.assertTrue(response["untrusted"])

    async def test_result_must_match_requested_subset_not_just_connection_allowlist(self) -> None:
        self.connection = replace(
            self.connection, scope=replace(self.scope, project_ids=("project-a", "project-b"))
        )
        self.reply = ProcessResult(
            0, b'{"documents":[{"document_id":"doc-1","project_id":"project-b"}]}', b""
        )
        with self.assertRaisesRegex(MemoryLakeReadError, "invalid_read_response"):
            await self.call(project_ids=["project-a"])

    async def test_cancelled_call_cleans_session_and_releases_admission(self) -> None:
        self.block = True
        task = asyncio.create_task(self.call())
        await self.entered.wait()
        directory = self.calls[0][2]
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertFalse(directory.exists())
        self.block = False
        response = await self.call()
        self.assertTrue(response["untrusted"])

    async def test_loader_errors_are_sanitized(self) -> None:
        async def failure():
            raise ValueError("sensitive loader details")

        self.service._load_connection = failure
        with self.assertRaisesRegex(MemoryLakeReadError, "connection_unavailable") as raised:
            await self.call()
        self.assertNotIn("sensitive", repr(raised.exception))
        self.assertEqual(self.calls, [])
