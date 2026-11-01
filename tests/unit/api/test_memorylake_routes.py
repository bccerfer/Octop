"""Actual registered API + native middleware + real SQLite, fake transport only."""

import asyncio
import unittest
from unittest.mock import patch

import httpx
from tests.unit.api import test_memorylake_native_ingress as native_fixture

from octop.api.app import build_app
from octop.api.routers.memorylake import router
from octop.infra.connectors.memorylake.reviews import FileReviewRegistry
from octop.infra.connectors.memorylake.runner import ProcessResult

BASE = "/api/memorylake/agents/agent/bindings/binding"


class MemoryLakeRoutesTests(unittest.IsolatedAsyncioTestCase):
    setUp = native_fixture.NativeIngressTests.setUp
    multipart = native_fixture.NativeIngressTests.multipart
    trusted_binding = native_fixture.NativeIngressTests.trusted_binding

    async def asyncSetUp(self):
        await native_fixture.NativeIngressTests.asyncSetUp(self)
        await self.client.aclose()
        self.server._started = True
        outer = self

        class FakeApplication:
            runtime = outer.runtime
            reviews = FileReviewRegistry()

            def check_current(self):
                if self.runtime.closed:
                    raise ValueError("application_unavailable")

            def status(self):
                return {
                    "enabled": True,
                    "available": True,
                    "reason": "ready",
                    "max_file_bytes": 64,
                    "upload_ttl_seconds": 60,
                }

            def transport_provider(self, caller, binding_id, connector_id, check_invocation):
                check_invocation()
                return outer.transport_factory

        self.application = self.server.memorylake_application = FakeApplication()
        self.app = build_app(self.server)
        # This must be production-factory registration, not include_router in test.
        self.assertTrue(router.routes)
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app), base_url="http://synthetic"
        )
        self.addAsyncCleanup(self.client.aclose)
        self.headers = {"Authorization": "Bearer " + self.token}

    def transport_factory(self, connector_id, load_binding):
        outer = self

        class FakeTransport:
            async def upload(self, binding, snapshot, *, lease):
                outer.remote_calls.append("upload")
                return ProcessResult(0, b'{"item_id":"item:1"}', b"")

            async def import_file(self, binding, item_id, *, lease):
                outer.remote_calls.append("import")
                return ProcessResult(
                    0,
                    b'{"success_count":1,"failure_count":0,"duplicate_count":0,"details_truncated":false,"details":[{"result":"success","drive_item_id":"item:1","document_id":"doc:1"}]}',
                    b"",
                )

            async def get_document(self, binding, document_id):
                outer.remote_calls.append("get")
                return ProcessResult(0, b'{"id":"doc:1","status":"okay"}', b"")

        return FakeTransport()

    async def upload_review(self):
        uploaded = await self.client.post(
            BASE + "/uploads",
            content=self.multipart(),
            headers={
                **self.headers,
                "Content-Type": "multipart/form-data; boundary=" + self.boundary.decode(),
            },
        )
        self.assertEqual(uploaded.status_code, 200, uploaded.text)
        upload_id = uploaded.json()["upload_id"]
        review = await self.client.post(BASE + f"/uploads/{upload_id}/review", headers=self.headers)
        self.assertEqual(review.status_code, 200, review.text)
        self.assertEqual(self.remote_calls, [])
        with self.pool.connect() as conn:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM memorylake_submission_approvals").fetchone()[0],
                0,
            )
        return upload_id, review.json()

    async def test_registered_upload_review_explicit_submit_status_and_retry_no_duplicate_write(
        self,
    ):
        upload_id, review = await self.upload_review()
        self.assertEqual(
            (review["filename"], review["size"], review["project_id"]),
            ("synthetic.txt", len(self.payload), "project"),
        )
        body = {"review_id": review["review_id"], "confirm": True}
        url = BASE + f"/uploads/{upload_id}/submit"
        result = await self.client.post(url, json=body, headers=self.headers)
        self.assertEqual(result.status_code, 200, result.text)
        self.assertEqual(result.json()["state"], "processing")
        self.assertEqual(self.remote_calls, ["upload", "import"])
        repeated = await self.client.post(url, json=body, headers=self.headers)
        self.assertEqual(repeated.status_code, 200, repeated.text)
        self.assertEqual(self.remote_calls, ["upload", "import"])
        self.assertEqual(repeated.json()["submission_id"], review["submission_id"])
        with self.pool.transaction() as conn:
            conn.execute("UPDATE memorylake_write_policies SET enabled=0")
        status = await self.client.get(
            BASE + "/submissions/" + review["submission_id"], headers=self.headers
        )
        self.assertEqual(status.status_code, 200, status.text)
        self.assertEqual(status.json()["state"], "ready")
        self.assertEqual(self.remote_calls, ["upload", "import", "get"])

    async def test_status_recovers_every_persisted_phase_without_replaying_unobservable_write(self):
        upload_id, review = await self.upload_review()
        result = await self.client.post(
            BASE + f"/uploads/{upload_id}/submit",
            json={"review_id": review["review_id"], "confirm": True},
            headers=self.headers,
        )
        self.assertEqual(result.status_code, 200, result.text)
        url = BASE + "/submissions/" + review["submission_id"]
        for state in ("approved", "uploading", "uploaded", "importing", "unknown"):
            with self.subTest(state=state):
                with self.pool.transaction() as conn:
                    conn.execute(
                        "UPDATE memorylake_submissions SET state=?, document_id=NULL, "
                        "import_accepted=0 WHERE submission_id=?",
                        (state, review["submission_id"]),
                    )
                response = await self.client.get(url, headers=self.headers)
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(
                    response.json(),
                    {
                        "submission_id": review["submission_id"],
                        "state": state,
                        "requires_reconciliation": True,
                    },
                )
                self.assertEqual(self.remote_calls, ["upload", "import"])
        with self.pool.transaction() as conn:
            conn.execute("UPDATE memorylake_bindings SET enabled=0")
        denied = await self.client.get(url, headers=self.headers)
        self.assertGreaterEqual(denied.status_code, 400)
        self.assertNotIn(review["submission_id"], denied.text)
        self.assertEqual(self.remote_calls, ["upload", "import"])

    async def test_confirm_must_be_literal_true_and_review_must_match_current_intent(self):
        upload_id, review = await self.upload_review()
        url = BASE + f"/uploads/{upload_id}/submit"
        for body in (
            {"review_id": review["review_id"], "confirm": False},
            {"review_id": review["review_id"], "confirm": 1},
            {"review_id": "review_wrong", "confirm": True},
            {"review_id": review["review_id"], "confirm": True, "as_user": 1},
        ):
            with self.subTest(body_keys=tuple(body)):
                response = await self.client.post(url, json=body, headers=self.headers)
                self.assertGreaterEqual(response.status_code, 400)
                self.assertEqual(self.remote_calls, [])
        with self.pool.transaction() as conn:
            conn.execute("UPDATE memorylake_write_policies SET revision=revision+1")
        changed = await self.client.post(
            url, json={"review_id": review["review_id"], "confirm": True}, headers=self.headers
        )
        self.assertGreaterEqual(changed.status_code, 400)
        self.assertEqual(self.remote_calls, [])

    async def test_concurrent_confirmation_fails_closed_then_reads_same_receipt(self):
        upload_id, review = await self.upload_review()
        entered, release = asyncio.Event(), asyncio.Event()
        original_factory = self.transport_factory

        def factory(*args):
            transport = original_factory(*args)
            original_upload = transport.upload

            async def blocked(*a, **k):
                entered.set()
                await release.wait()
                return await original_upload(*a, **k)

            transport.upload = blocked
            return transport

        url = BASE + f"/uploads/{upload_id}/submit"
        body = {"review_id": review["review_id"], "confirm": True}
        with patch.object(self, "transport_factory", side_effect=factory):
            first = asyncio.create_task(self.client.post(url, json=body, headers=self.headers))
            try:
                async with asyncio.timeout(2):
                    await entered.wait()
                repeated = await self.client.post(url, json=body, headers=self.headers)
                # A persisted in-flight receipt is safe to observe, never replay.
                self.assertEqual(repeated.status_code, 200, repeated.text)
                self.assertEqual(repeated.json()["state"], "uploading")
                self.assertTrue(repeated.json()["requires_reconciliation"])
                self.assertEqual(self.remote_calls, [])
            finally:
                release.set()
                finished = await first
            self.assertEqual(finished.status_code, 200, finished.text)
        self.assertEqual(self.remote_calls, ["upload", "import"])
        with self.pool.connect() as conn:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM memorylake_submissions").fetchone()[0], 1
            )
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM memorylake_submission_approvals").fetchone()[0],
                1,
            )

    async def test_explicit_discard_after_write_optout_only_releases_local_snapshot(self):
        upload_id, review = await self.upload_review()
        with self.pool.transaction() as conn:
            conn.execute("UPDATE memorylake_write_policies SET enabled=0")
        url = BASE + f"/uploads/{upload_id}"
        wrong = await self.client.delete(
            url.replace("/bindings/binding/", "/bindings/other/"), headers=self.headers
        )
        self.assertGreaterEqual(wrong.status_code, 400)
        self.assertIn(upload_id, self.runtime.source._entries)
        result = await self.client.delete(url, headers=self.headers)
        self.assertEqual(result.status_code, 200, result.text)
        self.assertEqual(result.json(), {"discarded": True})
        self.assertNotIn(upload_id, self.runtime.source._entries)
        self.assertEqual(self.runtime.source._bytes, 0)
        repeated = await self.client.delete(url, headers=self.headers)
        self.assertEqual(repeated.status_code, 200, repeated.text)
        with self.pool.connect() as conn:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM memorylake_submission_approvals").fetchone()[0],
                0,
            )
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM memorylake_submissions").fetchone()[0], 0
            )
        self.assertEqual(self.remote_calls, [])
        self.assertNotIn(review["submission_id"], result.text)

    async def test_json_body_read_is_owned_by_runtime_until_close_rejection(self):
        entered, release = asyncio.Event(), asyncio.Event()

        async def body():
            entered.set()
            await release.wait()
            yield b'{"api_key":"synthetic-sensitive-sentinel"}'

        task = asyncio.create_task(
            self.client.post(
                "/api/memorylake/agents/agent/bindings",
                content=body(),
                headers={**self.headers, "Content-Type": "application/json"},
            )
        )
        drain = None
        try:
            async with asyncio.timeout(2):
                await entered.wait()
            self.assertTrue(self.runtime._holders, "JSON body escaped runtime operation admission")
            self.runtime.close()
            drain = asyncio.create_task(self.runtime.drain())
            await asyncio.sleep(0)
            self.assertFalse(drain.done(), "runtime drained with a request body still pending")
        finally:
            release.set()
            response = await task
            if drain is not None:
                await drain
        self.assertGreaterEqual(response.status_code, 400)
        self.assertNotIn("synthetic-sensitive-sentinel", response.text)
        self.assertEqual(self.remote_calls, [])

    async def test_connection_json_is_bounded_strict_and_error_does_not_echo_key(self):
        url = "/api/memorylake/agents/agent/bindings"
        sentinel = "synthetic-sensitive-sentinel"
        for body in (
            '{"api_key":"' + sentinel + '","api_key":"other"}',
            '{"api_key":"' + sentinel + '"}',
            '{"api_key":"' + sentinel + '"' + " " * 16384 + "}",
        ):
            with self.subTest(size=len(body)):
                response = await self.client.post(
                    url, content=body, headers={**self.headers, "Content-Type": "application/json"}
                )
                self.assertGreaterEqual(response.status_code, 400)
                self.assertNotIn(sentinel, response.text)
        with self.pool.connect() as conn:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM memorylake_bindings").fetchone()[0], 1
            )
        self.assertEqual(self.remote_calls, [])

    async def test_connection_api_current_owner_and_policy_cas_never_returns_secret(self):
        root = "/api/memorylake/agents/agent/bindings"
        response = await self.client.post(
            root,
            headers=self.headers,
            json={
                "api_key": "synthetic-secret",
                "base_url": "https://app.memorylake.ai/openapi/memorylake",
                "workspace_id": "work2",
                "project_id": "project2",
                "library_parent_id": "folder2",
                "display_name": "Synthetic",
                "writes_enabled": False,
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertNotIn("synthetic-secret", response.text)
        binding = response.json()
        self.assertFalse(binding["writes_enabled"])
        listed = await self.client.get(root, headers=self.headers)
        self.assertEqual(listed.status_code, 200, listed.text)
        self.assertIn(binding["binding_id"], [x["binding_id"] for x in listed.json()["bindings"]])
        policy = await self.client.patch(
            root + "/" + binding["binding_id"] + "/write-policy",
            json={"enabled": True, "expected_revision": binding["policy_revision"]},
            headers=self.headers,
        )
        self.assertEqual(policy.status_code, 200, policy.text)
        self.assertTrue(policy.json()["writes_enabled"])
        with self.pool.transaction() as conn:
            conn.execute("UPDATE users SET disabled=1 WHERE id=?", (self.owner,))
        denied = await self.client.get(root, headers=self.headers)
        self.assertGreaterEqual(denied.status_code, 400)
        self.assertNotIn(binding["binding_id"], denied.text)

    async def test_default_disabled_status_exists_but_file_body_never_consumed(self):
        self.server.memorylake_application = None
        status = await self.client.get("/api/memorylake/status", headers=self.headers)
        self.assertEqual(status.status_code, 200, status.text)
        self.assertFalse(status.json()["available"])
        consumed = []

        async def body():
            consumed.append(True)
            yield b"private payload"

        denied = await self.client.post(BASE + "/uploads", content=body(), headers=self.headers)
        self.assertGreaterEqual(denied.status_code, 400)
        self.assertFalse(consumed)
        self.assertFalse(self.runtime.source._entries)
        self.assertEqual(self.remote_calls, [])
