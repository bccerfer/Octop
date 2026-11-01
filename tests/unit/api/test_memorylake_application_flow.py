"""Real optional application and registered native app, synthetic CLI runner only."""

import json
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
from cryptography.fernet import Fernet
from tests.unit.connectors import test_memorylake_application as fixture

from octop.api.app import build_app
from octop.infra.auth.tokens import sign_token
from octop.infra.connectors.memorylake.runner import ProcessResult
from octop.infra.crypto.fernet import PURPOSE_CONNECTOR
from octop.infra.users.manager import UserManager


class ApplicationFlowTests(unittest.IsolatedAsyncioTestCase):
    setUp = fixture.ApplicationTests.setUp

    async def asyncSetUp(self):
        services = self.server.services
        self.owner = services.user_repo.create(username="owner", role="user")
        services.agent_repo.create(agent_id="agent", user_id=self.owner, name="synthetic")
        key = b"J" * 32
        services.secret_repo.get_or_create("jwt", lambda: key)
        self.headers = {
            "Authorization": "Bearer "
            + sign_token(key, sub=self.owner, uname="owner", role="user", ttl_seconds=120)
        }
        users = UserManager(services)
        await users.boot()
        self.server.app_runtime.user_manager = users
        self.server.app_runtime.bridge_manager = SimpleNamespace(
            bind_asgi_app=lambda *a, **kw: None, tunnel_http=AsyncMock()
        )
        self.calls, self.roots = [], []
        self.secret = "synthetic-application-secret"
        self.after_get = lambda: None
        self.after_search = lambda: None
        self.search_project = "project"
        outer = self

        class FakeRunner:
            def __init__(self, executable):
                outer.assertEqual(executable, outer.binary)

            async def run(self, argv, *, env, cwd):
                outer.assertEqual(
                    argv[:4],
                    (
                        "--profile",
                        "octop",
                        "--base-url",
                        "https://app.memorylake.ai/openapi/memorylake",
                    ),
                )
                outer.assertEqual(env["MEMORYLAKE_API_KEY"], outer.secret)
                root = Path(cwd)
                outer.roots.append(root)
                outer.assertNotIn(outer.secret, (root / "credentials.toml").read_text())
                args = argv[4:]
                if args[:2] == ("library", "upload"):
                    outer.calls.append("upload")
                    outer.assertEqual(
                        args[:-1],
                        (
                            "library",
                            "upload",
                            "--parent",
                            "folder",
                            "--name=synthetic.txt",
                            "--on-conflict",
                            "deny",
                            "--",
                        ),
                    )
                    outer.assertEqual(Path(args[-1]), root / "source" / "payload")
                    outer.assertEqual(Path(args[-1]).read_bytes(), b"synthetic upload bytes")
                    data = {"item_id": "item:1"}
                elif args[:3] == ("project", "document", "import"):
                    outer.calls.append("import")
                    outer.assertEqual(
                        args,
                        (
                            "project",
                            "document",
                            "import",
                            "--workspace",
                            "workspace",
                            "--project",
                            "project",
                            "--max-files",
                            "1",
                            "--",
                            "item:1",
                        ),
                    )
                    data = {
                        "success_count": 1,
                        "failure_count": 0,
                        "duplicate_count": 0,
                        "details_truncated": False,
                        "details": [
                            {"result": "success", "drive_item_id": "item:1", "document_id": "doc:1"}
                        ],
                    }
                elif args[:1] == ("search",):
                    outer.calls.append("search")
                    outer.assertEqual(
                        args,
                        (
                            "search",
                            "--workspace",
                            "workspace",
                            "--projects",
                            "project",
                            "--types",
                            "document",
                            "--top-k",
                            "5",
                            "--",
                            "synthetic query",
                        ),
                    )
                    data = {
                        "documents": [
                            {
                                "document_id": "doc:1",
                                "project_id": outer.search_project,
                                "document_name": "synthetic.txt",
                                "items": [
                                    {
                                        "text": "synthetic excerpt " + outer.secret,
                                        "upload_url": "https://private.invalid/signed",
                                    }
                                ],
                            }
                        ]
                    }
                    outer.after_search()
                else:
                    outer.calls.append("get")
                    outer.assertEqual(
                        args,
                        (
                            "project",
                            "document",
                            "get",
                            "--workspace",
                            "workspace",
                            "--project",
                            "project",
                            "--",
                            "doc:1",
                        ),
                    )
                    data = {"id": "doc:1", "status": "okay"}
                    outer.after_get()
                data["debug"] = outer.secret
                return ProcessResult(0, json.dumps(data).encode(), outer.secret.encode())

        self.enterContext(
            patch("octop.infra.connectors.memorylake.cli_transport.MemoryLakeRunner", FakeRunner)
        )
        self.enterContext(
            patch("octop.infra.connectors.memorylake.read_service.MemoryLakeRunner", FakeRunner)
        )
        await self.server._start_memorylake_application()
        await self.server.memorylake_application.settle_readiness()
        self.assertTrue(self.server.memorylake_application.status()["available"])
        self.assertEqual(self.probes, [("--version",)])
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=build_app(self.server)), base_url="http://synthetic"
        )
        self.addAsyncCleanup(self.client.aclose)

    async def request(self, method, path, **kwargs):
        response = await self.client.request(method, path, headers=self.headers, **kwargs)
        self.assertNotIn(self.secret, response.text)
        return response

    async def reviewed(self):
        response = await self.request(
            "POST",
            "/api/memorylake/agents/agent/bindings",
            json={
                "api_key": "  " + self.secret + "  ",
                "base_url": "https://app.memorylake.ai/openapi/memorylake",
                "workspace_id": "workspace",
                "project_id": "project",
                "library_parent_id": "folder",
                "display_name": "Synthetic",
                "writes_enabled": True,
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        connection = response.json()
        self.base = "/api/memorylake/agents/agent/bindings/" + connection["binding_id"]
        self.connection = connection
        body = (
            b'--synthetic\r\nContent-Disposition: form-data; name="file"; '
            b'filename="synthetic.txt"\r\n\r\nsynthetic upload bytes\r\n--synthetic--\r\n'
        )
        response = await self.client.post(
            self.base + "/uploads",
            content=body,
            headers={**self.headers, "Content-Type": "multipart/form-data; boundary=synthetic"},
        )
        self.assertEqual(response.status_code, 200, response.text)
        upload = response.json()["upload_id"]
        response = await self.request("POST", self.base + f"/uploads/{upload}/review")
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(self.calls, [])
        with self.pool.connect() as conn:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM memorylake_submission_approvals").fetchone()[0],
                0,
            )
        return upload, response.json()

    async def test_actual_application_api_upload_explicit_confirm_retry_rotate_optout_get(self):
        upload, review = await self.reviewed()
        path = self.base + f"/uploads/{upload}/submit"
        confirm = {"review_id": review["review_id"], "confirm": True}
        response = await self.request("POST", path, json=confirm)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["state"], "processing")
        repeated = await self.request("POST", path, json=confirm)
        self.assertEqual(repeated.status_code, 200, repeated.text)
        self.assertEqual(self.calls, ["upload", "import"])
        services = self.server.services
        self.secret = "synthetic-rotated-key"
        master = services.secret_repo.get(PURPOSE_CONNECTOR)
        services.connector_repo.upsert_credentials(
            instance_id=self.connection["connector_id"],
            blob=Fernet(master).encrypt(
                json.dumps(
                    {"instance_id": self.connection["connector_id"], "api_key": self.secret}
                ).encode()
            ),
        )
        response = await self.request(
            "PATCH",
            self.base + "/write-policy",
            json={"enabled": False, "expected_revision": self.connection["policy_revision"]},
        )
        self.assertEqual(response.status_code, 200, response.text)
        response = await self.request("GET", self.base + "/submissions/" + review["submission_id"])
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["state"], "ready")
        self.assertEqual(self.calls, ["upload", "import", "get"])
        searched = await self.request(
            "POST", self.base + "/search", json={"query": "synthetic query", "top_k": 5}
        )
        self.assertEqual(searched.status_code, 200, searched.text)
        self.assertEqual(searched.json()["data"]["documents"][0]["document_id"], "doc:1")
        self.assertEqual(self.calls, ["upload", "import", "get", "search"])
        self.assertTrue(all(not root.exists() for root in self.roots + self.probe_roots))

    async def test_current_owner_revocation_during_get_never_discloses_receipt(self):
        upload, review = await self.reviewed()
        response = await self.request(
            "POST",
            self.base + f"/uploads/{upload}/submit",
            json={"review_id": review["review_id"], "confirm": True},
        )
        self.assertEqual(response.status_code, 200, response.text)

        def revoke():
            with self.pool.transaction() as conn:
                conn.execute("UPDATE users SET disabled=1 WHERE id=?", (self.owner,))

        self.after_get = revoke
        response = await self.request("GET", self.base + "/submissions/" + review["submission_id"])
        self.assertGreaterEqual(response.status_code, 400)
        self.assertNotIn(review["submission_id"], response.text)
        self.assertNotIn("doc:1", response.text)

    async def test_registered_read_search_then_get_after_write_optout_is_scoped(self):
        await self.reviewed()
        response = await self.request(
            "PATCH",
            self.base + "/write-policy",
            json={
                "enabled": False,
                "expected_revision": self.connection["policy_revision"],
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        response = await self.request(
            "POST", self.base + "/search", json={"query": "synthetic query", "top_k": 5}
        )
        self.assertEqual(response.status_code, 200, response.text)
        result = response.json()
        self.assertTrue(result["untrusted"])
        self.assertEqual(result["request_scope"]["project_ids"], ["project"])
        self.assertEqual(
            result["data"]["documents"][0]["items"], [{"text": "synthetic excerpt [redacted]"}]
        )
        self.assertNotIn("private.invalid", response.text)
        response = await self.request("GET", self.base + "/documents/doc:1")
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["data"], {"id": "doc:1", "status": "okay"})
        self.assertEqual(self.calls, ["search", "get"])

    async def test_read_search_rejects_scope_arguments_outside_results_and_revoked_owner(self):
        await self.reviewed()
        for extra in (
            {"project_ids": ["other"]},
            {"endpoint": "https://private.invalid"},
            {"top_k": True},
        ):
            response = await self.request(
                "POST", self.base + "/search", json={"query": "synthetic query", **extra}
            )
            self.assertGreaterEqual(response.status_code, 400)
        self.assertEqual(self.calls, [])
        self.search_project = "other"
        response = await self.request(
            "POST", self.base + "/search", json={"query": "synthetic query", "top_k": 5}
        )
        self.assertGreaterEqual(response.status_code, 400)
        self.assertNotIn("synthetic excerpt", response.text)
        self.search_project = "project"

        def revoke():
            with self.pool.transaction() as conn:
                conn.execute("UPDATE users SET disabled=1 WHERE id=?", (self.owner,))

        self.after_search = revoke
        response = await self.request(
            "POST", self.base + "/search", json={"query": "synthetic query", "top_k": 5}
        )
        self.assertGreaterEqual(response.status_code, 400)
        self.assertNotIn("synthetic excerpt", response.text)
        self.assertEqual(self.calls, ["search", "search"])

    async def test_binding_list_does_not_disclose_after_owner_revoked_in_worker(self):
        await self.reviewed()
        from octop.infra.connectors.memorylake.connections import ConnectionManager

        original = ConnectionManager.list_owned

        def revoke_after_query(manager, user_id, agent_id):
            rows = original(manager, user_id, agent_id)
            with self.pool.transaction() as conn:
                conn.execute("UPDATE users SET disabled=1 WHERE id=?", (self.owner,))
            return rows

        with patch.object(ConnectionManager, "list_owned", revoke_after_query):
            response = await self.request("GET", "/api/memorylake/agents/agent/bindings")
        self.assertGreaterEqual(response.status_code, 400)
        self.assertNotIn(self.connection["binding_id"], response.text)
