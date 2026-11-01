"""Dormant real SQLite/Fernet/binding/CLI composition; runner is NEVER executed."""

import base64
import io
import json
import stat
import sys
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from cryptography.fernet import Fernet
from fastapi import UploadFile
from tests.support.memorylake import offline_test_guard

from octop.api.routers.memorylake_upload import capture_runtime_memorylake_upload
from octop.infra.connectors.crypto import decrypt_credentials_snapshot
from octop.infra.connectors.memorylake.cli_transport import MemoryLakeCliTransport
from octop.infra.connectors.memorylake.contract import CallerContext
from octop.infra.connectors.memorylake.credentials import StoredMemoryLakeCredentials
from octop.infra.connectors.memorylake.file_binding import MemoryLakeFileBinding
from octop.infra.connectors.memorylake.file_runtime import MemoryLakeFileRuntime
from octop.infra.connectors.memorylake.runner import ProcessResult
from octop.infra.connectors.memorylake.submissions import Approval, SubmissionError
from octop.infra.connectors.memorylake.write_binding import StoredWriteBindingLoader
from octop.infra.crypto.fernet import PURPOSE_CONNECTOR
from octop.infra.db.migrate import run_migrations
from octop.infra.db.pool import SqlitePool
from octop.infra.db.repos.agents import AgentRepo
from octop.infra.db.repos.connectors import ConnectorRepo
from octop.infra.db.repos.memorylake import MemoryLakeBindingRepo
from octop.infra.db.repos.memorylake_submissions import MemoryLakeSubmissionRepo
from octop.infra.db.repos.users import UserRepo


class FileCliBindingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())
        root = self.enterContext(TemporaryDirectory())
        self.pool = SqlitePool(Path(root) / "db")
        self.addCleanup(self.pool.close)
        run_migrations(self.pool)
        self.owner = UserRepo(self.pool).create(username="synthetic-owner", role="user")
        AgentRepo(self.pool).create(agent_id="agent", user_id=self.owner, name="synthetic")
        self.connectors = ConnectorRepo(self.pool)
        self.connectors.create(
            instance_id="connector",
            user_id=self.owner,
            kind="memorylake",
            display_name="synthetic",
            mcp_server_name="synthetic-ml",
        )
        self.master = base64.urlsafe_b64encode(b"m" * 32)
        with self.pool.transaction() as conn:
            conn.execute(
                "INSERT INTO secrets(k,v,created_at) VALUES (?,?,0)",
                (PURPOSE_CONNECTOR, self.master),
            )
        self.secret = "synthetic-cli-binding-key"
        self.store_key(self.secret)
        self.repo = MemoryLakeBindingRepo(self.pool)
        self.repo.create(
            binding_id="binding",
            connector_id="connector",
            owner_user_id=self.owner,
            agent_id="agent",
            base_url="https://app.memorylake.ai/openapi/memorylake",
            workspace_id="workspace",
            project_ids_json='["project"]',
            enabled=True,
        )
        self.submissions = MemoryLakeSubmissionRepo(self.pool)
        self.submissions.replace_policy(
            binding_id="binding",
            expected_revision=0,
            project_id="project",
            library_parent_id="folder",
            enabled=True,
        )
        self.caller = CallerContext(self.owner, "agent", self.owner, 1)
        self.calls, self.roots, self.envs, self.factories = [], [], [], []
        self.content = b"synthetic newly supplied file, not a workspace attachment"
        self.filename = "-synthetic.txt"
        self.expected_key = self.secret

        class FakeRunner:
            def __init__(inner, executable):
                self.assertEqual(executable, Path(sys.executable))

            async def run(inner, argv, *, env, cwd):
                self.assertEqual(
                    argv[:4],
                    (
                        "--profile",
                        "octop",
                        "--base-url",
                        "https://app.memorylake.ai/openapi/memorylake",
                    ),
                )
                self.assertEqual(env["MEMORYLAKE_API_KEY"], self.expected_key)
                root = Path(cwd)
                self.assertEqual(root, Path(env["MEMORYLAKE_CONFIG_DIR"]))
                self.assertEqual(stat.S_IMODE(root.stat().st_mode), 0o700)
                self.assertNotIn(self.expected_key, (root / "credentials.toml").read_text())
                args = argv[4:]
                self.calls.append((args[:3], self.expected_key))
                self.roots.append(root)
                self.envs.append(env)
                if args[:2] == ("library", "upload"):
                    self.assertEqual(
                        args[:-1],
                        (
                            "library",
                            "upload",
                            "--parent",
                            "folder",
                            "--name=" + self.filename,
                            "--on-conflict",
                            "deny",
                            "--",
                        ),
                    )
                    staged = Path(args[-1])
                    self.assertEqual(staged, root / "source" / "payload")
                    self.assertEqual(staged.read_bytes(), self.content)
                    self.assertEqual(stat.S_IMODE(staged.stat().st_mode), 0o600)
                    payload = {"item_id": "item:1"}
                elif args[:3] == ("project", "document", "import"):
                    self.assertEqual(
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
                    payload = {
                        "success_count": 1,
                        "failure_count": 0,
                        "duplicate_count": 0,
                        "details_truncated": False,
                        "details": [
                            {"result": "success", "drive_item_id": "item:1", "document_id": "doc:1"}
                        ],
                    }
                else:
                    self.assertEqual(
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
                    payload = {"id": "doc:1", "status": "okay"}
                payload["untrusted_debug"] = {"secret": self.expected_key}
                return ProcessResult(0, json.dumps(payload).encode(), self.expected_key.encode())

        self.enterContext(
            patch("octop.infra.connectors.memorylake.cli_transport.MemoryLakeRunner", FakeRunner)
        )

    def store_key(self, key):
        blob = Fernet(self.master).encrypt(
            json.dumps({"instance_id": "connector", "api_key": "  " + key + "  "}).encode()
        )
        self.connectors.upsert_credentials(instance_id="connector", blob=blob)

    async def asyncSetUp(self):
        self.runtime = MemoryLakeFileRuntime(
            authorize_factory=lambda admission: (
                StoredWriteBindingLoader(
                    caller_user_id=self.owner,
                    agent_id="agent",
                    is_group=False,
                    binding_id="binding",
                    connector_id="connector",
                    repo=self.repo,
                    admission=admission,
                ).authorize
            )
        )
        self.addAsyncCleanup(self.runtime.aclose)

    async def create(self):
        def factory(connector_id, load_binding):
            self.assertEqual(connector_id, "connector")
            self.factories.append(connector_id)
            credentials = StoredMemoryLakeCredentials(
                caller=self.caller,
                binding_id="binding",
                connector_id=connector_id,
                repo=self.repo,
                admission=self.runtime.database,
                check_invocation=lambda: None,
            )
            self.assertIs(credentials._admission, self.runtime.database)
            return MemoryLakeCliTransport(
                connector_id,
                executable=Path(sys.executable),
                load_binding=load_binding,
                load_credentials=credentials.load,
            )

        binding = await MemoryLakeFileBinding.create(
            runtime=self.runtime,
            caller=self.caller,
            binding_id="binding",
            connector_id="connector",
            binding_repo=self.repo,
            submission_repo=self.submissions,
            transport_factory=factory,
            check_invocation=lambda: None,
        )

        async def close_binding():
            binding.close()

        self.addAsyncCleanup(close_binding)
        return binding

    async def approve_upload(self, binding):
        upload = UploadFile(
            file=io.BytesIO(self.content), filename=self.filename, size=len(self.content)
        )
        self.addAsyncCleanup(upload.close)
        attachment = await capture_runtime_memorylake_upload(
            self.runtime, self.caller, upload, scoped_source=binding.source
        )
        intent = await binding.prepare(attachment)
        self.assertEqual((intent.filename, intent.size), (self.filename, len(self.content)))
        self.assertIsNone(self.submissions.get_approval("approval"))
        self.assertEqual(self.calls, [])
        await binding.create_approval(Approval("approval", intent, time.time() + 60))
        return attachment

    def assert_safe(self, result):
        self.assertNotIn(self.secret, repr(result))
        self.assertNotIn(self.expected_key, repr(result))
        self.assertNotIn(self.master.decode(), repr(result))

    async def test_capture_approval_cli_submit_then_rotated_optout_readonly_reconcile(self):
        binding = await self.create()
        attachment = await self.approve_upload(binding)
        record = await binding.submit(attachment, "approval")
        self.assertEqual(
            (record.state, record.item_id, record.document_id), ("processing", "item:1", "doc:1")
        )
        self.assert_safe(record)
        self.expected_key = "synthetic-cli-rotated-key"
        self.store_key(self.expected_key)
        self.submissions.replace_policy(
            binding_id="binding",
            expected_revision=1,
            project_id="project",
            library_parent_id="folder",
            enabled=False,
        )
        record = await binding.check_status("approval")
        self.assertEqual(record.state, "ready")
        self.assert_safe(record)
        self.assertEqual(
            [item[0][:2] for item in self.calls],
            [("library", "upload"), ("project", "document"), ("project", "document")],
        )
        self.assertEqual(
            [item[1] for item in self.calls], [self.secret, self.secret, self.expected_key]
        )
        self.assertEqual(self.factories, ["connector"])
        self.assertTrue(all(not root.exists() for root in self.roots))
        self.assertTrue(all(not env for env in self.envs))

    async def assert_decrypt_race(self, revoke):
        binding = await self.create()
        attachment = await self.approve_upload(binding)

        def decrypt(master, blob):
            result = decrypt_credentials_snapshot(master, blob)
            if revoke:
                with self.pool.transaction() as conn:
                    conn.execute(
                        "UPDATE memorylake_bindings SET enabled=0 WHERE binding_id='binding'"
                    )
            else:
                self.store_key("synthetic-concurrent-rotation")
            return result

        with patch(
            "octop.infra.connectors.memorylake.credentials.decrypt_credentials_snapshot",
            side_effect=decrypt,
        ):
            if revoke:
                with self.assertRaises(SubmissionError) as caught:
                    await binding.submit(attachment, "approval")
                self.assert_safe(caught.exception)
            else:
                record = await binding.submit(attachment, "approval")
                self.assertEqual(record.state, "unknown")
                self.assert_safe(record)
        self.assertEqual(self.calls, [])
        saved = self.submissions.get("approval")
        self.assertEqual(saved.state, "unknown")
        self.assertIsNone(saved.item_id)
        # A durable upload lease was granted: zero fake-runner calls does not
        # justify describing arbitrary production unknown outcomes as "not sent".
        self.assertGreater(saved.revision, 1)

    async def test_rotation_during_decrypt_never_reaches_runner(self):
        await self.assert_decrypt_race(False)

    async def test_revocation_during_decrypt_never_reaches_runner(self):
        await self.assert_decrypt_race(True)
