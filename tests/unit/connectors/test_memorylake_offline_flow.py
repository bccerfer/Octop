"""Composition smoke tests, not proof of an implemented upload/API workflow."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tests.support.memorylake import offline_test_guard, protected_fake_python

from octop.infra.connectors.memorylake.contract import CallerContext, ReadScope, plan_read
from octop.infra.connectors.memorylake.documents import (
    parse_document_result,
    parse_import_result,
    parse_upload_result,
)
from octop.infra.connectors.memorylake.runner import MemoryLakeRunner
from octop.infra.connectors.memorylake.session import isolated_cli_session

_FAKE_CLI = """
import json, os, pathlib, sys, tomllib
root = pathlib.Path(os.environ["MEMORYLAKE_CONFIG_DIR"])
profile = tomllib.loads((root / "credentials.toml").read_text())["profiles"]["octop"]
assert profile == {"api_key": "", "login_method": "api_key"}
assert os.environ["MEMORYLAKE_API_KEY"] == "dummy-only"
assert "HTTP_PROXY" not in os.environ
assert sys.argv[1:5] == ["--profile", "octop", "--base-url", "https://fake.invalid"]
args = sys.argv[5:]
if args[0] == "search":
    assert args[args.index("--workspace") + 1] == "workspace-a"
    assert args[args.index("--types") + 1] == "document"
    assert args[args.index("--projects") + 1] == "project-a"
    assert args[-2] == "--"
    print(json.dumps({"documents": [{"document_id": "doc-1"}], "facts": [], "databases": []}))
elif args[:2] == ["library", "upload"]:
    assert "--workspace" not in args
    assert args[args.index("--on-conflict") + 1] == "deny"
    assert pathlib.Path(args[-1]).read_text() == "fabricated offline fixture"
    print(json.dumps({"item_id": "file-1", "name": "dummy.txt", "uri": "not-a-real-uri"}))
elif args[:3] == ["project", "document", "import"]:
    assert args[-1] == "file-1"
    print(json.dumps({"success_count": 1, "failure_count": 0, "duplicate_count": 0,
        "details": [{"result": "success", "drive_item_id": "file-1", "document_id": "doc-1"}],
        "details_truncated": False}))
    sys.exit(7)
elif args[:3] == ["project", "document", "get"]:
    assert args[-1] == "doc-1"
    print(json.dumps({"id": "doc-1", "status": "okay"}))
else:
    sys.exit(9)
"""


@unittest.skipUnless(os.name == "posix", "private sessions and process groups are POSIX-only")
class MemoryLakeOfflineFlowTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.enterContext(offline_test_guard())

    async def test_plan_private_session_runner_and_result_contracts_compose(self) -> None:
        scope = ReadScope("connection-a", 11, "agent-a", "workspace-a", ("project-a",))
        caller = CallerContext(11, "agent-a", 11, 1)
        runner = MemoryLakeRunner(Path(sys.executable), timeout_s=2)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            script = root / "fake_cli.py"
            source = root / "dummy.txt"
            await asyncio.to_thread(script.write_text, protected_fake_python(_FAKE_CLI))
            await asyncio.to_thread(source.write_text, "fabricated offline fixture")
            with patch.dict(os.environ, {"HTTP_PROXY": "https://poison.invalid"}):
                # Test-only local setup; production session I/O must be offloaded.
                with isolated_cli_session("dummy-only") as session:
                    prefix = (
                        str(script),
                        "--profile",
                        session.profile,
                        "--base-url",
                        "https://fake.invalid",
                    )

                    async def invoke(arguments: tuple[str, ...]):
                        return await runner.run(
                            (*prefix, *arguments),
                            env=session.environment,
                            cwd=Path(session.environment["MEMORYLAKE_CONFIG_DIR"]),
                        )

                    search = plan_read(scope, caller, "search_documents", {"query": "--not-a-flag"})
                    response = await invoke(search.arguments)
                    self.assertEqual(response.returncode, 0)
                    self.assertEqual(
                        json.loads(response.stdout)["documents"][0]["document_id"], "doc-1"
                    )

                    # Manual test composition only: no approval or upload service exists yet.
                    response = await invoke(
                        (
                            "library",
                            "upload",
                            "--parent",
                            "folder-a",
                            "--on-conflict",
                            "deny",
                            "--",
                            str(source),
                        )
                    )
                    upload = parse_upload_result(response.returncode, response.stdout.decode())
                    self.assertEqual(upload.state, "uploaded")
                    self.assertEqual(upload.item_id, "file-1")

                    response = await invoke(
                        (
                            "project",
                            "document",
                            "import",
                            "--workspace",
                            "workspace-a",
                            "--project",
                            "project-a",
                            "--max-files",
                            "1",
                            "--",
                            upload.item_id,
                        )
                    )
                    imported = parse_import_result(response.returncode, response.stdout.decode())
                    self.assertEqual(response.returncode, 7)
                    self.assertEqual(imported.state, "accepted")
                    self.assertTrue(imported.requires_reconciliation)
                    self.assertEqual(imported.document_ids, ("doc-1",))

                    command = plan_read(
                        scope,
                        caller,
                        "get_document",
                        {"project_id": "project-a", "document_id": imported.document_ids[0]},
                    )
                    response = await invoke(command.arguments)
                    status = parse_document_result(
                        response.returncode, response.stdout.decode(), imported.document_ids[0]
                    )
                    self.assertEqual(status.state, "ready")
                    # Retrieval verification is a separate read after indexing.
                    response = await invoke(search.arguments)
                    self.assertEqual(
                        json.loads(response.stdout)["documents"][0]["document_id"],
                        status.document_id,
                    )
                self.assertEqual(session.environment, {})
