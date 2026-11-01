"""Synthetic inert CLI artifacts only: hash inspection never executes a binary."""

import hashlib
import os
import unittest
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from tests.support.memorylake import offline_test_guard

from octop.config import MemoryLakeCliConfig
from octop.infra.connectors.memorylake.deployment import inspect_cli_artifact


class CliDeploymentTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())
        self.root = Path(self.enterContext(TemporaryDirectory()))
        self.binary = self.root / "memorylake-synthetic"
        self.content = b"INERT SYNTHETIC ARTIFACT - NEVER EXECUTE\n"
        self.binary.write_bytes(self.content)
        self.binary.chmod(0o755)
        self.config = MemoryLakeCliConfig(
            enabled=True,
            executable=str(self.binary),
            expected_version="synthetic-1.0",
            sha256=hashlib.sha256(self.content).hexdigest(),
        )
        self.spawn = self.enterContext(
            patch("subprocess.Popen", side_effect=AssertionError("no exec"))
        )

    def test_disabled_does_not_touch_filesystem_or_require_cli_for_native_config(self):
        with patch("os.open", side_effect=AssertionError("must not inspect disabled CLI")):
            result = inspect_cli_artifact(MemoryLakeCliConfig())
        self.assertEqual(result.status, "disabled")
        self.assertIsNone(result.artifact)
        self.spawn.assert_not_called()

    @unittest.skipUnless(os.name == "posix", "POSIX-only runner/deployment candidate")
    def test_hash_match_is_only_content_evidence_not_version_probe_or_feature_activation(self):
        result = inspect_cli_artifact(self.config)
        self.assertEqual(result.status, "content_verified")
        artifact = result.artifact
        self.assertEqual(artifact.executable, self.binary)
        self.assertEqual(artifact.expected_version, "synthetic-1.0")
        self.assertEqual((artifact.sha256, artifact.size), (self.config.sha256, len(self.content)))
        self.assertFalse(result.version_probed)
        self.assertNotIn(str(self.binary), repr(result))
        self.spawn.assert_not_called()

    def test_missing_wrong_hash_relative_or_unpinned_is_unavailable_not_exception(self):
        for values in (
            {"executable": str(self.root / "missing")},
            {"executable": "memorylake"},
            {"executable": "~/memorylake"},
            {"executable": "https://example.invalid/memorylake"},
            {"sha256": "0" * 64},
            {"sha256": ""},
            {"sha256": "g" * 64},
            {"expected_version": ""},
            {"expected_version": "version\nsensitive"},
        ):
            with self.subTest(fields=tuple(values)):
                result = inspect_cli_artifact(replace(self.config, **values))
                self.assertEqual(result.status, "unavailable")
                self.assertIsNone(result.artifact)
                self.assertNotIn(str(self.binary), repr(result))
        self.spawn.assert_not_called()

    @unittest.skipUnless(os.name == "posix", "POSIX-only runner/deployment candidate")
    def test_nonregular_symlink_nonexecuting_or_writable_file_is_denied(self):
        linked = self.root / "linked"
        linked.symlink_to(self.binary)
        fifo = self.root / "fifo"
        os.mkfifo(fifo)
        loop = self.root / "loop"
        loop.symlink_to(loop)
        for path in (self.root, linked, fifo, loop):
            with self.subTest(kind=path.name):
                result = inspect_cli_artifact(replace(self.config, executable=str(path)))
                self.assertEqual(result.status, "unavailable")
        for mode in (0o644, 0o777, 0o4755):
            with self.subTest(mode=mode):
                self.binary.chmod(mode)
                self.assertEqual(inspect_cli_artifact(self.config).status, "unavailable")
        self.spawn.assert_not_called()

    @unittest.skipUnless(os.name == "posix", "POSIX-only runner/deployment candidate")
    def test_file_replacement_during_hashing_is_not_reported_verified(self):
        import octop.infra.connectors.memorylake.deployment as deployment

        original = deployment.os.read
        changed = False

        def replacing(fd, count):
            nonlocal changed
            chunk = original(fd, count)
            if chunk and not changed:
                changed = True
                other = self.root / "replacement"
                other.write_bytes(self.content)
                other.chmod(0o755)
                os.replace(other, self.binary)
            return chunk

        with patch.object(deployment.os, "read", side_effect=replacing):
            result = inspect_cli_artifact(self.config)
        self.assertTrue(changed)
        self.assertEqual(result.status, "unavailable")
        self.assertIsNone(result.artifact)

    def test_oversized_or_empty_file_denies_before_reading_payload(self):
        self.binary.write_bytes(b"")
        self.assertEqual(inspect_cli_artifact(self.config).status, "unavailable")
        # Sparse synthetic oversized file: don't allocate its payload in memory.
        with self.binary.open("wb") as stream:
            stream.truncate(256 * 1024 * 1024 + 1)
        with patch("os.read", side_effect=AssertionError("oversize must not be read")):
            result = inspect_cli_artifact(self.config)
        self.assertEqual(result.status, "unavailable")
