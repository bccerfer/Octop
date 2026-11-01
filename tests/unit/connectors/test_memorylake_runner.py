"""Real local subprocess tests; scripts never use network or real credentials."""

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

from octop.infra.connectors.memorylake.runner import (
    MemoryLakeCancelledError,
    MemoryLakeRunner,
    MemoryLakeRunnerError,
    ProcessResult,
)


@unittest.skipUnless(os.name == "posix", "process-group isolation requires POSIX")
class MemoryLakeRunnerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.enterContext(offline_test_guard())
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.runner = MemoryLakeRunner(Path(sys.executable), max_output_bytes=1024, timeout_s=2)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def script(self, body: str) -> tuple[str, ...]:
        path = self.root / "fake.py"
        path.write_text(protected_fake_python(body))
        return (str(path),)

    async def run_script(self, body: str) -> ProcessResult:
        return await self.runner.run(self.script(body), env={}, cwd=self.root)

    async def test_nonzero_preserves_partial_json_and_hides_output_in_repr(self) -> None:
        result = await self.run_script(
            'import sys\nprint(\'{"document_id":"accepted"}\')\n'
            'print("private-error", file=sys.stderr)\nsys.exit(7)\n'
        )
        self.assertEqual(result.returncode, 7)
        self.assertEqual(json.loads(result.stdout), {"document_id": "accepted"})
        self.assertEqual(result.stderr, b"private-error\n")
        self.assertNotIn("accepted", repr(result))
        self.assertNotIn("private-error", repr(result))

    async def test_env_cwd_and_closed_stdin_are_explicit(self) -> None:
        arguments = self.script(
            'import os,sys,json\nprint(json.dumps([os.getenv("POISON"),'
            'os.getenv("ONLY"),os.getcwd(),sys.stdin.read()]))\n'
        )
        with patch.dict(os.environ, {"POISON": "do-not-inherit"}):
            result = await self.runner.run(arguments, env={"ONLY": "fake"}, cwd=self.root)
        self.assertEqual(json.loads(result.stdout), [None, "fake", str(self.root), ""])

    async def test_arguments_are_not_shell_interpreted(self) -> None:
        arguments = self.script('import sys,json\nprint(json.dumps(sys.argv[1:]))\n')
        query = "--base-url=bad; $(touch never-created)"
        result = await self.runner.run((*arguments, query), env={}, cwd=self.root)
        self.assertEqual(json.loads(result.stdout), [query])
        self.assertFalse((self.root / "never-created").exists())

    async def test_combined_stdout_stderr_limit(self) -> None:
        with self.assertRaisesRegex(MemoryLakeRunnerError, "^output_limit$"):
            await self.run_script(
                'import os\nos.write(1,b"a"*600)\nos.write(2,b"b"*600)\n'
            )

    async def test_large_output_is_stopped(self) -> None:
        with self.assertRaisesRegex(MemoryLakeRunnerError, "^output_limit$"):
            await asyncio.wait_for(self.run_script('import os\nwhile True: os.write(1,b"x"*65536)\n'), 3)

    async def test_timeout_does_not_echo_sensitive_arguments(self) -> None:
        self.runner = MemoryLakeRunner(Path(sys.executable), timeout_s=0.1)
        args = self.script('import time\ntime.sleep(30)\n')
        with self.assertRaisesRegex(MemoryLakeRunnerError, "^timeout$") as raised:
            await self.runner.run((*args, "sensitive-body"), env={}, cwd=self.root)
        self.assertNotIn("sensitive-body", repr(raised.exception))
        self.assertIsNone(raised.exception.__cause__)

    async def test_timeout_preserves_already_accepted_json(self) -> None:
        # The guarded fake imports stdlib helpers before printing its receipt.
        # Leave startup headroom; the test still exercises the real deadline.
        self.runner = MemoryLakeRunner(Path(sys.executable), timeout_s=1.0)
        with self.assertRaisesRegex(MemoryLakeRunnerError, "^timeout$") as raised:
            await self.run_script(
                "import time\nprint('{\"item_id\":\"private-accepted\"}',flush=True)\n"
                'time.sleep(30)\n'
            )
        error = raised.exception
        self.assertEqual(error.args, ("timeout",))
        self.assertNotIn("private-accepted", repr(error))
        self.assertIsNotNone(error.partial_result)
        result = error.partial_result
        self.assertEqual(json.loads(result.stdout), {"item_id": "private-accepted"})
        self.assertIsInstance(result.returncode, int)

    async def test_overflow_preserves_json_and_bounded_chunk_prefix(self) -> None:
        with self.assertRaisesRegex(MemoryLakeRunnerError, "^output_limit$") as raised:
            await self.run_script(
                "import os,time\nprint('{\"document_id\":\"private-accepted\"}',flush=True)\n"
                'time.sleep(0.05)\nos.write(2,b"private-stderr"*10000)\n'
            )
        error = raised.exception
        self.assertEqual(error.args, ("output_limit",))
        self.assertNotIn("private-accepted", repr(error))
        self.assertNotIn("private-stderr", repr(error))
        result = error.partial_result
        self.assertIsNotNone(result)
        self.assertEqual(json.loads(result.stdout), {"document_id": "private-accepted"})
        self.assertEqual(len(result.stdout) + len(result.stderr), 1024)
        self.assertTrue(result.stderr.startswith(b"private-stderr"))
        self.assertNotIn("private-accepted", repr(result))

    async def test_cancellation_preserves_accepted_json_as_typed_evidence(self) -> None:
        marker = self.root / "accepted"
        args = self.script(
            "import pathlib,time\nprint('{\"item_id\":\"private-accepted\"}',flush=True)\n"
            f"pathlib.Path({str(marker)!r}).touch()\ntime.sleep(30)\n"
        )
        task = asyncio.create_task(self.runner.run(args, env={}, cwd=self.root))
        async with asyncio.timeout(2):
            while not marker.exists():
                await asyncio.sleep(0.01)
            # Let the subprocess pipe reader consume the flushed response.
            await asyncio.sleep(0.05)
        task.cancel()
        with self.assertRaises(MemoryLakeCancelledError) as raised:
            await task
        error = raised.exception
        self.assertIsInstance(error, asyncio.CancelledError)
        self.assertTrue(task.cancelled())
        self.assertEqual(error.args, ("cancelled",))
        self.assertNotIn("private-accepted", repr(error))
        self.assertNotIn("private-accepted", str(error))
        self.assertIsNotNone(error.partial_result)
        self.assertEqual(json.loads(error.partial_result.stdout), {"item_id": "private-accepted"})
        self.assertLess(error.partial_result.returncode, 0)

    async def test_cancellation_reaps_process_and_releases_slot(self) -> None:
        marker = self.root / "started"
        args = self.script(
            f'import pathlib,time,os\npathlib.Path({str(marker)!r}).write_text(str(os.getpid()))\ntime.sleep(30)\n'
        )
        task = asyncio.create_task(self.runner.run(args, env={}, cwd=self.root))
        async with asyncio.timeout(2):
            while not marker.exists():
                await asyncio.sleep(0.01)
        pid = int(marker.read_text())
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(task, 2)
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)
        result = await self.run_script('print("next")\n')
        self.assertEqual(result.stdout, b"next\n")

    async def test_exited_parent_with_child_holding_pipes_is_killed_on_timeout(self) -> None:
        self.runner = MemoryLakeRunner(Path(sys.executable), timeout_s=1.0)
        with self.assertRaisesRegex(MemoryLakeRunnerError, "^timeout$"):
            await asyncio.wait_for(
                self.run_script(
                    'import os,time\nif os.fork() == 0:\n time.sleep(30)\nelse:\n os._exit(0)\n'
                ),
                3,
            )

    async def test_spawn_failure_is_sanitized(self) -> None:
        runner = MemoryLakeRunner(self.root / "secret-missing-binary")
        with self.assertRaisesRegex(MemoryLakeRunnerError, "^spawn_failed$") as raised:
            await runner.run(("private-argument",), env={"TOKEN": "fake-only"}, cwd=self.root)
        self.assertNotIn("secret", repr(raised.exception))
        self.assertIsNone(raised.exception.partial_result)
        self.assertIsNone(raised.exception.__cause__)

    async def test_instance_concurrency_limit(self) -> None:
        args = self.script(
            'import pathlib,time\np=pathlib.Path("exclusive")\n'
            'with p.open("x"): time.sleep(0.1)\np.unlink()\n'
        )
        results = await asyncio.gather(
            self.runner.run(args, env={}, cwd=self.root),
            self.runner.run(args, env={}, cwd=self.root),
        )
        self.assertEqual([result.returncode for result in results], [0, 0])

    async def test_cancellation_during_spawn_recovers_and_reaps_child(self) -> None:
        actual_spawn = asyncio.create_subprocess_exec
        created = asyncio.Event()
        release = asyncio.Event()
        spawned = []

        async def delayed_spawn(*args, **kwargs):
            process = await actual_spawn(*args, **kwargs)
            spawned.append(process)
            created.set()
            await release.wait()
            return process

        args = self.script('import time\ntime.sleep(30)\n')
        with patch(
            "octop.infra.connectors.memorylake.runner.asyncio.create_subprocess_exec",
            side_effect=delayed_spawn,
        ):
            task = asyncio.create_task(self.runner.run(args, env={}, cwd=self.root))
            await asyncio.wait_for(created.wait(), 2)
            task.cancel()
            release.set()
            with self.assertRaises(MemoryLakeCancelledError) as raised:
                await task
        self.assertIsNotNone(raised.exception.partial_result)
        self.assertEqual(raised.exception.partial_result.returncode, spawned[0].returncode)
        self.assertIsNotNone(spawned[0].returncode)
        with self.assertRaises(ProcessLookupError):
            os.kill(spawned[0].pid, 0)

    async def test_spawn_interruption_drains_unread_accepted_json_within_budget(self) -> None:
        timeout_s = 1.0
        for reason in ("cancelled", "timeout"):
            with self.subTest(reason=reason):
                self.runner = MemoryLakeRunner(Path(sys.executable), timeout_s=timeout_s, max_output_bytes=64)
                marker = self.root / (reason + "-flushed")
                args = self.script(
                    "import pathlib,time,os\nprint('{\"item_id\":\"accepted-unread\"}',flush=True)\n"
                    f"os.write(2,b\"d\"*4096)\npathlib.Path({str(marker)!r}).touch()\ntime.sleep(30)\n"
                )
                actual_spawn = asyncio.create_subprocess_exec
                created = asyncio.Event()
                release = asyncio.Event()

                async def delayed_spawn(*args, **kwargs):
                    process = await actual_spawn(*args, **kwargs)
                    while not marker.exists():
                        await asyncio.sleep(0.01)
                    created.set()
                    await release.wait()
                    return process

                with patch(
                    "octop.infra.connectors.memorylake.runner.asyncio.create_subprocess_exec",
                    side_effect=delayed_spawn,
                ):
                    task = asyncio.create_task(self.runner.run(args, env={}, cwd=self.root))
                    await asyncio.wait_for(created.wait(), 2)
                    if reason == "cancelled":
                        task.cancel()
                    else:
                        await asyncio.sleep(timeout_s + 0.05)
                    release.set()
                    expected = MemoryLakeCancelledError if reason == "cancelled" else MemoryLakeRunnerError
                    with self.assertRaises(expected) as raised:
                        await task
                error = raised.exception
                self.assertEqual(str(error), reason)
                self.assertNotIn("accepted-unread", repr(error))
                self.assertIsNotNone(error.partial_result)
                self.assertEqual(json.loads(error.partial_result.stdout), {"item_id": "accepted-unread"})
                self.assertLess(error.partial_result.returncode, 0)
                self.assertEqual(
                    len(error.partial_result.stdout) + len(error.partial_result.stderr),
                    64,
                )

    async def test_exact_limit_succeeds(self) -> None:
        result = await self.run_script('import os\nos.write(1,b"a"*512)\nos.write(2,b"b"*512)\n')
        self.assertEqual(len(result.stdout) + len(result.stderr), 1024)

    async def test_relative_cwd_is_rejected(self) -> None:
        with self.assertRaisesRegex(MemoryLakeRunnerError, "^invalid_cwd$"):
            await self.runner.run((), env={}, cwd=Path("relative"))

    def test_relative_binary_is_rejected(self) -> None:
        with self.assertRaisesRegex(MemoryLakeRunnerError, "^invalid_executable$"):
            MemoryLakeRunner(Path("python"))

    def test_windows_fails_closed(self) -> None:
        with patch("octop.infra.connectors.memorylake.runner.os.name", "nt"):
            with self.assertRaisesRegex(MemoryLakeRunnerError, "^unsupported_platform$"):
                MemoryLakeRunner(Path(sys.executable))
