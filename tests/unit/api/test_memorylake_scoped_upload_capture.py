"""Scoped real UploadFiles under one dormant resource owner, never HTTP/remote I/O."""

import asyncio
import io
import threading
import unittest
from tempfile import SpooledTemporaryFile

from fastapi import UploadFile
from tests.support.memorylake import offline_test_guard

from octop.api.routers.memorylake_upload import capture_runtime_memorylake_upload
from octop.infra.connectors.memorylake.contract import CallerContext
from octop.infra.connectors.memorylake.file_runtime import FileRuntimeLimits, MemoryLakeFileRuntime


class ScopedCaptureTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())
        self.caller = CallerContext(1, "agent", 1, 1)
        self.calls = []

    def runtime(self):
        def deny_factory(admission):
            async def denied(caller):
                raise AssertionError("no unscoped authorization")

            return denied

        runtime = MemoryLakeFileRuntime(
            authorize_factory=deny_factory,
            limits=FileRuntimeLimits(max_file_bytes=3, max_total_bytes=12, max_io_active=1),
        )
        self.addAsyncCleanup(runtime.aclose)
        return runtime

    def scope(self, runtime, name):
        async def authorize(caller):
            self.calls.append((name, runtime.database))
            await runtime.database.submit(lambda: None)

        return runtime.source.bind(
            binding_id=f"binding-{name}",
            connector_id=f"connector-{name}",
            caller=self.caller,
            authorize=authorize,
        )

    async def test_scoped_capture_has_no_unscoped_or_cross_binding_fallback(self):
        runtime = self.runtime()
        first, second = self.scope(runtime, "a"), self.scope(runtime, "b")
        upload = UploadFile(io.BytesIO(b"abc"), filename="s.txt")
        try:
            key = await capture_runtime_memorylake_upload(
                runtime, self.caller, upload, scoped_source=first
            )
            for source in (runtime.source, second):
                with self.subTest(source=type(source).__name__), self.assertRaises(ValueError):
                    await source.read(self.caller, key)
            self.assertEqual((await first.read(self.caller, key)).content, b"abc")
            self.assertEqual(self.calls, [("a", runtime.database)] * 3)
        finally:
            await upload.close()

    async def test_foreign_runtime_scope_rejected_before_auth_or_file_read(self):
        first_runtime, second_runtime = self.runtime(), self.runtime()
        foreign_scope = self.scope(second_runtime, "a")

        class NoRead(io.BytesIO):
            def read(self, size=-1):
                raise AssertionError("foreign scope must not read")

        upload = UploadFile(NoRead(b"abc"), filename="s.txt")
        try:
            with self.assertRaises(ValueError):
                await capture_runtime_memorylake_upload(
                    first_runtime, self.caller, upload, scoped_source=foreign_scope
                )
            self.assertEqual(self.calls, [])
            self.assertFalse(first_runtime.source._entries)
            self.assertFalse(second_runtime.source._entries)
        finally:
            await upload.close()

    async def test_invalid_closed_or_wrong_caller_scope_never_falls_back(self):
        runtime = self.runtime()
        scope = self.scope(runtime, "a")
        scope.close()
        upload = UploadFile(io.BytesIO(b"abc"), filename="s.txt")
        try:
            for value in (scope, False, object()):
                with self.subTest(scope=type(value).__name__), self.assertRaises(ValueError):
                    await capture_runtime_memorylake_upload(
                        runtime, self.caller, upload, scoped_source=value
                    )
            live = self.scope(runtime, "a")
            other = CallerContext(2, "agent", 2, 1)
            with self.assertRaises(ValueError):
                await capture_runtime_memorylake_upload(runtime, other, upload, scoped_source=live)
            self.assertEqual(self.calls, [])
            self.assertEqual(upload.file.tell(), 0)
        finally:
            await upload.close()

    async def test_two_scopes_share_io_pool_and_late_closed_scope_cannot_publish(self):
        runtime = self.runtime()
        first, second = self.scope(runtime, "a"), self.scope(runtime, "b")
        entered, release = threading.Event(), threading.Event()
        threads = []

        class RolledFile(SpooledTemporaryFile):
            def read(inner, size=-1):
                threads.append(threading.current_thread().name)
                entered.set()
                if not release.wait(3):
                    raise RuntimeError("synthetic worker timeout")
                return super().read(size)

        file = RolledFile(max_size=1, mode="w+b")
        file.write(b"abc")
        file.seek(0)
        upload_a = UploadFile(file, filename="a.txt")
        upload_b = UploadFile(io.BytesIO(b"xyz"), filename="b.txt")
        capture = asyncio.create_task(
            capture_runtime_memorylake_upload(runtime, self.caller, upload_a, scoped_source=first)
        )
        try:
            self.assertTrue(await asyncio.to_thread(entered.wait, 1))
            self.assertEqual(runtime.source._pending_bytes, 6)
            with self.assertRaises(ValueError):
                await capture_runtime_memorylake_upload(
                    runtime, self.caller, upload_b, scoped_source=second
                )
            self.assertEqual(upload_b.file.tell(), 0)
            self.assertEqual([name for name, _ in self.calls], ["a"])
            first.close()
            release.set()
            with self.assertRaises(ValueError):
                await capture
            self.assertFalse(runtime.source._entries)
            self.assertEqual(runtime.source._pending_bytes, 0)
            key = await capture_runtime_memorylake_upload(
                runtime, self.caller, upload_b, scoped_source=second
            )
            self.assertEqual((await second.read(self.caller, key)).content, b"xyz")
            self.assertTrue(all(name.startswith("memorylake-upload") for name in threads))
        finally:
            release.set()
            await asyncio.gather(capture, return_exceptions=True)
            await upload_a.close()
            await upload_b.close()
