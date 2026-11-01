"""Unregistered resource-owner + real UploadFile composition, not JWT/HTTP routing."""

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


class RuntimeCaptureTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())
        self.caller = CallerContext(1, "agent", 1, 1)
        self.auth_admissions = []

        def factory(admission):
            async def authorize(caller):
                self.assertEqual(caller, self.caller)
                self.auth_admissions.append(admission)
                await asyncio.shield(admission.submit(lambda: None))

            return authorize

        self.runtime = MemoryLakeFileRuntime(
            authorize_factory=factory,
            limits=FileRuntimeLimits(max_file_bytes=4, max_total_bytes=8),
        )
        self.addAsyncCleanup(self.runtime.aclose)

    async def test_real_upload_uses_unified_owner_and_stops_after_close(self):
        upload = UploadFile(io.BytesIO(b"abc"), filename="synthetic.txt")
        try:
            key = await capture_runtime_memorylake_upload(self.runtime, self.caller, upload)
            with self.runtime.operation():
                self.assertEqual((await self.runtime.source.read(self.caller, key)).content, b"abc")
            self.assertEqual(self.auth_admissions, [self.runtime.database] * 3)
            await self.runtime.aclose()
            with self.assertRaises(ValueError):
                await capture_runtime_memorylake_upload(self.runtime, self.caller, upload)
        finally:
            await upload.close()

    async def test_runtime_shutdown_during_real_file_read_drains_before_close(self):
        entered, release, finished = threading.Event(), threading.Event(), threading.Event()

        class RolledFile(SpooledTemporaryFile):
            def read(inner, size=-1):
                entered.set()
                try:
                    if not release.wait(3):
                        raise RuntimeError("synthetic blocked reader timed out")
                    return super().read(size)
                finally:
                    finished.set()

        file = RolledFile(max_size=1, mode="w+b")
        file.write(b"abc")
        file.seek(0)
        upload = UploadFile(file, filename="synthetic.txt")
        capture = asyncio.create_task(
            capture_runtime_memorylake_upload(self.runtime, self.caller, upload)
        )
        closing = None
        try:
            self.assertTrue(await asyncio.to_thread(entered.wait, 1))
            closing = asyncio.create_task(self.runtime.aclose())
            await asyncio.sleep(0.02)
            closing.cancel()
            capture.cancel()
            await asyncio.sleep(0.02)
            self.assertFalse(capture.done())
            self.assertFalse(closing.done())
            self.assertFalse(finished.is_set())
            self.assertFalse(file.closed)
            self.assertEqual(self.runtime.source._pending_bytes, 8)
        finally:
            release.set()
            await asyncio.gather(capture, *([closing] if closing else []), return_exceptions=True)
            await self.runtime.aclose()
            self.assertTrue(finished.is_set())
            await upload.close()
        self.assertTrue(capture.cancelled())
        self.assertTrue(closing.cancelled())
        self.assertTrue(self.runtime.drained)
        self.assertFalse(self.runtime.source._entries)
