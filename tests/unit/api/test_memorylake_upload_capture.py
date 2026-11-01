"""Real UploadFile/helper only; no route, JWT, native attachment or network I/O."""

import asyncio
import io
import threading
import unittest
from tempfile import SpooledTemporaryFile
from unittest.mock import patch

from fastapi import UploadFile
from tests.support.memorylake import offline_test_guard

from octop.api.routers.memorylake_upload import capture_new_memorylake_upload
from octop.infra.connectors.memorylake.contract import CallerContext
from octop.infra.connectors.memorylake.upload_io import UploadReadAdmission
from octop.infra.connectors.memorylake.upload_source import EphemeralUploadSource


class UploadCaptureTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())
        self.caller = CallerContext(1, "agent", 1, 1)
        self.active = True
        self.io = UploadReadAdmission(max_active=4)

        async def authorize(caller):
            if not self.active or caller != self.caller:
                raise ValueError("sensitive authentication body")

        self.source = EphemeralUploadSource(
            authorize=authorize,
            max_file_bytes=4,
            max_total_bytes=8,
        )
        self.addAsyncCleanup(self.close_source)

    async def close_source(self):
        self.source.close()
        await self.io.aclose()

    def upload(self, data=b"abc", filename="synthetic.txt", size=None):
        upload = UploadFile(io.BytesIO(data), filename=filename, size=size)
        self.addAsyncCleanup(upload.close)
        return upload

    async def test_real_uploadfile_capture_and_filename_snapshot(self):
        upload = self.upload()
        original = upload.file.read

        def read(size=-1):
            upload.filename = "changed.txt"
            return original(size)

        with patch.object(upload.file, "read", side_effect=read):
            key = await capture_new_memorylake_upload(
                self.source, self.caller, upload, io_admission=self.io
            )
        snapshot = await self.source.read(self.caller, key)
        self.assertEqual((snapshot.filename, snapshot.content), ("synthetic.txt", b"abc"))

    async def test_declared_and_actual_oversize_empty_and_invalid_filename(self):
        for upload, should_read in (
            (self.upload(b"abc", size=5), False),
            (self.upload(b"abcde", size=1), True),
            (self.upload(b"abcde"), True),
            (self.upload(b""), True),
            (self.upload(filename="../private"), False),
        ):
            with self.subTest(filename=upload.filename, size=upload.size):
                with patch.object(upload.file, "read", wraps=upload.file.read) as reader:
                    with self.assertRaises(ValueError):
                        await capture_new_memorylake_upload(
                            self.source, self.caller, upload, io_admission=self.io
                        )
                    self.assertEqual(bool(reader.call_count), should_read)
                self.assertFalse(self.source._entries)
                self.assertEqual(self.source._pending_bytes, 0)

    async def test_auth_or_capacity_rejection_never_reads_multipart_body(self):
        upload = self.upload()
        self.active = False
        with patch.object(upload.file, "read", wraps=upload.file.read) as reader:
            with self.assertRaises(ValueError) as caught:
                await capture_new_memorylake_upload(
                    self.source, self.caller, upload, io_admission=self.io
                )
            reader.assert_not_called()
            self.assertNotIn("sensitive", str(caught.exception))
        self.active = True
        await self.source.add(self.caller, "existing.txt", b"x")
        with patch.object(upload.file, "read", wraps=upload.file.read) as reader:
            with self.assertRaises(ValueError):
                await capture_new_memorylake_upload(
                    self.source, self.caller, upload, io_admission=self.io
                )
            reader.assert_not_called()

    async def test_revocation_during_read_does_not_publish_id(self):
        upload = self.upload()
        original = upload.file.read

        def read(size=-1):
            data = original(size)
            self.active = False
            return data

        with patch.object(upload.file, "read", side_effect=read), self.assertRaises(ValueError):
            await capture_new_memorylake_upload(
                self.source, self.caller, upload, io_admission=self.io
            )
        self.assertFalse(self.source._entries)
        self.assertEqual(self.source._pending_bytes, 0)

    async def test_cancellation_during_read_releases_reservation_no_snapshot(self):
        entered, release = threading.Event(), threading.Event()
        upload = self.upload()

        def blocked(size=-1):
            entered.set()
            if not release.wait(3):
                raise RuntimeError("synthetic blocked reader timed out")
            return b"abc"

        with patch.object(upload.file, "read", side_effect=blocked):
            task = asyncio.create_task(
                capture_new_memorylake_upload(
                    self.source, self.caller, upload, io_admission=self.io
                )
            )
            self.assertTrue(await asyncio.to_thread(entered.wait, 1))
            task.cancel()
            await asyncio.sleep(0)
            self.assertFalse(task.done())
            self.assertEqual(self.source._pending_bytes, 8)
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertFalse(self.source._entries)
        self.assertEqual(self.source._pending_bytes, 0)

    async def test_rolled_upload_read_worker_retains_budget_after_cancel_and_timeout(self):
        for action in ("cancel", "timeout"):
            with self.subTest(action=action):
                self.source.close()
                self.source = EphemeralUploadSource(
                    authorize=self.source._authorize,
                    max_file_bytes=4,
                    max_total_bytes=8,
                    ttl_s=0.03 if action == "timeout" else 60,
                )
                entered, release, finished = threading.Event(), threading.Event(), threading.Event()

                class RolledFile(SpooledTemporaryFile):
                    reads = 0
                    closed_while_reading = False

                    def read(
                        inner, size=-1, *, entered=entered, release=release, finished=finished
                    ):
                        inner.reads += 1
                        entered.set()
                        try:
                            if not release.wait(3):
                                raise RuntimeError("synthetic blocked reader timed out")
                            return super().read(size)
                        finally:
                            finished.set()

                    def close(inner, *, entered=entered, finished=finished):
                        if entered.is_set() and not finished.is_set():
                            inner.closed_while_reading = True
                        super().close()

                file = RolledFile(max_size=1, mode="w+b")
                file.write(b"abc")
                file.seek(0)
                self.assertTrue(file._rolled)
                upload = UploadFile(file, filename="rolled.txt")
                task = asyncio.create_task(
                    capture_new_memorylake_upload(
                        self.source, self.caller, upload, io_admission=self.io
                    )
                )
                try:
                    self.assertTrue(await asyncio.to_thread(entered.wait, 1))
                    if action == "cancel":
                        for _ in range(3):
                            task.cancel()
                            await asyncio.sleep(0)
                    await asyncio.sleep(0.06)
                    self.assertFalse(finished.is_set())
                    self.assertEqual(self.source._pending_bytes, 8)
                    self.assertFalse(task.done())
                    with self.assertRaises(ValueError):
                        await capture_new_memorylake_upload(
                            self.source, self.caller, self.upload(), io_admission=self.io
                        )
                finally:
                    release.set()
                    await asyncio.gather(task, return_exceptions=True)
                    self.assertTrue(await asyncio.to_thread(finished.wait, 1))
                    await upload.close()
                if action == "cancel":
                    self.assertTrue(task.cancelled())
                else:
                    self.assertIsInstance(task.exception(), ValueError)
                self.assertEqual(file.reads, 1)
                self.assertFalse(file.closed_while_reading)
                self.assertEqual(self.source._pending_bytes, 0)
                self.assertFalse(self.source._entries)

    async def test_cancel_all_capture_tasks_cannot_abandon_running_worker(self):
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
        upload = UploadFile(file, filename="rolled.txt")
        children = []
        create_task = asyncio.create_task

        def tracked(coro, **kwargs):
            job = create_task(coro, **kwargs)
            children.append(job)
            return job

        with patch("octop.api.routers.memorylake_upload.asyncio.create_task", side_effect=tracked):
            task = asyncio.get_running_loop().create_task(
                capture_new_memorylake_upload(
                    self.source, self.caller, upload, io_admission=self.io
                )
            )
            try:
                self.assertTrue(await asyncio.to_thread(entered.wait, 1))
                # Controlled equivalent of shutdown cancelling the whole capture
                # task subtree, never other pytest/server tasks in this loop.
                for child in children:
                    child.cancel()
                task.cancel()
                self.source.close()  # Stop admission before draining the I/O owner.
                self.io.close()
                draining = asyncio.get_running_loop().create_task(self.io.drain())
                await asyncio.sleep(0.03)
                self.assertFalse(draining.done())
                self.assertFalse(finished.is_set())
                self.assertEqual(self.source._pending_bytes, 8)
                self.assertFalse(task.done())
            finally:
                release.set()
                await asyncio.gather(task, *children, return_exceptions=True)
                await self.io.aclose()
                self.assertTrue(await asyncio.to_thread(finished.wait, 1))
                await upload.close()
        self.assertTrue(task.cancelled())
        self.assertEqual(self.source._pending_bytes, 0)
        self.assertFalse(self.source._entries)

    async def test_two_capture_helpers_cannot_read_same_file_concurrently(self):
        entered, release = threading.Event(), threading.Event()
        reads = []

        class RolledFile(SpooledTemporaryFile):
            def read(inner, size=-1):
                reads.append(size)
                entered.set()
                if not release.wait(3):
                    raise RuntimeError("synthetic blocked reader timed out")
                return super().read(size)

        file = RolledFile(max_size=1, mode="w+b")
        file.write(b"abc")
        file.seek(0)
        first_upload = UploadFile(file, filename="first.txt")
        second_upload = UploadFile(file, filename="second.txt")
        other_source = EphemeralUploadSource(
            authorize=self.source._authorize,
            max_file_bytes=4,
            max_total_bytes=8,
        )
        first = asyncio.create_task(
            capture_new_memorylake_upload(
                self.source, self.caller, first_upload, io_admission=self.io
            )
        )
        second = None
        try:
            self.assertTrue(await asyncio.to_thread(entered.wait, 1))
            second = asyncio.create_task(
                capture_new_memorylake_upload(
                    other_source, self.caller, second_upload, io_admission=self.io
                )
            )
            await asyncio.sleep(0.03)
            self.assertEqual(len(reads), 1)
            self.assertTrue(second.done())
            self.assertIsInstance(second.exception(), ValueError)
        finally:
            first.cancel()
            if second is not None:
                second.cancel()
            release.set()
            await asyncio.gather(
                first, *([second] if second is not None else []), return_exceptions=True
            )
            other_source.close()
            await first_upload.close()
        self.assertFalse(self.source._entries)
        self.assertFalse(other_source._entries)

    async def test_api_file_reads_use_owned_pool_not_default_executor(self):
        upload = self.upload()
        original = upload.file.read
        thread_names = []

        def read(size=-1):
            thread_names.append(threading.current_thread().name)
            return original(size)

        with patch.object(upload.file, "read", side_effect=read):
            await capture_new_memorylake_upload(
                self.source, self.caller, upload, io_admission=self.io
            )
        self.assertTrue(thread_names)
        self.assertTrue(all(name.startswith("memorylake-upload") for name in thread_names))
