"""Server-owned upload reader admission; synthetic resources, no file/network I/O."""

import asyncio
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from tests.support.memorylake import offline_test_guard

from octop.infra.connectors.memorylake.upload_io import UploadReadAdmission


class UploadReadAdmissionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())

    async def test_same_underlying_file_is_exclusive_across_wrappers(self):
        admission = UploadReadAdmission(max_active=2)
        resource = object()
        wrappers = [SimpleNamespace(file=resource), SimpleNamespace(file=resource)]
        with admission.exclusive(wrappers[0].file):
            admission.check_current(resource)
            await asyncio.sleep(0)
            with (
                self.assertRaisesRegex(ValueError, "upload_read_busy"),
                admission.exclusive(wrappers[1].file),
            ):
                self.fail("duplicate resource admitted")
        with self.assertRaisesRegex(ValueError, "upload_read_unavailable"):
            admission.check_current(resource)
        with admission.exclusive(resource):
            admission.check_current(resource)

    async def test_capacity_and_cancellation_release(self):
        admission = UploadReadAdmission(max_active=1)
        started = asyncio.Event()
        first, second = object(), object()

        async def holder():
            with admission.exclusive(first):
                started.set()
                await asyncio.Event().wait()

        task = asyncio.create_task(holder())
        try:
            await started.wait()
            with (
                self.assertRaisesRegex(ValueError, "upload_read_busy"),
                admission.exclusive(second),
            ):
                self.fail("capacity exceeded")
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            with admission.exclusive(second):
                admission.check_current(second)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_close_revokes_checks_but_drain_waits_for_holder_exit(self):
        admission = UploadReadAdmission()
        resource = object()
        with self.assertRaisesRegex(ValueError, "upload_read_not_closed"):
            await admission.drain()
        with admission.exclusive(resource):
            admission.close()
            with self.assertRaisesRegex(ValueError, "upload_read_closed"):
                admission.check_current(resource)
            with (
                self.assertRaisesRegex(ValueError, "upload_read_closed"),
                admission.exclusive(object()),
            ):
                self.fail("closed admission accepted resource")
            drain = asyncio.create_task(admission.drain())
            await asyncio.sleep(0)
            self.assertFalse(drain.done())
        await drain
        await admission.aclose()

    async def test_cancelled_drain_does_not_release_holder_or_prevent_later_drain(self):
        admission = UploadReadAdmission()
        with admission.exclusive(object()):
            admission.close()
            drain = asyncio.create_task(admission.drain())
            await asyncio.sleep(0)
            drain.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await drain
            again = asyncio.create_task(admission.drain())
            await asyncio.sleep(0)
            self.assertFalse(again.done())
        await again

    async def test_strong_identity_reference_and_no_resource_close(self):
        import weakref

        class Resource:
            closed = False

            def close(inner):
                inner.closed = True

        admission = UploadReadAdmission()
        resource = Resource()
        ref = weakref.ref(resource)
        with admission.exclusive(resource):
            del resource
            self.assertIsNotNone(ref())
            self.assertFalse(ref().closed)
        self.assertIsNone(ref())

    async def test_limits_and_other_loop_are_rejected(self):
        for value in (True, 0, -1, 1.5):
            with (
                self.subTest(value=value),
                self.assertRaisesRegex(ValueError, "invalid_upload_read_limit"),
            ):
                UploadReadAdmission(max_active=value)
        for value in (True, 0, 17, 1.5):
            with (
                self.subTest(workers=value),
                self.assertRaisesRegex(ValueError, "invalid_upload_read_workers"),
            ):
                UploadReadAdmission(workers=value)
        admission = UploadReadAdmission()
        resource = object()
        with admission.exclusive(resource):

            async def other_loop():
                with self.assertRaisesRegex(ValueError, "upload_read_event_loop_changed"):
                    admission.check_current(resource)

            await asyncio.to_thread(lambda: asyncio.run(other_loop()))
            admission.check_current(resource)

    async def test_read_uses_owned_pool_and_cannot_be_cancelled_out_of_accounting(self):
        admission = UploadReadAdmission(max_active=1, workers=1)
        started, release = threading.Event(), threading.Event()
        thread_names = []

        class File:
            def read(inner, size):
                thread_names.append(threading.current_thread().name)
                started.set()
                release.wait(3)
                return b"x" * size

        file = File()
        loop = asyncio.get_running_loop()
        with patch.object(loop, "run_in_executor", wraps=loop.run_in_executor) as submit:
            try:
                with admission.exclusive(file):
                    result = admission.submit_read(file, 1)
                    self.assertIsNotNone(submit.call_args.args[0])
                    self.assertTrue(await asyncio.to_thread(started.wait, 1))
                    result.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await result
                    with self.assertRaisesRegex(ValueError, "upload_read_busy"):
                        admission.submit_read(file, 1)
                # Mistaken early holder exit must retain global capacity too.
                with (
                    self.assertRaisesRegex(ValueError, "upload_read_busy"),
                    admission.exclusive(object()),
                ):
                    self.fail("unfinished worker lost quota")
                admission.close()
                drain = asyncio.create_task(admission.drain())
                await asyncio.sleep(0)
                self.assertFalse(drain.done())
                release.set()
                await drain
                self.assertTrue(thread_names[0].startswith("memorylake-upload"))
            finally:
                release.set()
                await admission.aclose()

    async def test_aclose_cancellation_waits_for_worker_and_actual_pool_shutdown(self):
        admission = UploadReadAdmission(workers=1)
        started, release = threading.Event(), threading.Event()
        shutdown_started, shutdown_release = threading.Event(), threading.Event()

        class File:
            def read(inner, size):
                started.set()
                release.wait(3)
                raise ValueError("synthetic private read error")

        file = File()
        with admission.exclusive(file):
            future = admission.submit_read(file, 1)
            self.assertTrue(await asyncio.to_thread(started.wait, 1))
        real_shutdown = admission._pool.shutdown

        def blocked_shutdown():
            shutdown_started.set()
            shutdown_release.wait(3)
            real_shutdown()

        with patch.object(admission._pool, "shutdown", side_effect=blocked_shutdown) as shutdown:
            close = asyncio.create_task(admission.aclose())
            try:
                await asyncio.sleep(0)
                close.cancel()
                await asyncio.sleep(0)
                self.assertFalse(close.done())
                release.set()
                with self.assertRaises(ValueError):
                    await future
                self.assertTrue(await asyncio.to_thread(shutdown_started.wait, 1))
                close.cancel()
                await asyncio.sleep(0)
                self.assertFalse(close.done())
                shutdown_release.set()
                with self.assertRaises(asyncio.CancelledError):
                    await close
                await admission.aclose()
                shutdown.assert_called_once()
            finally:
                release.set()
                shutdown_release.set()
                await asyncio.gather(close, return_exceptions=True)
                await admission.aclose()

    async def test_cancel_all_close_tasks_cannot_abandon_actual_worker(self):
        admission = UploadReadAdmission(workers=1)
        started, release = threading.Event(), threading.Event()

        class File:
            def read(inner, size):
                started.set()
                release.wait(3)
                return b"x"

        file = File()
        with admission.exclusive(file):
            read = admission.submit_read(file, 1)
            self.assertTrue(await asyncio.to_thread(started.wait, 1))
        before = asyncio.all_tasks()
        close = asyncio.create_task(admission.aclose())
        try:
            await asyncio.sleep(0)
            # Model a server cancel-all, including any hidden shutdown Task.
            for task in asyncio.all_tasks() - before:
                task.cancel()
            for _ in range(3):
                await asyncio.sleep(0)
            self.assertFalse(close.done())
            release.set()
            self.assertEqual(await read, b"x")
            with self.assertRaises(asyncio.CancelledError):
                await close
            await admission.aclose()
        finally:
            release.set()
            await asyncio.gather(read, close, return_exceptions=True)
            # Also clean up the broken pre-fix implementation after red failure.
            await asyncio.to_thread(admission._pool.shutdown)

    async def test_concurrent_closers_share_one_actual_shutdown_future(self):
        admission = UploadReadAdmission()
        started, release = threading.Event(), threading.Event()
        original = admission._pool.shutdown

        def shutdown():
            started.set()
            release.wait(3)
            original()

        with patch.object(admission._pool, "shutdown", side_effect=shutdown) as close_pool:
            first = asyncio.create_task(admission.aclose())
            second = None
            try:
                self.assertTrue(await asyncio.to_thread(started.wait, 1))
                second = asyncio.create_task(admission.aclose())
                await asyncio.sleep(0)
                self.assertFalse(first.done())
                self.assertFalse(second.done())
                self.assertNotIsInstance(admission._closing, asyncio.Task)
                close_pool.assert_called_once()
            finally:
                release.set()
                await first
                if second is not None:
                    await second
            close_pool.assert_called_once()

    async def test_abandoned_shield_view_late_failure_is_observed_without_logging(self):
        import gc
        import weakref

        admission = UploadReadAdmission(workers=1)
        started, release = threading.Event(), threading.Event()
        loop = asyncio.get_running_loop()
        previous_handler = loop.get_exception_handler()
        errors = []
        loop.set_exception_handler(lambda loop, context: errors.append(context))
        refs = []

        class File:
            def read(inner, size):
                started.set()
                release.wait(3)
                raise ValueError("synthetic private failure")

        file = File()

        async def caller():
            with admission.exclusive(file):
                view = admission.submit_read(file, 1)
                refs.append(weakref.ref(view))
                await asyncio.shield(view)

        task = asyncio.create_task(caller())
        try:
            self.assertTrue(await asyncio.to_thread(started.wait, 1))
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            del task
            release.set()
            await admission.aclose()
            await asyncio.sleep(0)
            gc.collect()
            self.assertEqual(errors, [])
            self.assertIsNone(refs[0]())
        finally:
            release.set()
            await admission.aclose()
            loop.set_exception_handler(previous_handler)
