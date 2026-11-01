"""Dormant server-owned lifecycle with synthetic callbacks/bytes; no production ingress."""

import asyncio
import threading
import unittest

from tests.support.memorylake import offline_test_guard

from octop.infra.connectors.memorylake.contract import CallerContext
from octop.infra.connectors.memorylake.file_runtime import FileRuntimeLimits, MemoryLakeFileRuntime


class FileRuntimeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())
        self.caller = CallerContext(1, "agent", 1, 1)

    def runtime(self, **limits):
        used = []

        def factory(admission):
            async def authorize(caller):
                used.append(admission)
                await asyncio.shield(admission.submit(lambda: None))

            return authorize

        runtime = MemoryLakeFileRuntime(
            authorize_factory=factory, limits=FileRuntimeLimits(**limits)
        )
        self.addAsyncCleanup(runtime.aclose)
        return runtime, used

    async def test_factory_uses_one_source_database_and_io_owner(self):
        runtime, used = self.runtime()
        with runtime.operation():
            key = await runtime.source.add(self.caller, "s.txt", b"abc")
            self.assertEqual((await runtime.source.read(self.caller, key)).content, b"abc")
        self.assertEqual(used, [runtime.database, runtime.database])
        self.assertFalse(runtime.closed)
        await runtime.aclose()
        self.assertTrue(runtime.closed)
        self.assertTrue(runtime.drained)
        self.assertFalse(runtime.source._entries)
        with self.assertRaises(ValueError), runtime.operation():
            self.fail("closed entry must not run")
        with self.assertRaises(ValueError):
            runtime.database.submit(lambda: None)

    async def test_stop_admission_keeps_db_open_for_active_operation_outcome_save(self):
        runtime, _ = self.runtime()
        active, finish = asyncio.Event(), asyncio.Event()
        events = []

        async def operation():
            with runtime.operation():
                active.set()
                await finish.wait()
                await asyncio.shield(
                    runtime.database.submit(lambda: events.append("outcome_saved"))
                )

        task = asyncio.create_task(operation())
        await active.wait()
        shutdown = asyncio.create_task(runtime.aclose())
        await asyncio.sleep(0)
        self.assertTrue(runtime.closed)
        self.assertFalse(shutdown.done())
        with self.assertRaises(ValueError), runtime.operation():
            self.fail("new operation must not run")
        finish.set()
        await task
        await shutdown
        self.assertEqual(events, ["outcome_saved"])
        self.assertTrue(runtime.drained)

    async def test_cancelled_read_and_cancelled_shutdown_wait_for_actual_db_worker(self):
        runtime, _ = self.runtime()
        entered, release = threading.Event(), threading.Event()

        def blocked():
            entered.set()
            release.wait(3)

        async def reader():
            with runtime.operation():
                await runtime.database.submit(blocked)

        task = asyncio.create_task(reader())
        shutdown = None
        try:
            self.assertTrue(await asyncio.to_thread(entered.wait, 1))
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            shutdown = asyncio.create_task(runtime.aclose())
            await asyncio.sleep(0)
            shutdown.cancel()
            await asyncio.sleep(0.02)
            self.assertFalse(shutdown.done())
            self.assertFalse(runtime.drained)
        finally:
            release.set()
            await asyncio.gather(task, *([shutdown] if shutdown else []), return_exceptions=True)
        self.assertTrue(shutdown.cancelled())
        self.assertTrue(runtime.drained)

    async def test_nested_use_and_self_drain_fail_closed(self):
        runtime, _ = self.runtime(max_operations=1)
        active, release = asyncio.Event(), asyncio.Event()

        async def hold():
            with runtime.operation():
                with self.assertRaises(ValueError), runtime.operation():
                    pass
                runtime.close()
                with self.assertRaises(ValueError):
                    await runtime.drain()
                active.set()
                await release.wait()

        task = asyncio.create_task(hold())
        await active.wait()
        try:
            with self.assertRaises(ValueError), runtime.operation():
                pass
        finally:
            release.set()
            await task
        await runtime.aclose()

    async def test_drain_requires_close_and_concurrent_closes_are_safe(self):
        runtime, _ = self.runtime()
        with self.assertRaises(ValueError):
            await runtime.drain()
        await asyncio.gather(runtime.aclose(), runtime.aclose())
        self.assertTrue(runtime.drained)

    async def test_invalid_configuration_rejected_without_starting_resources(self):
        for values in (
            {"max_operations": True},
            {"max_operations": 0},
            {"io_workers": 0},
            {"max_file_bytes": 0},
            {"ttl_s": float("nan")},
        ):
            with self.subTest(values=values), self.assertRaises(ValueError):
                self.runtime(**values)

    async def test_active_operation_limit_without_closing(self):
        runtime, _ = self.runtime(max_operations=1)
        entered, release = asyncio.Event(), asyncio.Event()

        async def holder():
            with runtime.operation():
                entered.set()
                await release.wait()

        task = asyncio.create_task(holder())
        await entered.wait()
        try:
            self.assertFalse(runtime.closed)
            with self.assertRaises(ValueError), runtime.operation():
                self.fail("second active operation must not be admitted")
        finally:
            release.set()
            await task
        with runtime.operation():
            self.assertFalse(runtime.closed)
