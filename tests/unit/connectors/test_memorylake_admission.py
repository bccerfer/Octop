"""Shared admission, fake runners only; no remote traffic."""

from __future__ import annotations

import asyncio
import os
import sys
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from tests.support.memorylake import offline_test_guard

from octop.infra.connectors.memorylake.admission import ReadAdmission, ReadAdmissionError
from octop.infra.connectors.memorylake.contract import CallerContext, ReadScope
from octop.infra.connectors.memorylake.read_service import (
    MemoryLakeReadService,
    ReadConnection,
    ReadCredentials,
)
from octop.infra.connectors.memorylake.runner import ProcessResult


class AdmissionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())

    async def test_global_capacity_queue_bound_and_reuse(self):
        admission = ReadAdmission(max_active=1, max_queued=1)
        entered = asyncio.Event()

        async def waiting():
            async with admission.acquire("b"):
                entered.set()

        async with admission.acquire("a"):
            task = asyncio.create_task(waiting())
            await asyncio.sleep(0)
            self.assertFalse(entered.is_set())
            with self.assertRaisesRegex(ReadAdmissionError, "^read_admission_full$"):
                async with admission.acquire("c"):
                    self.fail("over capacity")
        await task
        self.assertTrue(entered.is_set())
        async with admission.acquire("c"):
            pass

    async def test_different_connectors_can_overlap(self):
        admission = ReadAdmission(max_active=2, max_queued=0)
        async with admission.acquire("a"), admission.acquire("b"):
            with self.assertRaises(ReadAdmissionError):
                async with admission.acquire("a"):
                    self.fail("same connector concurrent")

    async def test_cancel_waiter_and_active_reclaims_all_slots(self):
        admission = ReadAdmission(max_active=1, max_queued=1)
        entered = asyncio.Event()

        async def block():
            async with admission.acquire("a"):
                entered.set()
                await asyncio.Event().wait()

        active = asyncio.create_task(block())
        await entered.wait()
        pending = asyncio.create_task(block())
        await asyncio.sleep(0)
        pending.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await pending
        replacement = asyncio.create_task(block())
        await asyncio.sleep(0)
        replacement.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await replacement
        active.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await active
        async with admission.acquire("a"):
            pass

    async def test_invalid_limits(self):
        for kwargs in (
            {"max_active": 0},
            {"max_active": True},
            {"max_queued": -1},
            {"max_queued": 1.5},
            {"max_active": 1000000},
            {"max_queued": 1000000},
        ):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                ReadAdmission(**kwargs)

    @unittest.skipUnless(os.name == "posix", "private CLI sessions require POSIX")
    async def test_two_services_share_connector(self):
        await self._two_services(revoke=False)

    @unittest.skipUnless(os.name == "posix", "private CLI sessions require POSIX")
    async def test_two_services_check_queued_revocation(self):
        await self._two_services(revoke=True)

    async def _two_services(self, *, revoke):
        admission = ReadAdmission(max_active=2, max_queued=2)
        scope = ReadScope("connection", 1, "agent", "workspace", ("project",))
        connection = ReadConnection(scope, "https://app.memorylake.ai/openapi/memorylake", 1)
        caller = CallerContext(1, "agent", 1, 1)
        entered, release = asyncio.Event(), asyncio.Event()
        calls = []
        active = peak = 0

        async def load():
            return connection

        async def credentials():
            return ReadCredentials("DUMMY", 1)

        class Runner:
            async def run(self, *args, **kwargs):
                nonlocal active, peak
                calls.append(1)
                active += 1
                peak = max(peak, active)
                entered.set()
                try:
                    await release.wait()
                    return ProcessResult(0, b'{"documents":[]}', b"")
                finally:
                    active -= 1

        with patch(
            "octop.infra.connectors.memorylake.read_service.MemoryLakeRunner",
            side_effect=lambda *a: Runner(),
        ):
            services = [
                MemoryLakeReadService(
                    "connection",
                    executable=Path(sys.executable),
                    load_connection=load,
                    load_credentials=credentials,
                    admission=admission,
                )
                for _ in range(2)
            ]
        first = asyncio.create_task(services[0].call(caller, "search_documents", {"query": "q"}))
        await entered.wait()
        second = asyncio.create_task(services[1].call(caller, "search_documents", {"query": "q"}))
        await asyncio.sleep(0)
        self.assertEqual(len(calls), 1)
        if revoke:
            connection = replace(connection, scope=replace(scope, enabled=False))
        release.set()
        results = await asyncio.gather(first, second, return_exceptions=True)
        if revoke:
            self.assertTrue(all(isinstance(result, ValueError) for result in results))
            self.assertEqual(len(calls), 1)
        else:
            self.assertTrue(all(isinstance(result, dict) for result in results))
            self.assertEqual(len(calls), 2)
        self.assertEqual(peak, 1)
