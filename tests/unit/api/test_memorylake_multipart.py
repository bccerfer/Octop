"""In-memory ASGI multipart edge cases; no route, spool, network or real upload."""

import asyncio
import logging
import unittest
from unittest.mock import patch

from starlette.requests import Request
from tests.support.memorylake import offline_test_guard

from octop.api.common.memorylake_multipart import read_memorylake_multipart


def body(content=b"synthetic", filename="合成文件.txt", headers=None):
    if headers is None:
        headers = (
            'Content-Disposition: form-data; name="file"; filename="' + filename + '"'
        ).encode()
    return b"--boundary\r\n" + headers + b"\r\n\r\n" + content + b"\r\n--boundary--"


def request(chunks, headers=None):
    frames = list(chunks)
    received = []

    async def receive():
        received.append(True)
        part = frames.pop(0) if frames else b""
        return {"type": "http.request", "body": part, "more_body": bool(frames)}

    return Request(
        {
            "type": "http",
            "headers": headers
            if headers is not None
            else [(b"content-type", b"multipart/form-data; boundary=boundary")],
        },
        receive,
    ), received


class MemoryLakeMultipartTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())

    async def parse(self, req, **kwargs):
        return await read_memorylake_multipart(
            req,
            max_file_bytes=kwargs.pop("max_file_bytes", 1024),
            check_current=kwargs.pop("check_current", lambda: None),
            **kwargs,
        )

    async def test_every_split_single_byte_and_near_boundary_content(self):
        content = b"one\r\n--boundarX\r\n--boundary-not-end\r\ntwo"
        data = body(content)
        for index in range(1, len(data)):
            with self.subTest(index=index):
                req, _ = request([data[:index], data[index:]])
                self.assertEqual(await self.parse(req), ("合成文件.txt", content))
        req, _ = request([data[i : i + 1] for i in range(len(data))])
        self.assertEqual(await self.parse(req), ("合成文件.txt", content))

    async def test_all_truncation_prefixes_and_regular_boundary_then_eof_reject(self):
        data = body()
        for length in range(len(data)):
            with (
                self.subTest(length=length),
                self.assertRaisesRegex(ValueError, "^memorylake_multipart_unavailable$"),
            ):
                await self.parse(request([data[:length]])[0])
        with self.assertRaises(ValueError):
            await self.parse(request([data[:-2] + b"\r\n"])[0])

    async def test_parts_headers_and_filename_strict_subset(self):
        bad_headers = [
            b'Content-Disposition: form-data; name="other"; filename="x"',
            b'Content-Disposition: form-data; name="FILE"; filename="x"',
            b'Content-Disposition: form-data; name="file"',
            b'Content-Disposition: form-data; name="file"; name="file"; filename="x"',
            b'Content-Disposition: form-data; name="file"; filename="x"; filename="y"',
            b"Content-Disposition: form-data; name=\"file\"; filename*=utf-8''x",
            b'Content-Disposition: form-data; name="file"; filename="x"; unknown="y"',
            b'Content-Disposition: form-data; name="file"; filename="x',
            b'Content-Disposition: form-data; name="file"; filename="\xff"',
        ]
        good = b'Content-Disposition: form-data; name="file"; filename="x"'
        bad_headers += [
            good + b"\r\n" + extra
            for extra in (
                good,
                b"X-Private: secret",
                b"Content-Transfer-Encoding: base64",
                b"Content-Type: text/plain; charset=utf-8",
                b"Content-Type: text/plain\r\nContent-Type: text/plain",
                b"Content-Type: " + b"x" * 5000,
            )
        ]
        for header in bad_headers:
            with self.subTest(header=header), self.assertRaises(ValueError):
                await self.parse(request([body(headers=header)])[0])
        for filename in ("../x", "C:\\private\\x", ".", "..", "x:y", "x\x00y", "x\ny", "x\u200by"):
            with self.subTest(filename=filename), self.assertRaises(ValueError):
                await self.parse(request([body(filename=filename)])[0])
        for data in (body(b""), b"--boundary--", body()[:-2] + b"\r\n" + body()[12:]):
            with self.subTest(data=data), self.assertRaises(ValueError):
                await self.parse(request([data])[0])
        self.assertEqual(
            (await self.parse(request([body(headers=good + b"\r\nContent-Type: text/plain")])[0]))[
                0
            ],
            "x",
        )

    async def test_invalid_request_headers_reject_before_receive(self):
        good = (b"content-type", b"multipart/form-data; boundary=boundary")
        cases = [
            [],
            [good, good],
            [(b"content-type", b"text/plain")],
            [(b"content-type", b"multipart/form-data; boundary=" + b"x" * 71)],
            [(b"content-type", b"multipart/form-data; boundary=boundary; boundary=other")],
            [good, (b"content-length", b"1"), (b"content-length", b"1")],
            [good, (b"content-length", b"-1")],
            [good, (b"content-length", b" 3")],
            [good, (b"content-length", b"99999999")],
        ]
        for headers in cases:
            with self.subTest(headers=headers):
                req, calls = request([body()], headers)
                with self.assertRaises(ValueError):
                    await self.parse(req)
                self.assertEqual(calls, [])

    async def test_file_body_lengths_epilogue_and_no_spooling(self):
        data = body(b"abc")
        with (
            patch("tempfile.SpooledTemporaryFile", side_effect=AssertionError("no spool")),
            patch.object(
                asyncio.get_running_loop(),
                "run_in_executor",
                side_effect=AssertionError("no executor"),
            ),
        ):
            req, _ = request(
                [data],
                [
                    (b"content-type", b'multipart/form-data; boundary="boundary"'),
                    (b"content-length", str(len(data)).encode()),
                ],
            )
            self.assertEqual(await self.parse(req, max_file_bytes=3), ("合成文件.txt", b"abc"))
            with self.assertRaises(ValueError):
                await self.parse(request([body(b"abcd")])[0], max_file_bytes=3)
            capped = data + b"e" * (3 + 16384 - len(data))
            self.assertEqual((await self.parse(request([capped])[0], max_file_bytes=3))[1], b"abc")
            with self.assertRaises(ValueError):
                await self.parse(request([capped + b"e"])[0], max_file_bytes=3)
            req, _ = request(
                [data],
                [
                    (b"content-type", b"multipart/form-data; boundary=boundary"),
                    (b"content-length", b"1"),
                ],
            )
            with self.assertRaises(ValueError):
                await self.parse(req)

    async def test_cancellation_timeout_revocation_and_no_sensitive_logging(self):
        started = asyncio.Event()

        async def blocked():
            started.set()
            await asyncio.Event().wait()

        req = Request(
            {
                "type": "http",
                "headers": [(b"content-type", b"multipart/form-data; boundary=boundary")],
            },
            blocked,
        )
        task = asyncio.create_task(self.parse(req))
        await started.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        with self.assertRaisesRegex(ValueError, "^memorylake_multipart_unavailable$"):
            await self.parse(req, timeout_s=0.01)
        active = True

        async def revoked_receive():
            nonlocal active
            active = False
            return {"type": "http.request", "body": body(), "more_body": False}

        def check():
            if not active:
                raise ValueError("private auth detail")

        req = Request(req.scope, revoked_receive)
        with self.assertRaisesRegex(ValueError, "^memorylake_multipart_unavailable$"):
            await self.parse(req, check_current=check)
        with patch.object(logging.Logger, "_log") as log:
            with self.assertRaisesRegex(ValueError, "^memorylake_multipart_unavailable$"):
                await self.parse(request([b"--SENSITIVE-mismatch"])[0])
            log.assert_not_called()

    async def test_large_frame_is_fed_in_bounded_chunks_with_current_checks(self):
        from python_multipart.multipart import MultipartParser

        feeds, checks = [], []
        original = MultipartParser.write

        def write(parser, data):
            feeds.append(len(data))
            return original(parser, data)

        content = b"x" * (150 * 1024)
        req, _ = request([body(content)])
        with patch.object(MultipartParser, "write", write):
            result = await self.parse(
                req, max_file_bytes=len(content), check_current=lambda: checks.append(True)
            )
        self.assertEqual(result[1], content)
        self.assertGreater(len(feeds), 2)
        self.assertLessEqual(max(feeds), 64 * 1024)
        self.assertGreater(len(checks), 3 * len(feeds))

    async def test_auth_and_invalid_limits_reject_before_receive_empty_frames_time_out(self):
        def deny():
            raise ValueError("synthetic private identity")

        req, calls = request([body()])
        with self.assertRaisesRegex(ValueError, "^memorylake_multipart_unavailable$"):
            await self.parse(req, check_current=deny)
        self.assertEqual(calls, [])
        for options in (
            {"max_file_bytes": True},
            {"max_file_bytes": 0},
            {"timeout_s": float("inf")},
            {"timeout_s": 0},
        ):
            with self.subTest(options=options), self.assertRaises(ValueError):
                await self.parse(req, **options)
        self.assertEqual(calls, [])

        async def empty():
            return {"type": "http.request", "body": b"", "more_body": True}

        with self.assertRaisesRegex(ValueError, "^memorylake_multipart_unavailable$"):
            await self.parse(Request(req.scope, empty), timeout_s=0.01)
