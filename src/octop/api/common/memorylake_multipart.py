"""Unregistered strict, bounded NEW-upload parser; no authentication or approval.

The authorized caller must reserve capacity before calling and supply a fresh,
otherwise unconsumed Request. Boundary framing is exclusively python-multipart's
public parser. We accept bounded MIME epilogue, counted in the body limit.

Supported part headers: form-data; name="file"; filename="UTF-8" in that order,
plus optional parameter-free Content-Type. No quoted escapes/extended parameters.
Payload storage is one preallocated max_file_bytes buffer plus the final copy;
feed copies are <=64 KiB. ASGI's already allocated frame, Python metadata and
upstream HTTP buffering are outside this bound. No disk/spool/executor is used.
"""

from __future__ import annotations

import asyncio
import logging
import math
import re
import unicodedata
from collections.abc import Callable
from typing import cast

from python_multipart.multipart import MultipartParser
from starlette.requests import Request

_FEED = 64 * 1024
_HEADER = 4096
_TYPE = re.compile(
    rb'multipart/form-data[ \t]*;[ \t]*boundary=(?:([A-Za-z0-9\'+_.-]{1,70})|"([A-Za-z0-9\'()+_,./:=?-]{1,70})")',
    re.IGNORECASE,
)
_DISPOSITION = re.compile(
    rb'(?i:form-data)[ \t]*;[ \t]*(?i:name)="file"[ \t]*;[ \t]*(?i:filename)="([^"\\]+)"',
)
_MEDIA = re.compile(rb"[A-Za-z0-9!#$&^_.+-]+/[A-Za-z0-9!#$&^_.+-]+")


class MemoryLakeMultipartError(ValueError):
    """Constant public reason only; no parser/header/body details."""


def _request_headers(request: Request, cap: int) -> tuple[bytes, int | None]:
    selected: dict[bytes, bytes] = {}
    for name, value in request.scope["headers"]:
        name = name.lower()
        if name in (b"content-type", b"content-length", b"content-encoding"):
            if name in selected:
                raise ValueError
            selected[name] = value
    raw = selected.get(b"content-type", b"")
    if len(raw) > 256:
        raise ValueError
    match = _TYPE.fullmatch(raw)
    if match is None or selected.get(b"content-encoding", b"identity") != b"identity":
        raise ValueError
    declared = None
    if b"content-length" in selected:
        raw_length = selected[b"content-length"]
        if not 1 <= len(raw_length) <= 20 or re.fullmatch(rb"[0-9]+", raw_length) is None:
            raise ValueError
        declared = int(raw_length)
        if declared > cap:
            raise ValueError
    return match.group(1) or match.group(2), declared


class _SingleFile:
    def __init__(self, cap: int) -> None:
        self.buffer = bytearray(cap)
        self.size = 0
        self.started = False
        self.headers_done = False
        self.part_done = False
        self.ended = False
        self.name = bytearray()
        self.value = bytearray()
        self.headers: dict[bytes, bytes] = {}
        self.filename = ""

    def begin(self) -> None:
        if self.started:
            raise ValueError
        self.started = True

    def field(self, data: bytes, start: int, end: int) -> None:
        if len(self.name) + len(self.value) + end - start > _HEADER:
            raise ValueError
        self.name.extend(memoryview(data)[start:end])

    def header_value(self, data: bytes, start: int, end: int) -> None:
        if len(self.name) + len(self.value) + end - start > _HEADER:
            raise ValueError
        self.value.extend(memoryview(data)[start:end])

    def header_end(self) -> None:
        name = bytes(self.name).lower()
        if name not in (b"content-disposition", b"content-type") or name in self.headers:
            raise ValueError
        self.headers[name] = bytes(self.value)
        self.name.clear()
        self.value.clear()

    def headers_finished(self) -> None:
        match = _DISPOSITION.fullmatch(self.headers.get(b"content-disposition", b""))
        if match is None:
            raise ValueError
        filename = match.group(1).decode("utf-8", errors="strict")
        if (
            not 1 <= len(filename) <= 255
            or filename in (".", "..")
            or any(c in '/\\:"' or unicodedata.category(c).startswith("C") for c in filename)
        ):
            raise ValueError
        media = self.headers.get(b"content-type")
        if media is not None and _MEDIA.fullmatch(media) is None:
            raise ValueError
        self.filename = filename
        self.headers_done = True

    def data(self, data: bytes, start: int, end: int) -> None:
        size = end - start
        if not self.headers_done or self.part_done or self.size + size > len(self.buffer):
            raise ValueError
        memoryview(self.buffer)[self.size : self.size + size] = memoryview(data)[start:end]
        self.size += size

    def part_end(self) -> None:
        if not self.headers_done or self.part_done or not self.size:
            raise ValueError
        self.part_done = True

    def end(self) -> None:
        if not self.part_done or self.ended:
            raise ValueError
        self.ended = True


async def read_memorylake_multipart(
    request: Request,
    *,
    max_file_bytes: int,
    check_current: Callable[[], None],
    timeout_s: float = 30,
) -> tuple[str, bytes]:
    """Receive one file under caller-owned authorization/capacity, never publish it."""
    part = None
    try:
        if (
            type(max_file_bytes) is not int
            or not 1 <= max_file_bytes <= 20 * 1024 * 1024
            or type(timeout_s) not in (int, float)
            or not math.isfinite(timeout_s)
            or not 0 < timeout_s <= 300
            or not callable(check_current)
        ):
            raise ValueError
        loop = asyncio.get_running_loop()
        expires = loop.time() + timeout_s

        def check() -> None:
            task = asyncio.current_task()
            if task is not None and task.cancelling():
                raise asyncio.CancelledError
            if loop.time() >= expires or cast("Callable[[], object]", check_current)() is not None:
                raise ValueError

        check()
        cap = max_file_bytes + 16 * 1024
        boundary, declared = _request_headers(request, cap)
        part = _SingleFile(max_file_bytes)
        parser = MultipartParser(
            boundary,
            {
                "on_part_begin": part.begin,
                "on_header_field": part.field,
                "on_header_value": part.header_value,
                "on_header_end": part.header_end,
                "on_headers_finished": part.headers_finished,
                "on_part_data": part.data,
                "on_part_end": part.part_end,
                "on_end": part.end,
            },
            max_size=cap,
            max_header_count=2,
            max_header_size=_HEADER,
        )
        # Per-parser logger only: do not mutate global library logging policy.
        parser.logger = logging.Logger("memorylake.multipart.silent")
        parser.logger.disabled = True
        total = 0
        async with asyncio.timeout(timeout_s):
            while True:
                check()
                frame = await request.receive()
                check()
                if frame["type"] != "http.request":
                    raise ValueError
                chunk = frame.get("body", b"")
                more = frame.get("more_body", False)
                if type(chunk) is not bytes or type(more) is not bool:
                    raise ValueError
                total += len(chunk)
                if total > cap or (declared is not None and total > declared):
                    raise ValueError
                for offset in range(0, len(chunk), _FEED):
                    check()
                    feed = chunk[offset : offset + _FEED]
                    if parser.write(feed) != len(feed):
                        raise ValueError
                    del feed  # Do not retain the prior slice while allocating the next.
                    check()
                    await asyncio.sleep(0)
                    check()
                if not more:
                    break
                # Also yield on empty ASGI frames, avoiding a zero-byte busy loop.
                await asyncio.sleep(0)
                check()
            parser.finalize()  # Library finalize alone does not validate closure.
            check()
            if not part.ended or not part.part_done or (declared is not None and total != declared):
                raise ValueError
            result = bytes(memoryview(part.buffer)[: part.size])
            check()
            return part.filename, result
    except Exception:
        raise MemoryLakeMultipartError("memorylake_multipart_unavailable") from None
    finally:
        if part is not None:
            part.buffer.clear()
