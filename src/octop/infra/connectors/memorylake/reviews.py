"""Bounded, short-lived explicit UI reviews, not approvals or replayable leases.

One uploaded snapshot maps to a stable planned submission ID. Review creates no
DB approval and makes no remote call. Confirmation is a non-waiting process-local
claim; durable submission/step CAS remains the authoritative duplicate-write guard.
Restart loses reviews/bytes, never authorizes replay of an existing submission.
"""

from __future__ import annotations

import hashlib
import json
import math
import secrets
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field

from octop.infra.connectors.memorylake.contract import CallerContext, _identifier
from octop.infra.connectors.memorylake.submission_storage import _encode
from octop.infra.connectors.memorylake.submissions import SubmissionIntent


@dataclass(frozen=True)
class FileReview:
    review_id: str
    submission_id: str
    binding_id: str
    intent: SubmissionIntent = field(repr=False)
    expires_at: float
    monotonic_deadline: float = field(repr=False)


class FileReviewRegistry:
    def __init__(
        self,
        *,
        max_reviews: int = 64,
        ttl_s: float = 30,
        clock: Callable[[], float] = time.time,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if type(max_reviews) is not int or not 1 <= max_reviews <= 1024 or not 0 < ttl_s <= 60:
            raise ValueError("invalid_review_limits")
        self._limit, self._ttl, self._clock, self._monotonic = max_reviews, ttl_s, clock, monotonic
        self._entries: dict[str, FileReview] = {}
        self._active: set[str] = set()
        self._closed = False

    def close(self) -> None:
        self._closed = True
        self._entries.clear()

    def _now(self) -> tuple[float, float]:
        wall, mono = self._clock(), self._monotonic()
        if self._closed or not math.isfinite(wall) or not math.isfinite(mono):
            self.close()
            raise ValueError("review_unavailable")
        for key, item in tuple(self._entries.items()):
            if wall >= item.expires_at or mono >= item.monotonic_deadline:
                self._entries.pop(key)
        return wall, mono

    @staticmethod
    def submission_id(caller: CallerContext, binding_id: str, upload_id: str) -> str:
        # Opaque upload IDs are randomly issued by the source, not client paths.
        data = json.dumps(
            [
                caller.user_id,
                _identifier(caller.agent_id),
                _identifier(binding_id),
                _identifier(upload_id),
            ],
            separators=(",", ":"),
        )
        return "mlsub_" + hashlib.sha256(data.encode()).hexdigest()

    def create(self, binding_id: str, intent: SubmissionIntent) -> FileReview:
        now, mono = self._now()
        _encode(intent)  # Validate exact canonical, bounded scope/file metadata.
        binding_id = _identifier(binding_id)
        submission_id = self.submission_id(intent.caller, binding_id, intent.attachment_id)
        previous = self._entries.get(submission_id)
        if previous is not None and previous.intent == intent:
            return previous  # Never extend a review on a repeated request.
        if submission_id in self._active or (
            previous is None and len(self._entries) >= self._limit
        ):
            raise ValueError("review_capacity_exceeded")
        review = FileReview(
            "review_" + secrets.token_urlsafe(24),
            submission_id,
            binding_id,
            intent,
            now + self._ttl,
            mono + self._ttl,
        )
        self._entries[submission_id] = review
        return review

    @contextmanager
    def confirm(
        self,
        caller: CallerContext,
        binding_id: str,
        upload_id: str,
        review_id: str,
    ) -> Iterator[FileReview]:
        self._now()
        key = self.submission_id(caller, binding_id, upload_id)
        review = self._entries.get(key)
        if (
            review is None
            or review.review_id != _identifier(review_id)
            or review.intent.caller != caller
            or review.binding_id != binding_id
            or review.intent.attachment_id != upload_id
            or key in self._active
        ):
            raise ValueError("review_unavailable")
        self._active.add(key)
        try:
            yield review
        finally:
            self._active.remove(key)
