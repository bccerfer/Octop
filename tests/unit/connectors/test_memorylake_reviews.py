"""Explicit UI review state: no implicit approval or remote call."""

import unittest
from dataclasses import replace

from octop.infra.connectors.memorylake.contract import CallerContext, ReadScope
from octop.infra.connectors.memorylake.reviews import FileReviewRegistry
from octop.infra.connectors.memorylake.submissions import SubmissionIntent, WriteBinding


class ReviewTests(unittest.TestCase):
    def setUp(self):
        self.now = 100.0
        self.registry = FileReviewRegistry(clock=lambda: self.now, monotonic=lambda: self.now)
        caller = CallerContext(1, "agent", 1, 1)
        binding = WriteBinding(
            ReadScope("connector", 1, "agent", "workspace", ("project",)),
            "project",
            "folder",
            "https://app.memorylake.ai/openapi/memorylake",
            1,
            True,
        )
        self.intent = SubmissionIntent(caller, binding, "upload_test", "test.txt", "a" * 64, 3)

    def test_submission_identity_hash_has_unambiguous_field_boundaries(self):
        first = self.registry.submission_id(
            replace(self.intent.caller, agent_id="agent:part"), "binding", "upload"
        )
        second = self.registry.submission_id(
            replace(self.intent.caller, agent_id="agent"), "part:binding", "upload"
        )
        self.assertNotEqual(first, second)

    def test_repeat_review_preserves_fixed_expiry_and_submission_id(self):
        first = self.registry.create("binding", self.intent)
        self.now += 1
        again = self.registry.create("binding", self.intent)
        self.assertEqual(first, again)
        self.assertEqual(first.expires_at, 130)
        self.assertRegex(first.submission_id, r"^[a-z0-9_]+$")
        self.assertNotIn("test.txt", repr(first))

    def test_wrong_identity_intent_expiry_and_concurrent_confirmation_failclosed(self):
        review = self.registry.create("binding", self.intent)
        for caller, binding_id, upload_id, review_id in (
            (replace(self.intent.caller, user_id=2), "binding", "upload_test", review.review_id),
            (self.intent.caller, "other", "upload_test", review.review_id),
            (self.intent.caller, "binding", "other", review.review_id),
            (self.intent.caller, "binding", "upload_test", "review_other"),
        ):
            with (
                self.subTest(binding=binding_id),
                self.assertRaises(ValueError),
                self.registry.confirm(caller, binding_id, upload_id, review_id),
            ):
                self.fail("unexpected confirmation")
        with (
            self.registry.confirm(self.intent.caller, "binding", "upload_test", review.review_id),
            self.assertRaises(ValueError),
            self.registry.confirm(self.intent.caller, "binding", "upload_test", review.review_id),
        ):
            self.fail("duplicate concurrent confirmation")
        self.now = 131
        with (
            self.assertRaises(ValueError),
            self.registry.confirm(self.intent.caller, "binding", "upload_test", review.review_id),
        ):
            self.fail("expired confirmation")

    def test_capacity_clock_and_close_never_grant_approval(self):
        registry = FileReviewRegistry(
            max_reviews=1, clock=lambda: self.now, monotonic=lambda: self.now
        )
        registry.create("binding", self.intent)
        with self.assertRaises(ValueError):
            registry.create("binding", replace(self.intent, attachment_id="other"))
        registry.close()
        with self.assertRaises(ValueError):
            registry.create("binding", self.intent)
        self.now = float("nan")
        with self.assertRaises(ValueError):
            self.registry.create("binding", self.intent)
