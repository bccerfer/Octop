"""Offline MemoryLake write-result contracts; no CLI, network, or credentials."""

from __future__ import annotations

import json
import unittest

from octop.infra.connectors.memorylake.documents import (
    parse_document_result,
    parse_import_result,
    parse_upload_result,
)
from tests.support.memorylake import offline_test_guard


class MemoryLakeDocumentContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.enterContext(offline_test_guard())

    def test_upload_is_not_indexed(self) -> None:
        receipt = parse_upload_result(0, '{"uri":"private","item_id":"file-1","name":"a"}')
        self.assertEqual(receipt.state, "uploaded")
        self.assertEqual(receipt.item_id, "file-1")

    def test_uncertain_upload_preserves_known_id(self) -> None:
        receipt = parse_upload_result(1, '{"item_id":"file-1"}')
        self.assertEqual(receipt.state, "unknown")
        self.assertEqual(receipt.item_id, "file-1")

    def test_malformed_upload_never_claims_success(self) -> None:
        for stdout in ("", "[]", "{}", '{"item_id":42}', "not json"):
            with self.subTest(stdout=stdout):
                self.assertEqual(parse_upload_result(0, stdout).state, "unknown")

    def test_import_acceptance_is_not_ready(self) -> None:
        receipt = parse_import_result(0, self._import_payload())
        self.assertEqual(receipt.state, "accepted")
        self.assertEqual(receipt.document_ids, ("doc-1",))
        self.assertEqual(receipt.items[0].file_id, "file-1")
        self.assertFalse(receipt.requires_reconciliation)

    def test_nonzero_import_keeps_accepted_ids_for_reconciliation(self) -> None:
        receipt = parse_import_result(1, self._import_payload())
        self.assertEqual(receipt.state, "accepted")
        self.assertEqual(receipt.document_ids, ("doc-1",))
        self.assertTrue(receipt.requires_reconciliation)

    def test_duplicate_retains_existing_document(self) -> None:
        payload = self._import_payload(
            success_count=0,
            duplicate_count=1,
            details=[
                {"result": "duplicate", "drive_item_id": "file-1", "document_id": "doc-1"}
            ],
        )
        receipt = parse_import_result(0, payload)
        self.assertEqual(receipt.state, "accepted")
        self.assertEqual(receipt.document_ids, ("doc-1",))

    def test_partial_import_is_not_a_total_failure(self) -> None:
        receipt = parse_import_result(
            1,
            self._import_payload(
                failure_count=1,
                details=[
                    {"result": "success", "drive_item_id": "file-1", "document_id": "doc-1"},
                    {"result": "failed", "drive_item_id": "file-2"},
                ],
            ),
        )
        self.assertEqual(receipt.state, "partial")
        self.assertEqual(receipt.document_ids, ("doc-1",))
        self.assertTrue(receipt.requires_reconciliation)

    def test_complete_rejection_is_failed_not_unknown(self) -> None:
        receipt = parse_import_result(
            1,
            self._import_payload(
                success_count=0, failure_count=1, details=[{"result": "failed"}]
            ),
        )
        self.assertEqual(receipt.state, "failed")

    def test_missing_or_truncated_ids_require_reconciliation(self) -> None:
        for overrides in (
            {"details_truncated": True, "details": []},
            {"details": [{"result": "success", "drive_item_id": "file-1"}]},
        ):
            with self.subTest(overrides=overrides):
                receipt = parse_import_result(0, self._import_payload(**overrides))
                self.assertEqual(receipt.state, "accepted")
                self.assertTrue(receipt.requires_reconciliation)

    def test_contradictory_result_counts_are_unknown_and_preserve_ids(self) -> None:
        for overrides in (
            {"success_count": 2},
            {"success_count": 0, "failure_count": 1},
            {"duplicate_count": 1},
            {"failure_count": 1},
        ):
            with self.subTest(overrides=overrides):
                receipt = parse_import_result(0, self._import_payload(**overrides))
                self.assertEqual(receipt.state, "unknown")
                self.assertEqual(receipt.document_ids, ("doc-1",))
                self.assertTrue(receipt.requires_reconciliation)

    def test_missing_untruncated_details_are_unknown(self) -> None:
        receipt = parse_import_result(0, self._import_payload(details=[]))
        self.assertEqual(receipt.state, "unknown")
        self.assertTrue(receipt.requires_reconciliation)

    def test_unknown_results_are_normalized_without_discarding_ids(self) -> None:
        for result in ("secret diagnostic text", None, {"error": "secret"}):
            with self.subTest(result=result):
                receipt = parse_import_result(
                    0,
                    self._import_payload(details=[{"result": result, "document_id": "doc-1"}]),
                )
                self.assertEqual(receipt.state, "unknown")
                self.assertIsNone(receipt.items[0].result)
                self.assertEqual(receipt.document_ids, ("doc-1",))
                self.assertTrue(receipt.requires_reconciliation)

    def test_truncated_details_cannot_exceed_authoritative_counts(self) -> None:
        receipt = parse_import_result(
            0, self._import_payload(success_count=0, failure_count=1, details_truncated=True)
        )
        self.assertEqual(receipt.state, "unknown")
        self.assertEqual(receipt.document_ids, ("doc-1",))

    def test_invalid_detail_shapes_are_unknown(self) -> None:
        for details in (None, {}, [42], [{"result": "success", "document_id": "doc-1"}, None]):
            with self.subTest(details=details):
                receipt = parse_import_result(0, self._import_payload(details=details))
                self.assertEqual(receipt.state, "unknown")
                self.assertTrue(receipt.requires_reconciliation)

    def test_schema_drift_preserves_ids_but_never_claims_acceptance(self) -> None:
        receipt = parse_import_result(0, '{"details":[{"document_id":"doc-1"}]}')
        self.assertEqual(receipt.state, "unknown")
        self.assertEqual(receipt.document_ids, ("doc-1",))
        self.assertTrue(receipt.requires_reconciliation)

    def test_invalid_counts_are_unknown(self) -> None:
        for count in (True, -1, "1", None):
            with self.subTest(count=count):
                receipt = parse_import_result(0, self._import_payload(success_count=count))
                self.assertEqual(receipt.state, "unknown")
                self.assertEqual(receipt.document_ids, ("doc-1",))

    def test_empty_or_malformed_import_is_unknown(self) -> None:
        for stdout in ("", "null", "{}", "{", "[]"):
            with self.subTest(stdout=stdout):
                self.assertEqual(parse_import_result(0, stdout).state, "unknown")

    def test_only_okay_document_is_ready(self) -> None:
        for status, expected in (
            ("okay", "ready"),
            ("error", "failed"),
            ("pending", "processing"),
            ("running", "processing"),
            ("ready", "unknown"),
            ("reindexing", "unknown"),
        ):
            with self.subTest(status=status):
                receipt = parse_document_result(
                    0, json.dumps({"id": "doc-1", "status": status}), "doc-1"
                )
                self.assertEqual(receipt.state, expected)

    def test_wrong_document_or_failed_get_cannot_be_ready(self) -> None:
        for code, identifier in ((1, "doc-1"), (0, "another-doc")):
            receipt = parse_document_result(
                code, json.dumps({"id": identifier, "status": "okay"}), "doc-1"
            )
            self.assertEqual(receipt.state, "unknown")

    def test_failed_document_id_does_not_cover_an_accepted_item_without_id(self) -> None:
        for result in ("success", "duplicate"):
            with self.subTest(result=result):
                receipt = parse_import_result(
                    0,
                    self._import_payload(
                        success_count=int(result == "success"),
                        duplicate_count=int(result == "duplicate"),
                        failure_count=1,
                        details=[
                            {"result": result, "drive_item_id": "file-1"},
                            {"result": "failed", "document_id": "failed-doc"},
                        ],
                    ),
                )
                self.assertEqual(receipt.state, "partial")
                self.assertEqual(receipt.document_ids, ("failed-doc",))
                self.assertTrue(receipt.requires_reconciliation)

    def test_untrusted_ids_are_not_exposed_as_receipt_fields(self) -> None:
        for identifier in ("secret\nbody", "secret body", "x" * 129, "文档", "-flag", "a/b"):
            with self.subTest(identifier=identifier):
                upload = parse_upload_result(0, json.dumps({"item_id": identifier}))
                self.assertEqual(upload.state, "unknown")
                self.assertIsNone(upload.item_id)
                for key in ("drive_item_id", "document_id"):
                    detail = {"result": "success", "drive_item_id": "file-1", "document_id": "doc-1"}
                    detail[key] = identifier
                    receipt = parse_import_result(0, self._import_payload(details=[detail]))
                    self.assertIsNone(
                        receipt.items[0].file_id if key == "drive_item_id" else receipt.items[0].document_id
                    )
                    self.assertTrue(receipt.requires_reconciliation)
                    self.assertNotIn(identifier, repr(receipt))

    def test_invalid_expected_document_id_is_not_echoed(self) -> None:
        for identifier in ("secret\nbody", "x" * 129, ""):
            with self.subTest(identifier=identifier):
                receipt = parse_document_result(
                    0, json.dumps({"id": identifier, "status": "okay"}), identifier
                )
                self.assertEqual(receipt.state, "unknown")
                self.assertIsNone(receipt.document_id)

    def test_bounded_ascii_ids_support_library_colons(self) -> None:
        for identifier in ("file:abc_123-def", "x" * 128):
            with self.subTest(identifier=identifier):
                upload = parse_upload_result(0, json.dumps({"item_id": identifier}))
                self.assertEqual(upload.state, "uploaded")
                self.assertEqual(upload.item_id, identifier)
                document = parse_document_result(
                    0, json.dumps({"id": identifier, "status": "okay"}), identifier
                )
                self.assertEqual(document.state, "ready")
                self.assertEqual(document.document_id, identifier)

    @staticmethod
    def _import_payload(**overrides: object) -> str:
        payload: dict[str, object] = {
            "success_count": 1,
            "failure_count": 0,
            "duplicate_count": 0,
            "details": [
                {"result": "success", "drive_item_id": "file-1", "document_id": "doc-1"}
            ],
            "details_truncated": False,
        }
        payload.update(overrides)
        return json.dumps(payload)

    def test_complete_report_is_independent_of_nonzero_exit(self) -> None:
        receipt = parse_import_result(1, self._import_payload())
        self.assertTrue(receipt.complete_report)
        self.assertTrue(receipt.requires_reconciliation)

    def test_incomplete_reports_never_set_complete_report(self) -> None:
        for overrides in ({"details_truncated": True}, {"success_count": 2}, {"details": [{"result": "success"}]}):
            with self.subTest(overrides=overrides):
                self.assertFalse(parse_import_result(0, self._import_payload(**overrides)).complete_report)
