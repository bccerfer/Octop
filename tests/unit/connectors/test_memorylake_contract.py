"""Offline MemoryLake boundary tests; no CLI executable or credentials are used."""

from __future__ import annotations

import unittest
from dataclasses import replace

from octop.infra.connectors.memorylake.contract import (
    CallerContext,
    MemoryLakeContractError,
    ReadScope,
    plan_read,
)
from tests.support.memorylake import offline_test_guard


class MemoryLakeReadContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.enterContext(offline_test_guard())
        self.scope = ReadScope(
            connector_id="connection-a",
            owner_user_id=11,
            agent_id="agent-a",
            workspace_id="workspace-a",
            project_ids=("project-a", "project-b"),
            actor_id="actor-a",
            authorization_revision=3,
        )
        self.caller = CallerContext(
            user_id=11,
            agent_id="agent-a",
            agent_owner_user_id=11,
            authorization_revision=3,
        )

    def plan(self, name: str, **arguments: object) -> tuple[str, ...]:
        return plan_read(self.scope, self.caller, name, arguments).arguments

    def test_document_search_never_includes_facts_or_default_workspace(self) -> None:
        self.assertEqual(
            self.plan("search_documents", query="预算"),
            (
                "search", "--workspace", "workspace-a", "--projects", "project-a,project-b",
                "--types", "document", "--top-k", "10", "--", "预算",
            ),
        )

    def test_fact_search_uses_explicit_actor_not_project_filter(self) -> None:
        self.assertEqual(
            self.plan("search_facts", query="preferences", top_k=4),
            (
                "search", "--workspace", "workspace-a", "--actors", "actor-a",
                "--types", "fact", "--top-k", "4", "--", "preferences",
            ),
        )

    def test_project_subset_is_allowed(self) -> None:
        args = self.plan("search_documents", query="q", project_ids=["project-b"])
        self.assertEqual(args[args.index("--projects") + 1], "project-b")

    def test_document_get_is_scoped(self) -> None:
        self.assertEqual(
            self.plan("get_document", project_id="project-a", document_id="doc-1"),
            ("project", "document", "get", "--workspace", "workspace-a", "--project", "project-a", "--", "doc-1"),
        )

    def test_cross_user_agent_shared_group_and_stale_context_are_denied(self) -> None:
        for changes in (
            {"user_id": 12}, {"agent_id": "agent-b"}, {"agent_owner_user_id": 12},
            {"agent_owner_user_id": None}, {"is_group": True}, {"authorization_revision": 2},
        ):
            with self.subTest(changes=changes), self.assertRaises(MemoryLakeContractError):
                plan_read(self.scope, replace(self.caller, **changes), "search_documents", {"query": "q"})

    def test_disabled_binding_is_denied(self) -> None:
        with self.assertRaises(MemoryLakeContractError):
            plan_read(replace(self.scope, enabled=False), self.caller, "search_documents", {"query": "q"})

    def test_empty_scope_does_not_fall_back_to_remote_defaults(self) -> None:
        for changes, operation in (({"project_ids": ()}, "search_documents"), ({"actor_id": None}, "search_facts")):
            with self.subTest(operation=operation), self.assertRaises(MemoryLakeContractError):
                plan_read(replace(self.scope, **changes), self.caller, operation, {"query": "q"})

    def test_empty_outside_or_injected_project_selection_is_denied(self) -> None:
        for projects in ([], ["outside"], ["project-a", "outside"], ["project-a,project-b"], "project-a"):
            with self.subTest(projects=projects), self.assertRaises(MemoryLakeContractError):
                self.plan("search_documents", query="q", project_ids=projects)

    def test_document_get_denies_other_project(self) -> None:
        with self.assertRaises(MemoryLakeContractError):
            self.plan("get_document", project_id="outside", document_id="doc-1")

    def test_unknown_arguments_cannot_override_authorization_or_runtime(self) -> None:
        for key in ("workspace", "actor_id", "owner_user_id", "profile", "base_url", "api_key", "command"):
            with self.subTest(key=key), self.assertRaises(MemoryLakeContractError):
                self.plan("search_documents", query="q", **{key: "attacker"})

    def test_no_unscoped_reads_or_writes_or_raw_commands(self) -> None:
        for name in ("library_upload", "fact_add", "run_cli", "auth", "delete", "search"):
            with self.subTest(name=name), self.assertRaises(MemoryLakeContractError):
                self.plan(name, query="q")

    def test_fact_get_and_trace_always_use_bound_actor(self) -> None:
        for operation, command in (("get_fact", "get"), ("trace_fact", "trace")):
            with self.subTest(operation=operation):
                self.assertEqual(
                    self.plan(operation, fact_id="fact-1"),
                    ("fact", command, "--workspace", "workspace-a", "--actor", "actor-a", "--", "fact-1"),
                )
                for arguments in ({}, {"fact_id": "--help"}, {"fact_id": "fact-1", "actor": "other"}, {"fact_id": "fact-1", "project": "project-a"}):
                    with self.assertRaises(MemoryLakeContractError):
                        plan_read(self.scope, self.caller, operation, arguments)
                with self.assertRaises(MemoryLakeContractError):
                    plan_read(replace(self.scope, actor_id=None), self.caller, operation, {"fact_id": "fact-1"})

    def test_authorization_types_do_not_accept_bool_float_or_string_coercion(self) -> None:
        scope = replace(self.scope, owner_user_id=1, authorization_revision=1)
        caller = replace(self.caller, user_id=1, agent_owner_user_id=1, authorization_revision=1)
        for changes in ({"authorization_revision": True}, {"authorization_revision": 1.0}, {"agent_owner_user_id": True}, {"agent_owner_user_id": 1.0}, {"is_group": None}):
            with self.subTest(changes=changes), self.assertRaises(MemoryLakeContractError):
                plan_read(scope, replace(caller, **changes), "search_facts", {"query": "q"})
        for enabled in ("false", 1, None):
            with self.subTest(enabled=enabled), self.assertRaises(MemoryLakeContractError):
                plan_read(replace(scope, enabled=enabled), caller, "search_facts", {"query": "q"})

    def test_query_is_one_positional_argument_even_with_flags_or_shell_text(self) -> None:
        for query in ("--base-url=https://invalid.example", "$(cat secret); exit", "第一行\n第二行"):
            with self.subTest(query=query):
                args = self.plan("search_documents", query=query)
                self.assertEqual(args[-2:], ("--", query))

    def test_blank_null_oversized_or_non_string_queries_are_denied(self) -> None:
        for query in ("", " \n", "\0secret", "x" * 4097, 42, None):
            with self.subTest(query=type(query).__name__), self.assertRaises(MemoryLakeContractError):
                self.plan("search_documents", query=query)

    def test_top_k_rejects_bool_float_text_and_out_of_range(self) -> None:
        for value in (True, 1.5, "5", 0, 51, -1):
            with self.subTest(value=value), self.assertRaises(MemoryLakeContractError):
                self.plan("search_documents", query="q", top_k=value)

    def test_invalid_bound_ids_fail_closed(self) -> None:
        for field, value in (("workspace_id", ""), ("actor_id", "--actors=other"), ("connector_id", "../other")):
            with self.subTest(field=field), self.assertRaises(MemoryLakeContractError):
                plan_read(replace(self.scope, **{field: value}), self.caller, "search_facts", {"query": "q"})

    def test_errors_and_command_repr_do_not_echo_query(self) -> None:
        secret_query = "sensitive-private-body"
        command = plan_read(self.scope, self.caller, "search_documents", {"query": secret_query})
        self.assertNotIn(secret_query, repr(command))
        with self.assertRaises(MemoryLakeContractError) as raised:
            self.plan("search_documents", query=secret_query, api_key="secret-api-key")
        self.assertNotIn(secret_query, str(raised.exception))
        self.assertNotIn("secret-api-key", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
