"""Recorded SQL contracts, not a live PostgreSQL integration test."""

from contextlib import contextmanager

from tests.support.memorylake import offline_test_guard

from octop.infra.db.pool import qmark_to_pyformat
from octop.infra.db.repos.memorylake import MemoryLakeBindingRepo


class _RecordedPool:
    def __init__(self):
        self.calls = []

    @contextmanager
    def connect(self):
        yield self

    def execute(self, sql, values):
        self.calls.append((sql, values))
        return self

    def fetchone(self):
        return None


def test_unscoped_binding_query_has_no_untyped_null_parameter():
    with offline_test_guard():
        pool = _RecordedPool()
        MemoryLakeBindingRepo(pool).load_owned_active("binding", caller_user_id=1, agent_id="agent")
        sql, values = pool.calls[0]
        assert "? IS NULL" not in sql
        assert "%s IS NULL" not in qmark_to_pyformat(sql)
        assert values == ("binding", 1, "agent")


def test_dashboard_thread_predicate_is_same_query_with_bound_values():
    with offline_test_guard():
        pool = _RecordedPool()
        # SQL-only repo receives values, never interpolates even malformed IDs.
        thread = "thread' OR true--"
        MemoryLakeBindingRepo(pool).load_owned_active(
            "binding",
            caller_user_id=1,
            agent_id="agent",
            dashboard_thread_id=thread,
            dashboard_session_key="agent:dashboard:1:dm",
        )
        assert len(pool.calls) == 1
        sql, values = pool.calls[0]
        assert "? IS NULL" not in sql
        assert "EXISTS (SELECT 1 FROM threads" in sql
        assert "t.user_id=b.owner_user_id" in sql
        assert "c.credential_revision" in sql
        assert thread not in sql
        assert values == ("binding", 1, "agent", thread, "agent:dashboard:1:dm")


def test_partial_dashboard_scope_denies_without_query():
    with offline_test_guard():
        for scope in ({"dashboard_thread_id": "thread"}, {"dashboard_session_key": "session"}):
            pool = _RecordedPool()
            assert (
                MemoryLakeBindingRepo(pool).load_owned_active(
                    "binding", caller_user_id=1, agent_id="agent", **scope
                )
                is None
            )
            assert pool.calls == []


def test_write_policy_and_identity_are_one_query_without_credential_blob_projection():
    with offline_test_guard():
        pool = _RecordedPool()
        MemoryLakeBindingRepo(pool).load_owned_write_binding(
            "binding",
            caller_user_id=1,
            agent_id="agent",
            dashboard_thread_id="thread",
            dashboard_session_key="agent:dashboard:1:dm",
        )
        assert len(pool.calls) == 1
        sql, values = pool.calls[0]
        projection = sql.split("FROM", 1)[0]
        assert "credential_blob" not in projection
        assert "JOIN memorylake_write_policies" in sql
        assert "EXISTS (SELECT 1 FROM threads" in sql
        assert "p.enabled=1" not in sql  # Disabled writes can still be reconciled read-only.
        assert "? IS NULL" not in sql
        assert values == ("binding", 1, "agent", "thread", "agent:dashboard:1:dm")


def test_write_query_partial_dashboard_scope_denies_without_query():
    with offline_test_guard():
        for scope in ({"dashboard_thread_id": "thread"}, {"dashboard_session_key": "session"}):
            pool = _RecordedPool()
            assert (
                MemoryLakeBindingRepo(pool).load_owned_write_binding(
                    "binding",
                    caller_user_id=1,
                    agent_id="agent",
                    **scope,
                )
                is None
            )
            assert pool.calls == []


def test_credential_master_and_binding_share_parameterized_snapshot():
    with offline_test_guard():
        pool = _RecordedPool()
        purpose = "synthetic' OR true--"
        MemoryLakeBindingRepo(pool).load_owned_credentials(
            "binding", caller_user_id=1, agent_id="agent", master_key_name=purpose
        )
        assert len(pool.calls) == 1
        sql, values = pool.calls[0]
        assert purpose not in sql
        assert "(SELECT v FROM secrets WHERE k=?) AS master_key" in sql
        assert "JOIN users" in sql
        assert "u.disabled=0" in sql
        assert "c.credential_blob" in sql
        assert "? IS NULL" not in sql
        assert qmark_to_pyformat(sql).count("%s") == len(values) == 4
        assert values == (purpose, "binding", 1, "agent")
