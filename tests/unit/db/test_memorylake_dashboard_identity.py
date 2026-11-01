"""Owner-dashboard identity snapshots against real temporary SQLite; no network."""

import pytest
from tests.support.memorylake import offline_test_guard

from octop.infra.db.migrate import run_migrations
from octop.infra.db.pool import SqlitePool
from octop.infra.db.repos.agents import AgentRepo
from octop.infra.db.repos.memorylake import MemoryLakeBindingRepo
from octop.infra.db.repos.threads import ThreadRepo
from octop.infra.db.repos.users import UserRepo


@pytest.fixture
def identity(tmp_path):
    with offline_test_guard():
        pool = SqlitePool(tmp_path / "identity.db")
        try:
            run_migrations(pool)
            users = UserRepo(pool)
            owner = users.create(username="owner", role="user")
            other = users.create(username="other", role="user")
            admin = users.create(username="admin", role="admin")
            AgentRepo(pool).create(agent_id="agent", user_id=owner, name="agent")
            session = f"agent:dashboard:{owner}:dm"
            ThreadRepo(pool).insert(
                thread_id="thread",
                agent_id="agent",
                user_id=owner,
                channel_type="dashboard",
                session_key=session,
            )
            yield pool, MemoryLakeBindingRepo(pool), owner, other, admin, session
        finally:
            pool.close()


def check(identity, **changes):
    _, repo, owner, _, _, session = identity
    args = {
        "caller_user_id": owner,
        "agent_id": "agent",
        "thread_id": "thread",
        "session_key": session,
    }
    args.update(changes)
    return repo.owns_dashboard_thread(**args)


def test_owner_single_query_without_binding_or_secret(identity):
    pool, _, _, _, _, _ = identity
    statements = []
    with pool.connect() as conn:
        conn.set_trace_callback(statements.append)
        try:
            assert check(identity) is True
        finally:
            conn.set_trace_callback(None)
    selects = [sql for sql in statements if sql.lstrip().upper().startswith("SELECT")]
    assert len(selects) == 1
    assert "credential" not in selects[0] and "memorylake_bindings" not in selects[0]


def test_other_and_admin_cannot_borrow_owner_identity(identity):
    _, _, _, other, admin, _ = identity
    for user in (other, admin, True, 0, -1, "1"):
        assert check(identity, caller_user_id=user) is False


@pytest.mark.parametrize(
    "change",
    [
        {"agent_id": "other"},
        {"thread_id": "missing"},
        {"session_key": "wrong"},
        {"session_key": "agent:dashboard:1:group"},
    ],
)
def test_exact_identity_required(identity, change):
    assert check(identity, **change) is False


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE users SET disabled=1 WHERE username='owner'",
        "UPDATE agents SET enabled=0",
        "UPDATE agents SET is_shared=1",
        "UPDATE agents SET kind='team'",
        "UPDATE agents SET kind='unknown'",
        "UPDATE agents SET user_id=(SELECT id FROM users WHERE username='other')",
        "UPDATE agents SET user_id=NULL",
        "UPDATE threads SET user_id=(SELECT id FROM users WHERE username='other')",
        "UPDATE threads SET channel_type='feishu'",
        "UPDATE threads SET session_key='changed'",
        "DELETE FROM threads",
    ],
)
def test_current_state_revocation_is_observed(identity, sql):
    pool, _, _, _, _, _ = identity
    assert check(identity) is True
    with pool.transaction() as conn:
        conn.execute(sql)
    assert check(identity) is False


def test_existing_group_session_is_not_dm(identity):
    pool, _, owner, _, _, _ = identity
    group = f"agent:dashboard:{owner}:group"
    with pool.transaction() as conn:
        conn.execute("UPDATE threads SET session_key=?", (group,))
    assert check(identity, session_key=group) is False
