"""Unregistered ledger storage: real SQLite, synthetic metadata, no remote side effects."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

import pytest
from tests.support.memorylake import offline_test_guard

from octop.infra.db import migrate
from octop.infra.db.pool import SqlitePool
from octop.infra.db.repos.agents import AgentRepo
from octop.infra.db.repos.connectors import ConnectorRepo
from octop.infra.db.repos.memorylake import MemoryLakeBindingRepo
from octop.infra.db.repos.memorylake_submissions import MemoryLakeSubmissionRepo
from octop.infra.db.repos.users import UserRepo


@pytest.fixture(autouse=True)
def offline():
    with offline_test_guard():
        yield


@pytest.fixture
def setup(tmp_path):
    pool = SqlitePool(tmp_path / "ledger.db")
    discover = migrate._discover
    with patch.object(
        migrate,
        "_discover",
        side_effect=lambda dialect="sqlite": [(v, p) for v, p in discover(dialect) if v <= 25],
    ):
        migrate.run_migrations(pool)
    sql = Path("src/octop/infra/db/migrations/026_memorylake_submissions.sql").read_text()
    with pool.connect() as conn:
        conn.executescript(sql)
    owner = UserRepo(pool).create(username="owner", role="user")
    AgentRepo(pool).create(agent_id="agent", user_id=owner, name="agent")
    connectors = ConnectorRepo(pool)
    connectors.create(
        instance_id="connector",
        user_id=owner,
        kind="memorylake",
        display_name="ml",
        mcp_server_name="ml",
    )
    connectors.upsert_credentials(instance_id="connector", blob=b"synthetic", expires_at=1000)
    MemoryLakeBindingRepo(pool).create(
        binding_id="binding",
        connector_id="connector",
        owner_user_id=owner,
        agent_id="agent",
        base_url="https://app.memorylake.ai/openapi/memorylake",
        workspace_id="workspace",
        project_ids_json='["project"]',
        enabled=True,
    )
    repo = MemoryLakeSubmissionRepo(pool, clock=lambda: 100.0)
    policy = repo.replace_policy(
        binding_id="binding",
        expected_revision=0,
        project_id="project",
        library_parent_id="folder",
        enabled=True,
    )
    assert policy.revision == 1
    data = {
        "approval_id": "approval",
        "approver_user_id": owner,
        "binding_id": "binding",
        "connector_id": "connector",
        "caller_user_id": owner,
        "agent_id": "agent",
        "authorization_revision": 1,
        "policy_revision": 1,
        "credential_revision": 1,
        "base_url": "https://app.memorylake.ai/openapi/memorylake",
        "workspace_id": "workspace",
        "project_id": "project",
        "library_parent_id": "folder",
        "intent_json": '{"version":1,"sha256":"synthetic","size":3}',
        "expires_at": 200.0,
    }
    yield pool, repo, data
    pool.close()


def approve(repo, data):
    return repo.create_approval(**data)


def claim(repo):
    return repo.claim(
        "approval", expected_intent_json='{"version":1,"sha256":"synthetic","size":3}'
    )


def test_defaults_and_policy_cas(setup):
    _, repo, _ = setup
    row = repo.replace_policy(
        binding_id="other", expected_revision=0, project_id="project", library_parent_id="folder"
    )
    assert row.enabled is False
    assert (
        repo.replace_policy(
            binding_id="binding",
            expected_revision=0,
            project_id="project",
            library_parent_id="folder",
        )
        is None
    )


def test_claim_once_exact_intent_and_step_cas(setup):
    _, repo, data = setup
    approve(repo, data)
    with pytest.raises(ValueError):
        repo.claim("approval", expected_intent_json="different")
    record = claim(repo)
    assert record.state == "approved" and record.revision == 1
    with pytest.raises(ValueError):
        claim(repo)
    grant = repo.begin_step(
        "approval", expected_revision=1, expected_state="approved", phase="uploading"
    )
    assert grant.record.state == "uploading" and grant.record.revision == 2
    assert grant.expires_at <= data["expires_at"]
    with pytest.raises(ValueError):
        repo.begin_step(
            "approval", expected_revision=1, expected_state="approved", phase="uploading"
        )
    assert not hasattr(repo.get("approval"), "expires_at")


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE users SET disabled=1",
        "UPDATE agents SET enabled=0",
        "UPDATE agents SET is_shared=1",
        "UPDATE agents SET kind='team'",
        "UPDATE connectors SET shared=1",
        "UPDATE connectors SET credential_revision=credential_revision+1",
        "UPDATE connectors SET credential_expires_at=99",
        "UPDATE memorylake_bindings SET authorization_revision=2",
        "UPDATE memorylake_write_policies SET enabled=0",
        "UPDATE memorylake_write_policies SET revision=2",
    ],
)
def test_atomic_step_live_authorization(setup, sql):
    pool, repo, data = setup
    approve(repo, data)
    claim(repo)
    with pool.transaction() as conn:
        conn.execute(sql)
    with pytest.raises(ValueError):
        repo.begin_step(
            "approval", expected_revision=1, expected_state="approved", phase="uploading"
        )
    assert repo.get("approval").state == "approved"


def test_revoke_expiry_and_approver(setup):
    _, repo, data = setup
    with pytest.raises(ValueError):
        approve(repo, dict(data, approver_user_id=data["caller_user_id"] + 1))
    with pytest.raises(ValueError):
        approve(repo, dict(data, expires_at=99))
    approve(repo, data)
    claim(repo)
    assert repo.revoke_approval("approval", approver_user_id=data["caller_user_id"])
    with pytest.raises(ValueError):
        repo.begin_step(
            "approval", expected_revision=1, expected_state="approved", phase="uploading"
        )


def test_concurrent_independent_pool_claim(setup):
    pool, repo, data = setup
    approve(repo, data)
    second = SqlitePool(pool.path)
    other = MemoryLakeSubmissionRepo(second, clock=lambda: 100.0)

    def attempt(r):
        try:
            claim(r)
            return True
        except ValueError:
            return False

    try:
        with ThreadPoolExecutor(max_workers=2) as workers:
            assert sorted(workers.map(attempt, (repo, other))) == [False, True]
    finally:
        second.close()


def test_partial_save_cas_and_no_replay(setup):
    _, repo, data = setup
    approve(repo, data)
    claim(repo)
    repo.begin_step("approval", expected_revision=1, expected_state="approved", phase="uploading")
    assert repo.save(
        "approval",
        expected_revision=2,
        expected_state="uploading",
        expected_intent_json=data["intent_json"],
        state="unknown",
        item_id="item:known",
        document_id=None,
        import_accepted=False,
    )
    assert not repo.save(
        "approval",
        expected_revision=2,
        expected_state="uploading",
        expected_intent_json=data["intent_json"],
        state="uploaded",
        item_id="item:known",
        document_id=None,
        import_accepted=False,
    )
    row = repo.get("approval")
    assert row.item_id == "item:known" and row.state == "unknown"
    with pytest.raises(ValueError):
        repo.begin_step(
            "approval", expected_revision=3, expected_state="unknown", phase="uploading"
        )


def test_expiry_rechecked_after_final_lock(setup):
    _, repo, data = setup
    approve(repo, data)
    claim(repo)
    original = repo._lock

    def lock(conn, table, column, value):
        original(conn, table, column, value)
        if table == "memorylake_submissions":
            repo._clock = lambda: 201.0

    with patch.object(repo, "_lock", side_effect=lock), pytest.raises(ValueError):
        repo.begin_step(
            "approval", expected_revision=1, expected_state="approved", phase="uploading"
        )
    assert repo.get("approval").state == "approved"


def test_concurrent_steps_and_restart_no_regrant(setup):
    pool, repo, data = setup
    approve(repo, data)
    claim(repo)
    second = SqlitePool(pool.path)
    other = MemoryLakeSubmissionRepo(second, clock=lambda: 100.0)

    def attempt(r):
        try:
            r.begin_step(
                "approval", expected_revision=1, expected_state="approved", phase="uploading"
            )
            return True
        except ValueError:
            return False

    try:
        with ThreadPoolExecutor(max_workers=2) as workers:
            assert sorted(workers.map(attempt, (repo, other))) == [False, True]
        assert other.get("approval").state == "uploading"
        assert attempt(other) is False
    finally:
        second.close()


def test_deletion_keeps_history_but_denies_live_grant(setup):
    pool, repo, data = setup
    approve(repo, data)
    claim(repo)
    ConnectorRepo(pool).delete("connector")
    assert repo.get("approval").state == "approved"
    assert repo.get_approval("approval").intent_json == data["intent_json"]
    with pytest.raises(ValueError):
        repo.begin_step(
            "approval", expected_revision=1, expected_state="approved", phase="uploading"
        )


def test_pg_lock_sql_shape_only_not_live_postgres(setup):
    """Record PG branch statements, stripping lock syntax for a SQLite harness.

    This proves statement order/targets only, NOT PostgreSQL execution/isolation.
    """
    from contextlib import contextmanager

    pool, repo, data = setup
    approve(repo, data)
    claim(repo)
    statements = []

    class Proxy:
        def __init__(self, conn):
            self.conn = conn

        def execute(self, sql, params=()):
            statements.append(sql)
            return self.conn.execute(sql.removesuffix(" FOR UPDATE"), params)

    class RecordedPool:
        dialect = "postgresql"

        @contextmanager
        def transaction(self):
            with pool.transaction() as conn:
                yield Proxy(conn)

    recorded = MemoryLakeSubmissionRepo(RecordedPool(), clock=lambda: 100.0)
    recorded.begin_step(
        "approval", expected_revision=1, expected_state="approved", phase="uploading"
    )
    tables = [
        sql.split(" FROM ")[1].split(" WHERE ")[0]
        for sql in statements
        if sql.endswith(" FOR UPDATE")
    ]
    assert list(dict.fromkeys(tables)) == [
        "users",
        "agents",
        "connectors",
        "memorylake_bindings",
        "memorylake_write_policies",
        "memorylake_submission_approvals",
        "memorylake_submissions",
    ]
    assert statements.index(
        next(s for s in statements if s.startswith("UPDATE memorylake_submissions"))
    ) > max(i for i, s in enumerate(statements) if s.endswith(" FOR UPDATE"))


def test_full_upload_import_and_readonly_save_after_rotation(setup):
    pool, repo, data = setup
    approve(repo, data)
    claim(repo)
    repo.begin_step("approval", expected_revision=1, expected_state="approved", phase="uploading")
    assert repo.save(
        "approval",
        expected_revision=2,
        expected_state="uploading",
        expected_intent_json=data["intent_json"],
        state="uploaded",
        item_id="item:1",
        document_id=None,
        import_accepted=False,
    )
    grant = repo.begin_step(
        "approval", expected_revision=3, expected_state="uploaded", phase="importing"
    )
    assert grant.record.revision == 4
    assert repo.save(
        "approval",
        expected_revision=4,
        expected_state="importing",
        expected_intent_json=data["intent_json"],
        state="unknown",
        item_id="item:1",
        document_id="document:1",
        import_accepted=True,
    )
    ConnectorRepo(pool).upsert_credentials(instance_id="connector", blob=b"rotated")
    repo.replace_policy(
        binding_id="binding",
        expected_revision=1,
        project_id="project",
        library_parent_id="folder",
        enabled=False,
    )
    # Outcome CAS deliberately does not require original write credentials or
    # consent. The domain's fresh read authorization controls reconciliation.
    assert repo.save(
        "approval",
        expected_revision=5,
        expected_state="unknown",
        expected_intent_json=data["intent_json"],
        state="ready",
        item_id="item:1",
        document_id="document:1",
        import_accepted=True,
    )
    assert repo.get("approval").state == "ready"


def test_get_policy_snapshot(setup):
    _, repo, _ = setup
    assert repo.get_policy("missing") is None
    current = repo.get_policy("binding")
    assert current.enabled and current.revision == 1
    changed = repo.replace_policy(
        binding_id="binding",
        expected_revision=1,
        project_id="project",
        library_parent_id="folder",
        enabled=False,
    )
    assert repo.get_policy("binding") == changed


@pytest.mark.parametrize("now", [float("nan"), float("inf"), float("-inf"), True, "100"])
def test_invalid_clock_fails_closed_before_grant(setup, now):
    _, repo, data = setup
    approve(repo, data)
    claim(repo)
    repo._clock = lambda: now
    with pytest.raises(ValueError):
        repo.begin_step(
            "approval", expected_revision=1, expected_state="approved", phase="uploading"
        )
    assert repo.get("approval").state == "approved"
