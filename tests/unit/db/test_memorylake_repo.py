"""Real temporary SQLite coverage; no MemoryLake service or credentials."""

from pathlib import Path
from unittest.mock import patch

import pytest

from octop.infra.db import migrate
from octop.infra.db.pool import SqlitePool
from octop.infra.db.repos.agents import AgentRepo
from octop.infra.db.repos.connectors import ConnectorRepo
from octop.infra.db.repos.memorylake import MemoryLakeBindingRepo
from octop.infra.db.repos.users import UserRepo
from octop.infra.db.services import RepoBundle


@pytest.fixture(autouse=True)
def memorylake_offline_safety():
    from tests.support.memorylake import offline_test_guard

    with offline_test_guard():
        yield


@pytest.fixture
def setup(tmp_path: Path):
    pool = SqlitePool(tmp_path / "test.db")
    migrate.run_migrations(pool)
    users = UserRepo(pool)
    owner = users.create(username="owner", role="user")
    other = users.create(username="other", role="user")
    AgentRepo(pool).create(agent_id="agent", user_id=owner, name="agent")
    connectors = ConnectorRepo(pool)
    connectors.create(
        instance_id="connector",
        user_id=owner,
        kind="memorylake",
        display_name="ml",
        mcp_server_name="ml",
    )
    connectors.upsert_credentials(instance_id="connector", blob=b"FAKE-CIPHERTEXT", expires_at=123)
    repo = MemoryLakeBindingRepo(pool)
    row = repo.create(
        binding_id="binding",
        connector_id="connector",
        owner_user_id=owner,
        agent_id="agent",
        base_url="https://api.memorylake.ai",
        workspace_id="workspace",
        project_ids_json='["project"]',
    )
    yield pool, repo, connectors, owner, other, row
    pool.close()


def enable(repo, owner, revision=1):
    return repo.replace_scope(
        binding_id="binding",
        owner_user_id=owner,
        expected_revision=revision,
        base_url="https://api.memorylake.ai",
        workspace_id="workspace",
        project_ids_json='["project"]',
        actor_id=None,
        enabled=True,
    )


def load(repo, owner):
    return repo.load_owned_active("binding", caller_user_id=owner, agent_id="agent")


def test_default_disabled_cas_and_di(setup):
    pool, repo, _, owner, other, row = setup
    assert row.enabled is False and row.authorization_revision == 1
    assert load(repo, owner) is None
    assert enable(repo, other) is None
    changed = enable(repo, owner)
    assert changed.authorization_revision == 2
    assert enable(repo, owner) is None
    assert isinstance(RepoBundle.from_pool(pool).memorylake_binding_repo, MemoryLakeBindingRepo)
    snapshot = load(repo, owner)
    assert snapshot.binding == changed
    assert snapshot.credential_revision == 1
    assert snapshot.credential_expires_at == 123
    assert "FAKE-CIPHERTEXT" not in repr(snapshot)
    assert load(repo, other) is None
    assert (
        repo.load_owned_active(binding_id="binding", caller_user_id=owner, agent_id="wrong") is None
    )


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE users SET disabled=1",
        "UPDATE agents SET enabled=0",
        "UPDATE agents SET is_shared=1",
        "UPDATE agents SET kind='team'",
        "UPDATE agents SET kind='unknown'",
        "UPDATE agents SET user_id=NULL",
        "UPDATE agents SET user_id=(SELECT id FROM users WHERE username='other')",
        "UPDATE connectors SET shared=1",
        "UPDATE connectors SET status='disabled'",
        "UPDATE connectors SET kind='other'",
        "UPDATE connectors SET credential_blob=NULL",
        "UPDATE connectors SET credential_blob=X''",
        "UPDATE connectors SET user_id=(SELECT id FROM users WHERE username='other')",
    ],
)
def test_current_authorization_rejects(setup, statement):
    pool, repo, _, owner, _, _ = setup
    enable(repo, owner)
    assert load(repo, owner) is not None
    with pool.transaction() as conn:
        conn.execute(statement)
    assert load(repo, owner) is None


def test_same_second_rotation_and_independent_pool(setup, tmp_path):
    _, repo, connectors, owner, _, _ = setup
    enable(repo, owner)
    second = SqlitePool(tmp_path / "test.db")
    try:
        reader = MemoryLakeBindingRepo(second)
        with patch("octop.infra.db.repos.connectors.now_ts", return_value=10):
            for revision in (2, 3):
                connectors.upsert_credentials(instance_id="connector", blob=b"FAKE-NEW")
                assert load(reader, owner).credential_revision == revision
                assert connectors.get("connector").credential_revision == revision
        assert enable(reader, owner, 2).authorization_revision == 3
        assert enable(repo, owner, 2) is None
    finally:
        second.close()


def test_v24_upgrade_and_idempotence(tmp_path):
    pool = SqlitePool(tmp_path / "old.db")
    discover = migrate._discover
    with patch.object(
        migrate,
        "_discover",
        side_effect=lambda dialect="sqlite": [(v, p) for v, p in discover(dialect) if v <= 24],
    ):
        migrate.run_migrations(pool)
    owner = UserRepo(pool).create(username="owner", role="user")
    with pool.transaction() as conn:
        for name, blob in [("with", b"FAKE"), ("without", None)]:
            conn.execute(
                "INSERT INTO connectors(instance_id,user_id,kind,display_name,status,mcp_server_name,credential_blob,created_at,updated_at) VALUES (?,?,'memorylake',?,'active',?,?,0,0)",
                (name, owner, name, name, blob),
            )
    migrate.run_migrations(pool)
    migrate.run_migrations(pool)
    assert ConnectorRepo(pool).get("with").credential_revision == 1
    assert ConnectorRepo(pool).get("without").credential_revision == 0
    with pool.connect() as conn:
        assert conn.execute("SELECT version FROM _schema_version").fetchone()[0] == 27
    pool.close()


def test_connector_delete_cascades_read_binding(setup):
    _, repo, connectors, owner, _, _ = setup
    enable(repo, owner)
    connectors.delete("connector")
    assert load(repo, owner) is None


def test_replay_preserves_rotated_revision(setup):
    pool, _, connectors, _, _, _ = setup
    connectors.upsert_credentials(instance_id="connector", blob=b"FAKE-ROTATED")
    with pool.transaction() as conn:
        conn.execute("UPDATE _schema_version SET version=24")
    migrate.run_migrations(pool)
    assert connectors.get("connector").credential_revision == 2


def test_partial_v24_without_connectors(tmp_path):
    pool = SqlitePool(tmp_path / "partial.db")
    with pool.transaction() as conn:
        conn.execute("CREATE TABLE _schema_version(version INTEGER)")
        conn.execute("INSERT INTO _schema_version VALUES(24)")
    path = next(p for version, p in migrate._discover() if version == 25)
    # Exercise precisely the existing partial-schema migration compatibility path.
    migrate._apply_sqlite_migration(pool, 25, path)
    with pool.connect() as conn:
        assert conn.execute("SELECT version FROM _schema_version").fetchone()[0] == 25
        assert conn.execute(
            "SELECT 1 FROM sqlite_master WHERE name='memorylake_bindings'"
        ).fetchone()
        assert not conn.execute("SELECT 1 FROM sqlite_master WHERE name='connectors'").fetchone()
    pool.close()


def test_pg_sql_static_contract_only():
    """Static parity check, explicitly NOT a PostgreSQL execution test."""
    sqlite_path = next(p for version, p in migrate._discover() if version == 25)
    pg_path = next(p for version, p in migrate._discover("postgresql") if version == 25)
    assert (
        sqlite_path.read_text().replace(
            "id INTEGER PRIMARY KEY AUTOINCREMENT", "id BIGSERIAL PRIMARY KEY"
        )
        == pg_path.read_text()
    )
    assert len(migrate._split_pg_sql(pg_path.read_text())) == 4


@pytest.mark.parametrize("watermark", [25, 26])
def test_v24_physical_schema_with_advanced_watermark_is_repaired(tmp_path, watermark):
    from tests.support.memorylake import offline_test_guard

    with offline_test_guard():
        pool = SqlitePool(tmp_path / "advanced.db")
        try:
            discover = migrate._discover
            with patch.object(
                migrate,
                "_discover",
                side_effect=lambda dialect="sqlite": [
                    (v, p) for v, p in discover(dialect) if v <= 24
                ],
            ):
                migrate.run_migrations(pool)
            owner = UserRepo(pool).create(username="owner", role="user")
            with pool.transaction() as conn:
                conn.execute("UPDATE _schema_version SET version=?", (watermark,))
                conn.execute(
                    "INSERT INTO connectors(instance_id,user_id,kind,display_name,status,"
                    "mcp_server_name,credential_blob,created_at,updated_at) "
                    "VALUES ('old',?,'memorylake','old','active','old',?,0,0)",
                    (owner, b"SYNTHETIC"),
                )
            migrate.run_migrations(pool)
            connectors = ConnectorRepo(pool)
            assert connectors.get("old").credential_revision == 1
            assert connectors.list_by_user(owner)[0].credential_revision == 1
            with pool.connect() as conn:
                assert conn.execute("SELECT COUNT(*) FROM memorylake_bindings").fetchone()[0] == 0
                assert conn.execute("SELECT version FROM _schema_version").fetchone()[0] == max(
                    27, watermark
                )
            connectors.upsert_credentials(instance_id="old", blob=b"SYNTHETIC-ROTATED")
            migrate.run_migrations(pool)
            assert connectors.get("old").credential_revision == 2
            with pool.connect() as conn:
                assert conn.execute("SELECT version FROM _schema_version").fetchone()[0] == max(
                    27, watermark
                )
        finally:
            pool.close()


@pytest.mark.parametrize("existing_revision", [None, 7])
def test_pre_v13_physical_connectors_repaired_before_memorylake(tmp_path, existing_revision):
    pool = SqlitePool(tmp_path / "pre13.db")
    try:
        initial = next(p for v, p in migrate._discover() if v == 1)
        with pool.connect() as conn:
            conn.executescript(initial.read_text())
            conn.execute("UPDATE _schema_version SET version=13")
            conn.execute(
                "INSERT INTO users(username,password_hash,role,created_at) "
                "VALUES ('owner','synthetic','admin',1)"
            )
            owner = conn.execute("SELECT id FROM users").fetchone()[0]
            conn.execute(
                "INSERT INTO connectors(instance_id,user_id,kind,display_name,mcp_server_name,"
                "credential_blob,created_at,updated_at) VALUES ('old',?,'memorylake','old','old',?,1,1)",
                (owner, b"SYNTHETIC"),
            )
            if existing_revision is not None:
                conn.execute(
                    "ALTER TABLE connectors ADD COLUMN credential_revision INTEGER NOT NULL DEFAULT 0"
                )
                conn.execute("UPDATE connectors SET credential_revision=?", (existing_revision,))
        migrate.run_migrations(pool)
        migrate.run_migrations(pool)
        row = ConnectorRepo(pool).get("old")
        assert row.credential_revision == (existing_revision or 1)
        assert row.credential_blob == b"SYNTHETIC"
        with pool.connect() as conn:
            targets = {
                r["table"] for r in conn.execute("PRAGMA foreign_key_list(memorylake_bindings)")
            }
            assert "connectors" in targets
            assert "connectors_legacy" not in targets
            assert not conn.execute("PRAGMA foreign_key_check").fetchall()
    finally:
        pool.close()
