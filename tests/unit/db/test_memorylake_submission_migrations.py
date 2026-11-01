"""Actual migration entry paths; temporary SQLite and synthetic metadata only."""

import sqlite3
from contextlib import ExitStack, contextmanager
from unittest.mock import patch

import pytest
from tests.support.memorylake import offline_test_guard

from octop.infra.db import migrate
from octop.infra.db.pool import SqlitePool

_TABLES = {
    "memorylake_write_policies",
    "memorylake_submission_approvals",
    "memorylake_submissions",
}


@pytest.fixture
def db(tmp_path):
    with offline_test_guard():
        pool = SqlitePool(tmp_path / "migrate.db")
        discover = migrate._discover
        with patch.object(
            migrate,
            "_discover",
            side_effect=lambda dialect="sqlite": [(v, p) for v, p in discover(dialect) if v <= 25],
        ):
            migrate.run_migrations(pool)
        try:
            yield pool
        finally:
            pool.close()


def tables(pool):
    with pool.connect() as conn:
        return {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def test_actual_v25_upgrade_and_replay_preserve_policy_revision(db):
    migrate.run_migrations(db)
    with db.transaction() as conn:
        conn.execute(
            "INSERT INTO memorylake_write_policies(binding_id,project_id,library_parent_id,revision) VALUES ('b','p','folder',7)"
        )
        conn.execute("UPDATE _schema_version SET version=25")
    migrate.run_migrations(db)
    assert tables(db) >= _TABLES
    with db.connect() as conn:
        assert conn.execute("SELECT revision,enabled FROM memorylake_write_policies").fetchone()[
            :
        ] == (7, 0)
        assert conn.execute("SELECT version FROM _schema_version").fetchone()[0] == 27


@pytest.mark.parametrize("watermark", [26, 27])
def test_actual_skipped_watermark_repairs_missing_ledger_without_downgrade(db, watermark):
    with db.transaction() as conn:
        conn.execute("UPDATE _schema_version SET version=?", (watermark,))
    migrate.run_migrations(db)
    assert tables(db) >= _TABLES
    with db.connect() as conn:
        assert conn.execute("SELECT version FROM _schema_version").fetchone()[0] == max(
            27, watermark
        )


def test_actual_partial_v26_table_is_reconciled(db):
    path = next(p for version, p in migrate._discover() if version == 26)
    first = migrate._split_pg_sql(path.read_text())[0]
    with db.transaction() as conn:
        conn.execute(first)
        conn.execute(
            "INSERT INTO memorylake_write_policies(binding_id,project_id,library_parent_id,revision) VALUES ('b','p','folder',9)"
        )
    migrate.run_migrations(db)
    assert tables(db) >= _TABLES
    with db.connect() as conn:
        assert conn.execute("SELECT revision FROM memorylake_write_policies").fetchone()[0] == 9


def test_actual_v26_failure_rolls_back_all_ddl_and_watermark(db, tmp_path):
    discover = migrate._discover
    path = next(p for version, p in discover() if version == 26)
    fault = tmp_path / "026_fault.sql"
    fault.write_text(
        path.read_text().replace(
            "CREATE TABLE memorylake_submission_approvals",
            "INVALID SYNTHETIC SQL;\nCREATE TABLE memorylake_submission_approvals",
        )
    )
    with (
        patch.object(
            migrate,
            "_discover",
            side_effect=lambda dialect="sqlite": [
                (v, fault if v == 26 else p) for v, p in discover(dialect)
            ],
        ),
        pytest.raises(sqlite3.DatabaseError),
    ):
        migrate.run_migrations(db)
    assert _TABLES.isdisjoint(tables(db))
    with db.connect() as conn:
        assert conn.execute("SELECT version FROM _schema_version").fetchone()[0] == 25
    migrate.run_migrations(db)
    assert tables(db) >= _TABLES


def test_repo_bundle_exposes_unregistered_submission_storage(db):
    from octop.infra.db.repos.memorylake_submissions import MemoryLakeSubmissionRepo
    from octop.infra.db.services import RepoBundle

    migrate.run_migrations(db)
    assert isinstance(RepoBundle.from_pool(db).memorylake_submission_repo, MemoryLakeSubmissionRepo)


def test_repair_preserves_inflight_phase_ids_revision_and_lease(db):
    migrate.run_migrations(db)
    with db.transaction() as conn:
        conn.execute(
            "INSERT INTO memorylake_submission_approvals(approval_id,approver_user_id,binding_id,connector_id,caller_user_id,agent_id,authorization_revision,policy_revision,credential_revision,base_url,workspace_id,project_id,library_parent_id,intent_json,expires_at,claimed) "
            "VALUES ('a',1,'b','c',1,'agent',2,3,4,'https://invalid.example','w','p','folder','{}',2000,1)"
        )
        conn.execute(
            "INSERT INTO memorylake_submissions(submission_id,approval_id,state,revision,item_id,document_id,import_accepted,lease_phase,lease_revision,lease_expires_at) "
            "VALUES ('a','a','importing',8,'item','doc',1,'importing',8,1000)"
        )
        before = tuple(conn.execute("SELECT * FROM memorylake_submissions").fetchone())
        conn.execute("UPDATE _schema_version SET version=25")
    migrate.run_migrations(db)
    migrate.run_migrations(db)
    with db.connect() as conn:
        assert tuple(conn.execute("SELECT * FROM memorylake_submissions").fetchone()) == before


@pytest.mark.parametrize("watermark", [25, 26, 27])
def test_pg_actual_orchestration_uses_transactional_idempotent_ddl_contract_only(watermark):
    """Recorded statements, NOT execution or isolation on a PostgreSQL server."""
    statements, transactions = [], []

    class RecordedPg:
        dialect = "postgresql"
        active = False

        @contextmanager
        def transaction(self):
            assert not self.active
            self.active = True
            transactions.append(True)
            try:
                yield self
            finally:
                self.active = False

        def execute(self, sql, params=()):
            assert self.active
            assert "PRAGMA" not in sql and "sqlite_master" not in sql
            statements.append(sql)

    path = next(p for version, p in migrate._discover("postgresql") if version == 26)
    with ExitStack() as stack:
        stack.enter_context(patch.object(migrate, "_discover", return_value=[(26, path)]))
        stack.enter_context(patch.object(migrate, "_current_version", return_value=watermark))
        for name in vars(migrate):
            if (
                name.startswith(("_ensure_", "_grant_", "_expand_"))
                and name != "_ensure_memorylake_submissions_schema"
            ):
                stack.enter_context(patch.object(migrate, name))
        migrate.run_migrations(RecordedPg())
    assert len(transactions) == (2 if watermark == 25 else 1)
    creates = [sql for sql in statements if sql.startswith("CREATE TABLE")]
    assert all(sql.startswith("CREATE TABLE IF NOT EXISTS memorylake_") for sql in creates)
    assert len(creates) == (6 if watermark == 25 else 3)
    bumps = [sql for sql in statements if sql.startswith("UPDATE _schema_version")]
    assert bumps == (
        ["UPDATE _schema_version SET version = 26 WHERE version < 26"] if watermark == 25 else []
    )
