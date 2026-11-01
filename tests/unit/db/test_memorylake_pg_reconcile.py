"""Recorded PostgreSQL SQL contracts, NOT execution against a PostgreSQL server."""

from contextlib import ExitStack, contextmanager
from unittest.mock import patch

import pytest
from tests.support.memorylake import offline_test_guard

from octop.infra.db import migrate


@pytest.fixture(autouse=True)
def offline_safety():
    with offline_test_guard():
        yield


class RecordedPostgres:
    dialect = "postgresql"

    def __init__(self, *, has_revision=False):
        self.has_revision = has_revision
        self.statements = []
        self.in_transaction = False
        self.transactions = 0

    @contextmanager
    def transaction(self):
        assert not self.in_transaction
        self.in_transaction = True
        self.transactions += 1
        try:
            yield self
        finally:
            self.in_transaction = False

    def execute(self, sql, params=()):
        assert self.in_transaction, "schema inspection and repair must share a transaction"
        assert "sqlite_master" not in sql and "PRAGMA" not in sql
        self.statements.append((sql, params))
        self.last = sql
        return self

    def fetchone(self):
        if "information_schema.tables" in self.last:
            return (1,)
        if "information_schema.columns" in self.last:
            return (1,) if self.has_revision else None
        raise AssertionError("unexpected query")

    def fetchall(self):
        assert "information_schema.columns" in self.last
        return [("credential_revision",)] if self.has_revision else []


def pg_path():
    return next(path for version, path in migrate._discover("postgresql") if version == 25)


@pytest.mark.parametrize("has_revision", [False, True])
def test_pg_repair_preserves_existing_revisions_and_sql_contract(has_revision):
    db = RecordedPostgres(has_revision=has_revision)
    migrate._ensure_memorylake_bindings_schema(db, pg_path())
    assert db.transactions == 1
    statements = [sql for sql, _ in db.statements]
    inspected = [sql for sql in statements if "information_schema" in sql]
    assert inspected and all("current_schema()" in sql for sql in inspected)
    expected = migrate._split_pg_sql(pg_path().read_text())
    assert (expected[0] in statements) is (not has_revision)
    assert (expected[1] in statements) is (not has_revision)
    assert expected[2].replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ", 1) in statements
    assert not any("UPDATE _schema_version" in sql for sql in statements)
    assert not any("AUTOINCREMENT" in sql for sql in statements)


@pytest.mark.parametrize("watermark", [25, 26])
def test_run_migrations_reconciles_pg_even_when_v25_skipped(watermark):
    db = RecordedPostgres()
    path = pg_path()
    with ExitStack() as stack:
        stack.enter_context(patch.object(migrate, "_discover", return_value=[(25, path)]))
        stack.enter_context(patch.object(migrate, "_current_version", return_value=watermark))
        # Isolate v25 orchestration; unrelated migration repair helpers are not PG emulated.
        for name in vars(migrate):
            if (
                name.startswith(("_ensure_", "_grant_", "_expand_"))
                and name != "_ensure_memorylake_bindings_schema"
            ):
                stack.enter_context(patch.object(migrate, name))
        migrate.run_migrations(db)
    assert db.transactions == 1
    assert any("CREATE TABLE IF NOT EXISTS memorylake_bindings" in sql for sql, _ in db.statements)
    assert not any("UPDATE _schema_version" in sql for sql, _ in db.statements)
