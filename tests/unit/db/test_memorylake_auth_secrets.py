"""Bounded JWT-secret reads from real SQLite; synthetic values, no bootstrap."""

from unittest.mock import patch

import pytest
from tests.support.memorylake import offline_test_guard

from octop.infra.db.migrate import run_migrations
from octop.infra.db.pool import SqlitePool
from octop.infra.db.repos.secrets import SecretRepo


@pytest.fixture(autouse=True)
def offline():
    with offline_test_guard():
        yield


@pytest.fixture
def stored(tmp_path):
    pool = SqlitePool(tmp_path / "db")
    run_migrations(pool)
    try:
        yield pool, SecretRepo(pool)
    finally:
        pool.close()


def put(pool, value):
    with pool.transaction() as conn:
        conn.execute(
            "INSERT INTO secrets(k,v,created_at) VALUES ('jwt',?,0) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            (value,),
        )


def test_absent_secret_read_never_bootstraps(stored):
    pool, repo = stored
    with patch.object(repo, "get_or_create", side_effect=AssertionError("must not bootstrap")):
        assert repo.get_bounded("jwt", max_bytes=4096) is None
    with pool.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM secrets WHERE k='jwt'").fetchone()[0] == 0


def test_valid_binary_at_boundary_returned_exactly(stored):
    pool, repo = stored
    put(pool, b"synthetic-key")
    assert repo.get_bounded("jwt", max_bytes=13) == b"synthetic-key"
    put(pool, b"x" * 4096)
    assert repo.get_bounded("jwt") == b"x" * 4096


@pytest.mark.parametrize("value", [b"", b"synthetic-too-large", 16, "synthetic-sensitive-text"])
def test_empty_oversized_integer_or_text_secret_is_rejected(stored, value):
    pool, repo = stored
    put(pool, value)
    # In particular, integer storage must not become bytes(integer), allocating
    # a bogus zero-filled key. No real keys or large allocations in this test.
    with pytest.raises(ValueError) as caught:
        repo.get_bounded("jwt", max_bytes=8)
    assert "synthetic" not in str(caught.value)
