"""Secrets table access."""

from __future__ import annotations

from collections.abc import Callable

from octop.infra.db.pool import DatabasePool
from octop.infra.db.repos._base import now_ts


class SecretRepo:
    def __init__(self, db: DatabasePool):
        self._db = db

    def get(self, k: str) -> bytes | None:
        with self._db.connect() as conn:
            r = conn.execute("SELECT v FROM secrets WHERE k = ?", (k,)).fetchone()
        return bytes(r["v"]) if r else None

    def get_bounded(self, k: str, *, max_bytes: int = 4096) -> bytes | None:
        """Read existing binary material without bootstrap or unbounded conversion.

        CASE keeps oversized values out of the driver result. Validate the type
        before bytes(): a corrupt SQLite integer must never become an allocation.
        The database still evaluates its own length expression; this is not a DB
        storage/page or SQL execution-time quota. Legacy get semantics are intact.
        """
        if type(max_bytes) is not int or not 1 <= max_bytes <= 65_536:
            raise ValueError("invalid_secret_limit")
        with self._db.connect() as conn:
            row = conn.execute(
                "SELECT CASE WHEN length(v) BETWEEN 1 AND ? THEN v ELSE NULL END AS v "
                "FROM secrets WHERE k = ?",
                (max_bytes, k),
            ).fetchone()
        if row is None:
            return None
        value = row["v"]
        if not isinstance(value, (bytes, bytearray, memoryview)):
            raise ValueError("invalid_secret_material")
        size = value.nbytes if isinstance(value, memoryview) else len(value)
        if not 1 <= size <= max_bytes:
            raise ValueError("invalid_secret_material")
        return bytes(value)

    def get_or_create(self, k: str, factory: Callable[[], bytes]) -> bytes:
        existing = self.get(k)
        if existing is not None:
            return existing
        value = factory()
        with self._db.transaction() as conn:
            # Check again inside transaction to avoid race
            r = conn.execute("SELECT v FROM secrets WHERE k = ?", (k,)).fetchone()
            if r is not None:
                return bytes(r["v"])
            conn.execute(
                "INSERT INTO secrets(k, v, created_at) VALUES (?, ?, ?)",
                (k, value, now_ts()),
            )
        return value

    def rotate(self, k: str, new_value: bytes) -> None:
        with self._db.transaction() as conn:
            conn.execute(
                "UPDATE secrets SET v = ?, rotated_at = ? WHERE k = ?",
                (new_value, now_ts(), k),
            )
