"""Fernet encryption for connector credentials."""

from __future__ import annotations

import json
from typing import Any

from cryptography.fernet import Fernet

from octop.infra.crypto.fernet import PURPOSE_CONNECTOR, decrypt_json, encrypt_json
from octop.infra.db.repos.secrets import SecretRepo


def encrypt_credentials(repo: SecretRepo, payload: dict[str, Any]) -> bytes:
    return encrypt_json(repo, PURPOSE_CONNECTOR, payload)


def decrypt_credentials(repo: SecretRepo, blob: bytes) -> dict[str, Any]:
    return decrypt_json(repo, PURPOSE_CONNECTOR, blob)


def decrypt_credentials_snapshot(master_key: bytes, blob: bytes) -> dict[str, Any]:
    """Decode an existing credential snapshot without repo access or key caching."""
    payload = json.loads(Fernet(master_key).decrypt(blob).decode("utf-8"))
    return payload if isinstance(payload, dict) else {}
