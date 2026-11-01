-- Schema v25: opt-in, owner-only MemoryLake read bindings; no write ledger.
ALTER TABLE connectors ADD COLUMN credential_revision INTEGER NOT NULL DEFAULT 0;
UPDATE connectors SET credential_revision = 1
WHERE credential_blob IS NOT NULL AND length(credential_blob) > 0;

CREATE TABLE memorylake_bindings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    binding_id TEXT NOT NULL UNIQUE,
    connector_id TEXT NOT NULL REFERENCES connectors(instance_id) ON DELETE CASCADE,
    owner_user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    agent_id TEXT NOT NULL REFERENCES agents(agent_id) ON DELETE CASCADE,
    base_url TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    project_ids_json TEXT NOT NULL,
    actor_id TEXT,
    enabled INTEGER NOT NULL DEFAULT 0 CHECK (enabled IN (0, 1)),
    authorization_revision INTEGER NOT NULL DEFAULT 1 CHECK (authorization_revision > 0),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE (connector_id, agent_id)
);
UPDATE _schema_version SET version = 25;
