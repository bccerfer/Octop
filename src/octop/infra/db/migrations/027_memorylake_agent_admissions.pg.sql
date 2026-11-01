-- Content-free durable admission marker. Deliberately no resource FK: deletion
-- or same-ID reconstruction must not erase the restriction on future sharing.
CREATE TABLE memorylake_agent_admissions (
    agent_id TEXT PRIMARY KEY,
    owner_user_id INTEGER NOT NULL,
    first_admitted_at INTEGER NOT NULL
);
UPDATE _schema_version SET version = 27;
