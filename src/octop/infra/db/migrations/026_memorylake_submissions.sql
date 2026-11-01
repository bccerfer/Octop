-- Unregistered submission ledger. Resource IDs deliberately have no resource FK:
-- deletion must not silently erase remote-side-effect evidence or newly block
-- existing resource deletion. Missing live resources deny authorization. Product
-- retention policy and same-ID identity ABA remain production registration gates.
CREATE TABLE memorylake_write_policies (
    binding_id TEXT PRIMARY KEY,
    enabled INTEGER NOT NULL DEFAULT 0 CHECK (enabled IN (0,1)),
    project_id TEXT NOT NULL,
    library_parent_id TEXT NOT NULL,
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0)
);
CREATE TABLE memorylake_submission_approvals (
    approval_id TEXT PRIMARY KEY,
    approver_user_id INTEGER NOT NULL,
    binding_id TEXT NOT NULL,
    connector_id TEXT NOT NULL,
    caller_user_id INTEGER NOT NULL,
    agent_id TEXT NOT NULL,
    authorization_revision INTEGER NOT NULL CHECK (authorization_revision > 0),
    policy_revision INTEGER NOT NULL CHECK (policy_revision > 0),
    credential_revision INTEGER NOT NULL CHECK (credential_revision > 0),
    base_url TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    library_parent_id TEXT NOT NULL,
    intent_json TEXT NOT NULL,
    expires_at REAL NOT NULL,
    revoked INTEGER NOT NULL DEFAULT 0 CHECK (revoked IN (0,1)),
    claimed INTEGER NOT NULL DEFAULT 0 CHECK (claimed IN (0,1))
);
CREATE TABLE memorylake_submissions (
    submission_id TEXT PRIMARY KEY,
    approval_id TEXT NOT NULL UNIQUE REFERENCES memorylake_submission_approvals(approval_id),
    state TEXT NOT NULL CHECK (state IN ('approved','uploading','uploaded','importing','processing','ready','failed','unknown')),
    revision INTEGER NOT NULL CHECK (revision > 0),
    item_id TEXT,
    document_id TEXT,
    import_accepted INTEGER NOT NULL DEFAULT 0 CHECK (import_accepted IN (0,1)),
    lease_phase TEXT CHECK (lease_phase IN ('uploading','importing')),
    lease_revision INTEGER,
    lease_expires_at REAL
);
UPDATE _schema_version SET version = 26;
