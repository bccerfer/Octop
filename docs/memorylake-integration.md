# MemoryLake integration — M0 development checkpoint

Status: **offline contract implementation, not an enabled connector**. No catalog
entry, HTTP API, dashboard UI or agent tool is registered. Existing Octop native
memory capture/recall and history are unchanged. This is not a migration.

## Source baseline

- Octop: `890111b7879665840414ca28ab168b05b0db0921` (`main`, initially clean).
- MemoryLake CLI: `40243c2547d6185e163465524db8d739c4725243` (read-only reference).
- Both checkouts are local siblings. No CLI edit, release or production
  registration is included. CLI changes require separate owner approval.
- The user authorized publication of this **WIP snapshot** on
  `feature/octop-memorylake`, based on the current `main` because `origin` has no
  `develop` branch. They explicitly approved skipping the unavailable pre-commit
  gate for this one commit only. Normal repository hooks remain enabled; this is
  not a full-regression pass or approval to merge, release or deploy.

## Implemented offline boundaries

| Module | Current responsibility | Not yet implemented |
| --- | --- | --- |
| `infra/connectors/memorylake/contract.py` | Typed identity/scope checks and fixed read argv planning | Production identity/permission loading or tool registration |
| `infra/connectors/memorylake/session.py` | Fresh POSIX 0700 profile directory, 0600 empty-key record, child-only environment; cancellation-safe off-loop setup/cleanup | Real CLI bootstrap verification, Windows ACLs, deployment isolation |
| `infra/connectors/memorylake/runner.py` | Bounded async POSIX execution, process-group cleanup, hidden partial results | Binary/version validation, per-connection production wiring, Windows process lifecycle |
| `infra/connectors/memorylake/results.py` | Scope/ID checks, explicit snippet field allowlists, bounded untrusted read results | Real-service response verification and transport-specific citations |
| `infra/connectors/memorylake/read_service.py` | Unregistered read executor; fresh authorization and atomic credential-revision binding before/after execution | Actual database loaders, trusted harness caller adapter, production registration |
| `infra/connectors/memorylake/submissions.py` | Unregistered submission orchestration, bounded read-only polling and approval/ledger/step-lease protocols, exercised with fakes | Transactional production ledger, approval UI, trusted source adapter, recovery worker |
| `infra/connectors/memorylake/cli_transport.py` | Unregistered bounded CLI file transport with private staging and lease checks | Real binary/source validation and production transport registration |
| `infra/connectors/memorylake/documents.py` | Upload/import/document response classification; keep known IDs across uncertain outcomes | Real-service schema and indexing behavior verification |

The runner preserves bounded stdout/stderr for nonzero exits and in a hidden
`partial_result` on timeout/output overflow and typed cancellation. Error strings never include argv,
environment or raw output. Raw result bytes remain internal and sensitive.
The timeout begins after obtaining the instance concurrency slot; queue deadlines
and deployment-wide/per-connection admission remain integration work.

Read operations are document search/get and personal fact search/get/trace.
Every command has an explicit workspace. Document search additionally fixes
`--types document` and an explicit, non-empty project subset. Fact operations
bind an explicit actor. Models cannot select endpoint, profile, credentials,
workspace or arbitrary argv. Shared agents, group conversations, cross-user
calls, disabled scopes and stale authorization revisions fail closed.

The caller context must be constructed from authenticated **current invocation**
state. The unregistered read service serializes admission per connection, loads
live authorization before secrets, verifies it again before process launch, and
rechecks after process completion and private-directory cleanup before exposing
results. A changed binding discards the result. This cannot retract a request
already received by MemoryLake. The credentials loader must atomically return
key plus credential revision; rotation must increment the same revision on the
connection record. The service does not cache keys or read user CLI profiles. The binding must be
freshly loaded, not retained in an agent-startup tool closure. The production
loader must also verify current agent ownership; a raw connector-row snapshot
is insufficient. A planned command is not an authorization token and must not be stored
for later execution after scope/consent changes.

### Critical CLI contracts verified in source only

The CLI commands below output JSON by default; there is no added `--json` flag.
All options precede `--`, so a leading-hyphen query is still one positional value.

```text
memorylake --profile octop --base-url APPROVED_URL search --workspace W --projects P --types document --top-k 10 -- QUERY
memorylake --profile octop --base-url APPROVED_URL search --workspace W --actors A --types fact --top-k 10 -- QUERY
memorylake --profile octop --base-url APPROVED_URL fact get --workspace W --actor A -- FACT_ID
memorylake --profile octop --base-url APPROVED_URL fact trace --workspace W --actor A -- FACT_ID
memorylake --profile octop --base-url APPROVED_URL project document get --workspace W --project P -- DOCUMENT_ID
```

- `search --projects` restricts **documents**, not facts. Omitted project filters
  mean no documents; omitted actor filters default to the authenticated actor.
  Octop must not depend on these implicit defaults.
- Profile credentials take precedence over `MEMORYLAKE_API_KEY`. An environment
  key alone is insufficient: a profile credential record is required. M0 writes
  a fresh `api_key = ""`, `login_method = "api_key"` record, fixes profile `octop`,
  and supplies a dummy key only in offline tests. `MEMORYLAKE_CONFIG_DIR` is
  explicit; inherited home, workspace, endpoint, proxy and debug settings are
  not copied into the child environment. No global environment is changed.
- This empty-key technique is coupled to the pinned source and remains an
  **unverified real-binary bootstrap candidate**. API keys in environment
  variables remain observable to privileged or same-UID processes. A private
  directory is not a multi-tenant OS security boundary. The current session
  helper rejects non-POSIX platforms rather than pretending chmod is a Windows
  ACL. It clears references on exit but cannot promise memory zeroization.
- `auth status`, login, refresh and switch can perform network requests. They
  are not suitable commands for inspecting a real user's local state offline.
- `library upload` has **no `--workspace`**. Its target is the credential's
  Library plus an explicit authorized parent folder. Upload success returns
  `item_id`; it is not evidence of project import or indexed readiness.
  The pinned CLI passes the explicit `--name` to Library creation and uploads
  binary chunks as `application/octet-stream`; the private staging basename
  does not determine the destination filename or MIME. Octop does not retry the
  whole command. The CLI itself can retry a chunk up to four attempts on transport
  failures, HTTP 429 or 5xx; that is not a submission-level idempotency guarantee.
- Import returns per-file details and document IDs. A nonzero exit can still
  contain accepted IDs (especially `--wait`). Preserve this result and reconcile;
  never retry the entire upload automatically or delete remote files to recover.
- Import acceptance coverage counts only success/duplicate entries. A failed
  entry's document ID is retained for reconciliation but cannot stand in for a
  missing accepted ID. Returned identifiers are bounded ASCII; malformed IDs
  never echo arbitrary upstream text.
- A document's successful terminal status is `okay`, not `ready` or `completed`.
  Unknown status, wrong document ID or a failed status read cannot establish
  readiness. Even accepted import is not searchable-readiness evidence.

Source locations in the CLI: `crates/core/src/config.rs` (profile resolution),
`crates/cli/src/commands/search.rs`, `crates/cli/src/commands/fact/scope.rs`,
`crates/core/src/api/search/types.rs`, `crates/core/src/api/library/types.rs`,
`crates/core/src/api/documents/types.rs`, and
`crates/cli/src/commands/project/document.rs`.

## Integration safety gates

The existing generic gateway closes over credentials in
`infra/connectors/gateway/langchain.py` and does not pass the current caller to
`GatewayAdapter.call_tool`. Internal MCP uses a connector token, not a per-turn
caller identity. Do not register MemoryLake there without an explicit binding.

An existing candidate for trusted per-invocation identity is the server-built
request in `infra/gateway/process/harness_request.py` and LangGraph's
`configurable.user`, already used by `infra/agents/middleware/browser_profile.py`.
This needs executable harness tests, including concurrent/shared-agent calls,
revocation, credential rotation and forged model `as_user` arguments.

Before enabling a connection:

1. Bind current caller, agent ownership and connection, and recheck live scope
   at execution. No admin impersonation or shared-agent fallback to owner keys.
2. Restrict the deployment-approved endpoint and pinned absolute CLI binary;
   verify version/integrity without auto-installing during chat.
3. Wire the bounded asynchronous runner with deployment/connection admission,
   pinned binary/version validation and timeout/cancel cleanup; preserve partial
   stdout without forwarding raw errors or secrets to the model. Cancellation
   evidence must be caught and persisted as unknown inside the future write
   operation boundary before cancellation propagates. It does not mean no write.
4. Wire the result boundary: no invented URL/rank/owner fields, no unknown
   nested metadata or presigned-link/token extras. Document blocks and trace
   entries use explicit excerpt field allowlists and text/item budgets; overflow
   is a visible error, not silent truncation. Retrieved text remains untrusted
   data, never executable instructions. Known API-key echoes are normalized and
   redacted after JSON decoding, including escaped values and object keys.
5. Before file submission, resolve an authorized attachment through
   `HarnessAgent.workspace` / `BackendWorkspace`, never a model-selected host
   path. `api/common/attachments.py::save_attachment` currently discards its
   `owner_id`; `StoredAttachment.owner_id` is not proof of permission to export.
   Resolve actual caller + current owner-only agent workspace, with no shared
   agent or admin-impersonation fallback for external data transfer. Confirm content hash/version, destination Library folder, project and
   data transfer; consume a server-issued one-time approval in a durable ledger.
   Existing HTTP `owner_only` helpers still allow administrator bypass, so they
   are not themselves the required export check. Existing `adownload_bytes`
   callers read whole objects, and documented local BackendWorkspace resolution
   can fall back between root/workspace/host paths. A concrete source adapter
   requires executable harness tests for bounded reads, attachment-reference
   resolution, symlinks/backend fallbacks and ownership changes; do not invent a
   path-isolation guarantee from a fake workspace.
6. Persist upload and import steps independently; after uncertain outcomes,
   reconcile known IDs instead of re-uploading. Poll until a known terminal state
   or show an explicitly unresolved outcome. Verify retrieval separately.

## File-submission persistence boundary (production implementation pending)

The file domain uses an immutable byte snapshot, an opaque attachment ID, a
SHA-256 hash, size and filename. A trusted UI must approve the complete intent:
caller/agent, connector, workspace/project, Library parent, deployment endpoint,
content snapshot and permission/credential revisions. An arbitrary model boolean
or a connector API key is not approval. Snapshots are currently limited to
1–20 MiB; this does not replace source-side bounded reads, MIME checks or scanning.

The ledger interface separates three responsibilities:

- `claim`: atomically validate an unexpired, unrevoked exact approval, consume it
  once and create one submission. Concurrent/replayed claims cannot send again.
- `begin_step`: atomically inspect current consent, current actor/agent/connector
  permission and credential revisions, then CAS the submission into its next
  in-flight phase and issue a bounded step lease. This transaction is the
  authorization linearization point. Alternating separate approval/binding reads
  is **not** an implementation of this guarantee.
- `save`: durable CAS of revision, phase and immutable intent, preserving safe
  item/document IDs even on uncertain outcomes. Never persist raw stderr or file
  bytes through this protocol.

The in-memory test ledger proves the domain contract only. A production adapter
must use the existing repository/SharedServices architecture and matching SQLite
and PostgreSQL migrations, not a shadow database. SQLite `BEGIN IMMEDIATE` or
PostgreSQL row locks/conditional updates must serialize claim/lease/revocation;
key and credential revision must be updated atomically. Cross-process uniqueness,
transaction failures, restart recovery and lease expiry require real storage tests.
The current `ConnectorRepo` has second-resolution `updated_at` and
`credential_rotated_at`, not monotonic authorization/credential revisions. Two
updates in one second can share these values; do not use them as revisions.
Production wiring therefore needs an atomic revision/storage change with real
repository/migration tests. Even importing the current connector repository is
blocked here by the absent `langchain_core` dependency.

`StepLease` is a trusted internal snapshot, **not** a user-facing bearer token or
an anti-replay mechanism. Only the service that received the atomic grant may
invoke its transport once. Never accept a model/API-supplied lease; a recovery
worker must not replay a saved lease or restart an uncertain write. It may only
reconcile existing safe identifiers or leave the operation explicitly unresolved.

Revoking approval blocks leases granted afterwards. A step whose lease was
already granted may have reached MemoryLake or may complete; revocation cannot
retract a remote request. A canceled/uncertain write remains `unknown`, with
bounded parsed evidence when available. The durable in-flight record is the
recovery barrier even if saving the later receipt fails. Never infer that a
cancellation means nothing was uploaded, and never re-upload to resolve ambiguity.

Read-only reconciliation is distinct from write opt-in. It may use current
credentials after a legitimate rotation or paused writes if the same owner,
connector, endpoint, workspace and original project remain authorized. It may
only inspect the known accepted document, never re-submit the file. A truncated,
contradictory or multi-file report is not sufficient to turn one observed `okay`
document into a claim that the approved single-file submission is complete.

The internal `poll_status` helper performs only read-only observations with
explicit check-count, total-time and interval bounds. Every observation rechecks
current authorization. Known `ready`/`failed` records stop polling; unfamiliar or
transient timeout results remain `unknown` and do not trigger another write.
Typed authorization/credential errors and ledger errors stop immediately. A
nonzero status-command exit cannot reliably distinguish HTTP 403 from a transient
failure, so it and other unclassified failures conservatively stop polling rather
than being retried. No error-body text is used to guess authorization state.
Timeout omits cached records,
which could otherwise disclose an old intent after revocation during a wait;
external cancellation propagates. This is not a scheduled recovery worker and
does not survive a process restart.
Read-runner cancellation is normalized inside the timeout context so Python
3.12 retains its own deadline-versus-user cancellation accounting; concurrent
external cancellation is not swallowed. Status responses are checked again for
current read permission after ledger persistence, which can itself suspend.

A baseline attempt to run the real migration pipeline in a disposable temporary
SQLite database also failed on the missing `octop_harness` dependency
(`migrate.py` → agent profile settings → ACP settings). No real user database was
read or changed. This confirms that a fake ledger pass cannot substitute for the
repository's real migration/regression gate.

## Milestone ordering

1. **M0:** offline contracts, identity isolation, subprocess safety; then a
   specifically authorized disposable-tenant/binary contract test. The real
   service part is not completed by mock tests.
2. **M1:** authorized read-only connection, search/get/trace, citations, UI and
   native-memory regression coverage.
3. **M1b:** user-confirmed Octop file submission → Library upload → project import
   → indexing status → retrieval verification, with durable approval/reconciliation.
   This file loop is prioritized before generic memory writes and auto-sync.
4. **M2:** explicit fact/conversation writes with approval and uncertain-write
   handling. CLI changes, if needed, are separate owner-reviewed requests.
5. **M3:** opt-in incremental sync, durable outbox, stable IDs, revoke/pause/restart
   behavior. Default remains off; no historical bulk export.
6. **M4:** multi-user failure testing, staged rollout and rollback.

## Verification and environment limitation

Verification checkpoint (2026-11-01): **214 offline tests passed** with asyncio
debug enabled and `ResourceWarning` promoted to an error. Source compilation and
whitespace checks also passed. These results do not include the full project
test suite, real database migrations, a real CLI executable or remote services.

In addition to the lower-level smoke test, a composition regression now joins
the actual `SubmissionService`, fake transactional ledger, `MemoryLakeCliTransport`
and `MemoryLakeReadService` with a temporary Python fake CLI. It exercises
approval, private staging, upload, nonzero-but-accepted import, current-credential
read-only reconciliation after write opt-out/key rotation, and final retrieval.
It first exposed incompatible domain/transport lease phase names; the adapter
now uses the domain's `uploading`/`importing` phases. This demonstrates compatible
offline orchestration, **not** a production transaction, workspace adapter or
live connection. No actual remote file was uploaded.

New tests use standard-library `unittest` so the isolated contracts can run in
this dependency-empty checkout. They are also collectable by pytest when the
normal project dependencies are available. Each module was introduced with a
failing test run before its implementation. Tests use dummy IDs/keys and fake
processes only: no real service, file upload, credentials or CLI auth commands.
The test-only stdlib guard temporarily clears inherited `MEMORYLAKE_*` values
and rejects Python IPv4/IPv6 sends/connects and DNS. Generated Python fake CLIs
install the same network guard while retaining their explicit dummy session
environment. Patches and environment are restored after each test, including
failures. This is an accidental-network regression tripwire, not an OS sandbox
or a production security control.

```bash
PYTHONPATH=src UV_CACHE_DIR=/tmp/octop-uv-cache PYTHONASYNCIODEBUG=1 \
  uv run --no-project --offline python -W error::ResourceWarning -m unittest discover \
  -s tests/unit/connectors -p 'test_memorylake_*.py' -v

UV_CACHE_DIR=/tmp/octop-uv-cache UV_OFFLINE=1 make all
```

The second command was attempted but stopped at its first Ruff invocation:
locked `ruff==0.15.20` is absent from cache. Full lint, typecheck and pytest have
**not passed**. No dependency versions or lockfiles were changed to hide this.
The checkout has no installed `pytest`, `ruff`, `mypy`, `octop_harness`, or
frontend node_modules. Official dependency sources from the lockfiles are
`pypi.org`, `files.pythonhosted.org` and `registry.npmjs.org`; they are outside the
current network allowlist. Full verification requires an authorized dependency
installation and rerun, not a claim that the offline unit slice equals `make all`.
The user has approved locked project dependency installation; source-domain
permission is still pending. No alternate mirror, proxy bypass or unapproved
network installation has been attempted. Rust/Cargo are also unavailable, so
the pinned CLI's own bootstrap tests have not run in this environment.

### Latest handoff and recovery commands

On 2026-11-01, the strict offline command above was rerun before Git snapshot
preparation: **214 tests passed**. Compilation and whitespace checks passed;
an added-file scan found no private-key, common access-token or URL-embedded
credential patterns. Fixtures were reviewed as synthetic/dummy data. Pattern
scanning is not a guarantee that every possible secret format is detectable.

`make install-hooks` installed the repository's required `.githooks` path.
The actual offline hook target was then attempted:

```bash
UV_CACHE_DIR=/tmp/octop-uv-cache UV_OFFLINE=1 make precommit
```

It exited 2 before Ruff executed: resolving the locked environment required
uncached `lxml==6.1.1`, and offline mode forbids its download. No tracked source
file was reformatted. Neither this attempt nor the standalone tests constitute
a successful hook, full `make all`, dashboard build or production acceptance.

After the official dependency domains are permitted, resume with the already
authorized locked project install, without changing versions or switching mirrors:

```bash
UV_CACHE_DIR=/tmp/octop-uv-cache uv sync --locked
(cd dashboard && npm ci --cache /tmp/octop-npm-cache --no-audit --no-fund)
UV_CACHE_DIR=/tmp/octop-uv-cache make all
UV_CACHE_DIR=/tmp/octop-uv-cache make precommit
```

Review every resulting diff before staging. Then implement and verify the actual
identity/attachment adapters and transactional SQLite/PostgreSQL ledger through
the existing architecture, including migration, revocation and restart tests.
Only afterwards consider tool/API/UI registration. A real CLI executable and
specifically authorized disposable-tenant service tests remain separate gates;
the commands above neither authorize a real upload nor enable the connector.

## Potential CLI requests — not implemented, not blockers for offline work

- Supported API-key stdin/fd bootstrap to avoid reliance on private TOML format.
- Query/content file or stdin inputs so sensitive text need not appear in argv.
- Stable machine-readable error envelopes/exit categories.
- Documented upload/import idempotency and reconciliation guarantees.

No such CLI change may be made without owner confirmation. MCP v2 remains a
read-only comparison candidate, not a silent replacement for CLI integration.
