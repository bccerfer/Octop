# MemoryLake integration — development checkpoints

Status: **default-off file API and dashboard are now registered through the standard
Octop app; real SQLite, application lifecycle and fake-CLI HTTP compositions are
under regression. Not production-validated or enabled in this workspace.** Agent
read tools remain unregistered; GlobalProcessor still rejects the experimental
MemoryLake turn carrier. Existing native memory/history remain unchanged.

This document contains chronological checkpoints. The current product wiring
section below supersedes older statements that *all* HTTP/UI adapters are dormant;
those earlier sections describe tested building blocks, not today's registration
state. No real MemoryLake account, key or file has been used.

## WIP publication checkpoint (2026-11-01)

The user authorized committing this integration snapshot to the existing
`feature/octop-memorylake` branch, with a separate one-time authorization to skip
the local commit hook. The actual hook runs `make precommit` (format/lint/typecheck
and dynamically selected testmon tests), then a dashboard build; it is not the
literal `make all` command. That hook was not run for this snapshot. The previously
denied full regression remains unexecuted; publication does not mean the full
ship bar passed or that production Agent integration is ready.

Immediately before publication, the scoped Python command below passed **928
tests / 1010 subtests** in 72.19 s, and the five-file frontend command passed **43
tests**. Whole-tree Ruff/format and strict Mypy passed; changed frontend ESLint /
Prettier and `npm run build` (including tsc) passed. Existing dependency warnings
and Vite chunk-size / stale Browserslist notices remain. Local verification logs:
`/tmp/ml-precommit-python.log`, `/tmp/ml-precommit-frontend-tests.log`,
`/tmp/ml-precommit-frontend-build.log`. These temporary logs are not committed.

The reviewed scope is 157 source/test/document/config files, including migrations
025–027 and the already-locked direct `python-multipart==0.0.32` declaration. No
credentials, database files, generated dashboard bundle, logs, CLI repository
changes, tags, releases or production-enable changes are included. The ordinary
Agent and old-shared-agent compatibility gates described below remain in force.

## Latest ordinary-Agent checkpoint (2026-11-01)

User choice is **option 2: ordinary full-capability Agent conversations and native
retention**, not a dedicated isolated read-only turn. This is still development:
the production AgentManager/GlobalProcessor gate remains closed. Previously
shared agents need a separately chosen compatibility policy; neither a silent
owner-only conversion nor per-user workspace/memory migration has been applied.

The same explicitly scoped Python command listed under "Latest permitted product
checkpoint" now passes **928 tests / 1010 subtests**, 67.82 s. Log:
`/tmp/ml-agent-option2-joint-final.log`. Its 23 warnings are existing Harness model
assignment and LDAP/pyasn1 deprecations. This is not full pytest or `make all`.

- Real public HarnessAgent + fake model/runner covers native tools and MemoryLake
  in the same loop, checkpoint/history retention, and rejection of new reads
  without a current trusted scope. Native retained content is not retroactively
  erased when the MemoryLake connection is revoked.
- HITL HTTP entry now checks the actual thread owner before pending resolution,
  policy changes or streaming. Real graph HITL approval does not resurrect an
  expired MemoryLake grant. This does not assert all production resume/observer
  paths are integrated.
- Migration 027 records content-free native-admission markers. Admission and
  future sharing share a transaction order. SQLite migration/repair and two-
  connection interleavings are tested; PostgreSQL is SQL-contract-only.
- Reader and both dashboard factories require the shared database admission;
  missing/None fails before SQL/CLI. The old unbounded fallback was removed.
- Actual native memory capture/recall proves the unresolved old-viewer race:
  an already-running viewer may read a newly captured owner answer after
  unshare. Blocking a new `set_shared` does not drain that viewer. Publication,
  peer/export propagation and combined-PATCH atomicity remain separate gates.
- The dormant JWT producer now maps expiry from clocks captured before awaiting
  authentication. Its four-file producer/origin/delivery regression independently
  passed **81 tests / 82 subtests**; log `/tmp/ml-producer-clock-final.log`.

The first expanded run was **920 passed / 8 failed**: seven stale latest-schema
assertions still expected 26, and one earlier sharing-risk probe now correctly
hit the new guard. The probe now asserts rejection while retaining the separate
in-flight-viewer risk test. Seven related DB files independently showed **14
failed / 48 passed**, then **62 passed** after only their latest-version
expectations changed to 27; historical fixture watermarks and v26-specific tests
were preserved.

Read-only whole-tree Ruff/format (**1375 files**) and strict Mypy (**646 sources**)
passed. Worktree: **54 modified + 100 untracked paths**, branch
`feature/octop-memorylake`, HEAD `e897c123`; no new commit/push, CLI sibling clean.
The detailed policy and evidence matrix is in
[Agent integration gates](memorylake-agent-integration-gates.md).

## Current product wiring (2026-11-01)

- Standard `build_app` mounts `/api/memorylake` without bypassing native setup,
  JWT, bridge, CORS or exception middleware. Default-off `/status` reports
  unavailable; upload/credential endpoints reject before body consumption when
  deployment is unavailable. Native synchronous auth/cache/renewal remains
  explicit technical debt, not a whole-gateway bounded-latency guarantee.
- The same-origin dashboard page `/connectors/memorylake` supports an owner-only
  private expert binding, encrypted API-key storage, revisioned write opt-in,
  bounded **new file** staging, review of filename/size/SHA-256/target, separate
  explicit confirmation, local unsubmitted-snapshot discard, and read-only
  status/recovery. Document search/get use the same scoped HTTP/UI path and are
  covered by the newer checkpoint below; this does not register an Agent tool. No existing workspace
  attachment export, native-memory replacement, generic fact write or auto-sync.
- Review creates no approval and invokes no CLI. It returns a stable planned
  submission ID, retained by the UI before submission. Exact confirmation creates
  fixed-expiry consent and uses durable claim/step CAS. Duplicate confirmation
  reads the existing receipt; it never replays upload/import. Timeout/cancel is
  not proof that no write occurred. Unobservable `approved/uploading/uploaded/
  importing/unknown` phases return safe ledger status, not a blind retry.
- Current owner/JWT checks precede body parsing; final management disclosure
  rechecks current ownership. File delivery uses current read-reconciliation
  permissions, independently of write opt-out and valid credential rotation.
  JWT wall expiry is additionally mapped to an immutable pre-authentication
  monotonic anchor. Error bodies expose no key, raw CLI output or recovery evidence.
- Optional `MemoryLakeApplication` is owned by the actual server lifecycle, with
  shared file/DB/I/O admission and stop/drain before database closure. Standard
  config/Compose accept a trusted fixed CLI path, expected version and SHA-256.
  Default-off performs no artifact check; enabling without a verified artifact
  leaves native Octop startup usable. Hash verification precedes a fixed isolated
  `--version` probe. No binary download, sidecar or external service deployment.
- The UI's installed-dependency `npm run build` succeeded and emitted the new
  page chunk. Generated `src/octop/dashboard` artifacts are gitignored; source
  changes are part of this feature-branch snapshot. Tests use ASGI/real native middleware/SQLite/Fernet
  and fake CLI runners, not a live HTTP server/browser or real MemoryLake service.

**Still open:** real trusted CLI artifact/provenance and explicitly authorized
live-service validation; production AgentManager/GlobalProcessor turn wiring and
observer/HITL/final-sink authorization; identity delete/recreate ABA; live PG and
multi-worker behavior; hard DB-write/shutdown deadlines; proxy-wide buffering and
whole-server resource policies. Full `make all` is still safety-blocked below.
The presence of opt-in routes is not evidence these gates are complete.

## Latest read + file integration checkpoint

The subsequent combined run passed **892 tests / 987 subtests**, 53.37 s,
with only the same two dependency warnings. It used the exact scoped command
below (not full pytest or `make all`); log:
`/tmp/ml-product-read-joint.log`. The real optional Application → native
`build_app` → SQLite/Fernet → fake runner test now performs upload → explicit
confirmation → import → `okay` → search returning the same synthetic document ID.
No real CLI binary, model, account, external service or real file was used.

New API paths, within the unchanged native middleware stack:

- `POST /api/memorylake/agents/{agent}/bindings/{binding}/search`: strict JSON
  `query` and optional `top_k` only; fixed current workspace/projects, document
  type filter, shared read/DB/runtime admission and 30-second read deadline.
- `GET .../documents/{document_id}`: scoped document **metadata/status**, not a
  full-text download. Historical multi-project bindings are refused for this
  operation rather than guessing a project. Write opt-out does not remove read
  permission; current revocation and credential rotation are rechecked.

The frontend now has manual search and metadata viewing, with literal React
text rendering, no HTML/remote link rendering, and old-response rejection on
new request, scope change or unmount. File polling after user stop also rejects
late success. **43 focused frontend tests** passed, plus ESLint, Prettier, tsc
and `npm run build`; log:
`/tmp/octop-memorylake-dashboard-search-build.log`. The built ignored artifact
contains the read panel, planned recovery ID and local-discard UI. This is the
manual retrieval flow, **not Agent integration**. The user has selected ordinary
full-capability Agent conversations with native retention (option 2). Compatibility
for previously shared agents is a separate pending choice; no isolation or
owner-only migration is silently applied. The actual framework sinks are recorded in
[Agent integration gates](memorylake-agent-integration-gates.md).

Reproduce the focused frontend result from `dashboard/`:

```bash
npm test -- src/api/modules/memorylake.test.ts \
  src/pages/Agent/MemoryLake/index.test.tsx \
  src/pages/Agent/MemoryLake/ReadPanel.test.tsx \
  src/routes/controlAdminPath.test.ts src/layouts/sidebarNav.test.ts
npm run build
```

The final independent repeat is logged at `/tmp/ml-frontend-final.log`.


Read-only whole-tree checks: Ruff/format **1369 files**, strict Mypy **646 source
files**. Worktree at this checkpoint: **51 modified + 92 untracked paths** on
`feature/octop-memorylake` at `e897c123`, CLI sibling clean, no new commit/push.

### Search-result project provenance

The read-only CLI source declares `SearchDocument.project_id: Option<String>`
(`crates/core/src/api/search/types.rs`), while document-get's `Document` type
contains no project field (`crates/core/src/api/documents/types.rs`). Octop checks
an explicit returned project against the requested scope and rejects a mismatch;
**missing or null project fields cannot independently establish each hit's
membership**. Such results currently rely on MemoryLake correctly applying the
explicit request filters. `request_scope` describes the request, not an asserted
per-result provenance. The UI shows missing provenance as “service did not
provide”, rather than filling in the requested project.

Making every project field mandatory would fail closed but reject responses
allowed by the current CLI contract (and all normal metadata-only get responses).
A separate membership read would still trust the same remote service. No live
service contract test is authorized yet; this compatibility/security boundary
must be reviewed against the eventual approved artifact/service before enabling
production. Do not call it cryptographic or independent per-document authorization.

## Latest permitted product checkpoint

Before the subsequent HTTP document-search additions, the expanded targeted run
completed **882 passed / 979 subtests**, 55.44 s, with two existing LDAP/pyasn1
warnings. Log: `/tmp/ml-product-joint.log`. Exact command:

```bash
UV_CACHE_DIR=/tmp/octop-uv-cache uv run --locked --offline pytest \
  tests/unit/connectors/test_memorylake_*.py \
  tests/unit/db/test_memorylake*.py tests/unit/db/test_db_pool.py \
  tests/unit/gateway/test_preempt_stop_cancel.py \
  tests/unit/gateway/test_memorylake*.py tests/unit/api/test_memorylake*.py \
  tests/unit/test_memorylake_server_lifecycle.py \
  tests/unit/test_memorylake_deployment_config.py \
  tests/unit/test_memorylake_docker_config.py tests/unit/test_config.py \
  tests/unit/test_docker_compose_database_env.py \
  tests/unit/gateway/test_dashboard_ws.py \
  tests/unit/gateway/steer/test_coordinator.py -q -m 'not live' --tb=short
```

The old 625 route-walk subtests asserted *no* MemoryLake API declaration; they
were superseded by actual default-disabled route and native middleware tests.
The lower subtest count is not a full-suite regression claim. Ruff check and
format check passed (**1366 files**); strict Mypy passed (**645 source files**).
UI targeted tests passed **35 tests**, ESLint/tsc and installed-dependency
`npm run build` passed. Its log is
`/tmp/octop-memorylake-dashboard-discard-build.log`. These counts overlap
component tests and must not be added together.

Additional red/green evidence: 5 unobservable-phase HTTP subtests initially
returned 409; they now return current-read-authorized safe ledger receipts. A
real application/native-app worker-after-owner-revocation test exposed stale
binding metadata and now rejects. Both owner/file JWT scopes reject wall-clock
rollback even during their first SQL await. JSON body wait is now owned by
runtime admission and drain. Explicit DELETE of a local staged upload went
404→200 under read permission, including write opt-out; no approval/ledger or
remote deletion occurs. Success releases its one-shot source reference only
after the actual submit returns and final authorization; observing an in-flight
receipt does not release its bytes. Default source budget is now **128 MiB**,
with unchanged `3×20 MiB + 64 KiB` per multipart capture reservation. This is
shared process-local admission, not fairness or a per-user reservation guarantee.
Other owners may receive capacity errors under aggregate pressure; expiry,
explicit discard and successful-submit cleanup release stored references. They
do not erase external copies, revoke granted leases or undo sent requests.

At this checkpoint the worktree was **51 modified + 88 untracked** paths on
`feature/octop-memorylake`, still at `e897c123`. CLI sibling clean; no subsequent
commit or push. Further search and Agent integration work is not covered by the
882-test result and receives its own later checkpoint.

## Source baseline

- Octop: `890111b7879665840414ca28ab168b05b0db0921` (`main`, initially clean).
- WIP snapshot: `e897c123` on `feature/octop-memorylake`, pushed by the root
  task under the user’s publication authorization. Subsequent changes described
  below are not yet committed or pushed.
- MemoryLake CLI: `40243c2547d6185e163465524db8d739c4725243` (read-only reference).
- Both checkouts are local siblings. No CLI edit or release is included. The subsequent default-off Octop file API
  is registered as described above. CLI changes require separate owner approval.
- The user authorized publication of this **WIP snapshot** on
  `feature/octop-memorylake`, based on the current `main` because `origin` has no
  `develop` branch. They explicitly approved skipping the unavailable pre-commit
  gate for this one commit only. Normal repository hooks remain enabled; this is
  not a full-regression pass or approval to merge, release or deploy.

## Deployment responsibility

MemoryLake is an **external service**. This project does not deploy, operate or
configure that service; Octop integrates by invoking the MemoryLake CLI. Octop
still validates its allowed HTTPS endpoint, caller credentials and interface/error
contracts. CLI source changes require owner confirmation. This clarification is
not authorization to use a real account/key or upload real files during testing.
The current runner expects a pinned executable in the Octop environment. Use
Octop's existing standard installation, Docker image and Compose/start/config
paths, adding only necessary CLI adaptation. Do not introduce another deployment
system, resident CLI/MCP service or sidecar. Binary provisioning/verification and
safe per-process invocation remain Octop-side integration work. Multi-instance
operation is unverified; process-local snapshots/quotas must not be advertised
as distributed support. No image build or real CLI provisioning has been run.

## Component boundaries (historical implementation inventory)

| Module | Current responsibility | Not yet implemented |
| --- | --- | --- |
| `infra/connectors/memorylake/contract.py` | Typed identity/scope checks and fixed read argv planning | Production identity/permission loading or tool registration |
| `infra/connectors/memorylake/session.py` | Fresh POSIX 0700 profile directory, 0600 empty-key record, child-only environment; cancellation-safe off-loop setup/cleanup | Real CLI bootstrap verification, Windows ACLs, deployment isolation |
| `infra/connectors/memorylake/runner.py` | Bounded async POSIX execution, process-group cleanup, hidden partial results | Binary/version validation, per-connection production wiring, Windows process lifecycle |
| `infra/connectors/memorylake/results.py` | Scope/ID checks, explicit snippet field allowlists, bounded untrusted read results | Real-service response verification and transport-specific citations |
| `infra/connectors/memorylake/read_service.py` | Unregistered read executor; fresh authorization and atomic credential-revision binding before/after execution | Trusted harness caller adapter and production registration |
| `infra/connectors/memorylake/storage.py` | Unregistered `StoredMemoryLakeReader`; current owner-only DB snapshot, scoped caller construction and current credential loading | Trusted invocation/harness registration and deployment validation |
| `infra/connectors/memorylake/admission.py` | Server-owned, process-local bounded admission shared across per-turn readers; same connector serialized | Server lifecycle ownership, cross-worker quotas and queue deadlines |
| `infra/connectors/memorylake/langchain.py` | Five actual LangChain `StructuredTool` read schemas; required consumed origin identity, callback-free async execution and current dashboard DM thread checks | Authenticated per-turn lifecycle wiring, graph/model observer policy, localized presentation and production registration |
| `infra/connectors/memorylake/middleware.py` | Dormant real agent-ID-bound dynamic middleware and private turn scope; live identity/config consistency, old global-tool removal and before/after-handler checks | Actual AgentManager registration, complete production stream/HITL/observer policy and verified eager-only deployment configuration |
| `infra/connectors/memorylake/origin.py` | Dormant same-process, one-use dashboard origin registry; short-lived identity, SQL job quota, exact-message and current owner/expert/DM checks | Authenticated issuance endpoint, actual consumer lifecycle, SQL timeout/shutdown integration |
| `infra/connectors/memorylake/delivery.py` | Dormant bounded consumer-ready/producer-commit handshake, explicit handoff receipt and finally-closed consumer identity | Production ingress/channel registration, nonblocking WS ingress scheduling, actual graph observer/stream policy |
| `api/routers/chat/memorylake.py` | Unregistered per-turn JWT producer helper, current auth verification, isolated text-only input snapshot and shared authentication/handoff deadline | WS/HTTP registration, server-owned delivery lifecycle, JWT session revocation and production consumer integration |
| `infra/gateway/ws/memorylake.py` | Unregistered front-of-channel consumer, isolated execution snapshot, consumed-identity routing recheck, finally-closed scope and no typed steer/HITL continuations | Production registration, per-observer/sink authorization and full stream lifecycle policy |
| `infra/db/repos/memorylake.py`, migration 025 | Opt-in read bindings, scope CAS, single-query current identity checks, atomic credential revision and skipped-watermark schema reconciliation | Identity ABA epochs and live PostgreSQL verification |
| `infra/connectors/memorylake/submissions.py` | Unregistered submission orchestration, bounded read-only polling and approval/ledger/step-lease protocols, exercised with fakes | Approval UI, trusted source adapter, recovery worker and production storage lifecycle |
| `infra/db/repos/memorylake_submissions.py`, migration 026 | Real transactional approvals, write-policy CAS, claim/step leases and durable submission evidence; SQLite and recorded-PG migration checks | Identity ABA, retention policy and live PostgreSQL verification |
| `infra/connectors/memorylake/submission_storage.py` | Unregistered real-DB domain ledger, canonical intent encoding and cancellation evidence; bounded executor jobs | Actual server registration, write statement timeouts and production shutdown integration |
| `infra/gateway/ws/ws_hub.py`, native chat ingress | Server-issued user/agent socket references, registration-generation fanout checks and shutdown/reboot invalidation | MemoryLake ingress registration and authorization at every downstream sink |
| `infra/connectors/memorylake/write_binding.py` | Current single-SQL read permission/write policy loader; explicit policy revision in approved intent | Production factory, identity ABA and credential/transport composition |
| `infra/connectors/memorylake/credentials.py` | Fixed-caller credential loader with shared actual-worker quota, atomic cipher/master snapshot and final revision/permission/expiry checks | Real credential use, deployed binary verification and production transport factory |
| `infra/connectors/memorylake/file_binding.py` | Trusted fixed-binding composition of current loader, scoped source, real ledger and injected transport; operation ownership and final read disclosure check | Authenticated production factory/confirmation UI, credentials and real CLI deployment |
| `infra/connectors/memorylake/file_runtime.py` | Dormant server resource owner: shared source/DB/IO admission, bounded operations and ordered stop/drain | Automatic composition, global/multi-worker policy and authenticated submission/approval endpoints |
| `infra/connectors/memorylake/upload_io.py` | Required shared original-file identity admission; stop-new/holder-drain protocol | Automatic ingress composition and coordination with every external file owner |
| `infra/connectors/memorylake/upload_source.py`, `api/routers/memorylake_upload.py` | New-byte-only bounded source, UploadFile capture and unregistered JWT/request-to-scoped-snapshot composition | Real build_app middleware integration, approval UI, upstream body limits, cross-worker routing and retention/lifecycle |
| `api/common/memorylake_multipart.py` | Public python-multipart callback parser, strict one-file header subset, bounded body and complete closing-boundary check | Production registration, proxy/ASGI buffering quotas and final HTTP response delivery authorization |
| `infra/connectors/memorylake/cli_transport.py` | Unregistered bounded CLI file transport with private staging and lease checks | Real binary/source validation and production transport registration |
| `infra/connectors/memorylake/documents.py` | Upload/import/document response classification; keep known IDs across uncertain outcomes | Real-service schema and indexing behavior verification |

The runner preserves bounded stdout/stderr for nonzero exits and in a hidden
`partial_result` on timeout/output overflow and typed cancellation. Error strings never include argv,
environment or raw output. Raw result bytes remain internal and sensitive.
The timeout begins after obtaining the instance concurrency slot. The LangChain
factory now requires an explicit shared `ReadAdmission`; production must supply
one server-owned object, not create one per turn. It permits one active read per
connector, at most eight active connectors and 32 queued calls by default, and
releases capacity on cancellation. It retains no caller tools or credentials.
Limits are process-local; FIFO, queue deadlines and cross-worker quotas are not
implemented. The lower-level service's optional standalone fallback does not
provide cross-turn coordination.

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

### Real framework adapter, still dormant

`build_dashboard_read_tools()` receives caller, agent, thread, binding and connector
IDs plus a required consumed `DashboardTurnIdentity` from a future **authenticated
server call site**, never from model arguments or `RunnableConfig`. Tools must be
built for that invocation, not cached on a shared
agent. The injected runtime context is only a consistency check, not proof of
authentication or a reusable capability. Current persisted thread ownership,
dashboard channel and exact DM session key are checked in the **same SQL snapshot**
as live user/agent/connector permissions, binding and credential revision, including
before CLI launch and after cleanup. No separate awaited thread lookup follows
the final authorization snapshot. This defines a check point, not a promise to
recall a remote request or prevent a later independent database commit.

This restriction is deliberate: existing IM request handling may map a platform
sender to the agent owner, and harness `ChatRequest.configurable` can override
base user/source fields. Neither establishes MemoryLake consent. The adapter
rejects IM, CLI, MCP, cron, group/peer/team thread contexts, shared agents and
context mismatches. It does not register itself in any of those paths.

Real `ChatRequest` and LangChain invocation tests cover strict schemas, injected
config exclusion, native `ToolCall`→`ToolMessage`, current SQLite/Fernet state,
revocation, rotation, scope rejection and cancellation. A framework-generated
subset schema initially dropped `extra="forbid"`; this adapter preserves its
explicit argument schema for both model export and runtime validation. Error
bodies are not forwarded, only machine codes pending localized presentation.
Tests stub the CLI runner, not the harness, LangChain, database or cryptography.
The 33 direct adapter tests exercise `ChatRequest.to_runnable_config()` and actual
`tool.ainvoke()`, not a complete authenticated dashboard turn or HITL lifecycle. A separate nine-test
probe now runs real `langchain.create_agent`/LangGraph and `ToolNode` with a fake
model, real SQLite/crypto/read adapters, and a **test-only** dynamic middleware
shim. It proves dynamic tool execution with an empty global tool map, constant
errors for forged arguments, no tools on a following unscoped invocation, private
session cleanup on cancellation, and no key/tool/scope objects in the inspected
checkpoints. One probe additionally joins the real accepted gateway queue path,
origin consumption, tool factory and graph; its consumer was created before
issuance and gets no producer ContextVar. Issuance still uses manually trusted
test inputs, and the consumer/middleware are test-only. This is not an authenticated
Octop `HarnessAgent` stream, QueueFull acknowledgement, production middleware or
proof of HITL/peer/Ask/Plan authorization. The test initially put
its observation in `bind_tools`; real graph execution showed that empty-tool
turns skip that call, so the observation was moved to actual model generation.

The adapter deliberately does **not** run BaseTool's callback lifecycle. Three
red regressions showed: `on_tool_start` received a query before an inactive-origin
check, a scheduled close between coroutine completion and the BaseTool parent
still exposed a result, and closing while `on_tool_end` was suspended still
returned success. Its dedicated async `arun` now creates no callback manager,
emits no tool start/end/error events, retains child-task cancellation, and checks
identity after child completion and synchronously before returning. Synchronous
invocation fails closed. Successful calls also assert no callback manager is
constructed; graph composition checks the scheduled-close case leaves neither
the final result nor inspected checkpoints with a document result. No framework
package, global tracing setting or previously denied regression was changed.
Tool-level tracing/metrics via those callbacks are intentionally unavailable.
Two further tests first reproduced swallowed `GraphBubbleUp`/`GraphInterrupt`
signals at both wrappers; those framework control signals now propagate unchanged.
This does not authorize HITL, parent-command or independent resume behavior.

This is a **tool-return checkpoint**, not atomic authorization of the entire
graph. ToolNode can gather multiple results, then graph/model callbacks and
checkpoint writes can await later. A previously valid returned result cannot
be recalled by closing the identity. Graph/model tracing and the final
consumer/checkpoint disclosure policy remain production gates; tool callback
suppression does not suppress those other observers or authorize external tracing.

Installed harness 1.0.5 has no per-turn `tools` request field. Its append/inject
helpers mutate the agent-wide tool inventory and must not be used for these
closures. The new `MemoryLakeReadMiddleware` uses real model/tool-call overrides
without registration in AgentManager. It is constructed against the server's
actual compiled agent ID, not the agent ID claimed in config. The private
`dashboard_read_turn` context builds tools from a minted identity and server-chosen
binding/connector, and closes that identity **before** resetting its ContextVar.
The context must enclose complete stream consumption, not iterator creation.
Nested replacement is rejected; inherited child contexts retain the same closed
identity after exit. No scope/tools/credentials are written to graph state or
`configurable`, which can be copied for HITL recovery.

The middleware strips all old `memorylake_*` BaseTools/function schemas before
selecting the current turn's five reads, and overrides tool dispatch rather than
trusting a global old caller closure. Unknown writes and synchronous MemoryLake
execution are rejected. A genuinely absent scope preserves native tools. A
present but expired/closed/mismatched scope **stops model execution**: review
reproduced that dropping tools but continuing a native model could still expose
an earlier MemoryLake ToolMessage. Both async and sync paths now check before
the handler and before returning; tool invalidation returns a constant error.
Unexpected Command outputs are not accepted as read results. Framework control
exceptions and actual caller task cancellation are not swallowed. Tests distinguish
LangGraph's conversion of a node-raised CancelledError to NodeCancelledError from
real external Task.cancel; the framework itself was not changed.

Runtime mode is also checked: only default/Craft is supported. Ask, Plan and
unknown modes fail closed even if configurable `conversation_mode_extra_read_tools`
lists MemoryLake names. A later Ask/Plan allowance or independent resume needs
explicit lifecycle verification, not a config-based grant. These are middleware
return checkpoints, not proof that graph/model callbacks or historical data are
recalled after revocation. Actual producer authentication, full stream consumption,
history/trajectory flushes, session auto-resume and observer policy remain gates.

Real harness ToolSearch client deferral was separately reproduced: it replaces
the strict input model with `_DeferredToolInput` through `model_copy`. Two
regressions first reached the model handler or a late schema TypeError. The tool
now rejects schema substitution immediately with a body-free unsupported error;
metadata/same-schema copies and the tested eager path preserve strict validation.
This **does not cover native deferral or a client tool already marked loaded**,
which avoid that replacement. Production must explicitly keep MemoryLake eager
and verify that actual configuration at registration; neither provider-native
search nor schema registration is claimed as tested or enabled.

Twenty-two middleware tests combine real graph and direct-handler probes with
actual SQLite/Fernet and a fake CLI. Two further compositions within that count
join the public ChannelManager/WebSocketChannel lifecycle, SQLite-backed origin
and delivery, the new turn scope and agent-bound middleware, and the real graph.
They use no old dynamic-tool shim or separate test permission ContextVar. One
verifies receipt/result and identity closure; the other stops the real manager
while the fake CLI is blocked and verifies cancellation, no delivered document,
closed identity and private-directory cleanup. The issuer's IDs and outer
processor wiring is still a test fixture, not the production JWT route
or GlobalProcessor. The two compositions now use the actual unregistered
MemoryLakeWebSocketChannel rather than a test channel subclass. Actual
producer/stream/observer registration remains unverified.

### Short-lived dashboard origin, still not registered

`DashboardOriginRegistry` reserves an opaque token against the exact original
`DashboardInbound` before awaited authorization SQL. Only that same object can
be consumed, once, on the registry's event loop; JSON, copies, replay and
premature consumption fail closed. Default lifetime is 60 seconds (maximum
300), with bounded pending issuance/consumption. Both issuance and consumption
check current owner-only enabled user/expert and exact dashboard DM thread.
Team hosts, unknown agent kinds, group markers in either subject or metadata,
impersonation, peer targeting and Ask/Plan are rejected by this initial policy.
No supplied message metadata is authentication evidence.

The consumed identity is non-copyable/non-serializable and bound to caller,
agent, thread and original expiry. A direct-construction regression first showed
that matching the Python type did not establish consumption. `check_current`
now also requires the registry's internal mint marker. This is a trusted-process
misuse guard, **not** a cryptographic capability or defense against malicious code
already running in that process. No externally serialized identity is accepted.
A future consumer must `close()` in `finally`
before clearing invocation context; inherited child tasks retain the same
invalidated object. Tools require this identity and recheck it around storage
awaits, before CLI launch, after queued admission, and before tool return. Closing
it does not wake a queued reader immediately or cancel an already-started CLI;
queue deadline/cancellation integration remains open. It cannot retract a
remote request, replace live binding/key checks, or establish JWT logout/session
revocation and identity disable/re-enable ABA history.

Event-controlled red/green tests cover revocation during issue/consume,
premature consumption, concurrent issuance capacity and message mutation.
Cancelled/revoked/expired awaits can leave executor SQL running, so a **separate
outstanding authorization-job quota** remains held until the shielded Future
actually completes. Late exceptions are retrieved without logging sensitive
bodies. Consumption cancellation also retains the quota. These bounds are not
a SQL execution timeout or shutdown/drain implementation; a future owner must
finish jobs before closing its database pool.

A later joint run exposed two quota-test failures. A deterministic regression
showed that a very fast executor Future can already be done before its completion
callback runs; awaiting `shield(done_future)` need not yield. Admission now reaps
only actually done Futures synchronously as well as in the callback. Still-running
SQL retains its slot. This fixes false capacity rejection without weakening the
cancel/revoke/expiry resource bound.

### JWT producer prototype and actual processor gate

The existing dashboard WS route authenticates its connection once. The new,
**unregistered** `dispatch_memorylake_dashboard_turn` helper instead verifies the
token on each attempted turn using the existing `resolve_user_from_token` and
`_decode` functions/current secret. It accepts no caller/user/impersonation input;
the signed subject is authoritative, while the registry's live SQL rejects
disabled users, other owners, shared/disabled/non-expert agents and wrong threads.
Cached UserManager state or an admin role is not an owner-permission bypass.
No token is copied into message metadata, runnable config or the handoff receipt.

Interface tests exercise real synthetic HS256 JWTs, a booted UserManager,
temporary SQLite repositories, origin issuance and delivery to a local test
consumer. They cover bad signatures, expiry, missing `exp`, invalid subjects,
cached-but-disabled users, signed actor hints, key rotation between dispatches,
media/HITL/peer/steer rejection, and cancellation/timeout while secret lookup is
still running. The last composition uses the new source channel adapter and real
GlobalProcessor: it receives a handoff receipt but the processor still rejects,
with zero processor repo/metrics/stream/CLI calls and a failed channel end.
That composition is not a successful production turn. Separate actual chat-route tests now cover server-issued connection registration; the helper itself remains unregistered.

Authentication and SQL share the registry's outstanding executor-job quota.
One deadline encloses both JWT verification and the consumer handoff. A timed-out
or cancelled await does not release a still-running auth job or stop its thread.
The auth callback's exception text is never returned. Invalid timeout values
are rejected before starting auth work. SQL timeouts and lifecycle drain remain
unimplemented; the server must not close the pool while such jobs still run.

JWT `exp` caps the registry's normal TTL using both an absolute wall-clock bound
and a monotonic deadline fixed at issuance. Wall-clock rollback cannot extend
the monotonic cap; once a claimed identity observes expiry it is permanently
closed, so a later rollback cannot revive it. Tests first exposed this revival
and the missing auth deadline before the fixes. They also exposed mutations to
prepared media or HITL policy during the auth await; the helper now deep-copies
and validates the input before its first await. Caller-owned mutation cannot
change the emitted snapshot. Key rotation is checked for the next dispatch,
**not** promised as immediate revocation of an already accepted turn. JWT
logout/session revocation and identity ABA remain production gates.

Both actual `GlobalProcessor.__call__` and `iter_turn_chunks` now reject
`DashboardInbound` and its subclasses before metrics, owner fallback, history,
trajectory, HITL or AgentManager execution. Four direct-entry regressions first
failed because metrics ran before rejection; two native InboundMessage cases
continue through the existing dashboard and IM pipelines. This guard is a hard
production gate, not a registered consumer or a path to successful processing.
**It does not establish a side-effect-free gateway:** the real WebSocketChannel
can persist media, track/rewrite subjects, subscribe and emit `turn.started`
before it calls the processor. A seventh real channel-to-processor test confirms
subject changes, subscription and `turn.started` precede rejection; the channel
then emits an error and failed `turn.ended`. It uses synthetic text with no media
backend: earlier media persistence is source evidence, not a tested file write.
An eventual authenticated consumer must enclose
that earlier boundary as well. No producer route or AgentManager middleware is
enabled by these changes.

### Unregistered front-of-channel consumer adapter

`MemoryLakeWebSocketChannel` now supplies that earlier wrapper as dormant source
code. Only typed MemoryLake messages enter `delivery.receive`, then a server-owned
scope factory; ordinary InboundMessage calls use the unchanged native path.
Missing, subclass, revoked or late origins fail before base parsing/media/subject
tracking/hub calls. Normal completion, errors and cancellation close identity
**before** scope exit/reset; scope factory/entry failure is covered by the
delivery context's finalizer. Typed error bodies are not returned by the wrapper.

Ten channel tests cover this boundary, including text-only media/HITL/attachment
rejection after awaited handoff. A later review reproduced routing changes after
origin consumption but before producer commit resumed the consumer: seven routing
variants and a blocked-media external mutation initially failed. The consumer now
makes a private deep execution copy after that final await, drops its one-use
origin token, retains the DashboardInbound carrier, and checks the complete shape
against the consumed identity before entering base code. It never consumes the
copy again. Base code may mutate its private routing object; external producer/
queue aliases no longer control it. This is isolation from external mutation,
not a frozen object or a defense against malicious trusted processor code.

An additional failing connection-id case led to binding `ws_connection_id` at
issuance and checking it on consumption and the execution snapshot. A string alone
is not proof of socket ownership. The producer now requires a server-issued
`ChatConnectionRef`, checked against the JWT subject, agent, current hub and exact
registration generation. Its source check is retained by the origin and consumed
identity; disconnect, replacement or hub shutdown invalidates later checks.

The actual native chat route registers that reference after its existing JWT
handshake; it still enqueues ordinary native turns, not MemoryLake turns. Generic
or notification registrations cannot mint it, and trusted chat registration does
not subscribe the chat socket to notification-only user fanout. Actual gateway
boot/shutdown/boot tests verify a new hub and new channel binding, while old
references stay invalid. Fanout snapshots include registration generation and
callback: same-ID re-registration cannot redirect an old thread/user frame to a
new user, including when the same callback is reused. These checks do not retract
an already-started send, validate every broadcast recipient's DB permissions or
supply per-output observer authorization.

The native channel has a per-message continuation policy hook, defaulting to its
original behavior. Only the new adapter's typed turns disable steer preemption
and continuation, discard pending steers through the real coordinator, and reject
`hitl_required` before publishing/parking it. Three coordinator/channel tests first
exposed an ordinary InboundMessage continuation inheriting the active typed scope,
and an unsupported HITL frame reaching the hub; both are now rejected, while the
native continuation case is unchanged. No shared coordinator is swapped per task.
This prevents initial typed grants from extending to reconstructed continuations;
it does not authorize a future independent resume path.

Production remains closed. In particular, the base channel catches Exception
itself, so the wrapper's GraphBubbleUp branch does **not** establish end-to-end
GraphInterrupt propagation. Per-output observer/checkpoint/stream reauthorization
and already-emitted-frame semantics remain unresolved. The real GlobalProcessor
still rejects every typed envelope, including the private execution copy.

### Consumer-acknowledged handoff, still not registered

The installed gateway only logs QueueFull and returns no enqueue success/failure
receipt; even its normal public `enqueue` return can precede the scheduled put.
Its pre-lock hook cannot authorize execution: hook exceptions are swallowed and
the channel handler still runs. No dependency or private queue was changed.

`DashboardDelivery` now supplies a different, explicitly **consumer-acknowledged
handoff**, not an immediate enqueue receipt. It reserves a bounded entry, issues
origin and calls public enqueue. The exact-message consumer redeems origin and
signals ready, but cannot enter its body until the ingress synchronously commits
and returns an immutable `accepted` receipt. The linearization point contains no
await. A receipt proves that handoff only, not processing success or CLI execution.
Before commit, timeout/cancellation/failure burns the pending origin and closes
any newly claimed identity; late queue delivery is rejected and never retried.
After commit, consumer `finally` owns closing the identity. Accepted turns keep
their inflight slot until consumer exit. `close()` also invalidates active claims
and wakes waiters, but does not cancel arbitrary bodies or drain SQL jobs.

Nineteen tests use the real ChannelManager public start/enqueue/stop and, where
appropriate, WebSocketChannel. A **test-only** channel subclass opens `receive`
before media persistence/subject rewriting and closes identity before resetting
its ContextVar. Cases cover full/missing/no-ack queues, capacity, duplicates,
sender and consumer cancellation, actual manager.stop, claim-before-commit
cancellation/expiry, and blocked consumption SQL retaining its executor quota.
The origin registry is real; these delivery tests use a permissive SQL-identity
stub, while separate repository/origin tests cover actual SQLite authorization.
No test interprets queue size, log text or enqueue return as success.
Cancelling the producer's continuation after acceptance does not revoke the
consumer-owned identity. Stopping the real manager before an enqueued turn is
claimed yields no receipt; the bounded acknowledgement deadline revokes it.
That is not an assertion that manager.stop directly notifies this dormant helper.

Review then reproduced a pre-consumer side effect: the existing real Gateway
pre-lock hook could still cancel an active stream for `/stop`/`/cancel` in a
mutated or revoked envelope. Origin issuance/consumption now reject those control
commands. The existing hook also refuses **all DashboardInbound envelopes,
including subclasses**, before attempting control processing, so enqueue-time
mutation or late delivery cannot bypass the exclusion. Ordinary native
InboundMessage stop/cancel behavior is preserved and directly regression-tested.
This is a fail-closed guard in the existing hook, not registration of a trusted
MemoryLake consumer. Do not generalize it to all possible pre-lock/interrupt
callbacks or claim an uncommitted queued message has no effects of any kind.

Another red regression supplied an enqueue callback that raised
`TimeoutError` with a synthetic secret. Dispatch now maps that and genuine ack
deadline failures to a constant body-free delivery error instead of returning
the callback's exception text. Timeout/cancellation still never imply that an
already committed turn or remote read was rolled back.

These helpers are still unregistered. Production must place the consumer context
at the handler entry, retain normal non-MemoryLake behavior, and define bounded
ingress scheduling so awaiting a receipt does not stall WS stop/steer messages.
The timeout is cooperative, not a hard interruption of blocking synchronous
enqueue code. Missing ack intentionally cannot distinguish queue overflow,
unavailable channel, lock wait or shutdown. Authenticated ingress, actual stream
lifetime, independent-resume and observer/checkpoint policies remain gates.

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

Server-built requests in `infra/gateway/process/harness_request.py` and
LangGraph's `configurable.user` carry useful consistency fields, but do not
establish trusted origin: configurable fields can be overridden or copied.
The dormant origin registry above is not yet connected to authenticated ingress
or the real consumer lifecycle. Neither existing browser-profile conventions nor
successful graph tests authorize registering MemoryLake tools globally.

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

## File-submission persistence boundary (real DB adapter, still unregistered)

The file domain uses an immutable byte snapshot, an opaque attachment ID, a
SHA-256 hash, size and filename. A trusted UI must approve the complete intent:
caller/agent, connector, workspace/project, Library parent, deployment endpoint,
content snapshot and permission/credential revisions. An arbitrary model boolean
or a connector API key is not approval. Snapshots are currently limited to
1 byte–20 MiB; this does not replace source-side bounded reads, MIME checks or scanning.

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

Migration 026 and `MemoryLakeSubmissionRepo` now implement the ledger on the
existing pool/RepoBundle architecture. SQLite uses `BEGIN IMMEDIATE`; PostgreSQL
uses fixed-order row locks and rechecks the live permission snapshot and finite
trusted time after locking. A default-disabled write policy, exact unexpired
approval, single claim, revision/phase CAS and bounded step grant are persisted.
Independent SQLite pools exercise competing claims. PostgreSQL has recorded-SQL
contract coverage, not a live-server result.

`DatabaseSubmissionLedger` maps canonical versioned intent JSON to the domain,
rejects disagreement between JSON and indexed columns, and does not store file
bytes or credentials. Mutation cancellation waits for the actual executor Future,
then attempts to persist `unknown`; a failed recovery CAS carries typed safe
proposed evidence without claiming it was committed. Service-level catches now
preserve that evidence rather than replacing it with an old record. Reads can
return promptly on cancellation or polling deadline, while their actual Future
continues to occupy the admission quota until completion; late exceptions are
observed without logging sensitive bodies. A `DatabaseJobAdmission` is now a required shared constructor dependency:
new ledger instances cannot silently allocate separate quotas. Actual future
completion, not caller cancellation, releases a job; done futures are also
reclaimed synchronously when their callback has not yet run. The production
factory must reuse a server-owned admission; cross-worker quotas, write DB
lock/statement timeouts and shutdown drain still require production wiring.

Submission/approval history deliberately keeps scalar resource IDs, not resource
foreign keys that would erase uncertain-write evidence or block native deletion.
Missing live resources deny new grants. Retention/user-deletion policy and
same-ID delete/recreate or disable/re-enable ABA remain unresolved production
requirements. This implementation must not be enabled before those decisions.

Both normal v25→v26 migration and ahead-watermark repair execute idempotent table
DDL and the conditional version update in one transaction. Real run_migrations
checks reproduce partial tables, interrupted DDL rollback, watermark 26/27 with
missing tables and preservation of in-flight phases/IDs/revisions. Repair creates
missing tables; it does not promise to reconstruct malformed existing columns.

Migration 025 now adds `connectors.credential_revision`: existing non-empty
credential blobs start at 1, otherwise 0; `upsert_credentials` increments the
revision in the same UPDATE as the encrypted blob. The read-binding table has
its own CAS authorization revision. Timestamps remain unsuitable as revisions.
These additions are **not** a write ledger or identity-history/ABA guarantee.
The read snapshot rejects disabled users, shared/disabled agents, shared/inactive
connectors, wrong connector kinds and cross-owner access. Binding configuration
starts disabled. Scope/endpoint validation is performed by the higher-level
adapter, not inferred from arbitrary connector configuration.

The real temporary-SQLite migration/repository tests cover v24→v25, repeated
migration and lowered-watermark replay without resetting revisions, partial
legacy schemas, CAS conflicts and visibility across independent pools. The
SQLite helper runs inspection and DDL in one `BEGIN IMMEDIATE` transaction.
Partial legacy databases lacking connectors receive the binding-table declaration
but no fabricated connector table or rows; they cannot provide an authorized
binding. Read bindings use cascading deletion and must not be reused unchanged
for durable unknown-write evidence. PostgreSQL has a matching SQL migration,
checked statically only; no live PostgreSQL test has been run.

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

The internal `poll_status` helper performs only read-only remote observations
with explicit check-count, requested total-time and interval bounds. **The total
time is not a hard wall-clock bound across durable ledger writes.** A real-adapter
characterization blocks `save` after a query: a 30 ms deadline remains pending
until the actual DB write completes; the row may become `ready` while the caller
then receives `timeout` with no cached record. Mutation futures are deliberately
not abandoned. A production hard bound requires server-owned bounded background
settlement, durable recovery and shutdown drain, or bounded DB lock/statement
execution. None of those is supplied by the read-cancellation fix. Every observation rechecks
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

Earlier migration/import attempts were blocked by missing `octop_harness` and
`langchain_core`. Locked dependency installation has now resolved that environment
blocker, and real disposable SQLite tests pass. No real user database was read or
changed. Production ledger lifecycle/ABA and attachment-source gates remain open.

## New-upload candidate and real write-binding loader (not registered)

`StoredWriteBindingLoader` uses the same current owner/expert/active/nonshared
identity query as the reader, adding the write policy in that **same SQL**. It
uses the shared database admission, reads no encrypted blob/master secret, and
checks the invocation before and after the await. The pure connection validator
is shared with the reader rather than duplicating scope/endpoint parsing.
A missing or disabled write policy cannot remove otherwise valid read-only
reconciliation. It produces no usable write target; status checks substitute only
the original approved project after checking that it remains in the current read
scope. The returned immutable binding includes the write-policy revision. A real SQLite
regression first accepted an old reviewed intent after a same-target policy
revision change; the revision now travels through the intent and is checked both
when approval is created and in the atomic claim/step transactions. Canonical
intent format is explicitly version **2**. Old experimental version-1 records are
not silently upgraded into a new authorization; domain loading rejects them while
raw rows and known item/document IDs remain intact. No user database was upgraded
in this work. If external hand-created v1 records exist, a separate reviewed
read-only recovery/migration path is required, not deletion or re-upload.
A snapshot returned by this loader is not itself a write lease.

`EphemeralUploadSource` is a server-only candidate for **new supplied bytes**,
not a reader of existing attachments. It accepts no path, URL, `as_user` or
workspace alias; it does not call `save_attachment` or use native file storage.
It binds owner/agent/revision and an opaque random ID, with repeat reads for
prepare→submit. The immutable filename, byte length and SHA-256 hash become part
of the exact approved intent. Capture does not create approval or contact a
remote service. The current combination joins this source, the real loader,
real SQLite ledger and service with a fake transport. A separate thin
`api/routers/memorylake_upload.py` helper now exercises actual FastAPI `UploadFile`
objects through the existing bounded reader. It has no router/decorator and does
not authenticate a request by itself: server-owned source/caller and fresh owner
SQL authorization remain required. Neither combination is a registered HTTP
endpoint, an approval UI or a real CLI/service validation.

A server-only `bind(binding_id, connector_id, caller, authorize)` view now places
entries in a fixed binding/connector namespace over **the same store**. Equal
owner/agent/authorization revision does not make two bindings interchangeable:
cross-binding, cross-connector and raw/scoped reads reject before authorization
or returning bytes. A view fixes its caller and uses its own current DB loader on
every access; closing it invalidates that view before and after awaited auth,
without closing another view or the whole store. An event-controlled regression
first found pending-read cancellation/closure also deleted the shared entry; this
now stops only the view/read and keeps that upload charged until TTL, explicit
revoke or store close. Other views must still reauthorize. Non-lifecycle failed
authorization retains the conservative shared-entry invalidation behavior, not a
promise of retryable retention after every error. These objects are trusted wiring,
not user-facing capabilities. A future factory must prove the supplied authorizer
belongs to the same binding; merely constructing a view does not do so or solve
resource-ID delete/recreate ABA.

The two-binding real-SQLite composition uses one runtime/source/DB admission,
separate current loaders and ledgers, and fake transports. It verifies no cross-ID
prepare/submit or borrowed approval, independent revocation, and that cancelling
binding A's auth wait does not free its actual DB job for B until the worker ends.
Byte/file/capture/pending-read budgets remain shared across views. The actual
UploadFile adapter also accepts a scoped view only from its own runtime; invalid,
closed, wrong-caller or foreign views cannot fall back to the raw prototype path.
Two scoped real files share one I/O pool/admission; closing a view during its
blocked rolled-file read prevents late publication, without pretending to stop
the worker. Runtime operations retain their original stop/drain ownership.

The candidate is process-local: default TTL 60 s (maximum 300 s), per-file maximum
20 MiB, original retained/pending-add budget 64 MiB (now 128 MiB, see
current checkpoint) and 64 entries, with explicit
close/revoke and expiry cleanup. These are prototype limits, not approved
production retention policy. Capture reserves one entry and **twice the per-file limit before reading**,
covering trusted reader chunk assembly and joining. It authorizes before any body
read and again afterwards; denied/oversized/cancelled/late results publish no ID.
Read authorization is separately bounded and carries only a generation nonce and
expiry across await, not old file bytes: weakref tests first reproduced expired
entries being retained by blocked reads. Authorization/read waits use the remaining
TTL; cancellation-ignoring callbacks keep their reservation until they actually
exit and cannot publish a late result. This does not force-stop such callbacks.
Store and executor admission must be server-owned
and reused across requests; creating one per request defeats global limits and
opaque-ID routing. Restart loses bytes and requires a new upload and approval;
never replay an uncertain remote upload. Clearing references does not promise
memory zeroization or invalidate a byte snapshot already returned to its caller.

Source expiry/revoke/close is **not withdrawal of database approval** and cannot
recall a granted lease or remote request. An eventual user cancellation must
revoke consent in the ledger as well as release local bytes; already-granted
steps may still complete. Submission concurrency must also bound snapshots held
outside the source. `read_upload_capped` and the new capture reservation bound
trusted handler-side assembly, not pre-handler multipart reception/temp files or
arbitrary callback allocations. Real HTTP/JWT routing and automatic runtime
construction remain unconnected; the explicit stop hook is described below. Different approvals can intentionally submit the same bytes again:
only per-approval claim uniqueness is provided, not file-level exactly-once.
A future confirmation API needs its own idempotency contract.

Actual rolled-to-disk UploadFile reads introduce a separate worker boundary.
Cancelling the asyncio request initially released capture quota while Starlette's
file-read thread kept running. Both cancellation and TTL deadline regressions
first failed with that early release. The first shielded-child fix still failed controlled shutdown cancel-all:
cancelling that intermediate child could mark it done while its hidden worker
continued. The adapter now owns a `run_in_executor` Future for the public
`UploadFile.file.read` method instead of a Starlette child Task. It shields and
waits for actual Future completion, then propagates cancellation without starting
another chunk. Real rolled SpooledTemporaryFile tests cover repeated cancellation,
controlled cancellation of the entire capture-task subtree, stop-admission then
drain, admission denial until settlement and no file close while read is running. This deliberately has **no hard I/O wall-clock bound**;
shutdown must drain these jobs before closing files/stopping the event loop.
The helper now additionally requires a server-owned `UploadReadAdmission`.
It claims the original file object's identity before its first await, across the
whole capture and actual executor settlement. Separate UploadFile wrappers and
sources cannot concurrently read the same file through this shared admission;
that real-file regression first observed two competing reads and now rejects the
second. A bounded holder count, close-to-stop-new and async drain-to-wait are
provided, with a controlled stop-source/stop-admission/drain/close-file test.
This is still a protocol, not real server registration. Unrelated code must not
seek, read or close the same file outside this owner. Both in-memory and rolled
file reads now use the admission's **owned ThreadPoolExecutor** (default two
workers), not the default DB/auth executor. Actual per-file jobs stay accounted
for even if a holder is incorrectly released early; cancellation-isolated Future
views cannot release that actual-work quota. Close joins the owned pool off-loop,
using one shared actual shutdown Future rather than a hidden cancellable Task.
Controlled cancellation of all close Tasks originally reproduced premature
completion; the corrected close waits before propagating cancellation.
A future production endpoint separately needs pre-handler total body size, file
and part counts, reception time and disk-spool quotas. Handler-side quotas do not
prevent an unauthenticated request from being parsed/spooled before the handler.

Existing attachments remain blocked on a different source contract: the locked
BackendWorkspace API reads complete objects and permits documented local
fallbacks; it has no verified bounded workspace-only export/version API.
Supporting that path requires an explicit upstream capability/ownership design,
not direct private-path reads or edits to the installed dependency/CLI. Production
retention, optional Octop file-list copies, multipart limits and cross-worker
routing must be decided before opening either path.

## Dormant file-runtime lifecycle

`MemoryLakeFileRuntime` is a reusable resource owner, **not** a registered server
service or authentication capability. Its factory receives one shared database
admission and builds the trusted current authorizer. One ephemeral source and
one independent I/O pool/admission are owned together. Defaults are configurable
and deliberately limited: four active operations, 16 DB jobs, four I/O holders,
two I/O workers, and the source limits above. These are per-runtime limits; a
future server must not construct one per request or claim they provide a global
multi-binding/multi-worker quota.

The new `capture_runtime_memorylake_upload` helper automatically encloses the
actual UploadFile capture in `runtime.operation()`. The trusted file-binding
methods below also hold that scope through their final durable outcome save;
future approval/recovery entry points must preserve it. It rejects new/nested/over-capacity operations without queuing.
Closing stops ingress, source publication and new I/O; it **does not revoke
approval** or close DB admission while an already admitted operation needs it.
After operation holders and actual I/O settle, it closes/drains DB admission,
then joins the independent I/O pool. It does not close caller-owned files or the
application database, and does not delete persisted approvals or remote items.
`aclose` delays cancellation propagation until that drain completes, with no hard
wall-clock bound for a stuck read/SQL operation.

A real temporary-SQLite composition pauses an accepted import receipt, closes the
runtime, rejects new operations, then verifies the existing operation saves its
final `ready` record before drain completes; only afterwards does the fixture
close its DB pool. A real rolled-file composition cancels both the capture and
shutdown while its worker is blocked and verifies neither reports completion nor
releases file memory early. These tests do not invoke a real remote transport.

Both database and I/O admission now return cancellation-isolated views while
tracking only actual executor Futures. Directly awaiting/cancelling a public view
originally made runtime shutdown report drained while its thread still ran; that
failure is covered. Actual and view Futures each observe late exceptions without
logging their bodies, including abandoned doubly-shielded views, to avoid a
`Future exception was never retrieved` leak.

`OctopServer` now has a default-`None`, explicit trusted singleton attachment slot.
It constructs no runtime and registers no ingress. Attachment requires an already
started, database-bound server and an open runtime, and rejects replacement or
attachment during shutdown. Actual `stop()` closes and drains an attached owner
**before** native teardown and its DB-closing `finally`. Cancellation during the
MemoryLake drain is propagated only after successful drain and native teardown;
failed/false drain leaves the DB and attachment in place for a safe retry. The
real method is tested with disposable SQLite and blocked actual workers, not a
look-alike shutdown shim. Actual `start()` was not invoked by these tests.

Setup database probe/apply and lower-level control-plane rebind now reject an
attached runtime, even if closed/drained or the synthetic user count falls to
zero. The early guard precedes probing, config persistence and pool replacement;
changing databases requires a restart, not a hidden detach/revoke/delete. A
stopping latch remains after successful drain/detach and across native-teardown
awaits/errors, until the next start; the default-`None` path never sets it.
An event-controlled test first reproduced both setup paths and lower rebind
entering that direct-concurrency window, then passed after the latch fix. This
is lifecycle defense, not a claim of an existing remotely reachable exploit in
the normal uvicorn-first shutdown sequence. Five initial guard cases also pass;
tests preserve a temporary config byte for byte, keep an active worker's old DB open, and verify default-`None` setup
still invokes its original mocked collaborators.

This proves only **MemoryLake drain ordering**, not cancellation safety of all
server shutdown. Two characterization tests retain existing behavior: a later
native shutdown exception or cancellation can skip later native closers while
the original `finally` still closes the DB. Concurrent server stops, actual
startup/factory composition, native gateway/auth ordering, the default executor
used for DB and pool joining, multipart reception, confirmation idempotency,
consent withdrawal, retention, identity/policy ABA and multi-worker routing remain
production gates. The read-tool observer/HITL hard gate is unchanged.

## Dormant trusted file-binding composition

`MemoryLakeFileBinding.create` now composes the same server-supplied
caller/binding/connector tuple into the current write-binding loader, source
namespace, DB ledger, service and transport factory. A live owner-only SQL check
precedes even constructing the transport. A mismatched owner, connector or
revision cannot borrow the source or another binding's approval. The factory and
invocation checker remain **trusted server dependencies**, not client selectors,
JWT proofs or self-authenticating capabilities. Repositories must come from the
same currently attached server DB. Production construction is not registered.

Creation, prepare, explicit approval creation, submit and status methods hold
`runtime.operation()` automatically, through final saves and checks. The `.source`
property is trusted wiring for the existing runtime-scoped UploadFile adapter;
direct source calls still need an operation. Approval is never auto-created from
a model request; `create_approval` is only for a later authenticated confirmation
path that already has the user's consent to the exact immutable intent. A failure
after saving approval does not prove the save never happened, and is not a reason
to blindly issue another approval ID.

The initial five real-SQLite/fake-transport cases failed before the module existed.
Subsequent review cases reproduced a cancelled transport factory leaving a live
loader, and binding/owner revocation during import still allowing a success result
with document IDs. Factory failure/cancellation now invalidates captured loaders.
Submit checks current **read-reconciliation** permission before delivering its
persisted receipt: original owner/connector/workspace/project must still be
readable, while write opt-out or valid credential rotation alone does not deny
read access. Failed delivery checks retain the correct ledger state and internal
recovery evidence, never auto-retry. Cancellation during this final read check
also carries the already saved receipt without relabeling it `unknown`.
These checks do not retract an already granted remote step or define an atomic
transaction with a future HTTP/socket sink.

Tests also pause fake import during actual runtime shutdown, verifying the
admitted operation persists its receipt before drain completes. Closing an
invocation after upload keeps its known item ID while preventing the next import
step. Closing after import keeps the persisted processing result but does not
return success. Evidence is internal: an unauthenticated/error response must not
serialize it. The initial assembly tests injected fake transports without
credential decoding. The subsequent synthetic-credential/CLI-adapter composition
is described below; neither executes a real CLI, sends a MemoryLake request or
registers a JWT upload endpoint/approval UI.
Read-tool observer/HITL, identity-history ABA, bounded DB statement execution,
multipart limits and approved recovery/idempotency policy remain open gates.

## Dormant shared-admission credential adapter

`StoredMemoryLakeCredentials` is bound to trusted caller/binding/connector inputs
and the same runtime DB admission, not a new per-request executor limit. A new
repo projection obtains current ownership/read permission, ciphertext, credential
revision and existing connector master in **one SELECT**. The secret-purpose value
is fixed by server code and parameterized, never a client selector. Missing
masters are not bootstrapped. This file path deliberately does not replace the
separate dashboard-thread-bound reader API.

Decryption uses the uncached snapshot helper and validates/strips the key and
fixed connector ID. Before returning a key to the trusted transport, another
shared-admission snapshot must still match read scope, revision and a private
master/ciphertext fingerprint. Expiry travels back to the event loop for a final
current-time check without a later await: a red regression showed an authorized
worker snapshot could otherwise cross expiry before its caller resumed. Keys,
masters, ciphertext and fingerprints are omitted from diagnostic reprs; upstream
error bodies are replaced by constant reasons. This is not Python memory
zeroization or an authorization promise after the returned key leaves the loader.

A damaged SQLite integer in a BLOB column originally reached `bytes(integer)`
before size validation; small sentinels reproduced that allocation path safely.
Repo projections now require bounded bytes-like values before conversion, using
`memoryview.nbytes` where appropriate. This also protects the existing read
binding projection. It is corruption/driver-shape hardening, not a demonstrated
client-triggered exploit, and does not bound allocations internal to a DB driver
before it returns a value. Recorded SQL covers PostgreSQL placeholder ordering;
no live PostgreSQL credential run occurred.

Real temporary-SQLite/synthetic-Fernet cases cover ownership before decrypt,
key/master rotation, master deletion, concurrent revocation, malformed values,
expiry, and cancelled decryption retaining its actual shared job until completion.
The new composition additionally joins actual `UploadFile`, scoped capture,
trusted file binding, explicit synthetic approval, real ledger and
`MemoryLakeCliTransport` with a fully patched **fake runner**. It checks private
staging and fixed argv, sanitization/cleanup, import/status and current-key
read-only reconciliation after write opt-out. Rotation/revocation during decrypt
prevents any fake-runner call while a previously granted step remains
conservatively `unknown`. No subprocess, real CLI, real key, account, endpoint or
remote upload was exercised. These passing compositions do not enable ingress or
replace real-binary/schema/tenant verification under specific authorization.

## Authenticated bounded request receiving — unregistered, actual ASGI composition

`authorize_memorylake_file_request` verifies the actual HS256 JWT against a
bounded, existing `jwt` secret using the shared actual-worker DB admission. It
accepts exactly one Bearer header, rejects query aliases/extra query inputs and
never trusts `request.state.octop_user`. A current owner-only SQLite binding
projection supplies the caller, connector and authorization revision. No body
is read during this setup. Shared agents, disabled users and disabled write
policies fail closed. A reproduced first-SQL-write-allowed/second-SQL-write-paused
race initially entered the body; `MemoryLakeFileBinding.create(require_write=True)`
now checks write permission in its last binding snapshot before the factory/yield.
This snapshot is not an execution lease or a promise against future revocation.
Read-only reconciliation still uses `require_write=False`.

`SecretRepo.get_bounded` keeps oversized binary values out of its driver result
with a parameterized SQL CASE and validates bytes-like type/length before copying;
there is no bootstrap, integer-to-bytes allocation or legacy `get` behavior change.
Auth timeout/cancellation returns without releasing the actual pending SQL job's
shared admission until the worker finishes. JWT expiry is checked after worker
completion, with monotonic invocation lifetime and latched wall-clock expiry.
The next request verifies a rotated signing key; instant in-flight key/session
revocation is not implemented. The auth context's `max_request_s` alone is only
identity validity at checks, not an active timeout on arbitrary code inside yield.

`receive_memorylake_file_request` supplies the actual full authentication/receive
absolute timeout and a runtime operation around capture. A scoped `capture_named`
reserves **3 × maximum file bytes + 64 KiB** before receiving headers/body, then
rechecks current SQL authorization before publishing the opaque snapshot ID.
Name/content are obtained from one trusted bounded callback; no model host path,
approval, submission row or MemoryLake call is created. Fresh same-scope views
can subsequently prepare the exact filename, size and SHA256; closing the request
view does not silently delete that shared snapshot. Failures/cancellation release
capture reservation; already-running SQL remains tracked by shared admission.

The parser uses **python-multipart 0.0.32 public callbacks**, not a custom boundary
state machine or Starlette's file spool/threadpool. Its existing locked version
is now a direct Octop dependency: pyproject adds one requirement, uv.lock adds only
two root dependency/metadata relationships; no package version/artifact changed.
Initial offline resolution lacked registry metadata. After adding those existing
locked root relationships, `uv lock --offline --check` and `uv lock --offline`
passed; no registry download/upgrade or external repository modification occurred.
The editable local Octop metadata was subsequently rebuilt by normal locked runs.

Supported multipart is deliberately narrow: exactly one nonempty file part,
`form-data; name="file"; filename="UTF-8"` in that order and optional parameter-free
Content-Type. UTF-8 Chinese filenames work; path/control characters, escaped or
extended filename parameters, repeated/unknown headers or params, extra fields
and additional files are rejected. One boundary (<=70 bytes), <=2 headers
(<=4096 bytes each), body <= file cap +16 KiB, actual file cap and consistent
Content-Length are enforced. Library `max_size` can silently truncate and
`finalize` does not reject incomplete bodies, so the wrapper independently
counts all input and requires complete `on_end`/one closed part. Bounded MIME
epilogue is accepted and counted, not mistaken for another parsed file.
Per-feed copies are <=64 KiB; parser payload is a preallocated file-cap buffer
plus final immutable copy. Its per-instance logger is disabled to prevent raw
parser diagnostics. No spool, file or executor is used by parsing. Already
allocated ASGI frames, Python object metadata, HTTP header framing and prior
proxy/middleware buffering are outside the resource bound. Cooperative deadlines
do not forcibly interrupt synchronous parsing or actual database threads.

Red/green evidence: missing auth helper and bounded getter first failed; source
named-capture and parser likewise failed before their minimal implementations.
The last-write-policy interleaving failed with one body read before its fix.
Real test-only FastAPI/ASGI apps use disposable SQLite, signed synthetic JWTs and
fake transports. Tests cover all boundary byte splits, truncated prefixes,
strict header failures, capacity denial before receive, pending receive cancellation
and timeout, scoped prepare, no approval/remote side effects, expiry/rotation,
retained actual SQL quota, and request finalization. Additional receive-time real DB user/binding/write-policy revocation, JWT expiry
and runtime closure cases leave no published snapshot or remaining reservation.
DB revocation is checked at post-receive authorization, not by querying on every
chunk. The isolated auth/source/
file-binding/ingress subset passed **33 tests /29 subtests**; the parser passed
**8 tests /291 subtests** (overlap the joint result below, do not add totals).

**Production gate, not a full gateway claim:** the initial isolated auth/parser
test apps do not use `build_app`; subsequent native-stack compositions below do.
Its existing global setup/JWT middleware runs before a route,
performs synchronous legacy `SecretRepo.get`/UserManager checks, and may perform
another key read for sliding renewal after the handler. Thus the bounded helper
does not yet make the real application's entire authentication/response chain
bounded. No global-auth bypass, exception allowlist or production router was added.
Actual build_app characterization is now recorded in
`test_memorylake_app_boundary.py` (real temporary SharedServices/SQLite and
UserManager.boot, no OctopServer.start): current count/key reads run synchronously
before and after a test-only ordinary route, near-expiry JWTs slide-renew, and a
DB-disabled user can remain in the old UserManager cache. This is a known legacy
behavior, not a claim that the new helper accepts that user. The candidate upload
URLs return 404; 625 route-walk subtests only assert absence of registration, not
625 independent security scenarios. Two ldap3/pyasn1 deprecation warnings arose
from these ordinary app imports. A narrowly scoped, server-owned integration is
still required, as are upstream receive limits, current authorization at final response
sinks, confirmation/idempotency UI/API, observer policy and production lifetime.

### Scoped ingress architecture review — candidate only, not implemented

The selected implementation direction is to **retain the entire native middleware
chain** and add current owner/JWT checks plus bounded parsing inside the handler.
`test_memorylake_native_ingress.py` now manually mounts the helper in actual
build_app, only within its disposable test app: **5 tests /7 subtests** pass.
Normal chunked upload produces a scoped snapshot with the correct prepared
filename/size/hash and no approval/remote command. Even when native cached-user
auth accepts a DB-disabled user, the handler's current SQL denies before the
HTTPX body generator is consumed; shared/write-paused, duplicate-header and
query-token cases also fail. Bridge headers do not tunnel this distinct local
namespace. Existing successful/error CORS response headers and native GET behavior
are retained, not replaced. There is no registry or middleware modification.

The same tests explicitly retain legacy synchronous count/key reads and sliding
renewal even on handler AUTH_FAILED responses. Unauthenticated OPTIONS is rejected
with 401 before CORS on both native and candidate paths, so response CORS headers
are NOT proof of usable browser cross-origin preflight; first use remains a
same-origin candidate. These limitations remain explicit technical debt and
production gates; handler-side quotas must not be advertised as whole-application
bounds. No production API is registered. The bypass alternative below is retained
only as a reviewed rejected-for-now candidate, not an implemented shortcut.

A three-middleware registry exemption followed by `call_next` is NOT sufficient:
each await permits route mutation before final dispatch, so a previously checked
route/handler can be replaced. A proposed alternative captures a fixed authenticated
endpoint in the outer setup middleware and invokes that exact function directly.
No such branch, registry, exemption or registrar test has been added. The following
inventory is a review result, not implemented compensation:

| Existing layer | Effect of proposed fixed dispatch; outstanding compensation |
| --- | --- |
| Setup lockdown | Skip synchronous user count; current owner SQL denies missing users, but original setup-required 503 semantics would change. |
| JWT middleware | Skip cached-user auth, query-token and sliding renewal; use standalone Bearer/DB auth, explicitly no automatic renewal or instant in-flight signing-key revocation. |
| Bridge proxy | Do not resolve tunnel headers or pre-read/forward the body; retain fixed local IDs, no bridge target substitution. |
| CORS | Inner CORS would not run. Rejecting nonempty cors_origins would only limit the candidate to same-origin configuration; it is not server-side Origin validation. |
| ExceptionMiddleware | OctopError/HTTPException adapters would not run; require explicit safe status/envelope mapping. |
| ServerErrorMiddleware | Still outside the proposed branch and can log raw unhandled errors; sanitize ordinary exceptions before reaching it, propagate cancellation without body/evidence logging. |
| FastAPI AsyncExitStack | File/dependency cleanup stack would not run. Current raw parser uses no UploadFile/Form/dependencies and owns finally cleanup; future dependencies cannot assume framework teardown. |
| Router/APIRoute | Skip matching, dependency solving, parameter/response validation and route error wrappers; only fixed trusted IDs and explicit bounded JSONResponse are admissible. |
| Session/CSRF/Host/HTTPS/GZip | Not installed in inspected build_app; neither the current app nor the candidate is proven to supply these protections. Bearer-only/no-cookie/no-query rules remain mandatory. |
| OpenAPI/lifespan | Hide any experimental schema to avoid implying APIRoute processing. HTTP dispatch does not bypass independent lifespan; registration must actually freeze before start, not merely call build_middleware_stack(). |
| Logs/proxy/final send | Upstream access logs, buffering and ASGI send are outside the helper; result-to-send authorization and disconnect/retained-snapshot policy remain unproven. |

Actual inspected stack: ServerError → setup BaseHTTPMiddleware → JWT → bridge →
optional CORS → ExceptionMiddleware → FastAPI AsyncExitStack → APIRouter. No
app-wide router dependencies were installed in the inspected build_app. Before any
implementation/activation, independently verify the expected stack cannot reorder,
exact method/raw-path matching, dynamic-route collisions, missing/mutated handler
denial, original paths unchanged, safe errors/cancellation and resource drain.
Unknown middleware or an unprovable configuration must keep this candidate closed.

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

## Historical verification checkpoint and restricted full-suite outcome

Checkpoint: **2026-11-01**. The authorized locked installation succeeded:
**218 Python packages** and **1067 npm packages**. The Python install used
`CC=gcc CXX=g++ UV_CACHE_DIR=/tmp/octop-uv-cache uv sync --locked`; dependency
versions and lockfiles were not changed. The earlier missing-dependency blocker
is historical, not the current reason the full suite is incomplete.

The root task pushed WIP commit `e897c123`; subsequent storage, formatting and
adapter work is still uncommitted. This is not approval to merge, release,
deploy, enable MemoryLake, or use real credentials.

Earlier permitted joint command (before product API/UI registration):

```bash
UV_CACHE_DIR=/tmp/octop-uv-cache uv run --locked --offline pytest \
  tests/unit/connectors/test_memorylake_*.py \
  tests/unit/db/test_memorylake*.py tests/unit/db/test_db_pool.py \
  tests/unit/gateway/test_preempt_stop_cancel.py \
  tests/unit/gateway/test_memorylake*.py \
  tests/unit/api/test_memorylake*.py \
  tests/unit/test_memorylake_server_lifecycle.py \
  tests/unit/gateway/test_dashboard_ws.py \
  tests/unit/gateway/steer/test_coordinator.py -q -m 'not live'
```

Historical result: **753 passed, 1539 subtests passed**, with 2 dependency deprecation warnings (35.94 s), including actual
LangChain/LangGraph and harness ToolSearch compositions, real temporary SQLite
read/write storage and migrations, origin/delivery/middleware/JWT helpers,
server-owned socket registration and full native gateway reboot, front-channel
and typed/native steer checks, plus original dashboard WS and stop/cancel tests.
This is a targeted regression result, not a full-suite or live PostgreSQL pass.
Read-only whole-tree checks passed: `ruff check src tests`,
`ruff format --check src tests` (**1348 files**) and `mypy --strict src/octop`
(**639 source files**), using `uv run --locked --offline`. These do not replace
the denied full regression. This checkpoint includes source-memory/TTL/capture fixes, deleted-write-policy
read reconciliation, canonical version-2 compatibility rejection, owned I/O pool,
file exclusivity, public Future cancellation isolation/late-error observation,
the dormant runtime's actual-file and real-SQLite lifecycle compositions, real
`OctopServer.stop` drain ordering and setup/rebind rejection including the
post-drain native-teardown window, plus fixed scoped-source namespaces, shared
DB/IO/byte budgets, view-local read cancellation/closure, and trusted file-binding
composition with current read authorization before receipt delivery, plus the
shared-admission credential adapter, atomic binary/master snapshot and its
expiry/corruption regressions and fake-runner file flow, plus authenticated
request scopes, bounded existing JWT-secret reads and actual ASGI multipart-to-
scoped-source composition (not the production middleware stack).

LangChain red/green evidence: the new test first failed collection because the
adapter did not exist; after implementation a real exported-schema regression
failed, then passed after the strict schema fix. The final isolated result was
**18 passed, 45 subtests** before the subsequent review fixes below. The read connector remains
unregistered. `git diff --check` passes; the CLI sibling is clean. Octop remains
on `feature/octop-memorylake` at `e897c123` with these later changes uncommitted
(41 modified and 67 untracked paths at this checkpoint). No later commit or push
was performed; the CLI sibling remains unchanged. The sole dependency change is
the existing python-multipart version becoming direct, as described above.

Review regressions were reproduced before fixes:

- Two thread/binding split-check failures: binding revocation during the final
  independent thread lookup still disclosed a result; thread deletion during
  decrypt still allowed CLI launch. The single authorization SQL fixes both.
- Distinct per-turn factories initially had no shared admission injection; the
  factory now requires it, and a real two-factory/storage composition verifies
  maximum one simultaneous fake CLI invocation for the same connector.
- Physical SQLite v24 with a watermark of 25 or 26 failed existing connector
  reads due to the missing new column. Startup now repairs missing v25 structure
  without lowering the watermark or resetting live credential revisions. The
  SQLite tests verify a rotated revision stays 2 after repeated repair.
- Four PostgreSQL recorded-SQL failures exposed the missing PostgreSQL repair
  branch. Repair now inspects `information_schema` and applies the actual PG DDL
  in one transaction, backfills only a newly added revision column, and does not
  lower the watermark. Seven SQL contracts in total also verify optional thread
  filtering uses fixed parameterized predicates, never an independent untyped
  `? IS NULL` parameter. No PostgreSQL server was used; live PG remains a gate.
- Two pre-v13 SQLite fixtures exposed a stale FK target (`connectors_legacy`)
  and loss of an existing revision 7 during connector reconstruction. v13 repair
  now precedes v25, and reconstruction preserves an existing revision. Tests
  verify ciphertext, revision, FK target and `foreign_key_check` after replay.
  This prevents the new migration from creating that corruption; it is not an
  automatic repair of already-corrupt binding tables.

An additional pure-DB regression passed **246 tests** (overlaps the joint suite;
do not add the totals). Command:

```bash
UV_CACHE_DIR=/tmp/octop-uv-cache uv run --locked --offline pytest tests/unit/db \
  --ignore=tests/unit/db/test_db_factory.py \
  --ignore=tests/unit/db/test_runtime_replace_services.py -q -m 'not live'
```

Schema assertions in existing DB tests were updated only where they exercise
all migrations: the current version is 26, while direct v25-only tests remain 25.
The first v26 run exposed 17 failures (stale version assertions and non-idempotent
DDL); the targeted DB suite now passes. Unrelated runtime/factory tests and the
denied full suite were not run by this command. Identity disable/re-enable ABA
history remains unimplemented and is still a production gate. Manually trusted
issuance and fake graph models do not establish production graph authorization.
POSIX-private-session graph/service tests are explicitly skipped on unsupported
platforms; no Windows run occurred.

The offline composition test joins `SubmissionService`, a fake ledger,
`MemoryLakeCliTransport` and `MemoryLakeReadService` with a temporary Python fake
CLI. It covers approval, private staging, upload, nonzero-but-accepted import,
current-credential reconciliation after write opt-out/rotation and retrieval.
It demonstrates compatible orchestration, not a production transaction or live
connection. Tests use synthetic data and dummy credentials, not real uploads.
The test-only network guard clears inherited `MEMORYLAKE_*` values and rejects
Python IPv4/IPv6 sends/connects and DNS; fake Python CLI processes install the
same guard. This is a regression tripwire, not an OS sandbox or production control.

### Full regression: stopped by a safety rejection, no final summary

The attempted full `make all` run has its original log at
`/tmp/octop-memorylake-make-all.log` (execution session `67951`). The log stopped
at approximately 30% at **07:46:13 UTC**, without a pytest completion summary.
The tool rejected continuation with this reason:

> The regression command is authorized, but this unexpected Microsoft telemetry
> connection is an external side effect not specifically authorized and may
> transmit test or environment metadata. Do not bypass this rejection through a
> workaround or indirect execution.

A subsequent attempt to confirm cancellation returned `Unknown process id 67951`.
That is not evidence of a successful completed test run. The exact component
responsible for the connection is **not established**; attributing it to a
particular Microsoft/ONNX dependency is only an inference, not a verified finding.
No full `make all` pass or full-suite failure total can be reported.

The run had introduced 11 extra packages. They were removed with the authorized
`UV_CACHE_DIR=/tmp/octop-uv-cache uv sync --locked --offline`; a subsequent dry run
reported **218 unchanged**. Installation and cleanup output was not saved to
separate log files. Preserve the original full-run log rather than inventing
missing output or a completion result.

**Recovery gate:** do not rerun `make all`, directly rerun the denied full suite,
modify tests/ONNX, disable telemetry, or use another command/environment to bypass
this rejection. Resumption requires a materially new, relevant authorization and
an approved resolution of the rejected external side effect; approval to install
locked dependencies or run regression generally is not such authorization.
Only the explicitly permitted targeted tests and read-only lint/type checks are
being continued. No alternate mirror, proxy or telemetry workaround is authorized.

Real CLI binary/version/bootstrap validation, explicitly authorized disposable
MemoryLake tenant tests, trusted BackendWorkspace attachment export, identity ABA
handling and Agent tool/observer registration remain separate gates. Durable
approval/lease storage and default-off file API/UI are now wired; mock success and
real SQLite success do not establish live-service or production readiness.

## Potential CLI requests — not implemented, not blockers for offline work

- Supported API-key stdin/fd bootstrap to avoid reliance on private TOML format.
- Query/content file or stdin inputs so sensitive text need not appear in argv.
- Stable machine-readable error envelopes/exit categories.
- Documented upload/import idempotency and reconciliation guarantees.

No such CLI change may be made without owner confirmation. MCP v2 remains a
read-only comparison candidate, not a silent replacement for CLI integration.
