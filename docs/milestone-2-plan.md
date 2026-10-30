# Milestone 2: Archive and Restore Plan

## Goal

Deliver a reproducible synthetic lifecycle:

```text
upload -> explain policy decision -> archive -> verify -> restore -> verify
```

PostgreSQL is authoritative for lifecycle intent, operation state, attempts, and retry scheduling. Redis Pub/Sub is only a best-effort wake-up channel. Every successful movement must be verified against the immutable content version and SHA-256. Never delete the last verified copy under the stated single-executor ownership assumptions.

This milestone is not a production-readiness claim. It does not implement full reconciliation, orphan cleanup, automatic staging expiration, performance benchmarks, production S3 deployment, tenant authentication, multi-worker fencing, or Glacier behavior.

## Decisions

- **Archive admission:** Manual and policy-driven archive requests use the same complete policy and mandatory safety guards. There is no manual bypass.
- **Retry:** An API-key-protected `POST /v1/admin/jobs/{job_id}/retry` creates a linked replacement for an eligible, settled failed job. It does not take over uncertain or running work.
- **Domain model:** Extend the existing `Job` and `jobs` table. Do not rename them or add a competing operation abstraction.
- **States:** Keep `ARCHIVE_REQUESTED` and `RESTORE_REQUESTED` as queued states. Complete restore directly to `ACTIVE`; preserve the legacy `RESTORED` enum value only where required for existing data compatibility.
- **Storage:** MinIO active and archive buckets represent abstract `ACTIVE` and `ARCHIVE` placement. They do not emulate AWS Glacier classes. Keep Amazon S3 as a later production target.
- **Execution:** Begin with one controlled lifecycle worker, concurrency one. PostgreSQL polling must work independently of Redis. Do not use Celery as lifecycle transport or add RabbitMQ.
- **Recovery:** A lost advisory lock or uncertain copy/delete outcome is not evidence that ownership is safe to transfer. Require controlled recovery after the old worker is stopped and in-flight requests are settled.
- **Signed downloads:** A presigned URL is a capability, not a storage lease. Previously issued links can stop working after archival removes the active copy. Do not promise uninterrupted downloads during a transition.

## Baseline and Code Reuse

The current implementation provides useful primitives, but does not yet implement archive/restore:

- [app/objects.py](../app/objects.py): upload initiation fingerprints canonical metadata and deduplicates owner-scoped idempotency keys. `complete_upload` uses a dedicated PostgreSQL advisory-lock connection and does storage I/O outside a database transaction. Its lock key is derived from the UUID. Extract this lock handling so ingestion, lifecycle admission/execution, access updates, and download issuance coordinate on the same object lock.
- [app/objects.py](../app/objects.py): `record_access` currently updates access metadata without the object lock. Serialize that update with lifecycle operations so policy inputs cannot be silently invalidated during a move.
- [app/objects.py](../app/objects.py): upload replay currently may issue a new staging form for any non-`ACTIVE` state, and completion permits `FAILED` if a staging key exists. Add explicit distinctions between never-verified ingestion failure and lifecycle failure to prevent replay from re-ingesting or restoring bytes incorrectly.
- [app/storage.py](../app/storage.py): `ObjectStorage.verify` streams stored bytes and validates actual size and SHA-256. `promote` demonstrates source verification, conditional copy, and destination verification. Reuse these checks for lifecycle movement; never treat ETag as a checksum.
- [app/models/object.py](../app/models/object.py): `bucket` and `key` currently identify the active copy. `content_version` is distinct from the optimistic row `version`. Add an explicit archive locator rather than changing the meaning of the active locator.
- [app/models/job.py](../app/models/job.py): `Job` has a type, status, object link, nullable idempotency key, attempt count, and legacy Celery task ID. It lacks immutable content bindings, retry scheduling, attempts, and outbox state.
- [app/models/policy.py](../app/models/policy.py): `Policy.config` is currently mutable JSON without rule validation or activation immutability.
- [app/models/audit.py](../app/models/audit.py): `AuditEvent.reason` is free text and is not suitable as a structured policy or attempt record. Add structured JSON evidence rather than encoding JSON into the text field.
- [app/config.py](../app/config.py): `admin_api_key` exists but is not enforced. An API key protects only the retry route; it does not authenticate normal API requests or authorize tenants.
- [docker/minio-policy.json](../docker/minio-policy.json): the API credential currently lacks delete and archive-object access. Preserve its least privilege and add a separate worker credential/policy. S3 copy permissions derive from source reads and destination writes; there is no standalone `s3:CopyObject` IAM action.

## Delivery Sequence

1. Verify the Milestone 1 branch through remote PR checks and merge it before branching for lifecycle work. Remote Actions, GHCR publication, and branch-protection settings are distinct from local test results.
2. Implement schema, state, and ownership changes; validate migrations and legacy rows before any destructive storage behavior.
3. Implement the pure policy evaluator and transactional request APIs.
4. Implement a polling-only worker and verified transfers. Prove correctness while Redis is stopped.
5. Add the outbox publisher and Pub/Sub hint subscriber. These may optimize latency but must not alter correctness.
6. Add controlled recovery, real-service failure tests, CI gates, and reproducible evidence.

Do not build notification adapters before the polling path has passed its tests. Pure policy development can proceed alongside schema work after the data contract is agreed. Worker permissions can be implemented alongside the transfer adapter. Publisher and subscriber work can proceed in parallel only after the job claim and notification contracts are fixed.

## Phase 1: Durable State, Schema, and Locking

### Job state

Use these states for new lifecycle jobs:

```text
PENDING -> RUNNING -> SUCCEEDED
              |  \
              |   -> RETRY_SCHEDULED -> RUNNING
              |  \
              |   -> FAILED
              |   -> RECOVERY_REQUIRED
```

`RECOVERY_REQUIRED` is nonterminal for per-object conflict prevention, but is never automatically picked up by the normal poller. Controlled recovery resumes the same job after ownership is established. Admin retry applies only to a settled `FAILED` job and creates a new linked job.

### Migration and models

Add a new Alembic revision after `0002`; do not rewrite applied migrations. Extend `Job` with fields sufficient to bind execution to immutable facts:

- `content_version`, expected SHA-256, and expected size.
- Source and destination bucket/key snapshots, populated from trusted catalog configuration at admission.
- Canonical request fingerprint and scoped idempotency key.
- `next_attempt_at`, `max_attempts`, start/finish timestamps, sanitized error class/reason, and `retry_of_job_id`.

Add `JobAttempt` records with attempt number, stage, timestamps, sanitized detail, and error classification. Attempt numbers are unique per job. Completed attempts are immutable; a recovery creates a new attempt rather than resetting history.

Add `OutboxEvent` records containing a versioned event type and minimal job identifier payload, publication attempt count, publication time, next publication time, and sanitized error. A successful Redis `PUBLISH` is a publication attempt, not a delivery acknowledgement.

Add object fields for archive bucket/key, `archive_copy_retained`, `restored_at`, and `ingestion_verified_at`. Keep the existing active `bucket/key` meaning unchanged. The archive physical key should be deterministic and private, such as `objects/{object_id}/v{content_version}` in the archive bucket. Snapshot both locations on the job so later configuration changes cannot redirect retries.

Add structured audit evidence: event type, actor kind, policy identity, and JSON decision details. Preserve the existing state and reason fields for compatibility. Do not store secrets, signed URLs, or content bytes.

### Constraints and indexes

- Unique scoped key `(object_id, idempotency_key)`, across archive and restore operation types. Reusing a key with changed semantics is a conflict.
- Partial unique index on `jobs(object_id)` for statuses `PENDING`, `RUNNING`, `RETRY_SCHEDULED`, and `RECOVERY_REQUIRED`.
- Unique `(job_id, attempt_number)`.
- Due-job index supporting ordered selection by `next_attempt_at`, creation time, and ID for pollable statuses.
- Pending-outbox index supporting bounded eligible publication.

Replace the legacy global idempotency-key uniqueness explicitly in the migration. Preflight conflicting legacy nonterminal jobs and fail with actionable diagnostics; do not discard history. Existing unbound jobs must not be assigned invented checksums or locations. Mark unsafe legacy active jobs `RECOVERY_REQUIRED` or otherwise exclude them from normal pickup. Backfill `ingestion_verified_at` only from trustworthy `INGESTION_VERIFIED` audit records; do not fabricate verification timestamps.

Account for PostgreSQL enum addition and autocommit requirements. Test upgrade, downgrade, re-upgrade, and Alembic drift on a disposable database. Downgrades must not destroy unrelated or wanted data.

### Shared object ownership

Extract a common advisory-lock helper from upload completion. Use the same UUID-derived session lock for upload completion, lifecycle admission/execution, application-access updates, download URL issuance, and admin retry. Keep transactions short and closed during storage I/O. Re-read the object and job after acquiring the lock, and use optimistic row versions for short commits; do not carry a stale ORM state across transfer I/O.

Lock contention returns `409` with retry guidance. An access update is never silently discarded. If it cannot acquire ownership during movement, return a retryable conflict; after it succeeds, worker revalidation must observe the updated access time. Verify the lock connection remains valid before every destructive operation. Never transparently reconnect and assume the session lock survived.

### Exit gate

Migration and drift checks pass; partial uniqueness is verified under concurrent transactions; state transitions and legacy rows are covered; upload replay, access update, download, and lifecycle operations share ownership. No archive copy/delete is enabled yet.

## Phase 2: Policy, Transactional Requests, and Admin Retry

### Policy contract

Implement a pure evaluator that accepts object metadata, an immutable policy version, an explicit UTC evaluation time, and execution context; return eligibility and stable reason codes.

Seed one rule, `archive-completed-cold-datasets` version 1:

- Workflow is `COMPLETE`.
- Application inactivity is at least 180 days.
- Remaining retention is at least 180 days.
- Size is at least 1 MiB.
- Current serving placement is verified `ACTIVE`.
- Legal hold is false and no conflicting job exists.
- Every required input is present and timezone-aware; unknown values reject with explicit reasons.

Thresholds are inclusive and evaluated in UTC. Manual and automatic archive admissions use the same evaluator and guards. A dry-run/evaluation command may take an explicit historical time for reproducibility; real admission and execution use server time, not a client-provided timestamp.

Make policy name/version unique and validate its configuration. Once activated, its name, version, priority, and config cannot be edited; changes create a new version. Store the original evaluation snapshot and re-evaluate current metadata against that same immutable version before mutation. A current policy activation must not silently change an already queued job's basis. The execution context recognizes its own current job as the owner rather than rejecting itself as an in-flight conflict.

Candidate selection is bounded and deterministic, using keyset pagination (for example, `(last_accessed_at, id)`). SQL filters must not omit eligible objects. A selected object's explanation returns direct reason codes; rows excluded by SQL are not reported as evaluated rejections. Add an index for the actual candidate query and inspect representative query plans without claiming a benchmark.

### API and transactional admission

Implement:

```text
POST /v1/objects/{object_id}/archive
POST /v1/objects/{object_id}/restore
GET  /v1/jobs/{job_id}
GET  /v1/jobs/{job_id}/attempts
POST /v1/admin/jobs/{job_id}/retry
```

Archive and restore requests require `Idempotency-Key` and a canonical fingerprint containing operation type, object version/content identity, and applicable policy context. Resolve an existing key before checking current eligibility so a replay after success returns the original job. Same key with changed content or operation is `409`; a different key competing with nonterminal work is also `409`. A fresh request for an already-target placement returns a conflict rather than inventing a successful job.

Under the object lock and one short PostgreSQL transaction:

1. Reload and validate object state, content identity, policy, legal hold, retention, and conflicting work.
2. Update desired placement and queued lifecycle state.
3. Insert the job, audit/decision record, and outbox event.
4. Flush parent rows before dependent audit/job references and commit atomically.

Do not call Redis or object storage inside the transaction. On rollback, no executable intent or outbox event remains. Source/destination paths and checksums come from the database, never from notification payloads or client requests. Return `202` with a job identifier/status link. Idempotent replays return the same job without adding duplicate audit/outbox rows.

Restore admission requires a verified archived state and known archive location, but does not require coldness or archive-policy eligibility. Restore is an availability operation; preserve retention and legal-hold metadata and retain the archive copy.

Job responses expose status, type, timestamps, attempt budget/count, next attempt time, sanitized reason, decision summary, retry linkage, and bounded attempts. They do not expose private storage keys, credentials, or signed URLs.

### Admin retry security

Protect retry with FastAPI `APIKeyHeader` (`X-Admin-API-Key`) and constant-time comparison to `ADMIN_API_KEY`. Authenticate before looking up the job. If no key is configured, return `503` (feature disabled); missing/wrong keys receive a generic `401`. Never log or place the key in query strings. Require a bounded reason and idempotency key.

Only a settled `FAILED` job is eligible. A `RUNNING`, `PENDING`, `RETRY_SCHEDULED`, `SUCCEEDED`, or `RECOVERY_REQUIRED` job returns `409`; uncertain work requires controlled recovery, not an admin bypass. A retry revalidates the original immutable binding and current guards, creates a linked replacement with a new attempt budget, and preserves original attempts and failure. Repeated retry with the same key returns the same replacement. A shared API key does not make other routes or owner metadata an authorization boundary.

### Exit gate

Unit tests cover policy boundaries, missing metadata, timezone handling, policy immutability, and stable reason codes. Real PostgreSQL tests cover same-key/different-key races, manual/automatic parity, replay after success, competing archive/restore, and transaction rollback of state/job/audit/outbox together. Admin tests cover unconfigured, missing, invalid, and valid keys, ensure rejection has no side effects, and prove retry linkage/history. Existing ingestion tests remain green.

## Phase 3: Polling-Only Worker and Verified Transfers

### Polling and claims

Implement one synchronous `execute_job(job_id)` path and a bounded PostgreSQL poller. Proposed starting configuration (validate/tune through testing): batch size 20, idle interval 2 seconds, maximum 5 attempts, exponential backoff base 2 seconds capped at 60 seconds with bounded jitter. Inject clock and jitter so tests need not sleep.

The poller selects due IDs in index order at startup and periodically. After obtaining the per-object session lock, reload job/object and atomically claim only eligible `PENDING` or due `RETRY_SCHEDULED` work. Record the attempt and stage in a short transaction. Duplicate poll results skip active/terminal work without storage mutations. Do not hold row locks or transactions while copying or hashing. Keep one bounded executor slot; do not build an unbounded in-memory queue.

Do not automatically reclaim `RUNNING` jobs on startup. A worker timeout or process restart is not proof that a previous storage request stopped. Uncertain mutation outcome or lost lock goes to `RECOVERY_REQUIRED` and blocks further execution until controlled recovery.

### Worker storage permissions

Create a separate worker MinIO user and policy. Grant only needed bucket listing/location, `GetObject`/`PutObject` for active and archive object prefixes, and `DeleteObject` for active objects after verification. No archive delete, staging write, or bucket administration. Keep the API user's current restrictions; presigned clients remain limited to staging uploads and authorized active reads. Test denied operations with both credentials.

### Archive algorithm

Under the object lock:

1. Reload the owning job and object. Recheck immutable content binding and current legal hold, retention, policy, and access metadata.
2. Inspect active source and pinned archive destination. If archive already exists, verify actual size and SHA-256 and reuse only if it matches the immutable version. Never blindly overwrite a conflicting destination.
3. If destination is absent, verify the active source, then conditionally copy using the verified source ETag as a race precondition.
4. Stream-verify the archive destination's stored bytes. Record a durable verification checkpoint.
5. Before deletion, verify the lock connection is still valid, reload required metadata under ownership, and revalidate safety guards and content identity. Persist delete intent.
6. Delete the active copy only after archive verification. Confirm the resulting locations and commit object state, observed placement, job success, and audit together.

Successful archive sets lifecycle `ARCHIVED`, desired/observed placement to `ARCHIVE`, and `archive_copy_retained=false` because archive is now the serving copy, not an extra replica. Do not report success before the final database commit.

### Archive recovery

If active is absent but the archive destination verifies against the job's immutable binding, finish the same job only when its durable stages show this job authorized the deletion. Missing source alone proves neither success nor ownership. If both copies are absent or evidence is inconsistent, set observed placement to `UNKNOWN` when necessary, record a failure/uncertain state, and block mutation for review. If active exists and archive is corrupt, preserve active and fail; do not silently overwrite ambiguity.

### Restore algorithm

1. Verify the pinned archive source against size and SHA-256.
2. Inspect the active destination. Reuse it only if it independently verifies; if absent, copy from archive.
3. Stream-verify active bytes.
4. Commit `ACTIVE`, observed placement `ACTIVE`, `archive_copy_retained=true`, `restored_at`, job success, and audit in one transaction.

Never delete archive during restore. A corrupt active destination blocks blind replacement. A completed restore rediscovered by polling returns without extra copies.

### Failure classification and controlled recovery

Automatically retry only classified transient failures with settled outcomes. Policy denials, malformed work, permission/configuration errors, checksum conflicts, and invalid states are not transient. A timeout or connection loss around copy/delete, or lost advisory lock, is uncertain: use `RECOVERY_REQUIRED`, not an immediate retry.

Graceful shutdown stops new pickup, closes Redis helpers when present, and allows bounded current work to finish. Recovery after an uncertain interruption requires the old worker to be stopped, in-flight storage requests to be settled using a documented provider/operational procedure, and an explicit bounded maintenance command for selected job IDs. Reacquire the same object lock, inspect actual bytes, append a recovery attempt, and resume the same job. If request settlement cannot be established, remain blocked for manual review. A database advisory lock is not distributed fencing.

Desired placement records accepted intent, not successful storage facts. Observed placement records the last verified serving placement; use `UNKNOWN` if evidence invalidates it. Job failure is distinct from ingestion failure. Keep evidence of any valid extra copy until a later reconciliation milestone can inspect it.

### Exit gate

Polling alone completes archive/restore with Redis stopped; all success states match real hashes; worker permissions are constrained; retries are bounded; destination reuse is verified; and controlled post-delete/precommit recovery is demonstrated without deleting the last verified copy.

## Phase 4: Redis Pub/Sub and Outbox

Only after polling-only execution works:

- Define a bounded, versioned notification such as `{schema_version, event_type: "job_ready", job_id}`. It contains no trusted storage location, lifecycle state, or object payload.
- A bounded publisher claims eligible outbox rows, ends its database transaction, issues `PUBLISH` with finite timeouts, then records the publication attempt. A subscriber count of zero is not failure or proof of delivery. A crash after publish and before recording can duplicate hints safely.
- Redis failure leaves outbox publication retryable, but never blocks database execution. Redis Pub/Sub is ephemeral; Redis AOF does not make notifications durable.
- Subscriber reconnects independently. It validates schema and job ID, then hands a hint to the same claim/executor used by polling. Hints never bypass due-time checks. Malformed or unknown events produce sanitized diagnostics and no storage I/O.
- Polling always runs at startup and bounded monotonic intervals, even during a continuous stream of hints. A full in-memory hint queue may drop/coalesce entries because PostgreSQL polling remains authoritative.
- Provide deterministic one-shot publisher/worker commands for tests. Avoid a separate microservice just for publishing.

Test publication before subscription, duplicate hints, successful publish with no delivery, subscriber disconnect/reconnect, poisoned messages, Redis restart/outage, and database polling while Redis is stopped. Demonstrate that notification behavior changes latency only, not correctness or retry budgets.

## Phase 5: Failure Evidence, CI, and Documentation

Reuse [tests/test_ingestion_integration.py](../tests/test_ingestion_integration.py) service setup. Add pure policy/job tests and focused real-service lifecycle tests; extract common fixtures only if genuinely needed. Integration success must perform real transfers, not mutate database state to simulate them.

Required evidence includes:

| Scenario | Required assertion |
|---|---|
| Happy archive/restore | Exact stored SHA-256 and size at both boundaries; active absent after archive; both copies valid after restore; archive retained. |
| Policy boundaries | Hold, workflow, exact size/age/retention boundaries, null metadata, timezone errors, and direct rejection evidence. |
| Idempotency/concurrency | Concurrent requests, competing operation types, duplicate polling/hints, changed key semantics, retries; one active job and no duplicate logical mutations. |
| Metadata changes | Queued job revalidation catches changed access/retention/hold; access contention is not silently discarded. |
| Ingestion replay | Initiate/finalize replay cannot issue a new upload form or restore bytes after lifecycle movement/failure. |
| Interrupted archive | Failure after claim/copy/verify/delete intent/delete/final commit; controlled recovery inspects bytes and resumes the same job. |
| Corruption/uncertainty | Corrupt/missing source/destination, copy precondition failure, ambiguous timeout, or lock loss never produces false success or a destructive guess. |
| Retry | Due-time enforcement, bounded attempts/exhaustion, injected jitter/clock, linked retry history; admin cannot take over active/uncertain work. |
| Authorization | Missing/wrong/unconfigured admin key causes no side effect; API and worker storage rights are independently tested; objects are private. |
| Redis | Lost/duplicate/poisoned hints, publisher outage, publish-before-subscribe; real PostgreSQL polling works with Redis down. |
| Download | New URLs refused for moving/archive/uncertain states; previously issued capability limitation documented; post-restore download matches bytes. |

Unit tests use injected clocks/failpoints, not sleep-based retries. Runtime kill/restart tests run only in isolated Compose with bounded condition/readiness checks. Prove the old executor is gone and in-flight requests settled before recovery. Clean only generated synthetic resources.

Extend [.github/workflows/checks.yml](../.github/workflows/checks.yml) to initialize worker credentials, run migration upgrade/downgrade/re-upgrade/drift checks, execute the complete suite without integration skips, build and boot the API/worker, run the scripted lifecycle and selected recovery checks, and upload sanitized test/coverage evidence. Preserve the existing 75% minimum coverage, runtime dependency audit, image vulnerability scan, frozen installs, lint/format, and exact tested-image promotion. Do not lower gates to accommodate the new code; specifically test each destructive path even if aggregate coverage is already above threshold.

Add a bounded synthetic script and [docs/milestone-2.md](milestone-2.md) only when implementation evidence exists. Record commands, tool/service versions, bytes, durations, recovery steps, actual outcomes, and limits. Keep [Milestone 1 evidence](milestone-1.md) historical. Update README and PLAN to distinguish implemented, locally tested, remotely tested, and planned behavior. Do not claim production readiness, scale, S3 equivalence, or MinIO fixture security from the application-image scan.

### Local verification gates

Use only the isolated synthetic Compose project/database; migration downgrade is destructive to that database.

```bash
uv sync --frozen --extra dev
uv run --frozen ruff check .
uv run --frozen ruff format --check .
uv run --frozen pytest -m 'not integration'

# With the isolated PostgreSQL/MinIO stack initialized:
uv run --frozen alembic upgrade head
uv run --frozen alembic downgrade base
uv run --frozen alembic upgrade head
uv run --frozen alembic check
RUN_INTEGRATION=1 uv run --frozen pytest --cov=app --cov-fail-under=75
```

Then build and boot the API and worker, run the synthetic lifecycle/recovery script, and rerun the dependency audit and image scan. Remote PR Actions must also pass. GHCR publication occurs only on the configured `main` push; repository branch protection, review requirements, package permissions, and any deployment environment approvals are settings outside workflow YAML. Do not push, merge, or deploy without separate authorization.

## Suggested File Ownership

Existing files to extend:

- [app/models/job.py](../app/models/job.py): job bindings/status plus `JobAttempt` and `OutboxEvent`; leave `FolderJob` deferred.
- [app/models/object.py](../app/models/object.py): archive location, retained-copy/restore/ingestion evidence, candidate index.
- [app/models/policy.py](../app/models/policy.py): immutable policy identity/version and activation rules.
- [app/models/audit.py](../app/models/audit.py): structured decision and actor evidence.
- [app/models/__init__.py](../app/models/__init__.py): export mapped models for Alembic.
- [app/enums.py](../app/enums.py): retry/recovery job states and direct `RESTORING -> ACTIVE` transition.
- [app/objects.py](../app/objects.py): guard upload replay; serialize access/download; share lifecycle admission.
- [app/storage.py](../app/storage.py): reuse verify/conditional copy and add explicit archive/restore/delete primitives.
- [app/schemas.py](../app/schemas.py): strict lifecycle/job/decision responses without key or credential leaks.
- [app/config.py](../app/config.py): bounded retry/poll settings and fail-closed admin key.
- [app/main.py](../app/main.py): register lifecycle/job/admin routes without changing health contracts.
- [docker/minio-init.sh](../docker/minio-init.sh) and [docker/minio-policy.json](../docker/minio-policy.json): add separate worker identity/policy while retaining API least privilege.
- [docker-compose.yml](../docker-compose.yml): add a single lifecycle worker; database polling must not depend on Redis.
- [tests/test_ingestion_integration.py](../tests/test_ingestion_integration.py): preserve and extend ingestion regressions.
- [.github/workflows/checks.yml](../.github/workflows/checks.yml), [README.md](../README.md), and [PLAN.md](../PLAN.md): add lifecycle gates and status/evidence.

Create new files only where ownership warrants them:

- `alembic/versions/0003_lifecycle_operations.py`
- `app/policies.py` for pure evaluation and bounded evaluation command
- `app/ownership.py` for extracted session advisory lock
- `app/lifecycle.py` for admission, claims, retries, and controlled recovery
- `app/tasks/lifecycle.py` for worker loop, publisher, and subscriber
- `docker/minio-worker-policy.json`
- `tests/test_policies.py` and `tests/test_lifecycle_integration.py`
- `scripts/verify_lifecycle.py`
- `docs/milestone-2.md` for results after they are actually measured

Avoid creating a broad repository/service abstraction hierarchy or a second operation model.

## Acceptance Checklist

- [ ] Real upload/policy/archive/restore round trip with exact checksums and correct states.
- [ ] Identical policy and safety guards for manual and automatic archive requests; decisions are explainable and versioned.
- [ ] State, job, audit, and outbox intent commit atomically; idempotency and one-nonterminal-job constraints hold under concurrency.
- [ ] PostgreSQL polling is sufficient for correctness and retries with Redis unavailable; hints never bypass persisted eligibility.
- [ ] Duplicate work causes no duplicate logical mutations; any reused destination is verified from actual bytes.
- [ ] No tested path deletes the last verified copy; restore retains archive; post-delete/database-failure recovery is demonstrated.
- [ ] Uncertain ownership cannot be automatically reclaimed or admin-retried; controlled same-job recovery is documented and tested.
- [ ] Retry exhaustion is inspectable; authenticated replacement retry preserves original failure/attempt history.
- [ ] Ingestion replay, access updates, and download issuance behave safely alongside lifecycle movement.
- [ ] Migrations, all local gates, security scans, and remote PR checks pass; evidence is documented without production or scale claims.
