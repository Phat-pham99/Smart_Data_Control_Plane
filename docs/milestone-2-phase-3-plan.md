# Milestone 2, Phase 3: Polling Worker and Verified Transfers

## Goal

Execute the archive and restore jobs that Phases 1-2 admit, using PostgreSQL polling only:

```text
PENDING job -> claim -> verify source -> copy -> verify destination -> (archive: delete source) -> commit
```

Correctness must not depend on Redis. Every successful movement is verified against the job's immutable binding (`content_version`, `expected_sha256`, `expected_size`, source/destination snapshots). No tested path may delete the last verified copy under the single-executor ownership assumption.

Out of scope: Redis Pub/Sub and the outbox publisher (Phase 4), reconciliation and orphan cleanup (Milestone 3), multi-worker fencing, production S3, metrics.

## Prerequisites

- Merge PR #11 (phases 1-2) into `main`, then branch `feature/milestone-2-worker` from it. This plan references code from that PR: `Job`/`JobAttempt`/`OutboxEvent`, `app/ownership.py`, `app/policies.py`, `app/lifecycle.py`.
- There is no CI workflow. Every gate below is a local run against the isolated Compose stack; record commands and results in the PR.

## Decisions

These choices are proposed. Confirm or change them before implementation.

| Topic | Proposal |
|---|---|
| Schema | No new migration expected. Stages live on `JobAttempt.stage`; settings carry poll/retry tuning. Add a migration only if a real gap appears. |
| Execution unit | One synchronous `execute_job(job_id)` shared by the poller and the recovery command. One executor slot, concurrency 1. |
| Stuck `RUNNING` jobs at startup | Never resume them. If the object lock is free, move the job to `RECOVERY_REQUIRED` (`WORKER_INTERRUPTED`). If the lock is busy, another session owns it, so leave it untouched. |
| Settled archive failure | Object returns to `ACTIVE` with desired `ACTIVE`. If the archive destination had already been verified, record it as a retained replica (`archive_bucket/key`, `archive_copy_retained=true`) rather than leaving unrecorded bytes. |
| Settled restore failure | Object returns to `ARCHIVED` with desired `ARCHIVE`. The archive copy is never deleted. |
| Recovery authorization | A bounded CLI requires explicit operator attestation that the old worker has stopped and in-flight requests have settled. The attestation is stored in the audit record. |
| Worker credentials | A separate MinIO user is required. The worker refuses to start with the API credentials or with no credentials. |

## Workstreams and Order

Each step should land with its own tests before the next one starts.

### 3a. Storage primitives and worker identity

`app/storage.py`:
- `inspect(bucket, key)`: `HEAD` that returns `None` for 404 and the size/ETag otherwise. Other errors propagate for classification.
- Reuse `verify()` for every source and destination check. It streams bytes, checks size and SHA-256, and returns the ETag. Never treat an ETag as a checksum.
- `copy(source, destination, source_etag, content_type, sha256)`: `copy_object` with `CopySourceIfMatch=source_etag` as the race precondition, plus `MetadataDirective=REPLACE`.
- `delete(bucket, key)`.
- `get_worker_storage()`: a client built from worker credentials (`WORKER_AWS_ACCESS_KEY_ID`, `WORKER_AWS_SECRET_ACCESS_KEY`). It fails closed when they are unset.

Do not rely on destination-conditional copy semantics. The worker is the only writer to the deterministic archive and active keys, and it always inspects the destination under the object lock before copying.

`docker/minio-worker-policy.json` and `docker/minio-init.sh`:
- `s3:ListBucket` and `s3:GetBucketLocation` on the active and archive buckets.
- `s3:GetObject` and `s3:PutObject` on `lifecycle-dev/objects/*` and `lifecycle-archive/objects/*`.
- `s3:DeleteObject` on `lifecycle-dev/objects/*` only. No archive delete, no staging access, no administration.
- Leave the API user's policy unchanged.

Tests (real MinIO): the worker can copy active to archive and delete active. The worker is denied archive deletion and staging writes. The API user is denied archive writes and active deletion.

### 3b. Claims, poller and failure classification

`app/lifecycle.py` (execution side):
- `due_job_ids(db, now, limit)`: select `PENDING` or `RETRY_SCHEDULED` jobs with `next_attempt_at <= now`, ordered by `(next_attempt_at, created_at, id)`. This query uses `ix_jobs_due`.
- `claim(db, job_id, lock, now)` runs under the object lock and only after reloading the job with `populate_existing`. In one short transaction it:
  - updates the job to `RUNNING` only if its status and due time still qualify (a conditional `UPDATE ... WHERE status IN (...)`);
  - increments `attempt_count` and sets `started_at`;
  - inserts `JobAttempt(attempt_number=attempt_count, stage="CLAIMED")`;
  - moves the object to `ARCHIVING` or `RESTORING`;
  - writes an audit event.
- If a duplicate poll finds the job already running or finished, it returns without touching storage.
- `record_stage(db, attempt, stage)`: a short commit per durable checkpoint. No transaction or row lock is held during storage I/O.

`app/tasks/lifecycle.py` (worker loop):
- `run_once(now, limit)` is deterministic and used by tests.
- `run()` loops forever: quarantine stuck `RUNNING` jobs at startup, poll at a bounded interval measured with a monotonic clock, and handle `SIGTERM` by stopping new pickup and letting the current job finish within a bounded time.
- The object lock is taken with `wait_seconds=0`. A busy object is skipped and polled again later. No in-memory queue.
- The clock and the jitter source are injected so tests never sleep.

Failure classes:

| Class | Examples | Outcome |
|---|---|---|
| Transient, settled | Connection or 5xx error on `HEAD`/`GET`/verify before any mutation in this attempt; database unavailable before the claim | `RETRY_SCHEDULED` with `next_attempt_at = now + min(cap, base * 2^(n-1)) * (1 + jitter)`. `FAILED` when `attempt_count >= max_attempts`. |
| Permanent | Source checksum or size mismatch, conflicting destination bytes, `AccessDenied`, `NoSuchBucket`, binding mismatch, policy rejection at revalidation | `FAILED` with an `error_class`; the object returns to its last verified serving state (see Decisions). |
| Uncertain | Timeout or connection loss around `copy` or `delete`; `OwnershipLost`; a commit failure after delete | `RECOVERY_REQUIRED`. Never retried automatically. If PostgreSQL is unreachable, the job stays `RUNNING` and startup quarantine or controlled recovery handles it. |

Starting settings in `app/config.py`, to be tuned by testing: `poll_batch_size=20`, `poll_interval_seconds=2`, `retry_base_seconds=2`, `retry_cap_seconds=60`, `retry_jitter_ratio=0.25`, `job_max_attempts=5` (already exists).

Update `ALLOWED_TRANSITIONS` and the tests in `tests/test_enums.py`:
- `ARCHIVE_REQUESTED/ARCHIVING -> ACTIVE` (settled failure)
- `RESTORE_REQUESTED/RESTORING -> ARCHIVED` (settled failure)
- `ARCHIVING -> ARCHIVED`
- `RESTORING -> ACTIVE`

### 3c. Archive and restore algorithms

Run every step while holding the object lock on its dedicated connection. Reload the job and object before each decision, and do not carry ORM state across storage I/O.

**Archive**
1. **Revalidate.** The binding must match the object (`content_version`, checksum, size). Re-run `evaluate()` against the job's own `policy_id` version with `owner_job_id=job.id` at server time. A change in access, retention or hold since admission is a permanent `POLICY_REJECTED`.
2. **Inspect** the source (active) and the destination (archive).
3. **Destination exists.** `verify()` it. If it matches, reuse it and skip the copy. If it doesn't, record a permanent `DESTINATION_CONFLICT`, keep the source, and never overwrite.
4. **Destination absent.** `verify()` the source to get its ETag, then `copy` with `CopySourceIfMatch`. Stage `COPIED`.
5. **Verify the destination** from its actual bytes. Stage `DESTINATION_VERIFIED`.
6. **Prepare deletion.**
   - Call `lock.verify()`.
   - Reload the object and revalidate the guards and binding again.
   - Persist stage `DELETE_INTENT`.
7. **Delete the source.** Stage `SOURCE_DELETED`.
8. **Commit in one transaction:**
   - object `ARCHIVED`, desired and observed `ARCHIVE`;
   - `archive_bucket/key` from the job's destination, `archive_copy_retained=false`;
   - job `SUCCEEDED` with `finished_at`;
   - attempt `COMMITTED`;
   - audit event.

**Archive resume rules** (applied by recovery or re-execution of the same job):
- Source absent and destination verifies: finish the same job only if this job's attempts include a `DELETE_INTENT` stage. Otherwise the job goes to `RECOVERY_REQUIRED` with `UNEXPLAINED_SOURCE_LOSS`.
- Both copies absent, or the evidence is inconsistent: set observed placement to `UNKNOWN` and the job to `RECOVERY_REQUIRED` with `DATA_MISSING`. No mutation.
- Source present and destination corrupt: keep the source and fail permanently as `DESTINATION_CONFLICT`.

**Restore**
1. Revalidate the binding. The object must be `RESTORE_REQUESTED/RESTORING`, and the job source must equal the object's archive locator.
2. `verify()` the archive source. A mismatch fails permanently and the archive is never touched.
3. Inspect the active destination. If present, reuse it only if `verify()` passes; otherwise record a permanent `DESTINATION_CONFLICT`. If absent, `copy` it.
4. Verify the active bytes, then commit in one transaction: object `ACTIVE`, observed `ACTIVE`, `archive_copy_retained=true`, `restored_at`, job `SUCCEEDED`, audit event.
5. Never delete the archive copy. Re-executing a completed restore returns without copying.

**Re-archive after restore:** the retained archive copy verifies, so the copy step is skipped and only the active copy is deleted after verification.

### 3d. Controlled recovery

Command: `python -m app.tasks.lifecycle recover --job-id ID --operator NAME --attest-worker-stopped --attest-requests-settled`

- It refuses unless the job is `RECOVERY_REQUIRED`, or `RUNNING` with a free object lock, and both attestation flags are present.
- It acquires the same object lock, appends a new attempt with stage `RECOVERY`, inspects the actual bytes, and resumes the same job using the rules above.
- Outcomes are success, a permanent failure, or remaining blocked for manual review. It never loops automatically.
- It records the operator and the attestation in the audit event details.
- Admin retry continues to reject `RECOVERY_REQUIRED` and `RUNNING` jobs, as implemented in Phase 2.

### 3e. Compose service, evidence script and documentation

- Add a `lifecycle-worker` service to `docker-compose.yml`:
  - command `python -m app.tasks.lifecycle run`;
  - worker MinIO credentials;
  - depends on `postgres` and `minio-init` only (no Redis).
  - Keep the legacy Celery worker behind its profile.
- `scripts/verify_lifecycle.py`, a bounded synthetic run:
  - upload a 1 MiB object;
  - run `evaluate` and `enqueue` with an explicit historical evaluation time for the dry run;
  - run the worker until the archive succeeds and check the hashes;
  - restore, check the hashes again, and download.
  - It prints object/job IDs and digests only, never URLs or credentials.
- Update the README status and API notes. Add `docs/milestone-2.md` only after measured evidence exists.

## Test Matrix

Unit tests use a fake in-memory storage with failpoints, an injected clock and injected jitter; they never sleep. Integration tests use real PostgreSQL and MinIO. Success paths must perform real transfers, not database edits.

| Scenario | Assertion |
|---|---|
| Happy archive then restore | Exact SHA-256 and size at each boundary; active copy absent after archive; both copies valid after restore; `archive_copy_retained=true`; download bytes match. |
| Re-archive after restore | Copy skipped, archive reused after verification, active copy deleted, `ARCHIVED`. |
| Polling only | Redis stopped (`docker compose stop redis`); `run_once` completes archive and restore. |
| Duplicate polling | Two `run_once` calls on the same job produce one claim, one attempt and no extra storage calls. |
| Busy object | Lock held by another session: job skipped, still `PENDING`, picked up later. |
| Revalidation | Access recorded or legal hold set after admission: `POLICY_REJECTED`, object `ACTIVE`, source intact. |
| Destination conflict | Corrupt bytes pre-planted at the archive key: no delete, `FAILED`, source intact. |
| Source corruption | Active bytes replaced out of band: `FAILED`, archive not written as success. |
| Copy race | Source changed between verify and copy: precondition failure, job not `SUCCEEDED`. |
| Transient read failure | Injected error on `verify`: `RETRY_SCHEDULED`, due time honoured, exhaustion leads to `FAILED` with attempt history. |
| Uncertain copy or delete | Timeout injected on `copy` or `delete`: `RECOVERY_REQUIRED`, no automatic retry, admin retry rejected. |
| Lost lock before delete | `lock.verify()` fails: no delete, `RECOVERY_REQUIRED`. |
| Commit failure after delete | Injected `Session.commit` failure: job blocked; `recover` verifies the archive copy and completes the same job; source never re-copied from a missing location. |
| Both copies missing | `observed_storage_class=UNKNOWN`, `RECOVERY_REQUIRED` with `DATA_MISSING`, no mutation. |
| Startup quarantine | A `RUNNING` job with a free lock becomes `RECOVERY_REQUIRED`; with a held lock it is untouched. |
| Recovery guard | Missing attestation flags, or a job not in a recoverable state: refused with no side effects. |
| Permissions | Worker and API credential denials as listed in 3a. |
| Ingestion and download | Upload replay after archive issues no form; a download request during `ARCHIVING` or `ARCHIVED` returns 409; after restore it succeeds. |

## Exit Gate

- Ruff lint and format pass. The full suite passes with `RUN_INTEGRATION=1` and no integration skips. Coverage is at least 75%, and every destructive path has an explicit test.
- Migrations are checked with upgrade, downgrade, re-upgrade and `alembic check` on a disposable database. This only applies if 3b adds a migration.
- `docker compose up` boots the API and the lifecycle worker. `scripts/verify_lifecycle.py` completes archive and restore with matching digests while Redis is stopped.
- A controlled recovery after a post-delete commit failure is demonstrated without deleting the last verified copy.
- Limits are documented:
  - local MinIO only;
  - single worker;
  - an advisory lock is not distributed fencing;
  - a presigned URL issued before archival may stop working after the active copy is deleted;
  - no CI runs.

## Open Questions

1. Should a settled archive failure that leaves a verified archive copy record it as a retained replica (proposed), or leave it unrecorded for Milestone 3 reconciliation?
2. Is startup quarantine of `RUNNING` jobs to `RECOVERY_REQUIRED` acceptable, or should the worker only report them?
3. Should recovery attestation be a pair of CLI flags (proposed), or also require `ADMIN_API_KEY`?
