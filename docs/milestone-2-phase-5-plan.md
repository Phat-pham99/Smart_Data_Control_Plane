# Milestone 2, Phase 5: Failure Evidence, Demo Script and Documentation

## Goal

Turn Phases 1-4 into a reproducible, documented Milestone 2 result:

- every scenario in the Milestone 2 evidence table has a named test or scripted run, with recorded outcomes;
- a scripted synthetic demo of upload, policy decision, archive, restore and verify that a new user can run;
- documentation that separates implemented, locally tested and planned behavior, and records limits honestly.

Phase 5 adds no new features. If evidence exposes a defect, fix it in a focused change with its own test and record it, including when an assumption turned out wrong.

## Prerequisites

- Phases 3 and 4 are merged.
- An isolated Compose project and database are used for every destructive check (for example `COMPOSE_PROJECT_NAME=lifecycle-m2-evidence`). Migration downgrade and failure injection run only there.

## Decisions

These choices are proposed. Confirm or change them before implementation.

| Topic | Proposal |
|---|---|
| CI | There is no workflow, so local gates are the only gates. Optionally restore a minimal workflow (lint, unit tests, migrations, integration tests with PostgreSQL, Redis and MinIO). This is a separate decision for you. |
| Runtime failpoints | Kill and restart tests need the worker to pause at a chosen stage. Use an env var `LIFECYCLE_FAILPOINT=<stage>:pause` that is honored only when `ENVIRONMENT=test`, ignored otherwise, and never set in the default Compose file. |
| Image scan | The pinned Trivy release was removed upstream. Run a currently published Trivy version locally and record its version, or record the scan as not run. Never claim a scan that did not happen. |
| PLAN.md consistency | PLAN.md still describes RabbitMQ, publisher confirms and a broker dead-letter queue. Update it to the Redis Pub/Sub plus polling design actually implemented. |

## Workstreams and Order

### 5a. Gap analysis against the evidence table

Map each row of the Milestone 2 evidence table to existing tests from Phases 1-4 and list the gaps. Expected gaps:

- Ingestion replay after archive and after a failed lifecycle job: no new upload form and no re-ingestion.
- Download behavior: new URLs refused while `ARCHIVE_REQUESTED`, `ARCHIVING`, `ARCHIVED` or `UNKNOWN`; a URL issued before archival stops working after the active copy is deleted (documented limitation); a download after restore matches the original bytes.
- Metadata changes during a queued job: a change to access time, retention or legal hold is caught by worker revalidation; an access update during movement returns a retryable `409` and is never silently dropped.
- Authorization: API and worker storage credentials tested independently; objects stay private to anonymous requests.
- Runtime interruption with a real process kill (5b).

### 5b. Runtime interruption and Redis scenarios

`scripts/verify_lifecycle.py` gains bounded scenarios. Each one waits on explicit conditions (job state, readiness), never fixed sleeps, and has a timeout.

| Scenario | Procedure | Required outcome |
|---|---|---|
| `happy` | Upload 1 MiB, evaluate with explicit `--at`, enqueue, run the worker, restore, download | Matching SHA-256 at each boundary; archive copy retained after restore |
| `polling-only` | `docker compose stop redis`, then `happy` | Same outcome; restart Redis afterwards |
| `redis-restart` | Restart Redis while jobs are queued | All jobs complete; outbox rows eventually published or left with `last_error` |
| `duplicate` | Same request twice, competing request, duplicate hints | One job, one execution, `409` for the competitor |
| `kill-after-copy` | Failpoint pauses after `DESTINATION_VERIFIED`; `docker compose kill lifecycle-worker`; confirm stopped; start the worker | Startup quarantines the job to `RECOVERY_REQUIRED`; the recovery command reuses the verified archive copy and completes the same job |
| `kill-after-delete` | Failpoint pauses after `SOURCE_DELETED`, then kill | Recovery finds the source gone and the archive verified, sees `DELETE_INTENT`, and completes; the last verified copy is never deleted |
| `corrupt-destination` | Plant wrong bytes at the archive key before running | `FAILED` with `DESTINATION_CONFLICT`; source intact |

Each run prints a JSON summary: scenario, object and job IDs, digests, byte counts, durations, final states, and any manual step. It never prints signed URLs or credentials. Cleanup removes only resources the script created, identified by a generated run-ID owner prefix.

### 5c. Local gates

Run in order and record the exact commands, tool and service versions, and results:

```bash
uv sync --frozen --extra dev
uv run --frozen ruff check . && uv run --frozen ruff format --check .
uv run --frozen pytest -m 'not integration'
# Isolated stack only: downgrade is destructive.
uv run --frozen alembic upgrade head && uv run --frozen alembic downgrade base
uv run --frozen alembic upgrade head && uv run --frozen alembic check
RUN_INTEGRATION=1 uv run --frozen pytest --cov=app --cov-fail-under=75
uv run --frozen pip-audit
docker compose up -d --build --wait api lifecycle-worker
uv run --frozen python scripts/verify_lifecycle.py --scenario all
```

The full suite must run with no integration skips. Do not lower the coverage gate. Check that each destructive path (delete, recovery completion, quarantine) has a direct test even if total coverage is already high.

### 5d. Documentation

- `docs/milestone-2.md`: an evidence report in the style of `docs/milestone-1.md`.
  - Scope, reproduction commands and environment.
  - Gate results and the scenario table with actual outcomes, durations, bytes stored and recovery steps.
  - Phase 4 latency and polling-load measurements.
  - Limits.
  - Defects found and fixed.
- `README.md`:
  - Milestone 2 status becomes "locally verified".
  - API list and lifecycle description match the code.
  - Demo instructions point to `scripts/verify_lifecycle.py`.
  - Stale claims are removed.
- `PLAN.md`: replace the RabbitMQ and dead-letter-queue design text with the Redis Pub/Sub, polling and outbox design, and update the failure table to name `RECOVERY_REQUIRED` and controlled recovery.
- Keep `docs/milestone-1.md` historical; do not rewrite its evidence.

## Exit Gate (Milestone 2 Acceptance)

- [ ] Real upload, policy, archive and restore round trip with exact checksums and correct states.
- [ ] Manual and policy-driven archive requests use the same guards; decisions are explainable and versioned.
- [ ] State, job, audit and outbox intent commit together; idempotency and the one-unfinished-job rule hold under concurrency.
- [ ] Polling alone is enough for correctness and retries with Redis unavailable; hints never bypass persisted eligibility.
- [ ] Duplicate work causes no duplicate effects; any reused destination is verified from its actual bytes.
- [ ] No tested path deletes the last verified copy; restore keeps the archive copy; post-delete recovery is demonstrated.
- [ ] Uncertain jobs cannot be reclaimed automatically or retried by an admin; controlled recovery of the same job is documented and tested.
- [ ] Retry exhaustion is inspectable; admin retry keeps the original failure and attempt history.
- [ ] Upload replay, access updates and download links behave safely alongside lifecycle movement.
- [ ] All local gates pass and are recorded; no production, scale, S3-equivalence or CI claims.

## What Remains After Milestone 2

These belong to Milestones 3 and 4:
- reconciliation of catalog intent against storage;
- persisted findings for missing, corrupt, orphaned and stuck objects;
- staging cleanup;
- minimal metrics;
- the complete failure-matrix demo;
- the discovery benchmark;
- the final evaluation report.

## Open Questions

1. Restore a minimal CI workflow, or keep local gates only?
2. Is an env-var failpoint enabled only under `ENVIRONMENT=test` acceptable for kill and restart evidence?
3. Should `PLAN.md` be updated in this phase (proposed) or in a separate documentation PR?
