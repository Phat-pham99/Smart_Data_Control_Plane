# Milestone 2, Phase 4: Redis Pub/Sub Wake-ups and Outbox Publisher

## Goal

Cut pickup delay for newly admitted jobs without changing correctness. Phase 3 already completes every job by polling PostgreSQL. Phase 4 adds best-effort notifications that wake the worker sooner.

```text
admission commit -> outbox row -> PUBLISH job_ready (best effort) -> subscriber hint -> same claim path as polling
                                      \-> relay retries unpublished rows; polling still finds every due job
```

Redis Pub/Sub is at-most-once. It has no persistence, acknowledgement or replay, and Redis AOF does not make messages durable. A lost, duplicated or malformed notification must never strand work, bypass due-time checks, or consume retry budget.

Out of scope: metrics and dashboards, multiple workers, broker dead-letter queues, replacing polling.

## Prerequisites

- Phase 3 is merged: `execute_job`, `run_once`, the poller and controlled recovery all exist and pass with Redis stopped.
- The claim path is the only way a job starts. Hints feed job IDs into it; they never execute work directly.

## Decisions

These choices are proposed. Confirm or change them before implementation.

| Topic | Proposal |
|---|---|
| Who publishes first | The API publishes right after the admission commit, with a short timeout. Failure never changes the HTTP result. If the worker were the only publisher, hints would arrive no faster than polling. |
| Relay | The worker loop also publishes outbox rows still unpublished after a grace period. Publishing twice is harmless. |
| Channel and payload | Channel `lifecycle:jobs:v1`. Payload `{"schema_version": 1, "event_type": "job_ready", "job_id": "<uuid>"}`, at most 256 bytes. No storage locations, state or object data. |
| Poll interval | Keep polling at startup and at a bounded interval. Once hints work, the interval may grow (for example from 2 s to 15 s), but that change must follow the latency/load measurement, not come before it. |
| Outbox budget | Retry with capped backoff (base 2 s, cap 60 s) up to 20 attempts, then leave the row unpublished with `last_error`. The job itself is unaffected because polling still finds it. |
| Schema | No migration expected. `outbox_events` already has `publish_attempts`, `next_publish_at`, `published_at` and `last_error`, plus the `ix_outbox_pending` index. |

## Workstreams and Order

### 4a. Notification contract and publisher

`app/notifications.py`:
- `JobReady` Pydantic model: `extra="forbid"`, `schema_version == 1`, `event_type == "job_ready"`, UUID `job_id`. `encode()` produces compact JSON; `decode(raw)` rejects payloads over 256 bytes, invalid JSON, unknown versions and unknown types.
- `redis_client()`: built from `REDIS_URL` with finite connect and socket timeouts (proposed 200 ms for the API, 2 s for the worker). No unbounded retries.
- `publish(client, event) -> int`: returns the subscriber count. A count of zero is not a failure and not proof of delivery.

Outbox state rules:
- **Claim:** in a short transaction, select due rows (`published_at IS NULL`, `next_publish_at <= now`, `publish_attempts < max`) with `FOR UPDATE SKIP LOCKED`. Push `next_publish_at` forward by a lease (for example 30 s) so a crash mid-publish delays the retry rather than losing it. Commit before any Redis call.
- **Publish:** `PUBLISH` with finite timeouts, outside any database transaction.
- **Record:** in a new short transaction. Success sets `published_at` and increments `publish_attempts`. Failure increments `publish_attempts`, sets the backoff `next_publish_at` and stores a sanitized `last_error` class (never connection strings or payloads).
- A crash between `PUBLISH` and the record step leads to a duplicate hint later, which is safe.

API integration in `app/lifecycle.py`:
- After `_enqueue` commits, call `publish_outbox_for_job(job_id)` best effort. Catch every Redis error, log a sanitized warning and return the same `202`.
- Do not publish inside the admission transaction or while holding the object lock.
- Replayed requests (same idempotency key) do not publish again; the relay covers anything unpublished.

Relay in the worker: `publish_once(db, client, now, limit)` runs once per loop iteration, covering rows the API failed to publish.

### 4b. Subscriber and hint handling

Worker process (`app/tasks/lifecycle.py`):
- A subscriber thread with its own Redis connection. It subscribes, reconnects with bounded backoff, and never blocks the polling loop. While Redis is down it logs and keeps retrying; polling continues.
- For each message: `decode()`. Invalid messages are counted and logged with a sanitized reason (`OVERSIZED`, `INVALID_JSON`, `UNKNOWN_SCHEMA`, `UNKNOWN_EVENT`, `INVALID_JOB_ID`) and cause no database or storage access.
- Valid job IDs go into a bounded, de-duplicating hint queue (proposed size 100). When it is full, new hints are dropped and counted. This is safe because polling remains authoritative.
- The executor loop waits on the hint queue with a timeout of "time until the next poll" on a monotonic clock. A hint triggers the normal claim for that job ID, which still checks status and `next_attempt_at`, so a hint cannot run a job early or a second time. Polling still runs on schedule even under a steady stream of hints.
- Shutdown closes the subscriber, stops new pickup, and lets the current job finish within a bounded time.

Deterministic test commands:
- `python -m app.tasks.lifecycle publish-once`
- `python -m app.tasks.lifecycle run-once`
- `python -m app.tasks.lifecycle hint --job-id ID`: injects a hint locally, test-only.

### 4c. Configuration and Compose

- `app/config.py`: `notification_channel`, `api_publish_timeout_ms`, `worker_redis_timeout_seconds`, `outbox_max_attempts`, `outbox_lease_seconds`, `hint_queue_size`. Each has bounds validation.
- `docker-compose.yml`: give the worker `REDIS_URL`, but do not make it depend on Redis being healthy. Correctness must not require Redis to start.
- API readiness keeps reporting Redis as a diagnostic only.

## Test Matrix

Unit tests use a fake Redis client and an injected clock. Integration tests use the real Redis container where noted. Redis outages are simulated with an unreachable URL inside the test suite; the real `docker compose stop/start redis` runs in the Phase 5 script.

| Scenario | Assertion |
|---|---|
| Hint wakes the worker | With a long poll interval, an admitted job is claimed after the hint without waiting for the poll. |
| Published before subscribing | Message lost; the next poll completes the job. |
| `PUBLISH` succeeds but nothing is delivered | Subscriber count 0 or the hint is dropped; the job still completes by polling; the row is marked published. |
| Duplicate hints | Repeated hints for the same job produce one claim and one attempt, with no extra storage calls. |
| Hint for a job not yet due (`RETRY_SCHEDULED`) | No claim before `next_attempt_at`. |
| Hint for a terminal, unknown or `RECOVERY_REQUIRED` job | Ignored with no side effects. |
| Malformed or poisoned messages | Each invalid class is logged and counted; no database or storage access. |
| Redis outage during admission | API returns `202` within the timeout budget; row stays unpublished; relay publishes after Redis returns; job completes by polling meanwhile. |
| Redis outage in the worker | Subscriber reconnects with backoff; polling keeps completing jobs; no crash loop. |
| Crash between `PUBLISH` and the record step | Row republished later; duplicate hint is harmless. |
| Outbox budget exhausted | Row stays unpublished with `last_error`; the job is unaffected. |
| Full hint queue | Hints dropped and counted; every job still completes. |
| Retry budget isolation | Notification failures never change `attempt_count`, `max_attempts` or `next_attempt_at`. |

Measurement, not a benchmark: pickup latency (admission commit to claim) at p50 and p95 over a fixed number of synthetic jobs, polling only versus hints plus polling, at two poll intervals. Record database query counts per minute for each setting. The results go into the Phase 5 evidence document.

## Exit Gate

- Every Phase 3 test still passes with Redis stopped.
- The test matrix passes. The full suite passes with `RUN_INTEGRATION=1`, with coverage of at least 75%.
- Latency and polling-load measurements are recorded with their limits: local only, one worker, small sample.
- Documentation states that notifications change latency only, not correctness, retry budgets or ordering.

## Open Questions

1. Should the API publish right after commit (proposed), or should only the worker relay publish, accepting less benefit?
2. Should the poll interval stay at 2 s after Phase 4, or be raised based on the measurement?
3. Is a 200 ms Redis timeout acceptable latency for an admission request during a Redis outage?
