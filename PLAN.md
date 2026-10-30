# Smart Data Control Plane

## Consolidated Implementation and Evaluation Plan

> **Purpose:** A production-inspired reference system and evidence-backed portfolio project, not an enterprise storage platform.
> **Principle:** The control plane decides. Object storage executes. The reconciler verifies.
> **Status:** This document specifies target behavior. A feature is demonstrated only when its implementation, tests, and reproducible evidence exist.

Milestone 1 now has [local ingestion and delivery-gate evidence](docs/milestone-1.md). Later milestones remain targets, not implemented features.

## 1. Problem and Engineering Question

Manage synthetic engineering-style objects using business metadata such as workflow completion, application access, legal hold, and retention requirements. PostgreSQL stores searchable metadata and durable intent; S3-compatible storage holds object bytes.

**Storage direction:** Use MinIO for the local MVP, integration tests, and demonstrations. A future production-ready version should use **Amazon S3 for globally accessed, terabyte-scale object data**. This is a deployment target, not a claim of demonstrated production readiness or physically tested scale.

Native storage lifecycle rules remain useful for storage-level conditions. This project investigates decisions that require application metadata and explicit recovery across independent systems. It does not claim that a custom control plane is always preferable.

**Central question:** Under what workload and failure conditions does an application-aware metadata control plane justify its additional consistency and operational overhead?

Evaluate three hypotheses rather than assuming the architecture is beneficial:

| Hypothesis | Discriminating evidence |
| --- | --- |
| Selective catalog queries reduce discovery latency and storage requests enough to justify metadata maintenance. | Equivalent-result baselines across row counts and selectivities, plus catalog write latency, index size, and synchronization work. |
| Durable intent, idempotent execution, and reconciliation recover from specified interruptions without deleting the last verified copy. | Failure injection at each durable boundary; record data integrity, terminal state, recovery time, and manual intervention. |
| Metadata-driven selection can reduce modeled total cost for some access patterns, but not all. | Optional sensitivity analysis including retained copies, retrieval, requests, transitions, and minimum-duration charges. |

The first two hypotheses define the core project. Cost modeling is a later extension, not a prerequisite for a credible demonstration. Negative results and failed assumptions are valid findings.

## 2. Scope and Completion Boundary

### Core demonstrable release

- Docker Compose: FastAPI, PostgreSQL, Redis, MinIO, and one subscriber/database-polling execution worker.
- Upload initiation and completion, verified ingestion, metadata discovery, and download.
- One deterministic, versioned archive policy with explainable acceptance and rejection.
- Asynchronous archive and restore with durable intent, bounded retries, and queryable failures.
- SHA-256 verification, per-object serialization, and request/message idempotency.
- Bounded reconciliation for missing, duplicate, corrupt, orphaned, and stuck objects or operations.
- Structured logs, audit history, minimal metrics, integration tests, and failure-injection tests.
- One reproducible demonstration and a short correctness/performance report.

**Admissions-ready boundary:** A clean checkout can execute upload -> evaluate -> archive -> restore -> verify, recover from injected failures, and reproduce a modest-scale benchmark. A technology list or architecture diagram alone does not meet this boundary.

### Deferred until the core evidence passes

- Rich policy administration, scheduled bulk enforcement, and folder fan-out.
- Grafana dashboard and broader operational metrics.
- Configurable cost estimator and 5 TB, 10 TB, and 20 TB logical scenarios.
- Larger metadata-only experiments, including optional 1M and 10M rows.
- Production deployment on Amazon S3, multi-worker scaling, enterprise identity, and infrastructure as code.
- Recommendation-only ML, only with a dataset, baseline, and a separate justification.

### Production storage roadmap after the core release

MinIO remains the backend for Milestones 1-4. After their exit criteria pass, target Amazon S3 as the managed storage backend for a production deployment serving globally distributed users and terabyte-scale datasets. Do not expand the MVP into a multi-cloud abstraction or treat S3-compatible APIs as proof of identical behavior.

Before declaring that deployment production-ready:

- Select AWS regions and document data residency, access latency, recovery objectives, and any replication requirements. S3 buckets are regional; global access does not imply automatic cross-region replication or a globally distributed control plane. Multi-region deployment remains a separate post-MVP decision.
- Establish authentication and object-level authorization, least-privilege IAM roles, private bucket access, encryption/key management, audit visibility, and storage permissions that preserve immutable object ownership.
- Run the ingestion, archive/restore, idempotency, and failure-recovery contracts against real S3. Validate signed requests, multipart uploads/copies, checksums, pagination, throttling/timeouts, and any enabled versioning or retention behavior. Do not assume MinIO tests cover AWS-specific semantics.
- Choose S3 storage classes based on access and retention requirements. Validate retrieval availability, restoration behavior, minimum-duration charges, and ownership of lifecycle rules before enabling archival classes; two local buckets do not emulate Glacier semantics.
- Measure catalog/query performance, worker and reconciliation throughput, recovery behavior, and request/data-transfer/storage costs under representative workloads. S3 storage capacity does not establish the scalability of the PostgreSQL catalog or single-worker control plane. Label projected capacity separately from physically exercised bytes.
- Document deployment, monitoring/alerting, metadata backup and recovery, verified data migration, and rollback procedures. Preserve the no-last-verified-copy-deletion invariant during migration.

### Not claimed by this project

- Exactly-once delivery or atomic transactions across PostgreSQL and object storage.
- Glacier-equivalent retrieval behavior from two MinIO buckets.
- Physical storage savings from merely moving bytes between local buckets.
- Production readiness, unrestricted public deployment, or unmeasured scale.
- Kubernetes, multi-region replication, a custom frontend, or event sourcing.

## 3. Starting Point and Target Architecture

The repository now contains verified catalog ingestion, real PostgreSQL/MinIO integration tests and delivery gates. The original Celery health bootstrap remains opt-in legacy code; it is not the future lifecycle execution adapter.

**Decision:** Keep Redis Pub/Sub as best-effort operation-ID wake-ups and independently poll PostgreSQL for durable work and due retries. Pub/Sub has no persistence, acknowledgement, replay, or redelivery. PostgreSQL, not Redis or a Celery result backend, is authoritative for operation status. Do not add another broker.

```text
Client --HTTP--> FastAPI --transactions--> PostgreSQL
  |                                         |
  +--presigned upload--> MinIO staging       +-- operations + outbox
                                            |
                                      outbox publisher
                                            |
                                        Redis Pub/Sub
                                            |
                                    subscriber + DB poller
                                      /          \
                               MinIO active   MinIO archive

Reconciler compares catalog intent, operation progress, and storage facts.
Logs, audit events, and metrics provide evidence throughout.
```

Use one codebase and the existing `app/`, `alembic/`, and `tests/` layout. Add modules only when an implemented behavior needs them; do not reorganize the repository into multiple applications for appearance. Keep policy predicates and state-transition rules independent of infrastructure adapters.

The publisher and reconciler may be bounded CLI commands invoked by a documented scheduler. The execution adapter independently polls pending work and due retries at startup and at bounded intervals, even without notifications. Start with one execution worker at concurrency 1; this limits the experiment and is not a substitute for database-enforced serialization.

## 4. Reliability Contract

### Identity and immutable bytes

- Assign each upload an object UUID and an immutable content version. Keep a user-facing logical key separate from the generated physical key.
- Upload into a short-lived staging namespace. Clients must not be able to overwrite verified active or archive objects.
- Enforce the demo upload limit at the upload boundary, validate size and SHA-256, promote to a private immutable key, and verify the promoted bytes before marking the catalog `ACTIVE`.
- Compute SHA-256 by streaming stored bytes during verification; do not equate an ETag or client-supplied metadata with a trusted checksum. Server-side copies are allowed, but verification still incurs reads.
- Retried initiation/completion returns the same resource for the same scoped idempotency key and request fingerprint. Reusing a key with different content or metadata is a conflict.
- New content creates a new version. Lifecycle operations bind to an exact immutable version and expected checksum.
- External administrative mutation is outside normal write ownership and is exercised only as a documented drift scenario. No safety guarantee assumes protection against an administrator modifying bytes during verification.

### Durable intent and delivery

- In one PostgreSQL transaction, validate the transition, persist desired state, create the operation, record the audit event, and insert an outbox event.
- The publisher emits ephemeral Redis Pub/Sub operation-ID notifications. Record publication attempts, not acknowledged delivery: `PUBLISH` success or subscriber count does not establish receipt or execution.
- A crash between publication and recording the attempt can duplicate a notification. Notification and polling paths load the persisted operation and skip terminal work without further storage mutation.
- Independently poll pending operations and due retries in bounded indexed batches, including on startup/reconnect. Missed notifications, Redis outages and subscriber disconnects must not strand committed work or block the database poller.
- Notifications contain validated operation identifiers, not authoritative storage locations or state. Workers load those facts from PostgreSQL.

### Concurrency and operation ownership

- Enforce at most one non-terminal lifecycle operation per object with a database constraint. Conflicting archive/restore requests return a conflict; duplicate requests return the existing operation.
- Serialize storage mutations with a per-object PostgreSQL session advisory lock held on a dedicated connection across the operation, including verification and source deletion. Do not hold a database transaction open during storage I/O.
- Reconciliation repairs and upload finalization obey the same object lock. Workers recheck the operation, object version, and allowed transition after acquiring it.
- If the lock connection is lost, stop further storage mutations and fail closed. Before replacement execution after an uncertain worker failure, terminate the old worker and ensure in-flight requests have settled. A timeout alone is not proof of exclusive ownership.
- The core release assumes controlled single-worker restart and bounded storage request timeouts. Live takeover and distributed fencing across concurrent workers are deferred and must not be claimed as solved.

### Archive and restore

1. Claim the persisted operation and record its started state.
2. Inspect source and destination. Reuse a destination only if its actual size and SHA-256 match the immutable catalog version.
3. Copy when necessary, then verify destination bytes before committing completion.
4. For archive, delete the active copy only after successful verification, then commit `ARCHIVED`, observed class `ARCHIVE`, operation success, and audit history.
5. For restore, retain the archive copy, verify the active copy, then commit `ACTIVE`, observed class `ACTIVE`, and `archive_copy_retained=true`.

If the database commit fails after source deletion, recovery verifies the surviving destination before completing the same operation. A missing source is not automatically a failure when a verified destination already exists. Corrupt or ambiguous destinations block destructive progress and generate an inspectable finding.

### Retry and terminal failure

- Persist attempts and retry deadlines; use bounded exponential backoff with jitter for classified transient failures.
- Do not retry malformed commands, checksum conflicts, or invalid transitions as transient errors.
- PostgreSQL retry scheduling is authoritative; Pub/Sub does not schedule retries.
- Exhausted operations remain `FAILED` with queryable attempt history in PostgreSQL. There is no broker-native dead-letter queue.
- Malformed notifications are rejected and logged/counted without storage actions; persisted operation failures remain available for administrative review.
- An explicit admin retry creates a linked replacement operation with its own idempotency key and attempt budget. Preserve the original failure and revalidate current state.

## 5. Minimal Domain Model and State Semantics

Map these concepts onto existing models through migrations; do not silently repurpose incompatible fields. Content version and optimistic row version are different concepts.

| Record | Required information |
| --- | --- |
| Storage object | UUID, logical key, immutable content version and physical key, bucket locations, size, SHA-256, content type, owner/dataset, workflow state, legal hold, retention deadline, last application access, desired/observed class, lifecycle state, retained-archive flag, selected policy version, timestamps, row version. |
| Policy version | Stable policy identity, version, enabled status, priority, validated conditions/action, activation/supersession timestamps. Activated versions are immutable. |
| Lifecycle operation | UUID, object/version, type, scoped idempotency key and request fingerprint, status, attempts/budget, retry deadline, error/reason, timestamps, replacement link. |
| Operation attempt | Operation ID, attempt number, stage reached, start/end timestamps, error classification and sanitized detail. |
| Outbox event | UUID, operation/event type, validated payload, publication-attempt state, claim/retry timestamps, last-published timestamp; never a delivery acknowledgement. |
| Audit event | Object/operation IDs, actor, event type, old/new state, reason, timestamp. |
| Reconciliation finding | Object or storage location, type, severity, evidence, first/last seen, resolution and repair operation. |

Object lifecycle states:

```text
INGESTING -> ACTIVE -> ARCHIVE_QUEUED -> ARCHIVING -> ARCHIVED
ARCHIVED -> RESTORE_QUEUED -> RESTORING -> ACTIVE

FAILED, MISSING, and DRIFTED describe exceptional states.
Recovery transitions require recorded evidence and explicit validation.
```

- Desired class is `ACTIVE` or `ARCHIVE`; observed class is `ACTIVE`, `ARCHIVE`, or `UNKNOWN` and records the last verified serving placement, not a complete replica inventory.
- Use separate findings and `archive_copy_retained` to explain replicas. An active object with an intentionally retained, valid archive copy is not drift.
- Restore has no separate `RESTORED` steady state; use `restored_at` and audit history.
- Operation states distinguish queued, running, retry-scheduled, succeeded, failed, and cancelled work.
- A failed or blocked operation must not invent a new observed storage fact. Record uncertainty explicitly.

Required database guarantees and indexes:

- Unique object logical identity/version and unique physical location.
- Unique scoped idempotency keys and at most one non-terminal operation per object.
- Partial archive-candidate index for active objects, ordered to support the actual selective predicates and cursor.
- Owner/archive-time lookup index; retry-deadline and pending-outbox indexes.
- Validate index choices with measured plans, not just their presence in a migration.

## 6. Deterministic Policy and Minimal Interfaces

Begin with one seeded, schema-validated policy version:

```yaml
name: archive-completed-cold-datasets
version: 1
priority: 100
conditions:
  workflow_state: COMPLETE
  inactive_for_days: 180
  minimum_remaining_retention_days: 180
  minimum_size_bytes: 1048576
action:
  desired_storage_class: ARCHIVE
```

Active placement, no in-flight operation, no legal hold, and a compatible retention deadline are mandatory guards, not optional policy settings. In this demo, legal hold blocks archive movement even though archival is not retention deletion. Null or unknown required metadata is rejected with a reason code.

Evaluation takes an explicit UTC evaluation time and returns eligibility, immutable policy identity/version, and reason codes for both accepted and rejected objects. Use stable priority/version/identity ordering if multiple policies are later introduced. Persist the decision inputs and evaluation time with the audit evidence. Immediately before persisting intent and before storage mutation, revalidate safety guards and metadata version.

Use bounded indexed candidate selection, then domain evaluation. Candidate filtering must not exclude objects that could satisfy the policy. Explain rejection examples by directly evaluating selected objects, not by pretending SQL-filtered rows were evaluated.

Core API, retaining the existing `/v1` convention:

```text
POST /v1/uploads/initiate
POST /v1/uploads/{object_id}/complete
GET  /v1/objects
GET  /v1/objects/{object_id}
GET  /v1/objects/{object_id}/download
POST /v1/objects/{object_id}/access
POST /v1/objects/{object_id}/archive
POST /v1/objects/{object_id}/restore
GET  /v1/jobs/{job_id}
POST /v1/admin/jobs/{job_id}/retry
GET  /health
GET  /health/ready
GET  /metrics
```

Provide bounded CLI commands for seeded-policy evaluation, outbox dispatch, retry dispatch, reconciliation, and finding inspection before adding dedicated admin APIs. Archive requests, whether manual or policy-driven, use the same safety guards. Application access timestamps are reported explicitly; they do not represent every direct storage read.

Object queries support owner, dataset, workflow, lifecycle, desired/observed class, creation/access time, and bounded cursor pagination with a deterministic tie-breaker. Downloads use short-lived URLs for verified active objects; return a conflict for archived or uncertain placement.

## 7. Reconciliation and Failure Evidence

Reconcile catalog rows and storage listings in separate bounded, resumable passes. Indexed discovery avoids bucket enumeration during ordinary queries; orphan detection still needs listing and is measured as maintenance overhead.

| Injected condition | Required outcome and recovery |
| --- | --- |
| Upload succeeds; final catalog commit fails | Inspect staging/private destination and finish verified registration, or report an orphan after a grace period. Never infer ownership from a key alone. |
| Commit succeeds; publication is interrupted | Independent database polling picks up the original operation; pending outbox publication may reduce latency. |
| Publication succeeds; attempt is not committed | Duplicate publication has no additional logical effect. |
| Notification is lost or Redis/subscriber restarts | Startup and periodic database polling recover eligible work without notification replay. |
| Worker stops after claim, copy, verification, or source deletion | After exclusive ownership is re-established, inspect real bytes and resume the same operation without deleting the last verified copy. |
| Duplicate command, notification or poll result | At most one active executor per object; completed work is skipped without new mutations. |
| Transient MinIO, PostgreSQL, or Redis failure | Bounded database-scheduled retries and visible attempt history; Redis outages cannot block database pickup. |
| Checksum mismatch or conflicting destination | No source deletion or success state; record finding and require review of ambiguous bytes. |
| Expected active/archive copy is missing | Mark missing or drifted; copy from a verified alternative only when catalog intent authorizes it. Otherwise require manual recovery. |
| Both copies exist | Accept explicitly retained restore copies; otherwise classify using operation stage and intent before any repair. |
| Unregistered storage object | Record orphan after upload grace period; no automatic adoption or deletion. |
| Operation is stuck | Record finding, distinguish pending delivery from uncertain ownership, and recover only after the ownership rule is satisfied. |
| Malformed notification or exhausted retry budget | No unsafe mutation; logged notification diagnostics and queryable database failure evidence. |

Safe automatic repairs are limited to republishing durable intent, resuming exclusively owned operations, verified authorized copies, and evidence-backed metadata completion. Destructive ambiguity requires manual review. Findings must remain visible even when repaired.

## 8. Delivery Milestones

Implement in order. At every milestone, update README status as implemented/tested/planned, run the full available test suite and required CI gates, and record commands, environment, results, and limitations. A blocked integration test is not a passed exit criterion.

### Milestone 1: Verified catalog round trip

Deliver MinIO buckets/permissions alongside existing Redis/PostgreSQL, required schema migrations, upload/finalization, download, indexed metadata query, audit history, and structured logs. Keep README claims aligned with the actual local architecture.

**Exit evidence:** A synthetic object is uploaded, verified, queried, and downloaded with matching SHA-256; duplicate initiation/completion creates no conflict; conflicting key reuse is rejected; metadata and bytes survive stack restart. Integration tests use real local PostgreSQL and MinIO, not mocks alone.

### Milestone 2: One complete lifecycle

Deliver the seeded deterministic policy, candidate selection, transactional outbox publisher, Pub/Sub subscriber plus independent database polling, per-object ownership, archive/restore worker, operation status, bounded retries and persisted failure review.

**Exit evidence:** Upload -> explain decision -> archive -> verify -> restore -> verify works. Legal hold and retention guards reject unsafe archival. Duplicate notifications/polling, conflicting commands, publication interruptions, subscriber disconnects, Redis outages and corrupt destinations satisfy the reliability contract. Measure database pickup latency and explain why a restored retained archive copy is expected.

### Milestone 3: Recovery and reproducible demonstration

Deliver bounded reconciliation, persisted findings, the failure matrix above, minimal metrics, and a scripted demonstration with explicit failure injection and cleanup restricted to generated test resources.

**Exit evidence:** Every failure scenario records its invariant, expected recovery, actual outcome, recovery time, and any manual action. Tests assert byte integrity and catalog/operation state, not only HTTP responses. No ambiguous finding triggers automatic destructive repair.

### Milestone 4: Evaluation and public evidence

Deliver a seeded workload generator, fair discovery benchmark, query plans, repeated measurements, architecture/state-machine/consistency documentation, and a short results report.

**Exit evidence:** A new user can reproduce the core demonstration and a modest-scale experiment from a clean checkout. The report answers the first two engineering hypotheses, quantifies overhead, separates logical from physical scale, and documents tested design assumptions, including any that failed or remained unsupported. Do not invent a design-change story if none occurred.

Completion of Milestones 1-4 is the core release. Only then consider the deferred roadmap.

Amazon S3 production deployment is a subsequent phase governed by the storage readiness gates in Section 2, not a requirement for MVP completion. Keep the MVP demonstrations and production-scale evidence separately labeled.

## 9. Experimental Method

### Discovery and maintenance

- Start with 100K catalog rows, plus a separately declared locally practical population of real small objects. Run larger logical datasets only when the initial experiment is reproducible.
- Generate reproducible tenant/dataset skew, size distributions, hot/warm/cold access, legal holds, and retention boundary cases using recorded seeds and a fixed evaluation time.
- Compare indexed SQL with the same catalog query without the candidate index to isolate index benefit. Use an isolated benchmark database and report plans, other indexes, and any forced planner settings.
- For namespace enumeration, materialize equivalent synthetic business metadata in S3 object metadata for the physically stored population. List keys and fetch metadata as needed; validate equal ordered result IDs against the catalog snapshot.
- Freeze updates during the equivalent-result comparison. Report separately that mutable metadata synchronization adds ongoing work and that bucket listing is not a transactional snapshot under concurrent changes.
- Match predicates, result limits, pagination, concurrency, and semantics; count LIST/HEAD/GET requests and bytes. Never compare 1M logical catalog rows to a much smaller physical namespace as if they represented equal workloads.
- Vary selectivity and tenant skew. Record warm/cold cache conditions, repetitions, p50/p95 latency, throughput, errors, and `EXPLAIN (ANALYZE, BUFFERS)`.
- Measure ingest/update latency with and without relevant indexes, table/index size, outbox lag, verification reads, and reconciliation duration/request volume. Discovery speed is not the whole system cost.

### Reliability and movement

- Record the injected boundary, repeat count, object sizes, retry limits, worker restart assumptions, and timestamps from failure to detection and recovery.
- Assert no acknowledged success with invalid destination bytes and no deletion of the last verified copy under the stated ownership assumptions.
- Measure archive/restore throughput and duration including checksum reads, retained-copy storage, and retries. Local MinIO timings are not Glacier restore timings.
- Report manual interventions, unrecovered cases, and invalidated hypotheses alongside successful runs. A finite test matrix does not prove correctness for all failures.

### Optional economic extension

Configure dated price assumptions for active/archive storage, requests, transitions, retrieval, minimum archive duration, and retained-copy lifetime. Specify GB/GiB and currency conventions.

Report baseline storage cost, policy storage cost, retained-copy cost, transition/retrieval/request charges, minimum-duration penalties, net savings, and break-even where it exists. Include access-rate and retention sensitivity and scenarios with no savings. Label 5/10/20 TB scenarios as modeled logical capacity, not processed data or observed financial savings.

Every report includes commit ID, commands, dependency versions, hardware/container limits, seeds, row counts, actual bytes stored, repetitions, raw results, and limitations.

## 10. Operational and Security Baseline

- Emit structured JSON logs with timestamp, service, operation/object ID, attempt, stage, transition, and sanitized error class. Never log credentials, presigned URLs, or object contents.
- Expose minimal counters/histograms for ingestion, operations by type/status, retries, verification failures, duration, outbox lag, and reconciliation findings/repairs. Do not use object IDs as metric labels.
- Preserve audit records for state changes, decisions, failures, repairs, and admin retries; metrics do not replace durable history.
- Use synthetic data only, generated physical keys, bounded uploads, validated metadata/policy/message schemas, and short-lived upload/download permissions.
- Keep secrets outside Git. Bind demo services to loopback by default, restrict admin commands, and apply least-privilege storage credentials, with clients limited to staging uploads and authorized reads.
- Owner fields are query metadata, not proof of identity. Do not expose the service to untrusted users until authentication and object-level authorization exist.
- Test malformed metadata, key traversal attempts, oversized uploads, expired capabilities, idempotency conflicts, unauthorized admin retries, poisoned messages, and checksum mismatches.
- CI gates include lint/format, unit tests, migrations, integration tests, and dependency/container scanning with a documented findings policy. Record what actually ran.

## 11. Final Evidence and Reviewer Narrative

The release must contain:

- [ ] One-command documented local startup and explicit implemented/tested/planned status.
- [ ] Verified upload/query/download and policy/archive/restore round trips.
- [ ] Durable-intent and duplicate-notification/polling tests, concurrency guards, and retry/failure-history evidence.
- [ ] Recovery results for every failure-matrix row, including limitations and manual cases.
- [ ] Indexed-discovery results with equivalent baselines and maintenance overhead.
- [ ] Architecture, state-machine, and consistency explanations matching the code.
- [ ] A short demo, raw results, reproducible commands, and passing required CI gates.
- [ ] Only synthetic/public material; no employer code, identifiers, schemas, diagrams, or confidential metrics.
- [ ] Claims clearly separated into implemented behavior, measured results, and modeled projections.

The demonstration should show one eligible and one rejected object, a verified archive/restore cycle, a harmless duplicate command, and an injected discrepancy detected by reconciliation. Show operational evidence, not just successful API responses.

**Narrative after completion:** Built and evaluated an application-aware object lifecycle control plane, tested its behavior under interrupted operations and duplicate delivery, and measured the trade-offs between indexed discovery and the consistency overhead of a separate metadata catalog.

Use this description only when supported by the repository. The project's value is demonstrated reasoning, correctness within stated assumptions, and reproducible evidence, not the number of technologies or endpoints.