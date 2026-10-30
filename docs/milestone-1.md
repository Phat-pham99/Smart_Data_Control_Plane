# Milestone 1: Verified Catalog Ingestion

## Scope

Feature branch: `feature/verified-catalog-ingestion`, based on `ac4e84e`. Changes are not committed or pushed by the implementation session. This is local verification, not a GitHub Actions run or a production deployment.

Implemented: immutable owner/logical-key/content-version identity; scoped request fingerprints; strict business metadata; bounded presigned staging POST; streamed source and destination size/SHA-256 verification; private promotion; advisory-lock completion; metadata discovery with stable cursor ordering; short-lived downloads; explicit application access; audit history; dependency readiness; and a frozen Python dependency lock.

The API must be used only on a trusted local network with synthetic data. Owner metadata is not authorization. The archive bucket exists but archive/restore, policy evaluation, outbox, Pub/Sub/polling execution and reconciliation are not implemented.

## Reproduce

Use Linux or WSL with Docker Compose, Python 3.12 and uv 0.12.3. These ports and the project name isolate test services from an existing development stack. Credentials below are synthetic local defaults, not production secrets.

```bash
export COMPOSE_PROJECT_NAME=lifecycle-phase1-ci
export POSTGRES_PORT=55432 REDIS_PORT=56379
export MINIO_PORT=59000 MINIO_CONSOLE_PORT=59001 API_PORT=58000
export DATABASE_URL=postgresql+psycopg://lifecycle:lifecycle@localhost:55432/lifecycle
export REDIS_URL=redis://localhost:56379/0
export S3_ENDPOINT_URL=http://localhost:59000 S3_PUBLIC_ENDPOINT_URL=http://localhost:59000
export AWS_ACCESS_KEY_ID=lifecycle-app AWS_SECRET_ACCESS_KEY=local-app-password
export RUN_INTEGRATION=1 API_BASE_URL=http://localhost:58000

uv sync --frozen --extra dev
docker compose up -d --build --wait postgres redis minio
docker compose build minio-init
docker compose run --rm --no-deps minio-init

# Destructive: only run downgrade against the isolated synthetic database.
uv run --frozen alembic upgrade head
uv run --frozen alembic downgrade base
uv run --frozen alembic upgrade head
uv run --frozen alembic check
uv run --frozen pytest --cov=app --cov-report=term-missing --cov-fail-under=75

docker compose up -d --build --wait api
curl --fail http://localhost:58000/health/ready
uv run --frozen python scripts/verify_restart.py seed
docker compose restart postgres minio
docker compose up -d --wait postgres minio api
uv run --frozen python scripts/verify_restart.py verify
```

The restart script stores only a synthetic object UUID and digest in the system temporary directory. It never stores signed URLs or credentials. `--state` and `--api` allow separate runs. Generated integration rows/objects are left for inspection; dispose only of your isolated project's resources when finished. Do not run `down --volumes` against a stack containing wanted data.

## Local Evidence

Executed on Linux x86-64 with Python 3.12.13, uv 0.12.3, PostgreSQL 16 and the pinned MinIO build. This is a small-object correctness run, not a load test or terabyte-scale measurement.

| Gate | Observed result |
| --- | --- |
| Frozen environment, Ruff lint/format | Passed |
| Migration upgrade, downgrade to base, upgrade, ORM drift check | Passed; no new upgrade operations |
| Real-service tests | Eleven passed without integration skips |
| Complete suite including startup regressions | 42 passed; 91.52% application coverage; 75% gate passed |
| Container startup/readiness | Non-root API booted; database, Redis and storage healthy |
| Restart persistence | Catalog state and exact bytes/digest survived PostgreSQL/MinIO restart |
| Runtime dependency audit | No known vulnerabilities reported by pip-audit |
| Image vulnerability scan | No fixable HIGH/CRITICAL findings reported by Trivy |
| Workflow syntax and scanner reference | actionlint 1.7.7 passed; scanner release tag resolved |
| Image promotion mechanics | Docker image export, compression and reload passed; no registry push |

Tested failures include invalid metadata/traversal, oversized upload, staging checksum mismatch, corrupt promoted bytes, interrupted final catalog commit, simultaneous initiation, duplicate completion, conflicting identity/key reuse, held advisory lock, malformed cursor, missing object and unauthorized direct storage access. Source and destination are verified before `ACTIVE`; corruption cannot produce successful registration. A failed final commit leaves retryable ingestion rather than claiming cross-store atomicity.

Starlette currently emits a TestClient/httpx deprecation warning. It is visible and does not fail the suite. Full lifecycle recovery, query performance/index benefit, real-S3 behavior and expiry enforcement are not established by these results.

## Delivery Contract

No CI workflow is configured. The gates above were run locally and must be rerun locally after dependency updates. The Trivy result is from that local run; the source-built development MinIO images were not scanned. No container image is published to a registry, and there is no automatic production rollout or cloud provisioning.

## Storage Dependencies

The MinIO/mc image distribution endpoints attempted during setup were unavailable (image pulls denied/no manifest; upstream binary downloads returned HTTP 410). To make the local fixture buildable, Compose builds upstream source pinned in [docker/minio.Dockerfile](../docker/minio.Dockerfile):

- MinIO source git object `01ce918d8279a20e4706b96a64396146894adee4`, release `RELEASE.2025-09-07T16-13-09Z`.
- mc source git object `d6541ea280b73a834b64d4097e21f2be77676104`, release `RELEASE.2025-08-13T08-35-41Z`.

Upstream sources are at <https://github.com/minio/minio> and <https://github.com/minio/mc>. Both dependencies are AGPL-3.0; their license text is copied into `/usr/share/licenses` inside the respective image. The build does not modify upstream source. Source dependencies and build instructions are available in the pinned repositories and this Dockerfile. Redistribution requires an appropriate corresponding-source delivery and compliance review; the MIT application license does not relicense these dependencies.

These older pinned sources are local test fixtures, not a supported production storage recommendation. Dependency updates, upstream availability, corresponding-source packaging and vulnerability review remain operational responsibilities. Amazon S3 is the later production target, subject to the separate readiness gates in the plan.

## Remaining Limits

- No authentication or tenant authorization; keep loopback bindings and synthetic data.
- No automatic staging expiration, orphan cleanup, reconciliation or background completion. A caller must retry interrupted ingestion. Staging copies remain and consume space.
- Single controlled executor ownership; advisory locks are not distributed fencing. An uncertain lock loss or in-flight request requires controlled recovery.
- Synchronous verification reads both source and destination; uploads are limited to 10 MiB by default. No multipart/TB-scale or global-access claim.
- Download issuance checks catalog state and object length, not a fresh full checksum on every GET. Privileged out-of-band mutation remains a future reconciliation concern.
- One local restart test is not backup/restore, disaster recovery, multi-region availability or production readiness.