from __future__ import annotations

import hashlib
import json
import logging
import secrets
import uuid
from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response, Security
from fastapi.security import APIKeyHeader
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError as DatabaseConflict
from sqlalchemy.orm import Session

from app.config import get_settings
from app.db.session import get_db
from app.enums import JobStatus, JobType, LifecycleState
from app.models import AuditEvent, Job, JobAttempt, OutboxEvent, Policy, StorageObject
from app.ownership import ObjectBusy, object_lock
from app.policies import (
    ObjectFacts,
    PolicyRef,
    active_job_id,
    evaluate,
    load_policy,
    select_candidates,
)
from app.schemas import AttemptView, JobView, RetryRequest

router = APIRouter(prefix="/v1", tags=["lifecycle"])
logger = logging.getLogger(__name__)
Database = Annotated[Session, Depends(get_db)]
IdempotencyKey = Annotated[str, Header(min_length=1, max_length=255)]
admin_key_header = APIKeyHeader(name="X-Admin-API-Key", auto_error=False)


class AdmissionError(Exception):
    def __init__(self, status: int, message: str, reasons: tuple[str, ...] = ()):
        super().__init__(message)
        self.status = status
        self.message = message
        self.reasons = reasons

    def as_http(self) -> HTTPException:
        detail = (
            {"message": self.message, "reasons": list(self.reasons)}
            if self.reasons
            else self.message
        )
        return HTTPException(self.status, detail)


def fingerprint(*parts) -> str:
    return hashlib.sha256(
        json.dumps(parts, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def log_event(job: Job, event: str) -> None:
    logger.info(
        json.dumps(
            {
                "timestamp": datetime.now(UTC).isoformat(),
                "service": "api",
                "object_id": str(job.object_id),
                "job_id": str(job.id),
                "event": event,
                "status": job.status.value,
            }
        )
    )


def _job_by_key(db: Session, object_id: uuid.UUID, key: str) -> Job | None:
    return db.scalar(select(Job).where(Job.object_id == object_id, Job.idempotency_key == key))


def _restorable(obj: StorageObject) -> bool:
    return (
        obj.lifecycle_state == LifecycleState.ARCHIVED
        and obj.observed_storage_class == "ARCHIVE"
        and bool(obj.archive_bucket and obj.archive_key)
    )


def _enqueue(
    db: Session,
    obj: StorageObject,
    job_type: JobType,
    key: str,
    request_fingerprint: str,
    actor: str,
    bindings: dict[str, str],
    decision: dict | None = None,
    policy_id: uuid.UUID | None = None,
    retry_of: uuid.UUID | None = None,
    reason: str | None = None,
) -> Job:
    """Persist state, job, audit and outbox intent in one short transaction."""

    now = datetime.now(UTC)
    previous = obj.lifecycle_state
    job = Job(
        id=uuid.uuid4(),
        job_type=job_type,
        status=JobStatus.PENDING,
        object_id=obj.id,
        idempotency_key=key,
        request_fingerprint=request_fingerprint,
        attempt_count=0,
        max_attempts=get_settings().job_max_attempts,
        content_version=obj.content_version,
        expected_sha256=obj.checksum_sha256,
        expected_size=obj.size_bytes,
        policy_id=policy_id,
        decision=decision,
        actor_kind=actor,
        next_attempt_at=now,
        retry_of_job_id=retry_of,
        **bindings,
    )
    if job_type == JobType.ARCHIVE:
        obj.lifecycle_state = LifecycleState.ARCHIVE_REQUESTED
        obj.desired_storage_class = "ARCHIVE"
    else:
        obj.lifecycle_state = LifecycleState.RESTORE_REQUESTED
        obj.desired_storage_class = "ACTIVE"
    db.add(job)
    db.flush()
    event_type = "RETRY_REQUESTED" if retry_of else f"{job_type.value}_REQUESTED"
    db.add(
        AuditEvent(
            object_id=obj.id,
            job_id=job.id,
            previous_state=previous,
            new_state=obj.lifecycle_state,
            reason=reason or event_type,
            event_type=event_type,
            actor_kind=actor,
            policy_id=policy_id,
            policy_version=decision["policy"]["version"] if decision else None,
            details={"decision": decision, "retry_of_job_id": str(retry_of) if retry_of else None},
        )
    )
    db.add(
        OutboxEvent(
            id=uuid.uuid4(),
            job_id=job.id,
            event_type="job_ready",
            schema_version=1,
            payload={"job_id": str(job.id)},
            publish_attempts=0,
        )
    )
    db.commit()
    log_event(job, "job_admitted")
    return job


def _replay(db: Session, object_id: uuid.UUID, key: str, request_fingerprint: str) -> Job | None:
    job = _job_by_key(db, object_id, key)
    if job is not None and job.request_fingerprint != request_fingerprint:
        raise AdmissionError(409, "Idempotency-Key was used with a different request")
    return job


def admit(
    db: Session,
    object_id: uuid.UUID,
    job_type: JobType,
    key: str,
    actor: str,
    policy: PolicyRef | None = None,
) -> tuple[Job, bool]:
    """Admit an archive/restore request; manual and policy-driven paths share every guard."""

    if not key.strip():
        raise AdmissionError(422, "Idempotency-Key cannot be blank")
    settings = get_settings()
    with object_lock(db.get_bind(), object_id, settings.object_lock_wait_seconds):
        obj = db.get(StorageObject, object_id, populate_existing=True)
        if obj is None:
            raise AdmissionError(404, "Object not found")
        if job_type == JobType.ARCHIVE and policy is None:
            model = load_policy(db, settings.archive_policy_name)
            if model is None:
                raise AdmissionError(503, "No activated archive policy is configured")
            policy = PolicyRef.from_model(model)
        identity = (job_type.value, str(obj.id), obj.content_version, obj.checksum_sha256)
        policy_context = (str(policy.id), policy.name, policy.version) if policy else None
        request_fingerprint = fingerprint(
            *identity, policy_context if job_type == JobType.ARCHIVE else None
        )
        existing = _replay(db, obj.id, key, request_fingerprint)
        if existing is not None:
            return existing, False
        if obj.ingestion_verified_at is None:
            raise AdmissionError(409, "Object ingestion has not been verified")
        current = active_job_id(db, obj.id)
        if current is not None:
            raise AdmissionError(409, f"Object already has in-flight job {current}")
        if job_type == JobType.ARCHIVE:
            if obj.lifecycle_state == LifecycleState.ARCHIVED:
                raise AdmissionError(409, "Object is already archived")
            decision = evaluate(ObjectFacts.from_model(obj, None), policy, datetime.now(UTC))
            if not decision.eligible:
                raise AdmissionError(409, "Archive policy rejected the object", decision.reasons)
            bindings = {
                "source_bucket": obj.bucket,
                "source_key": obj.key,
                "destination_bucket": settings.s3_archive_bucket,
                "destination_key": f"objects/{obj.id}/v{obj.content_version}",
            }
            record, policy_id = decision.as_record(), policy.id
        else:
            if obj.lifecycle_state == LifecycleState.ACTIVE:
                raise AdmissionError(409, "Object is already active")
            if not _restorable(obj):
                raise AdmissionError(409, "Object has no verified archive copy to restore")
            bindings = {
                "source_bucket": obj.archive_bucket,
                "source_key": obj.archive_key,
                "destination_bucket": obj.bucket,
                "destination_key": obj.key,
            }
            record, policy_id = None, None
        try:
            job = _enqueue(
                db, obj, job_type, key, request_fingerprint, actor, bindings, record, policy_id
            )
        except DatabaseConflict as error:
            db.rollback()
            if getattr(error.orig, "sqlstate", None) != "23505":
                raise
            existing = _replay(db, object_id, key, request_fingerprint)
            if existing is None:
                raise AdmissionError(409, "Object already has an in-flight job") from None
            return existing, False
        return job, True


def enqueue_candidates(db: Session, policy: PolicyRef, limit: int) -> list[dict]:
    """Policy-driven admission at server time through the same guarded path as manual requests."""

    results = []
    for obj in select_candidates(db, policy, datetime.now(UTC), limit):
        key = f"policy:{policy.name}:v{policy.version}:content-v{obj.content_version}"
        try:
            job, created = admit(db, obj.id, JobType.ARCHIVE, key, "POLICY", policy)
            results.append({"object_id": str(obj.id), "job_id": str(job.id), "created": created})
        except AdmissionError as error:
            db.rollback()
            results.append(
                {
                    "object_id": str(obj.id),
                    "rejected": error.message,
                    "reasons": list(error.reasons),
                }
            )
        except ObjectBusy:
            db.rollback()
            results.append({"object_id": str(obj.id), "rejected": "OBJECT_BUSY", "reasons": []})
    return results


def job_view(db: Session, job: Job) -> JobView:
    replacement = db.scalar(select(Job.id).where(Job.retry_of_job_id == job.id))
    return JobView.from_job(job, replacement)


def _admission_response(db, response, object_id, job_type, key) -> JobView:
    try:
        job, _ = admit(db, object_id, job_type, key, "API")
    except AdmissionError as error:
        db.rollback()
        raise error.as_http() from None
    response.headers["Location"] = f"/v1/jobs/{job.id}"
    return job_view(db, job)


@router.post("/objects/{object_id}/archive", response_model=JobView, status_code=202)
def request_archive(
    object_id: uuid.UUID, db: Database, response: Response, idempotency_key: IdempotencyKey
):
    return _admission_response(db, response, object_id, JobType.ARCHIVE, idempotency_key)


@router.post("/objects/{object_id}/restore", response_model=JobView, status_code=202)
def request_restore(
    object_id: uuid.UUID, db: Database, response: Response, idempotency_key: IdempotencyKey
):
    return _admission_response(db, response, object_id, JobType.RESTORE, idempotency_key)


@router.get("/jobs/{job_id}", response_model=JobView)
def get_job(job_id: uuid.UUID, db: Database):
    job = db.get(Job, job_id)
    if job is None:
        raise HTTPException(404, "Job not found")
    return job_view(db, job)


@router.get("/jobs/{job_id}/attempts", response_model=list[AttemptView])
def get_attempts(job_id: uuid.UUID, db: Database, limit: Annotated[int, Query(ge=1, le=100)] = 50):
    if db.get(Job, job_id) is None:
        raise HTTPException(404, "Job not found")
    return db.scalars(
        select(JobAttempt)
        .where(JobAttempt.job_id == job_id)
        .order_by(JobAttempt.attempt_number)
        .limit(limit)
    ).all()


def require_admin(api_key: Annotated[str | None, Security(admin_key_header)]) -> None:
    configured = get_settings().admin_api_key
    if not configured:
        raise HTTPException(503, "Admin retry is disabled")
    if api_key is None or not secrets.compare_digest(api_key.encode(), configured.encode()):
        raise HTTPException(401, "Invalid admin credentials")


@router.post(
    "/admin/jobs/{job_id}/retry",
    response_model=JobView,
    status_code=202,
    dependencies=[Depends(require_admin)],
)
def retry_job(
    job_id: uuid.UUID,
    payload: RetryRequest,
    db: Database,
    response: Response,
    idempotency_key: IdempotencyKey,
):
    try:
        job = retry(db, job_id, idempotency_key, payload.reason)
    except AdmissionError as error:
        db.rollback()
        raise error.as_http() from None
    response.headers["Location"] = f"/v1/jobs/{job.id}"
    return job_view(db, job)


def retry(db: Session, job_id: uuid.UUID, key: str, reason: str) -> Job:
    """Create a linked replacement for a settled failed job; never takes over uncertain work."""

    if not key.strip():
        raise AdmissionError(422, "Idempotency-Key cannot be blank")
    original = db.get(Job, job_id)
    if original is None or original.object_id is None:
        raise AdmissionError(404, "Job not found")
    with object_lock(db.get_bind(), original.object_id, get_settings().object_lock_wait_seconds):
        original = db.get(Job, job_id, populate_existing=True)
        request_fingerprint = fingerprint("RETRY", str(original.id))
        replacement = db.scalar(select(Job).where(Job.retry_of_job_id == original.id))
        if replacement is not None:
            if replacement.idempotency_key == key:
                return replacement
            raise AdmissionError(409, "Job already has a replacement")
        if _replay(db, original.object_id, key, request_fingerprint) is not None:
            raise AdmissionError(409, "Idempotency-Key was used with a different request")
        if original.status != JobStatus.FAILED:
            raise AdmissionError(
                409, f"Only settled FAILED jobs can be retried, not {original.status}"
            )
        obj = db.get(StorageObject, original.object_id, populate_existing=True)
        if original.expected_sha256 is None or original.source_key is None:
            raise AdmissionError(409, "Job has no immutable binding; use controlled recovery")
        if (obj.content_version, obj.checksum_sha256, obj.size_bytes) != (
            original.content_version,
            original.expected_sha256,
            original.expected_size,
        ):
            raise AdmissionError(409, "Object content no longer matches the job binding")
        if active_job_id(db, obj.id) is not None:
            raise AdmissionError(409, "Object already has an in-flight job")
        record, policy_id = None, None
        if original.job_type == JobType.ARCHIVE:
            policy = db.get(Policy, original.policy_id) if original.policy_id else None
            if policy is None:
                raise AdmissionError(409, "Original policy version is unavailable")
            decision = evaluate(
                ObjectFacts.from_model(obj, None), PolicyRef.from_model(policy), datetime.now(UTC)
            )
            if not decision.eligible:
                raise AdmissionError(409, "Archive policy rejected the object", decision.reasons)
            record, policy_id = decision.as_record(), policy.id
        elif not _restorable(obj) or (obj.archive_bucket, obj.archive_key) != (
            original.source_bucket,
            original.source_key,
        ):
            raise AdmissionError(409, "Object has no verified archive copy at the bound location")
        bindings = {
            name: getattr(original, name)
            for name in ("source_bucket", "source_key", "destination_bucket", "destination_key")
        }
        try:
            return _enqueue(
                db,
                obj,
                original.job_type,
                key,
                request_fingerprint,
                "ADMIN",
                bindings,
                record,
                policy_id,
                retry_of=original.id,
                reason=reason,
            )
        except DatabaseConflict as error:
            db.rollback()
            if getattr(error.orig, "sqlstate", None) != "23505":
                raise
            raise AdmissionError(
                409, "Job was retried or the object gained in-flight work"
            ) from None
