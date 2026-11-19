from __future__ import annotations

import json
import logging
import random
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from botocore.exceptions import BotoCoreError, ClientError
from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from app.config import get_settings
from app.db.session import SessionLocal
from app.enums import JobStatus, JobType, LifecycleState, can_transition
from app.models import AuditEvent, Job, JobAttempt, Policy, StorageObject
from app.ownership import ObjectBusy, ObjectLock, OwnershipLost, object_lock
from app.policies import ObjectFacts, PolicyRef, active_job_id, evaluate
from app.storage import IntegrityError

logger = logging.getLogger(__name__)
Clock = Callable[[], datetime]
Jitter = Callable[[], float]
CLAIMABLE = (JobStatus.PENDING, JobStatus.RETRY_SCHEDULED)
DENIED_CODES = {"403", "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch"}
MISCONFIGURED_CODES = {"NoSuchBucket", "InvalidBucketName"}
DELETE_STAGES = ("DELETE_INTENT", "SOURCE_DELETED")


def utcnow() -> datetime:
    return datetime.now(UTC)


class Outcome(Exception):
    """Ends an attempt; the subclass decides the job's next state."""

    def __init__(self, code: str, detail: str | None = None, invalidates_placement=False):
        super().__init__(code)
        self.code = code
        self.detail = detail
        self.invalidates_placement = invalidates_placement


class Transient(Outcome):
    """Settled failure with no storage mutation in doubt; safe to retry later."""


class Permanent(Outcome):
    """Settled failure that retrying cannot fix."""


class Uncertain(Outcome):
    """The storage outcome or ownership is unknown; block until controlled recovery."""


class RecoveryRefused(Exception):
    pass


@dataclass(frozen=True)
class Attestation:
    operator: str
    worker_stopped: bool
    requests_settled: bool


def retry_delay(attempt: int, jitter: float) -> timedelta:
    settings = get_settings()
    base = min(settings.retry_cap_seconds, settings.retry_base_seconds * 2 ** (attempt - 1))
    return timedelta(seconds=base * (1 + settings.retry_jitter_ratio * jitter))


def _read(call, *args):
    """Reads never change storage, so any unexplained failure is retryable."""

    try:
        return call(*args)
    except IntegrityError:
        raise
    except ClientError as error:
        code = error.response["Error"]["Code"]
        if code in DENIED_CODES:
            raise Permanent("STORAGE_DENIED", code) from error
        if code in MISCONFIGURED_CODES:
            raise Permanent("STORAGE_MISCONFIGURED", code) from error
        raise Transient("STORAGE_READ_FAILED", code) from error
    except BotoCoreError as error:
        raise Transient("STORAGE_UNAVAILABLE", type(error).__name__) from error


def _mutate(call, *args):
    """A rejected request changed nothing; a lost or failed response might have."""

    try:
        return call(*args)
    except ClientError as error:
        code = error.response["Error"]["Code"]
        if code in DENIED_CODES:
            raise Permanent("STORAGE_DENIED", code) from error
        if code in MISCONFIGURED_CODES:
            raise Permanent("STORAGE_MISCONFIGURED", code) from error
        if code in ("PreconditionFailed", "412"):
            raise Permanent("SOURCE_CHANGED", code) from error
        if code in ("NoSuchKey", "404"):
            raise Uncertain("UNEXPLAINED_SOURCE_LOSS", code, invalidates_placement=True) from error
        raise Uncertain("STORAGE_MUTATION_UNCERTAIN", code) from error
    except BotoCoreError as error:
        raise Uncertain("STORAGE_MUTATION_UNCERTAIN", type(error).__name__) from error


def log(job: Job, event: str, **fields) -> None:
    logger.info(
        json.dumps(
            {
                "timestamp": utcnow().isoformat(),
                "service": "worker",
                "job_id": str(job.id),
                "object_id": str(job.object_id),
                "event": event,
                **fields,
            },
            default=str,
        )
    )


def _move(obj: StorageObject, target: LifecycleState) -> None:
    if obj.lifecycle_state != target and can_transition(obj.lifecycle_state, target):
        obj.lifecycle_state = target


def _audit(db, job, obj, previous, event_type, actor="WORKER", details=None) -> None:
    db.add(
        AuditEvent(
            object_id=obj.id,
            job_id=job.id,
            previous_state=previous,
            new_state=obj.lifecycle_state,
            reason=event_type,
            event_type=event_type,
            actor_kind=actor,
            details=details,
        )
    )


def due_job_ids(db: Session, now: datetime, limit: int) -> list[uuid.UUID]:
    ids = list(
        db.scalars(
            select(Job.id)
            .where(Job.status.in_(CLAIMABLE), Job.next_attempt_at <= now)
            .order_by(Job.next_attempt_at, Job.created_at, Job.id)
            .limit(limit)
        )
    )
    db.commit()
    return ids


@dataclass
class Run:
    db: Session
    lock: ObjectLock
    storage: object
    job: Job
    attempt: JobAttempt
    clock: Clock
    recovery: bool
    replica_verified: bool = False

    def reload(self) -> tuple[Job, StorageObject]:
        """Fresh rows for each decision; end the read transaction before storage I/O."""

        job = self.db.get(Job, self.job.id, populate_existing=True)
        obj = self.db.get(StorageObject, job.object_id, populate_existing=True)
        self.db.commit()
        return job, obj

    def stage(self, stage: str) -> None:
        self.attempt.stage = stage
        self.db.commit()
        log(self.job, "stage", attempt=self.attempt.attempt_number, stage=stage)

    def verify(self, location: tuple[str, str], conflict: str, invalidates=False) -> str:
        try:
            return _read(
                self.storage.verify, *location, self.job.expected_size, self.job.expected_sha256
            )
        except IntegrityError as error:
            raise Permanent(conflict, str(error), invalidates) from error

    def require_binding(self, obj: StorageObject, states: set[LifecycleState]) -> None:
        job = self.job
        if (obj.content_version, obj.checksum_sha256, obj.size_bytes) != (
            job.content_version,
            job.expected_sha256,
            job.expected_size,
        ):
            raise Permanent("BINDING_MISMATCH", "Object content differs from the job binding")
        if job.job_type == JobType.ARCHIVE:
            locations = (obj.bucket, obj.key) == (job.source_bucket, job.source_key)
        else:
            locations = (obj.archive_bucket, obj.archive_key, obj.bucket, obj.key) == (
                job.source_bucket,
                job.source_key,
                job.destination_bucket,
                job.destination_key,
            )
        if not locations:
            raise Permanent("BINDING_MISMATCH", "Object locations differ from the job binding")
        if obj.lifecycle_state not in states:
            raise Uncertain("STATE_MISMATCH", obj.lifecycle_state.value)

    def finish_attempt(self, error_class: str | None = None, detail: str | None = None) -> None:
        self.attempt.finished_at = self.clock()
        self.attempt.error_class = error_class
        self.attempt.detail = detail[:500] if detail else None


def execute_job(
    job_id: uuid.UUID,
    storage,
    clock: Clock = utcnow,
    jitter: Jitter = random.random,
    attestation: Attestation | None = None,
) -> str:
    """Run one job under the object lock; also resumes a blocked job for controlled recovery."""

    if attestation is not None and not (
        attestation.operator.strip() and attestation.worker_stopped and attestation.requests_settled
    ):
        raise RecoveryRefused("Recovery requires an operator and both attestations")
    with SessionLocal() as db:
        job = db.get(Job, job_id)
        if job is None or job.object_id is None:
            if attestation is not None:
                raise RecoveryRefused("Job not found")
            return "NOT_FOUND"
        db.commit()
        try:
            with object_lock(db.get_bind(), job.object_id) as lock:
                return _execute(db, lock, storage, job_id, clock, jitter, attestation)
        except ObjectBusy:
            if attestation is not None:
                raise RecoveryRefused("Another session owns the object") from None
            return "BUSY"


def _execute(db, lock, storage, job_id, clock, jitter, attestation) -> str:
    job = db.get(Job, job_id, populate_existing=True)
    now = clock()
    attempt = _begin_recovery(db, job, attestation, now) if attestation else _claim(db, job, now)
    if attempt is None:
        db.rollback()
        return "SKIPPED"
    run = Run(db, lock, storage, job, attempt, clock, recovery=attestation is not None)
    try:
        if job.job_type == JobType.ARCHIVE:
            _archive(run)
        else:
            _restore(run)
        return JobStatus.SUCCEEDED.value
    except Transient as outcome:
        if run.recovery:
            return _block(run, outcome)
        return _schedule_retry(run, outcome, jitter)
    except Permanent as outcome:
        return _fail(run, outcome)
    except Uncertain as outcome:
        return _block(run, outcome)
    except OwnershipLost as error:
        return _block(run, Uncertain("OWNERSHIP_LOST", str(error)))
    except Exception as error:
        logger.exception("Unexpected lifecycle execution error")
        try:
            return _block(run, Uncertain("UNEXPECTED_ERROR", type(error).__name__))
        except Exception:
            # The job stays RUNNING; startup quarantine or controlled recovery handles it.
            logger.exception("Could not record RECOVERY_REQUIRED")
            return "UNSETTLED"


def _claim(db: Session, job: Job, now: datetime) -> JobAttempt | None:
    if job.status not in CLAIMABLE or job.next_attempt_at is None or job.next_attempt_at > now:
        return None
    claimed = db.execute(
        update(Job)
        .where(Job.id == job.id, Job.status.in_(CLAIMABLE), Job.next_attempt_at <= now)
        .values(
            status=JobStatus.RUNNING,
            attempt_count=Job.attempt_count + 1,
            started_at=func.coalesce(Job.started_at, now),
            next_attempt_at=None,
        )
    ).rowcount
    if claimed != 1:
        return None
    job = db.get(Job, job.id, populate_existing=True)
    obj = db.get(StorageObject, job.object_id, populate_existing=True)
    previous = obj.lifecycle_state
    _move(
        obj,
        LifecycleState.ARCHIVING if job.job_type == JobType.ARCHIVE else LifecycleState.RESTORING,
    )
    attempt = JobAttempt(
        job_id=job.id, attempt_number=job.attempt_count, stage="CLAIMED", started_at=now
    )
    db.add(attempt)
    _audit(db, job, obj, previous, "JOB_CLAIMED", details={"attempt": job.attempt_count})
    db.commit()
    log(job, "claimed", attempt=job.attempt_count)
    return attempt


def _begin_recovery(db: Session, job: Job, attestation: Attestation, now) -> JobAttempt:
    if job.status not in (JobStatus.RECOVERY_REQUIRED, JobStatus.RUNNING):
        raise RecoveryRefused(f"Job is {job.status.value}; only blocked jobs can be recovered")
    if job.expected_sha256 is None or job.source_key is None:
        raise RecoveryRefused("Job has no immutable binding; manual review is required")
    number = (
        db.scalar(select(func.max(JobAttempt.attempt_number)).where(JobAttempt.job_id == job.id))
        or 0
    ) + 1
    previous_status = job.status.value
    for interrupted in db.scalars(
        select(JobAttempt).where(JobAttempt.job_id == job.id, JobAttempt.finished_at.is_(None))
    ):
        interrupted.finished_at = now
        interrupted.error_class = interrupted.error_class or "WORKER_INTERRUPTED"
    job.status = JobStatus.RUNNING
    job.attempt_count = number
    attempt = JobAttempt(job_id=job.id, attempt_number=number, stage="RECOVERY", started_at=now)
    db.add(attempt)
    obj = job.object
    _audit(
        db,
        job,
        obj,
        obj.lifecycle_state,
        "JOB_RECOVERY_STARTED",
        actor="OPERATOR",
        details={
            "operator": attestation.operator,
            "attested_worker_stopped": attestation.worker_stopped,
            "attested_requests_settled": attestation.requests_settled,
            "previous_status": previous_status,
        },
    )
    db.commit()
    log(job, "recovery_started", attempt=number)
    return attempt


def _revalidate(run: Run) -> StorageObject:
    job, obj = run.reload()
    run.require_binding(obj, {LifecycleState.ARCHIVE_REQUESTED, LifecycleState.ARCHIVING})
    policy = run.db.get(Policy, job.policy_id) if job.policy_id else None
    if policy is None:
        raise Permanent("BINDING_MISMATCH", "Archive job has no policy version")
    facts = ObjectFacts.from_model(obj, active_job_id(run.db, obj.id))
    decision = evaluate(facts, PolicyRef.from_model(policy), run.clock(), owner_job_id=job.id)
    run.db.commit()
    if not decision.eligible:
        raise Permanent("POLICY_REJECTED", ",".join(decision.reasons))
    return obj


def _deletion_authorized(run: Run) -> bool:
    return bool(
        run.db.scalar(
            select(func.count())
            .select_from(JobAttempt)
            .where(JobAttempt.job_id == run.job.id, JobAttempt.stage.in_(DELETE_STAGES))
        )
    )


def _archive(run: Run) -> None:
    job, obj = run.reload()
    run.require_binding(obj, {LifecycleState.ARCHIVE_REQUESTED, LifecycleState.ARCHIVING})
    source = (job.source_bucket, job.source_key)
    destination = (job.destination_bucket, job.destination_key)
    source_found = _read(run.storage.inspect, *source)
    destination_found = _read(run.storage.inspect, *destination)

    if source_found is None:
        # Only this job's own recorded delete intent explains a missing source.
        if destination_found is None:
            raise Uncertain("DATA_MISSING", "Source and destination are absent", True)
        try:
            _read(run.storage.verify, *destination, job.expected_size, job.expected_sha256)
        except IntegrityError as error:
            raise Uncertain("DATA_MISSING", "No verified copy remains", True) from error
        authorized = _deletion_authorized(run)
        run.db.commit()
        if not authorized:
            raise Uncertain("UNEXPLAINED_SOURCE_LOSS", "Source absent without delete intent", True)
        run.replica_verified = True
        run.stage("DESTINATION_VERIFIED")
        _complete_archive(run)
        return

    _revalidate(run)
    if destination_found is not None:
        run.verify(destination, "DESTINATION_CONFLICT")
    else:
        etag = run.verify(source, "SOURCE_INTEGRITY", invalidates=True)
        _mutate(
            run.storage.copy, *source, *destination, etag, obj.content_type, job.expected_sha256
        )
        run.stage("COPIED")
        run.verify(destination, "DESTINATION_CONFLICT")
    run.replica_verified = True
    run.stage("DESTINATION_VERIFIED")

    run.lock.verify()
    _revalidate(run)
    run.stage("DELETE_INTENT")
    _mutate(run.storage.delete, *source)
    run.stage("SOURCE_DELETED")
    _complete_archive(run)


def _complete_archive(run: Run) -> None:
    job, obj = run.reload()
    previous = obj.lifecycle_state
    _move(obj, LifecycleState.ARCHIVED)
    obj.desired_storage_class = "ARCHIVE"
    obj.observed_storage_class = "ARCHIVE"
    obj.archive_bucket, obj.archive_key = job.destination_bucket, job.destination_key
    obj.archive_copy_retained = False
    _succeed(run, job, obj, previous)


def _restore(run: Run) -> None:
    job, obj = run.reload()
    run.require_binding(obj, {LifecycleState.RESTORE_REQUESTED, LifecycleState.RESTORING})
    source = (job.source_bucket, job.source_key)
    destination = (job.destination_bucket, job.destination_key)
    if _read(run.storage.inspect, *source) is None:
        active = _read(run.storage.inspect, *destination)
        raise Uncertain("DATA_MISSING" if active is None else "ARCHIVE_SOURCE_MISSING", None, True)
    etag = run.verify(source, "SOURCE_INTEGRITY", invalidates=True)
    if _read(run.storage.inspect, *destination) is not None:
        run.verify(destination, "DESTINATION_CONFLICT")
    else:
        _mutate(
            run.storage.copy, *source, *destination, etag, obj.content_type, job.expected_sha256
        )
        run.stage("COPIED")
        run.verify(destination, "DESTINATION_CONFLICT")
    run.stage("DESTINATION_VERIFIED")
    run.lock.verify()
    job, obj = run.reload()
    previous = obj.lifecycle_state
    _move(obj, LifecycleState.ACTIVE)
    obj.desired_storage_class = "ACTIVE"
    obj.observed_storage_class = "ACTIVE"
    obj.archive_copy_retained = True
    obj.restored_at = run.clock()
    _succeed(run, job, obj, previous)


def _succeed(run: Run, job: Job, obj: StorageObject, previous) -> None:
    job.status = JobStatus.SUCCEEDED
    job.finished_at = run.clock()
    job.error_class = job.error_message = None
    run.attempt.stage = "COMMITTED"
    run.finish_attempt()
    _audit(
        run.db,
        job,
        obj,
        previous,
        "JOB_SUCCEEDED",
        details={"sha256": job.expected_sha256, "size_bytes": job.expected_size},
    )
    run.db.commit()
    log(job, "succeeded", attempt=run.attempt.attempt_number)


def _settle(run: Run, outcome: Outcome, status: JobStatus, cause: Outcome | None = None) -> tuple:
    run.db.rollback()
    job, obj = run.reload()
    attempt = run.db.get(JobAttempt, run.attempt.id, populate_existing=True)
    run.attempt = attempt
    job.status = status
    job.error_class = outcome.code
    job.error_message = (outcome.detail or outcome.code)[:500]
    cause = cause or outcome
    run.finish_attempt(cause.code, cause.detail)
    if outcome.invalidates_placement:
        obj.observed_storage_class = "UNKNOWN"
    return job, obj, obj.lifecycle_state


def _schedule_retry(run: Run, outcome: Transient, jitter: Jitter) -> str:
    run.db.rollback()
    job, _ = run.reload()
    if job.attempt_count >= job.max_attempts:
        exhausted = Permanent("RETRY_EXHAUSTED", f"{outcome.code}: {outcome.detail}")
        return _fail(run, exhausted, cause=outcome)
    job, obj, previous = _settle(run, outcome, JobStatus.RETRY_SCHEDULED)
    job.next_attempt_at = run.clock() + retry_delay(job.attempt_count, jitter())
    _audit(run.db, job, obj, previous, "JOB_RETRY_SCHEDULED", details={"error": outcome.code})
    run.db.commit()
    log(job, "retry_scheduled", error_class=outcome.code, next_attempt_at=job.next_attempt_at)
    return JobStatus.RETRY_SCHEDULED.value


def _fail(run: Run, outcome: Permanent, cause: Outcome | None = None) -> str:
    job, obj, previous = _settle(run, outcome, JobStatus.FAILED, cause)
    job.finished_at = run.clock()
    if job.job_type == JobType.ARCHIVE:
        _move(obj, LifecycleState.ACTIVE)
        obj.desired_storage_class = "ACTIVE"
        if run.replica_verified:
            # A verified archive copy now exists; record it rather than leave unowned bytes.
            obj.archive_bucket, obj.archive_key = job.destination_bucket, job.destination_key
            obj.archive_copy_retained = True
    else:
        _move(obj, LifecycleState.ARCHIVED)
        obj.desired_storage_class = "ARCHIVE"
    _audit(run.db, job, obj, previous, "JOB_FAILED", details={"error": outcome.code})
    run.db.commit()
    log(job, "failed", error_class=outcome.code)
    return JobStatus.FAILED.value


def _block(run: Run, outcome: Uncertain) -> str:
    job, obj, previous = _settle(run, outcome, JobStatus.RECOVERY_REQUIRED)
    _audit(run.db, job, obj, previous, "JOB_RECOVERY_REQUIRED", details={"error": outcome.code})
    run.db.commit()
    log(job, "recovery_required", error_class=outcome.code)
    return JobStatus.RECOVERY_REQUIRED.value


def quarantine_interrupted(clock: Clock = utcnow) -> list[str]:
    """Block RUNNING jobs left by a stopped worker; never resume them automatically."""

    quarantined = []
    with SessionLocal() as db:
        job_ids = list(db.scalars(select(Job.id).where(Job.status == JobStatus.RUNNING)))
        db.commit()
        for job_id in job_ids:
            job = db.get(Job, job_id)
            if job is None or job.object_id is None:
                continue
            try:
                with object_lock(db.get_bind(), job.object_id):
                    job = db.get(Job, job_id, populate_existing=True)
                    if job.status != JobStatus.RUNNING:
                        db.commit()
                        continue
                    now = clock()
                    job.status = JobStatus.RECOVERY_REQUIRED
                    job.error_class = "WORKER_INTERRUPTED"
                    job.error_message = "Worker stopped while the job was running"
                    for attempt in db.scalars(
                        select(JobAttempt).where(
                            JobAttempt.job_id == job.id, JobAttempt.finished_at.is_(None)
                        )
                    ):
                        attempt.finished_at = now
                        attempt.error_class = "WORKER_INTERRUPTED"
                    obj = job.object
                    _audit(db, job, obj, obj.lifecycle_state, "JOB_RECOVERY_REQUIRED")
                    db.commit()
                    log(job, "quarantined", error_class="WORKER_INTERRUPTED")
                    quarantined.append(str(job_id))
            except ObjectBusy:
                db.rollback()
    return quarantined
