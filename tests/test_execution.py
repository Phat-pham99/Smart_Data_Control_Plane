from __future__ import annotations

import hashlib
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from app.config import get_settings
from app.db.session import SessionLocal, engine
from app.enums import JobStatus, JobType, LifecycleState
from app.execution import (
    Attestation,
    RecoveryRefused,
    execute_job,
    quarantine_interrupted,
    retry_delay,
)
from app.lifecycle import AdmissionError, admit, retry
from app.models import AuditEvent, Job, JobAttempt, StorageObject
from app.ownership import ObjectLock, OwnershipLost, object_lock
from app.storage import IntegrityError
from botocore.exceptions import ClientError, EndpointConnectionError, ReadTimeoutError
from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from tests.test_ingestion_integration import pytestmark  # noqa: F401

MIB = 1024 * 1024
PAYLOAD = (b"fake-cold-dataset " * (MIB // 18 + 1))[:MIB]
DIGEST = hashlib.sha256(PAYLOAD).hexdigest()
OPERATOR = Attestation("synthetic-operator", True, True)


def client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, "Synthetic")


class FakeStorage:
    """In-memory storage with one-shot failpoints; mirrors ObjectStorage semantics."""

    def __init__(self):
        self.objects: dict[tuple[str, str], bytes] = {}
        self.failpoints: dict[str, list] = {}
        self.calls: list[tuple[str, str, str]] = []

    def fail(self, operation, action):
        self.failpoints.setdefault(operation, []).append(action)

    def _hit(self, operation, bucket, key):
        self.calls.append((operation, bucket, key))
        if self.failpoints.get(operation):
            action = self.failpoints[operation].pop(0)
            if isinstance(action, BaseException):
                raise action
            action()

    @staticmethod
    def etag(data):
        return f'"{hashlib.md5(data).hexdigest()}"'

    def inspect(self, bucket, key):
        self._hit("inspect", bucket, key)
        data = self.objects.get((bucket, key))
        return None if data is None else {"size": len(data), "etag": self.etag(data)}

    def verify(self, bucket, key, size, sha256):
        self._hit("verify", bucket, key)
        data = self.objects.get((bucket, key))
        if data is None:
            raise client_error("NoSuchKey")
        if len(data) != size:
            raise IntegrityError("SIZE_MISMATCH")
        if hashlib.sha256(data).hexdigest() != sha256:
            raise IntegrityError("CHECKSUM_MISMATCH")
        return self.etag(data)

    def copy(self, source_bucket, source_key, bucket, key, etag, content_type, sha256):
        self._hit("copy", bucket, key)
        data = self.objects.get((source_bucket, source_key))
        if data is None:
            raise client_error("NoSuchKey")
        if self.etag(data) != etag:
            raise client_error("PreconditionFailed")
        self.objects[(bucket, key)] = data

    def delete(self, bucket, key):
        self._hit("delete", bucket, key)
        self.objects.pop((bucket, key), None)

    def count(self, operation):
        return sum(1 for call in self.calls if call[0] == operation)


def seed(storage: FakeStorage, **changes) -> uuid.UUID:
    now = datetime.now(UTC)
    object_id = uuid.uuid4()
    values = {
        "id": object_id,
        "bucket": get_settings().s3_bucket,
        "key": f"objects/{object_id}/v1",
        "filename": "fake.bin",
        "logical_key": f"fake/{object_id}.bin",
        "content_version": 1,
        "owner_id": f"fake-{object_id.hex}",
        "dataset_id": "fake-dataset",
        "workflow_state": "COMPLETE",
        "legal_hold": False,
        "retention_until": now + timedelta(days=400),
        "last_accessed_at": now - timedelta(days=200),
        "content_type": "application/octet-stream",
        "size_bytes": len(PAYLOAD),
        "checksum_sha256": DIGEST,
        "lifecycle_state": LifecycleState.ACTIVE,
        "desired_storage_class": "ACTIVE",
        "observed_storage_class": "ACTIVE",
        "ingestion_verified_at": now,
        **changes,
    }
    with SessionLocal() as db:
        db.add(StorageObject(**values))
        db.commit()
    storage.objects[(values["bucket"], values["key"])] = PAYLOAD
    return object_id


def request(object_id, job_type=JobType.ARCHIVE) -> uuid.UUID:
    with SessionLocal() as db:
        job, created = admit(db, object_id, job_type, uuid.uuid4().hex, "API")
        assert created
        return job.id


def state(job_id):
    with SessionLocal() as db:
        job = db.get(Job, job_id)
        obj = db.get(StorageObject, job.object_id)
        attempts = list(
            db.scalars(
                select(JobAttempt)
                .where(JobAttempt.job_id == job_id)
                .order_by(JobAttempt.attempt_number)
            )
        )
        return job, obj, attempts


def locations(job_id):
    job, _, _ = state(job_id)
    return (job.source_bucket, job.source_key), (job.destination_bucket, job.destination_key)


def archived(storage) -> tuple[uuid.UUID, uuid.UUID]:
    object_id = seed(storage)
    job_id = request(object_id)
    assert execute_job(job_id, storage) == "SUCCEEDED"
    return object_id, job_id


def test_archive_moves_verified_bytes_and_records_every_stage():
    storage = FakeStorage()
    object_id = seed(storage)
    job_id = request(object_id)
    source, destination = locations(job_id)
    assert execute_job(job_id, storage) == "SUCCEEDED"
    job, obj, [attempt] = state(job_id)
    assert job.status == JobStatus.SUCCEEDED and job.finished_at is not None
    assert (obj.lifecycle_state, obj.desired_storage_class, obj.observed_storage_class) == (
        LifecycleState.ARCHIVED,
        "ARCHIVE",
        "ARCHIVE",
    )
    assert (obj.archive_bucket, obj.archive_key) == destination
    assert obj.archive_copy_retained is False
    assert source not in storage.objects and storage.objects[destination] == PAYLOAD
    assert (attempt.stage, attempt.attempt_number, attempt.error_class) == ("COMMITTED", 1, None)
    calls = len(storage.calls)
    assert execute_job(job_id, storage) == "SKIPPED"
    assert len(storage.calls) == calls


def test_restore_retains_archive_and_rearchive_reuses_it():
    storage = FakeStorage()
    object_id, archive_job = archived(storage)
    _, archive_copy = locations(archive_job)
    restore_job = request(object_id, JobType.RESTORE)
    assert execute_job(restore_job, storage) == "SUCCEEDED"
    _, obj, _ = state(restore_job)
    assert (obj.lifecycle_state, obj.observed_storage_class) == (LifecycleState.ACTIVE, "ACTIVE")
    assert obj.archive_copy_retained is True and obj.restored_at is not None
    assert storage.objects[(obj.bucket, obj.key)] == PAYLOAD
    assert storage.objects[archive_copy] == PAYLOAD
    assert storage.count("delete") == 1

    copies = storage.count("copy")
    rearchive = request(object_id)
    assert execute_job(rearchive, storage) == "SUCCEEDED"
    assert storage.count("copy") == copies
    assert (obj.bucket, obj.key) not in storage.objects
    assert state(rearchive)[1].lifecycle_state == LifecycleState.ARCHIVED


def test_busy_object_is_skipped_and_left_pending():
    storage = FakeStorage()
    object_id = seed(storage)
    job_id = request(object_id)
    with object_lock(engine, object_id):
        assert execute_job(job_id, storage) == "BUSY"
    assert state(job_id)[0].status == JobStatus.PENDING
    assert storage.calls == []


def test_access_after_admission_is_caught_before_any_copy():
    storage = FakeStorage()
    object_id = seed(storage)
    job_id = request(object_id)
    with SessionLocal() as db:
        db.get(StorageObject, object_id).last_accessed_at = datetime.now(UTC)
        db.commit()
    assert execute_job(job_id, storage) == "FAILED"
    job, obj, _ = state(job_id)
    assert (job.error_class, obj.lifecycle_state, obj.desired_storage_class) == (
        "POLICY_REJECTED",
        LifecycleState.ACTIVE,
        "ACTIVE",
    )
    assert "RECENTLY_ACCESSED" in job.error_message
    assert storage.count("copy") == 0 and storage.count("delete") == 0


def test_hold_set_during_transfer_blocks_delete_and_records_the_replica():
    storage = FakeStorage()
    object_id = seed(storage)
    job_id = request(object_id)
    source, destination = locations(job_id)

    def legal_hold():
        with SessionLocal() as db:
            db.get(StorageObject, object_id).legal_hold = True
            db.commit()

    storage.fail("copy", legal_hold)
    assert execute_job(job_id, storage) == "FAILED"
    job, obj, _ = state(job_id)
    assert job.error_class == "POLICY_REJECTED"
    assert storage.objects[source] == PAYLOAD and storage.objects[destination] == PAYLOAD
    assert (obj.archive_bucket, obj.archive_key) == destination
    assert obj.archive_copy_retained is True
    assert (obj.lifecycle_state, obj.observed_storage_class) == (LifecycleState.ACTIVE, "ACTIVE")


def test_conflicting_destination_is_never_overwritten():
    storage = FakeStorage()
    object_id = seed(storage)
    job_id = request(object_id)
    source, destination = locations(job_id)
    storage.objects[destination] = b"x" * MIB
    assert execute_job(job_id, storage) == "FAILED"
    job, obj, _ = state(job_id)
    assert job.error_class == "DESTINATION_CONFLICT"
    assert storage.objects[destination] == b"x" * MIB and storage.objects[source] == PAYLOAD
    assert obj.archive_copy_retained is False and obj.archive_key is None
    assert storage.count("copy") == 0 and storage.count("delete") == 0


def test_corrupt_source_fails_and_marks_placement_unknown():
    storage = FakeStorage()
    object_id = seed(storage)
    job_id = request(object_id)
    source, destination = locations(job_id)
    storage.objects[source] = b"y" * MIB
    assert execute_job(job_id, storage) == "FAILED"
    job, obj, _ = state(job_id)
    assert job.error_class == "SOURCE_INTEGRITY"
    assert obj.observed_storage_class == "UNKNOWN"
    assert destination not in storage.objects


def test_source_changed_between_verify_and_copy_is_not_success():
    storage = FakeStorage()
    object_id = seed(storage)
    job_id = request(object_id)
    source, destination = locations(job_id)
    storage.fail("copy", lambda: storage.objects.__setitem__(source, b"z" * MIB))
    assert execute_job(job_id, storage) == "FAILED"
    assert state(job_id)[0].error_class == "SOURCE_CHANGED"
    assert destination not in storage.objects


def test_transient_reads_back_off_until_due_and_exhaust_with_history():
    storage = FakeStorage()
    object_id = seed(storage)
    job_id = request(object_id)
    with SessionLocal() as db:
        db.get(Job, job_id).max_attempts = 2
        db.commit()
    now = datetime.now(UTC)
    storage.fail("inspect", EndpointConnectionError(endpoint_url="http://storage"))
    assert execute_job(job_id, storage, clock=lambda: now, jitter=lambda: 0.0) == "RETRY_SCHEDULED"
    job, obj, attempts = state(job_id)
    assert job.next_attempt_at == now + retry_delay(1, 0.0)
    assert (job.error_class, len(attempts)) == ("STORAGE_UNAVAILABLE", 1)
    assert obj.lifecycle_state == LifecycleState.ARCHIVING
    assert execute_job(job_id, storage, clock=lambda: now) == "SKIPPED"

    later = now + timedelta(minutes=5)
    storage.fail("inspect", EndpointConnectionError(endpoint_url="http://storage"))
    assert execute_job(job_id, storage, clock=lambda: later) == "FAILED"
    job, obj, attempts = state(job_id)
    assert job.error_class == "RETRY_EXHAUSTED"
    assert [attempt.error_class for attempt in attempts] == ["STORAGE_UNAVAILABLE"] * 2
    assert obj.lifecycle_state == LifecycleState.ACTIVE


def test_retry_delay_is_capped_with_bounded_jitter():
    settings = get_settings()
    assert retry_delay(1, 0.0) == timedelta(seconds=settings.retry_base_seconds)
    assert retry_delay(30, 0.0) == timedelta(seconds=settings.retry_cap_seconds)
    assert retry_delay(30, 1.0) == timedelta(
        seconds=settings.retry_cap_seconds * (1 + settings.retry_jitter_ratio)
    )


def test_uncertain_copy_blocks_and_cannot_be_admin_retried():
    storage = FakeStorage()
    object_id = seed(storage)
    job_id = request(object_id)
    storage.fail("copy", ReadTimeoutError(endpoint_url="http://storage"))
    assert execute_job(job_id, storage) == "RECOVERY_REQUIRED"
    job, obj, _ = state(job_id)
    assert job.error_class == "STORAGE_MUTATION_UNCERTAIN"
    assert obj.lifecycle_state == LifecycleState.ARCHIVING
    assert execute_job(job_id, storage) == "SKIPPED"
    with SessionLocal() as db, pytest.raises(AdmissionError):
        retry(db, job_id, "admin-retry", "synthetic")


def test_uncertain_delete_then_controlled_recovery_completes_the_same_job():
    storage = FakeStorage()
    object_id = seed(storage)
    job_id = request(object_id)
    storage.fail("delete", ReadTimeoutError(endpoint_url="http://storage"))
    assert execute_job(job_id, storage) == "RECOVERY_REQUIRED"
    assert state(job_id)[2][0].stage == "DELETE_INTENT"
    assert execute_job(job_id, storage, attestation=OPERATOR) == "SUCCEEDED"
    job, obj, attempts = state(job_id)
    assert [attempt.stage for attempt in attempts] == ["DELETE_INTENT", "COMMITTED"]
    assert obj.lifecycle_state == LifecycleState.ARCHIVED
    with SessionLocal() as db:
        started = db.scalar(
            select(AuditEvent).where(
                AuditEvent.job_id == job_id, AuditEvent.event_type == "JOB_RECOVERY_STARTED"
            )
        )
        assert started.details["operator"] == "synthetic-operator"
        assert started.details["previous_status"] == "RECOVERY_REQUIRED"


def test_lost_ownership_before_delete_blocks_without_deleting(monkeypatch):
    storage = FakeStorage()
    object_id = seed(storage)
    job_id = request(object_id)
    source, _ = locations(job_id)

    def lost(self):
        raise OwnershipLost("synthetic")

    monkeypatch.setattr(ObjectLock, "verify", lost)
    assert execute_job(job_id, storage) == "RECOVERY_REQUIRED"
    assert state(job_id)[0].error_class == "OWNERSHIP_LOST"
    assert storage.objects[source] == PAYLOAD and storage.count("delete") == 0


def test_commit_failure_after_delete_is_recovered_from_the_verified_copy(monkeypatch):
    storage = FakeStorage()
    object_id = seed(storage)
    job_id = request(object_id)
    source, destination = locations(job_id)
    original_commit = Session.commit

    def interrupted(db):
        if any(isinstance(row, AuditEvent) and row.event_type == "JOB_SUCCEEDED" for row in db.new):
            raise OperationalError("commit", {}, RuntimeError("Injected commit failure"))
        return original_commit(db)

    with monkeypatch.context() as context:
        context.setattr(Session, "commit", interrupted)
        assert execute_job(job_id, storage) == "RECOVERY_REQUIRED"
    job, obj, [attempt] = state(job_id)
    assert (job.error_class, attempt.stage) == ("UNEXPECTED_ERROR", "SOURCE_DELETED")
    assert source not in storage.objects and storage.objects[destination] == PAYLOAD
    copies = storage.count("copy")
    assert execute_job(job_id, storage, attestation=OPERATOR) == "SUCCEEDED"
    assert storage.count("copy") == copies
    assert state(job_id)[1].lifecycle_state == LifecycleState.ARCHIVED


def test_missing_source_without_delete_intent_is_not_explained_away():
    storage = FakeStorage()
    object_id = seed(storage)
    job_id = request(object_id)
    source, destination = locations(job_id)
    storage.objects[destination] = storage.objects.pop(source)
    assert execute_job(job_id, storage) == "RECOVERY_REQUIRED"
    job, obj, _ = state(job_id)
    assert job.error_class == "UNEXPLAINED_SOURCE_LOSS"
    assert obj.observed_storage_class == "UNKNOWN"
    assert storage.count("delete") == 0


def test_both_copies_missing_is_blocked_as_data_missing():
    storage = FakeStorage()
    object_id = seed(storage)
    job_id = request(object_id)
    source, _ = locations(job_id)
    del storage.objects[source]
    assert execute_job(job_id, storage) == "RECOVERY_REQUIRED"
    job, obj, _ = state(job_id)
    assert (job.error_class, obj.observed_storage_class) == ("DATA_MISSING", "UNKNOWN")
    assert storage.count("copy") == 0 and storage.count("delete") == 0


def test_restore_failures_never_touch_the_archive_copy():
    storage = FakeStorage()
    object_id, archive_job = archived(storage)
    _, archive_copy = locations(archive_job)
    restore_job = request(object_id, JobType.RESTORE)
    _, active = locations(restore_job)
    storage.objects[active] = b"w" * MIB
    assert execute_job(restore_job, storage) == "FAILED"
    job, obj, _ = state(restore_job)
    assert job.error_class == "DESTINATION_CONFLICT"
    assert (obj.lifecycle_state, obj.desired_storage_class) == (LifecycleState.ARCHIVED, "ARCHIVE")
    assert storage.objects[archive_copy] == PAYLOAD
    assert storage.count("delete") == 1

    del storage.objects[active]
    storage.objects[archive_copy] = b"v" * MIB
    corrupt_restore = request(object_id, JobType.RESTORE)
    assert execute_job(corrupt_restore, storage) == "FAILED"
    job, obj, _ = state(corrupt_restore)
    assert (job.error_class, obj.observed_storage_class) == ("SOURCE_INTEGRITY", "UNKNOWN")
    assert storage.objects[archive_copy] == b"v" * MIB


def test_startup_quarantine_blocks_only_unowned_running_jobs():
    storage = FakeStorage()
    free_job = request(seed(storage))
    owned_object = seed(storage)
    owned_job = request(owned_object)
    with SessionLocal() as db:
        for job_id in (free_job, owned_job):
            db.get(Job, job_id).status = JobStatus.RUNNING
        db.commit()
    with object_lock(engine, owned_object):
        quarantined = quarantine_interrupted()
    assert str(free_job) in quarantined and str(owned_job) not in quarantined
    assert state(free_job)[0].status == JobStatus.RECOVERY_REQUIRED
    assert state(free_job)[0].error_class == "WORKER_INTERRUPTED"
    assert state(owned_job)[0].status == JobStatus.RUNNING
    assert execute_job(owned_job, storage, attestation=OPERATOR) == "SUCCEEDED"


@pytest.mark.parametrize(
    "attestation",
    [
        Attestation("synthetic-operator", False, True),
        Attestation("synthetic-operator", True, False),
        Attestation(" ", True, True),
    ],
)
def test_recovery_requires_operator_and_attestations(attestation):
    storage = FakeStorage()
    job_id = request(seed(storage))
    storage.fail("copy", ReadTimeoutError(endpoint_url="http://storage"))
    assert execute_job(job_id, storage) == "RECOVERY_REQUIRED"
    with pytest.raises(RecoveryRefused):
        execute_job(job_id, storage, attestation=attestation)
    assert state(job_id)[0].status == JobStatus.RECOVERY_REQUIRED
    assert len(state(job_id)[2]) == 1


def test_recovery_refuses_jobs_that_are_not_blocked():
    storage = FakeStorage()
    job_id = request(seed(storage))
    with pytest.raises(RecoveryRefused):
        execute_job(job_id, storage, attestation=OPERATOR)
    with pytest.raises(RecoveryRefused):
        execute_job(uuid.uuid4(), storage, attestation=OPERATOR)
    assert state(job_id)[0].status == JobStatus.PENDING
    assert storage.calls == []
