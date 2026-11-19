from __future__ import annotations

import json
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import pytest
from app.config import get_settings
from app.db.session import SessionLocal, engine
from app.enums import JobStatus, JobType, LifecycleState
from app.lifecycle import enqueue_candidates
from app.main import app
from app.models import AuditEvent, Job, JobAttempt, OutboxEvent, StorageObject
from app.ownership import OwnershipLost, object_lock
from app.policies import PolicyRef, load_policy
from app.policies import main as policy_cli
from fastapi.testclient import TestClient
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from tests.test_ingestion_integration import client, initiate, pytestmark, upload  # noqa: F401

MIB = 1024 * 1024
ADMIN_KEY = "synthetic-admin-key"


def iso_days(days: float) -> str:
    return (datetime.now(UTC) + timedelta(days=days)).isoformat()


def verified_object(payload=b"small synthetic artifact", **changes):
    metadata = {
        "workflow_state": "COMPLETE",
        "last_application_access_at": iso_days(-200),
        "retention_until": iso_days(400),
        **changes,
    }
    metadata, headers, session = initiate(payload, **metadata)
    assert upload(session, payload).status_code == 204
    completed = client.post(f"/v1/uploads/{session['object']['id']}/complete")
    assert completed.status_code == 200, completed.text
    return metadata, headers, session, uuid.UUID(completed.json()["id"])


def eligible_object(**changes):
    payload = (b"synthetic-cold-dataset " * (MIB // 23 + 1))[:MIB]
    return verified_object(payload, **changes)


def archive(object_id, key=None):
    return client.post(
        f"/v1/objects/{object_id}/archive", headers={"Idempotency-Key": key or uuid.uuid4().hex}
    )


def counts(object_id):
    with SessionLocal() as db:
        jobs = db.scalar(select(func.count()).select_from(Job).where(Job.object_id == object_id))
        outbox = db.scalar(
            select(func.count())
            .select_from(OutboxEvent)
            .join(Job, Job.id == OutboxEvent.job_id)
            .where(Job.object_id == object_id)
        )
        requested = db.scalar(
            select(func.count())
            .select_from(AuditEvent)
            .where(AuditEvent.object_id == object_id, AuditEvent.job_id.is_not(None))
        )
        return jobs, outbox, requested


def mark_archived(object_id):
    """Fixture state for restore admission only; Phase 3 performs real transfers."""

    with SessionLocal() as db:
        obj = db.get(StorageObject, object_id)
        obj.lifecycle_state = LifecycleState.ARCHIVED
        obj.desired_storage_class = "ARCHIVE"
        obj.observed_storage_class = "ARCHIVE"
        obj.archive_bucket = get_settings().s3_archive_bucket
        obj.archive_key = f"objects/{object_id}/v{obj.content_version}"
        db.commit()


def settle_failure(job_id):
    """Simulate the settled, non-destructive archive failure a worker would record."""

    with SessionLocal() as db:
        job = db.get(Job, job_id)
        job.status = JobStatus.FAILED
        job.error_class = "SYNTHETIC_FAILURE"
        job.finished_at = datetime.now(UTC)
        job.attempt_count = 1
        db.add(
            JobAttempt(
                job_id=job.id, attempt_number=1, stage="COPY", error_class="SYNTHETIC_FAILURE"
            )
        )
        job.object.lifecycle_state = LifecycleState.ACTIVE
        job.object.desired_storage_class = "ACTIVE"
        db.commit()


def test_archive_admission_is_atomic_idempotent_and_explained():
    metadata, headers, _, object_id = eligible_object()
    key = uuid.uuid4().hex
    response = archive(object_id, key)
    assert response.status_code == 202, response.text
    job = response.json()
    assert response.headers["Location"] == f"/v1/jobs/{job['id']}"
    assert job["status"] == "PENDING"
    assert job["actor_kind"] == "API"
    assert job["decision"]["eligible"] is True
    assert job["decision"]["policy_name"] == "archive-completed-cold-datasets"
    assert "destination_key" not in response.text and "lifecycle-archive" not in response.text
    replay = archive(object_id, key)
    assert replay.status_code == 202
    assert replay.json()["id"] == job["id"]
    assert counts(object_id) == (1, 1, 1)

    obj = client.get(f"/v1/objects/{object_id}").json()
    assert (obj["lifecycle_state"], obj["desired_storage_class"]) == (
        "ARCHIVE_REQUESTED",
        "ARCHIVE",
    )
    assert obj["observed_storage_class"] == "ACTIVE"
    with SessionLocal() as db:
        stored = db.get(Job, uuid.UUID(job["id"]))
        assert stored.expected_sha256 == metadata["sha256"]
        assert stored.expected_size == MIB
        assert stored.destination_bucket == get_settings().s3_archive_bucket
        assert stored.destination_key == f"objects/{object_id}/v1"
        event = db.scalar(select(OutboxEvent).where(OutboxEvent.job_id == stored.id))
        assert event.payload == {"job_id": job["id"]}
        assert event.published_at is None
        audit = db.scalar(select(AuditEvent).where(AuditEvent.job_id == stored.id))
        assert audit.event_type == "ARCHIVE_REQUESTED"
        assert audit.details["decision"]["inputs"]["workflow_state"] == "COMPLETE"

    assert archive(object_id).status_code == 409
    assert (
        client.post(
            f"/v1/objects/{object_id}/restore", headers={"Idempotency-Key": uuid.uuid4().hex}
        ).status_code
        == 409
    )
    assert counts(object_id) == (1, 1, 1)
    assert client.get(f"/v1/jobs/{job['id']}").json()["id"] == job["id"]
    assert client.get(f"/v1/jobs/{job['id']}/attempts").json() == []
    assert client.get(f"/v1/jobs/{uuid.uuid4()}").status_code == 404
    assert client.get(f"/v1/objects/{object_id}/download").status_code == 409

    replayed_upload = client.post("/v1/uploads/initiate", json=metadata, headers=headers)
    assert replayed_upload.status_code == 201
    assert replayed_upload.json()["upload"] is None
    completed = client.post(f"/v1/uploads/{object_id}/complete")
    assert completed.json()["lifecycle_state"] == "ARCHIVE_REQUESTED"


@pytest.mark.parametrize(
    "changes,reason",
    [
        ({"legal_hold": True}, "LEGAL_HOLD"),
        ({"last_application_access_at": iso_days(-10)}, "RECENTLY_ACCESSED"),
        ({"retention_until": iso_days(30)}, "RETENTION_TOO_SHORT"),
        ({"workflow_state": "IN_PROGRESS"}, "WORKFLOW_NOT_COMPLETE"),
        ({"last_application_access_at": None}, "LAST_ACCESS_UNKNOWN"),
        ({}, "TOO_SMALL"),
    ],
)
def test_policy_rejections_have_reason_codes_and_no_side_effects(changes, reason):
    _, _, _, object_id = verified_object(**changes)
    response = archive(object_id)
    assert response.status_code == 409
    assert reason in response.json()["detail"]["reasons"]
    assert counts(object_id) == (0, 0, 0)
    assert client.get(f"/v1/objects/{object_id}").json()["lifecycle_state"] == "ACTIVE"


def test_admission_requires_verified_object_and_key():
    _, _, session = initiate()
    assert archive(session["object"]["id"]).status_code == 409
    assert archive(uuid.uuid4()).status_code == 404
    assert client.post(f"/v1/objects/{uuid.uuid4()}/archive").status_code == 422


def test_concurrent_same_key_returns_one_job_and_different_keys_conflict():
    _, _, _, same = eligible_object()
    key = uuid.uuid4().hex
    with ThreadPoolExecutor(max_workers=4) as executor:
        responses = list(executor.map(lambda _: archive(same, key), range(4)))
    assert {response.status_code for response in responses} == {202}
    assert len({response.json()["id"] for response in responses}) == 1

    _, _, _, competing = eligible_object()
    with ThreadPoolExecutor(max_workers=4) as executor:
        responses = list(executor.map(lambda _: archive(competing), range(4)))
    assert sorted(response.status_code for response in responses) == [202, 409, 409, 409]
    assert counts(same)[:2] == (1, 1)
    assert counts(competing)[:2] == (1, 1)


def test_failed_commit_rolls_back_state_job_audit_and_outbox(monkeypatch):
    _, _, _, object_id = eligible_object()
    original_commit = Session.commit

    def interrupted_commit(db):
        if any(isinstance(row, OutboxEvent) for row in db.new):
            raise IntegrityError("synthetic", {}, RuntimeError("Injected commit failure"))
        return original_commit(db)

    with monkeypatch.context() as context:
        context.setattr(Session, "commit", interrupted_commit)
        with TestClient(app, raise_server_exceptions=False) as failing_client:
            response = failing_client.post(
                f"/v1/objects/{object_id}/archive", headers={"Idempotency-Key": "rollback"}
            )
            assert response.status_code == 500
    assert counts(object_id) == (0, 0, 0)
    obj = client.get(f"/v1/objects/{object_id}").json()
    assert (obj["lifecycle_state"], obj["desired_storage_class"]) == ("ACTIVE", "ACTIVE")
    assert archive(object_id, "rollback").status_code == 202


def test_restore_admission_requires_verified_archive_placement():
    _, _, _, object_id = verified_object()
    restore = {"Idempotency-Key": uuid.uuid4().hex}
    assert client.post(f"/v1/objects/{object_id}/restore", headers=restore).status_code == 409
    mark_archived(object_id)
    assert archive(object_id).status_code == 409
    response = client.post(f"/v1/objects/{object_id}/restore", headers=restore)
    assert response.status_code == 202, response.text
    assert response.json()["decision"] is None
    assert (
        client.post(f"/v1/objects/{object_id}/restore", headers=restore).json()["id"]
        == (response.json()["id"])
    )
    with SessionLocal() as db:
        job = db.get(Job, uuid.UUID(response.json()["id"]))
        obj = job.object
        assert (job.source_bucket, job.source_key) == (obj.archive_bucket, obj.archive_key)
        assert (job.destination_bucket, job.destination_key) == (obj.bucket, obj.key)
        assert obj.lifecycle_state == LifecycleState.RESTORE_REQUESTED
        assert obj.desired_storage_class == "ACTIVE"
    assert archive(object_id).status_code == 409


def test_policy_enqueue_uses_the_same_guards_as_manual_admission(capsys):
    _, _, _, eligible = eligible_object(last_application_access_at=iso_days(-3650))
    _, _, _, small = verified_object(last_application_access_at=iso_days(-3650))
    with SessionLocal() as db:
        ref = PolicyRef.from_model(load_policy(db, get_settings().archive_policy_name))
        results = {row["object_id"]: row for row in enqueue_candidates(db, ref, 500)}
    assert results[str(eligible)]["created"] is True
    assert str(small) not in results
    job = client.get(f"/v1/jobs/{results[str(eligible)]['job_id']}").json()
    assert job["actor_kind"] == "POLICY"
    assert job["decision"]["eligible"] is True
    with SessionLocal() as db:
        rerun = enqueue_candidates(db, ref, 500)
    assert str(eligible) not in {row["object_id"] for row in rerun}
    assert counts(eligible)[0] == 1
    assert policy_cli(["evaluate", "--object-id", str(small)]) == 0
    [explanation] = json.loads(capsys.readouterr().out)
    assert explanation["eligible"] is False
    assert explanation["reasons"] == ["TOO_SMALL"]
    assert explanation["policy"]["version"] == 1


def test_activated_policy_versions_are_immutable():
    with SessionLocal() as db:
        policy = load_policy(db, get_settings().archive_policy_name)
        policy.config = {**policy.config, "action": {"desired_storage_class": "ARCHIVE"}, "x": 1}
        with pytest.raises(IntegrityError):
            db.commit()
        db.rollback()
        policy = load_policy(db, get_settings().archive_policy_name)
        assert "x" not in policy.config


def test_admin_retry_security_linkage_and_history(monkeypatch):
    settings = get_settings()
    _, _, _, object_id = eligible_object()
    original = archive(object_id).json()
    url = f"/v1/admin/jobs/{original['id']}/retry"
    body = {"reason": "Synthetic operator retry"}
    headers = {"Idempotency-Key": "retry-1", "X-Admin-API-Key": ADMIN_KEY}

    monkeypatch.setattr(settings, "admin_api_key", None)
    assert client.post(url, json=body, headers=headers).status_code == 503
    monkeypatch.setattr(settings, "admin_api_key", ADMIN_KEY)
    assert client.post(url, json=body, headers={"Idempotency-Key": "x"}).status_code == 401
    wrong = {**headers, "X-Admin-API-Key": "wrong"}
    assert client.post(url, json=body, headers=wrong).status_code == 401
    assert client.post(f"/v1/admin/jobs/{uuid.uuid4()}/retry", json=body).status_code == 401
    assert client.post(url, json=body, headers=headers).status_code == 409
    assert counts(object_id)[0] == 1

    settle_failure(uuid.UUID(original["id"]))
    response = client.post(url, json=body, headers=headers)
    assert response.status_code == 202, response.text
    replacement = response.json()
    assert replacement["retry_of_job_id"] == original["id"]
    assert replacement["actor_kind"] == "ADMIN"
    assert replacement["status"] == "PENDING"
    assert client.post(url, json=body, headers=headers).json()["id"] == replacement["id"]
    assert (
        client.post(url, json=body, headers={**headers, "Idempotency-Key": "retry-2"}).status_code
        == 409
    )
    previous = client.get(f"/v1/jobs/{original['id']}").json()
    assert previous["status"] == "FAILED"
    assert previous["replaced_by_job_id"] == replacement["id"]
    assert len(client.get(f"/v1/jobs/{original['id']}/attempts").json()) == 1
    with SessionLocal() as db:
        audit = db.scalar(
            select(AuditEvent).where(AuditEvent.job_id == uuid.UUID(replacement["id"]))
        )
        assert (audit.reason, audit.actor_kind) == ("Synthetic operator retry", "ADMIN")
        db.get(Job, uuid.UUID(replacement["id"])).status = JobStatus.RECOVERY_REQUIRED
        db.commit()
    uncertain = f"/v1/admin/jobs/{replacement['id']}/retry"
    assert (
        client.post(uncertain, json=body, headers={**headers, "Idempotency-Key": "r3"}).status_code
        == 409
    )
    assert (
        client.post(f"/v1/admin/jobs/{uuid.uuid4()}/retry", json=body, headers=headers).status_code
        == 404
    )
    assert client.post(url, json={"reason": ""}, headers=headers).status_code == 422


def test_access_and_download_wait_for_object_ownership(monkeypatch):
    _, _, _, object_id = verified_object()
    monkeypatch.setattr(get_settings(), "object_lock_wait_seconds", 0.2)
    with object_lock(engine, object_id):
        assert client.post(f"/v1/objects/{object_id}/access").status_code == 409
        assert client.get(f"/v1/objects/{object_id}/download").status_code == 409
        assert archive(object_id).status_code == 409
    accessed = client.post(f"/v1/objects/{object_id}/access")
    assert accessed.status_code == 200
    assert client.get(f"/v1/objects/{object_id}/download").status_code == 200


@pytest.mark.parametrize("first_byte", [0x00, 0xFF])
def test_lock_verification_detects_lost_ownership(first_byte):
    object_id = uuid.UUID(bytes=bytes([first_byte]) + uuid.uuid4().bytes[1:])
    with object_lock(engine, object_id) as lock:
        lock.verify()
        lock.connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": lock.key})
        lock.connection.commit()
        with pytest.raises(OwnershipLost):
            lock.verify()


def test_one_nonterminal_job_per_object_under_concurrent_transactions():
    _, _, _, object_id = verified_object()

    def pending_job(db):
        db.add(
            Job(
                job_type=JobType.ARCHIVE,
                status=JobStatus.PENDING,
                object_id=object_id,
                idempotency_key=uuid.uuid4().hex,
                max_attempts=5,
                attempt_count=0,
            )
        )
        db.flush()

    with SessionLocal() as first, SessionLocal() as second:
        pending_job(first)
        with ThreadPoolExecutor(max_workers=1) as executor:
            blocked = executor.submit(pending_job, second)
            first.commit()
            with pytest.raises(IntegrityError):
                blocked.result(timeout=10)
    assert counts(object_id)[0] == 1
