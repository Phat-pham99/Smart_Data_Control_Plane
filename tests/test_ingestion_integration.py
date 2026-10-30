from __future__ import annotations

import hashlib
import os
import uuid
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest
from app.db.session import SessionLocal
from app.enums import LifecycleState
from app.main import app
from app.models import AuditEvent, StorageObject
from app.storage import get_storage
from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from tests.test_objects import request_data

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_INTEGRATION") != "1",
        reason="Set RUN_INTEGRATION=1 with the isolated Compose services",
    ),
]
client = TestClient(app)


def initiate(payload=b"synthetic engineering data", **changes):
    owner = f"test-{uuid.uuid4().hex}"
    metadata = request_data(
        owner_id=owner,
        size_bytes=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
        **changes,
    )
    headers = {"Idempotency-Key": uuid.uuid4().hex}
    response = client.post("/v1/uploads/initiate", json=metadata, headers=headers)
    assert response.status_code == 201, response.text
    return metadata, headers, response.json()


def upload(session, payload):
    form = session["upload"]
    return httpx.post(
        form["url"],
        data=form["fields"],
        files={"file": ("synthetic.bin", payload, "application/octet-stream")},
        timeout=15,
    )


def test_real_upload_catalog_query_download_and_access():
    payload = b"synthetic engineering data"
    metadata, headers, session = initiate(payload)
    object_id = session["object"]["id"]
    assert session["object"]["lifecycle_state"] == "INGESTING"
    assert client.get(f"/v1/objects/{object_id}/download").status_code == 409
    assert client.post(f"/v1/uploads/{object_id}/complete").status_code == 409
    assert upload(session, payload).status_code == 204
    completed = client.post(f"/v1/uploads/{object_id}/complete")
    assert completed.status_code == 200, completed.text
    assert completed.json()["observed_storage_class"] == "ACTIVE"
    assert client.post(f"/v1/uploads/{object_id}/complete").json() == completed.json()
    duplicate = client.post("/v1/uploads/initiate", json=metadata, headers=headers)
    assert duplicate.json()["object"]["id"] == object_id
    assert duplicate.json()["upload"] is None
    assert client.get(f"/v1/objects/{object_id}").json() == completed.json()
    listing = client.get(
        "/v1/objects",
        params={
            "owner_id": metadata["owner_id"],
            "dataset_id": metadata["dataset_id"],
            "lifecycle_state": "ACTIVE",
        },
    )
    assert [row["id"] for row in listing.json()["items"]] == [object_id]
    download = client.get(f"/v1/objects/{object_id}/download")
    assert download.status_code == 200
    result = httpx.get(download.json()["url"], timeout=15)
    assert result.status_code == 200
    assert result.content == payload
    assert hashlib.sha256(result.content).hexdigest() == metadata["sha256"]
    assert client.post(f"/v1/objects/{object_id}/access").json()["last_accessed_at"]
    with SessionLocal() as db:
        assert (
            db.scalar(
                select(func.count())
                .select_from(StorageObject)
                .where(StorageObject.owner_id == metadata["owner_id"])
            )
            == 1
        )
        reasons = list(
            db.scalars(
                select(AuditEvent.reason).where(AuditEvent.object_id == uuid.UUID(object_id))
            )
        )
        assert reasons.count("INGESTION_VERIFIED") == 1
        assert "APPLICATION_ACCESS_RECORDED" in reasons
    assert client.get("/health/ready").status_code == 200


def test_conflicting_idempotency_and_logical_identity():
    metadata, headers, session = initiate()
    changed = {**metadata, "dataset_id": "different-dataset"}
    assert client.post("/v1/uploads/initiate", json=changed, headers=headers).status_code == 409
    assert (
        client.post(
            "/v1/uploads/initiate", json=metadata, headers={"Idempotency-Key": uuid.uuid4().hex}
        ).status_code
        == 409
    )
    assert client.post("/v1/uploads/initiate", json=metadata).status_code == 422
    assert (
        client.post(
            "/v1/uploads/initiate", json=metadata, headers={"Idempotency-Key": " "}
        ).status_code
        == 422
    )
    assert session["object"]["observed_storage_class"] == "UNKNOWN"


def test_corruption_blocks_completion_and_can_be_retried():
    payload = b"good"
    metadata, headers, session = initiate(payload)
    object_id = session["object"]["id"]
    assert upload(session, b"evil").status_code == 204
    failed = client.post(f"/v1/uploads/{object_id}/complete")
    assert failed.status_code == 409
    assert failed.json()["detail"] == "CHECKSUM_MISMATCH"
    obj = client.get(f"/v1/objects/{object_id}").json()
    assert obj["lifecycle_state"] == "FAILED"
    assert obj["observed_storage_class"] == "UNKNOWN"
    assert client.get(f"/v1/objects/{object_id}/download").status_code == 409
    assert upload(session, payload).status_code == 204
    assert client.post(f"/v1/uploads/{object_id}/complete").json()["lifecycle_state"] == "ACTIVE"


def test_storage_rejects_oversized_presigned_upload():
    _, _, session = initiate(b"good")
    assert upload(session, b"too many bytes").status_code in (400, 403)
    oversized = request_data(owner_id=f"test-{uuid.uuid4().hex}", size_bytes=1024 * 1024 * 1024 + 1)
    assert (
        client.post(
            "/v1/uploads/initiate", json=oversized, headers={"Idempotency-Key": uuid.uuid4().hex}
        ).status_code
        == 413
    )


def test_cursor_pagination_and_validation():
    metadata, headers, session = initiate()
    for version in (2, 3):
        response = client.post(
            "/v1/uploads/initiate",
            json={**metadata, "content_version": version},
            headers={"Idempotency-Key": uuid.uuid4().hex},
        )
        assert response.status_code == 201, response.text
    seen = []
    cursor = None
    for _ in range(3):
        params = {"owner_id": metadata["owner_id"], "limit": 1}
        if cursor:
            params["cursor"] = cursor
        page = client.get("/v1/objects", params=params)
        assert page.status_code == 200, page.text
        seen.extend(row["id"] for row in page.json()["items"])
        cursor = page.json()["next_cursor"]
    assert cursor is None
    assert len(set(seen)) == 3
    assert client.get("/v1/objects", params={"cursor": "not-a-cursor"}).status_code == 422
    assert client.get("/v1/objects", params={"limit": 101}).status_code == 422


def test_object_lock_blocks_concurrent_completion():
    _, _, session = initiate()
    object_id = uuid.UUID(session["object"]["id"])
    from app.db.session import engine
    from sqlalchemy import text

    with engine.connect() as connection:
        key = int.from_bytes(object_id.bytes[:8], "big", signed=True)
        connection.execute(text("SELECT pg_advisory_lock(:key)"), {"key": key})
        connection.commit()
        try:
            assert client.post(f"/v1/uploads/{object_id}/complete").status_code == 409
        finally:
            connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": key})
            connection.commit()


def test_private_storage_and_bounded_upload_capability():
    payload = b"synthetic engineering data"
    _, _, session = initiate(payload)
    assert upload(session, payload).status_code == 204
    object_id = session["object"]["id"]
    assert client.post(f"/v1/uploads/{object_id}/complete").status_code == 200
    with SessionLocal() as db:
        obj = db.get(StorageObject, uuid.UUID(object_id))
        assert obj.lifecycle_state == LifecycleState.ACTIVE
        endpoint = (
            get_storage().settings.s3_public_endpoint_url or get_storage().settings.s3_endpoint_url
        )
        assert httpx.get(f"{endpoint}/{obj.bucket}/{obj.key}", timeout=10).status_code == 403
    form = session["upload"]
    changed_fields = {**form["fields"], "key": "objects/not-authorized"}
    result = httpx.post(
        form["url"], data=changed_fields, files={"file": ("synthetic.bin", payload)}, timeout=15
    )
    assert result.status_code == 403


def test_concurrent_initiation_returns_one_catalog_object():
    metadata = request_data(owner_id=f"test-{uuid.uuid4().hex}")
    headers = {"Idempotency-Key": uuid.uuid4().hex}
    with ThreadPoolExecutor(max_workers=2) as executor:
        responses = list(
            executor.map(
                lambda _: client.post("/v1/uploads/initiate", json=metadata, headers=headers),
                range(2),
            )
        )
    assert [response.status_code for response in responses] == [201, 201]
    assert responses[0].json()["object"]["id"] == responses[1].json()["object"]["id"]


def test_failed_final_commit_preserves_retryable_ingestion(monkeypatch):
    payload = b"synthetic engineering data"
    _, _, session = initiate(payload)
    object_id = session["object"]["id"]
    assert upload(session, payload).status_code == 204
    original_commit = Session.commit

    def interrupted_commit(db):
        if any(
            isinstance(row, AuditEvent) and row.reason == "INGESTION_VERIFIED" for row in db.new
        ):
            raise OperationalError(
                "synthetic failure", {}, RuntimeError("Injected commit interruption")
            )
        return original_commit(db)

    with monkeypatch.context() as context:
        context.setattr(Session, "commit", interrupted_commit)
        with TestClient(app, raise_server_exceptions=False) as failing_client:
            assert failing_client.post(f"/v1/uploads/{object_id}/complete").status_code == 500
    assert client.get(f"/v1/objects/{object_id}").json()["lifecycle_state"] == "INGESTING"
    assert client.post(f"/v1/uploads/{object_id}/complete").json()["lifecycle_state"] == "ACTIVE"


def test_destination_corruption_prevents_success(monkeypatch):
    payload = b"synthetic engineering data"
    _, _, session = initiate(payload)
    object_id = session["object"]["id"]
    assert upload(session, payload).status_code == 204
    storage = get_storage()
    original_copy = storage.client.copy_object

    def corrupt_destination(**options):
        result = original_copy(**options)
        storage.client.put_object(
            Bucket=options["Bucket"], Key=options["Key"], Body=b"x" * len(payload)
        )
        return result

    with monkeypatch.context() as context:
        context.setattr(storage.client, "copy_object", corrupt_destination)
        assert client.post(f"/v1/uploads/{object_id}/complete").status_code == 409
    assert client.get(f"/v1/objects/{object_id}").json()["observed_storage_class"] == "UNKNOWN"
    assert client.post(f"/v1/uploads/{object_id}/complete").json()["lifecycle_state"] == "ACTIVE"


def test_time_filters_and_missing_object():
    metadata, _, session = initiate()
    created = session["object"]["created_at"]
    params = {"owner_id": metadata["owner_id"], "created_before": created}
    assert client.get("/v1/objects", params=params).json()["items"] == []
    assert (
        client.get("/v1/objects", params={"created_before": "2026-01-01T00:00:00"}).status_code
        == 422
    )
    assert client.get(f"/v1/objects/{uuid.uuid4()}").status_code == 404
