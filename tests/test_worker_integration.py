from __future__ import annotations

import hashlib
import json
import uuid

import httpx
import pytest
from app.config import get_settings
from app.execution import execute_job
from app.storage import get_storage, get_worker_storage
from app.tasks import lifecycle as worker_cli
from app.tasks.lifecycle import run_once
from botocore.exceptions import ClientError

from tests.test_ingestion_integration import client, pytestmark  # noqa: F401
from tests.test_lifecycle_integration import archive, eligible_object

MIB = 1024 * 1024


def restore(object_id):
    return client.post(
        f"/v1/objects/{object_id}/restore", headers={"Idempotency-Key": uuid.uuid4().hex}
    )


def denied(call, **options):
    with pytest.raises(ClientError) as error:
        call(**options)
    assert error.value.response["Error"]["Code"] in ("AccessDenied", "403")


def test_real_archive_restore_and_rearchive_round_trip(monkeypatch):
    worker = get_worker_storage()
    metadata, headers, _, object_id = eligible_object()
    size, digest = MIB, metadata["sha256"]
    job = archive(object_id).json()
    assert execute_job(uuid.UUID(job["id"]), worker) == "SUCCEEDED"

    archived = client.get(f"/v1/objects/{object_id}").json()
    assert (archived["lifecycle_state"], archived["observed_storage_class"]) == (
        "ARCHIVED",
        "ARCHIVE",
    )
    settings = get_settings()
    active = (settings.s3_bucket, f"objects/{object_id}/v1")
    archive_copy = (settings.s3_archive_bucket, f"objects/{object_id}/v1")
    assert worker.inspect(*active) is None
    worker.verify(*archive_copy, size, digest)
    assert client.get(f"/v1/objects/{object_id}/download").status_code == 409
    assert (
        client.post("/v1/uploads/initiate", json=metadata, headers=headers).json()["upload"] is None
    )
    status = client.get(f"/v1/jobs/{job['id']}").json()
    assert status["status"] == "SUCCEEDED" and status["finished_at"]
    assert [row["stage"] for row in client.get(f"/v1/jobs/{job['id']}/attempts").json()] == [
        "COMMITTED"
    ]

    restore_job = restore(object_id)
    assert restore_job.status_code == 202, restore_job.text
    assert execute_job(uuid.UUID(restore_job.json()["id"]), worker) == "SUCCEEDED"
    restored = client.get(f"/v1/objects/{object_id}").json()
    assert (restored["lifecycle_state"], restored["archive_copy_retained"]) == ("ACTIVE", True)
    assert restored["restored_at"]
    worker.verify(*active, size, digest)
    worker.verify(*archive_copy, size, digest)
    download = client.get(f"/v1/objects/{object_id}/download")
    assert download.status_code == 200
    assert (
        hashlib.sha256(httpx.get(download.json()["url"], timeout=15).content).hexdigest() == digest
    )

    copies = []
    original_copy = worker.copy
    monkeypatch.setattr(worker, "copy", lambda *args: copies.append(args) or original_copy(*args))
    rearchive = archive(object_id)
    assert rearchive.status_code == 202, rearchive.text
    assert execute_job(uuid.UUID(rearchive.json()["id"]), worker) == "SUCCEEDED"
    assert copies == []
    assert worker.inspect(*active) is None
    worker.verify(*archive_copy, size, digest)


def test_run_once_polls_due_jobs_without_redis_and_ignores_finished_work():
    worker = get_worker_storage()
    _, _, _, object_id = eligible_object()
    job_id = archive(object_id).json()["id"]
    outcomes = {row["job_id"]: row["outcome"] for row in run_once(worker, limit=500)}
    assert outcomes[job_id] == "SUCCEEDED"
    assert job_id not in {row["job_id"] for row in run_once(worker, limit=500)}


def test_worker_and_api_storage_credentials_are_independently_scoped():
    settings = get_settings()
    worker, api = get_worker_storage(), get_storage()
    _, _, _, object_id = eligible_object()
    active_key = f"objects/{object_id}/v1"
    job_id = archive(object_id).json()["id"]
    assert execute_job(uuid.UUID(job_id), worker) == "SUCCEEDED"
    _, _, _, active_object = eligible_object()

    denied(worker.client.delete_object, Bucket=settings.s3_archive_bucket, Key=active_key)
    denied(
        worker.client.put_object,
        Bucket=settings.s3_staging_bucket,
        Key=f"uploads/{uuid.uuid4()}",
        Body=b"x",
    )
    denied(
        api.client.put_object,
        Bucket=settings.s3_archive_bucket,
        Key=f"objects/{uuid.uuid4()}/v1",
        Body=b"x",
    )
    denied(api.client.delete_object, Bucket=settings.s3_bucket, Key=f"objects/{active_object}/v1")
    denied(api.client.get_object, Bucket=settings.s3_archive_bucket, Key=active_key)
    assert worker.inspect(settings.s3_archive_bucket, active_key)["size"] == MIB


def test_worker_refuses_missing_or_shared_credentials(monkeypatch):
    settings = get_settings()
    get_worker_storage.cache_clear()
    try:
        monkeypatch.setattr(settings, "worker_aws_access_key_id", None)
        with pytest.raises(RuntimeError):
            get_worker_storage()
        monkeypatch.setattr(settings, "worker_aws_access_key_id", settings.aws_access_key_id)
        with pytest.raises(RuntimeError):
            get_worker_storage()
    finally:
        monkeypatch.undo()
        get_worker_storage.cache_clear()


def test_recover_command_refuses_without_attestation(capsys):
    job_id = str(uuid.uuid4())
    assert worker_cli.main(["recover", "--job-id", job_id, "--operator", "synthetic"]) == 2
    assert json.loads(capsys.readouterr().out)["job_id"] == job_id
