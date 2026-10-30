import hashlib
from datetime import UTC, datetime, timedelta
from io import BytesIO
from types import SimpleNamespace

import pytest
from app.schemas import UploadRequest
from app.storage import IntegrityError, ObjectStorage
from pydantic import ValidationError


def request_data(**changes):
    return {
        "logical_key": "synthetic/sample.bin",
        "owner_id": "demo-owner",
        "dataset_id": "demo-dataset",
        "size_bytes": 3,
        "sha256": "A" * 64,
        "retention_until": (datetime.now(UTC) + timedelta(days=365)).isoformat(),
        **changes,
    }


@pytest.mark.parametrize(
    "key", ["../secret", "/absolute", "a/../b", "a//b", "a\\b", "a/%2e%2e/b", "bad\nkey"]
)
def test_upload_rejects_unsafe_keys(key):
    with pytest.raises(ValidationError):
        UploadRequest(**request_data(logical_key=key))


def test_upload_normalizes_checksum():
    assert UploadRequest(**request_data()).sha256 == "a" * 64


@pytest.mark.parametrize(
    "changes",
    [
        {"size_bytes": -1},
        {"sha256": "not-a-checksum"},
        {"retention_until": "2027-01-01T00:00:00"},
        {"content_version": 0},
        {"unexpected": True},
    ],
)
def test_upload_rejects_invalid_metadata(changes):
    with pytest.raises(ValidationError):
        UploadRequest(**request_data(**changes))


@pytest.mark.parametrize(
    "payload,size,checksum",
    [
        (b"abc", 4, hashlib.sha256(b"abc").hexdigest()),
        (b"bad", 3, hashlib.sha256(b"abc").hexdigest()),
    ],
)
def test_storage_verification_rejects_wrong_bytes(payload, size, checksum):
    body = BytesIO(payload)
    storage = ObjectStorage.__new__(ObjectStorage)
    storage.settings = SimpleNamespace(max_upload_bytes=1024)
    storage.client = SimpleNamespace(
        get_object=lambda **kwargs: {"Body": body, "ContentLength": len(payload), "ETag": "test"}
    )
    with pytest.raises(IntegrityError):
        storage.verify("bucket", "key", size, checksum)
    assert body.closed
