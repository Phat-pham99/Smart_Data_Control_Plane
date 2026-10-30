from __future__ import annotations

import base64
import hashlib
import json
import logging
import uuid
from datetime import UTC, datetime
from typing import Annotated

from botocore.exceptions import BotoCoreError, ClientError
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import AwareDatetime
from sqlalchemy import select, text, tuple_
from sqlalchemy.exc import IntegrityError as DatabaseConflict
from sqlalchemy.orm import Session

from app.config import get_settings
from app.db.session import get_db
from app.enums import LifecycleState
from app.models import AuditEvent, StorageObject
from app.schemas import ObjectPage, ObjectView, UploadRequest, UploadSession
from app.storage import IntegrityError, ObjectStorage, get_storage

router = APIRouter(prefix="/v1", tags=["objects"])
logger = logging.getLogger(__name__)
Database = Annotated[Session, Depends(get_db)]
Storage = Annotated[ObjectStorage, Depends(get_storage)]


def audit(db, obj, previous, reason):
    db.add(
        AuditEvent(
            object_id=obj.id, previous_state=previous, new_state=obj.lifecycle_state, reason=reason
        )
    )


def log_event(obj, event):
    logger.info(
        json.dumps(
            {
                "timestamp": datetime.now(UTC).isoformat(),
                "service": "api",
                "object_id": str(obj.id),
                "event": event,
                "state": obj.lifecycle_state.value,
            }
        )
    )


def find_object(db, object_id):
    obj = db.get(StorageObject, object_id)
    if obj is None:
        raise HTTPException(404, "Object not found")
    return obj


def storage_error(error):
    if isinstance(error, IntegrityError):
        return HTTPException(409, str(error))
    if isinstance(error, ClientError) and error.response["Error"]["Code"] in (
        "404",
        "NoSuchKey",
        "NoSuchBucket",
        "PreconditionFailed",
    ):
        return HTTPException(409, "Upload or object is missing or changed; retry after uploading")
    return HTTPException(503, "Object storage is unavailable; retry later")


@router.post("/uploads/initiate", response_model=UploadSession, status_code=201)
def initiate_upload(
    payload: UploadRequest,
    db: Database,
    storage: Storage,
    idempotency_key: Annotated[str, Header(min_length=1, max_length=255)],
):
    settings = get_settings()
    if not idempotency_key.strip():
        raise HTTPException(422, "Idempotency-Key cannot be blank")
    if payload.size_bytes > settings.max_upload_bytes:
        raise HTTPException(413, "Upload exceeds the configured size limit")
    fingerprint = hashlib.sha256(
        json.dumps(payload.model_dump(mode="json"), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    statement = select(StorageObject).where(
        StorageObject.owner_id == payload.owner_id, StorageObject.ingestion_key == idempotency_key
    )
    obj = db.scalar(statement)
    if obj is None:
        object_id = uuid.uuid4()
        obj = StorageObject(
            id=object_id,
            bucket=settings.s3_bucket,
            key=f"objects/{object_id}/v{payload.content_version}",
            filename=payload.logical_key.rsplit("/", 1)[-1],
            logical_key=payload.logical_key,
            content_version=payload.content_version,
            owner_id=payload.owner_id,
            dataset_id=payload.dataset_id,
            workflow_state=payload.workflow_state,
            legal_hold=payload.legal_hold,
            retention_until=payload.retention_until,
            last_accessed_at=payload.last_application_access_at,
            content_type=payload.content_type,
            size_bytes=payload.size_bytes,
            checksum_sha256=payload.sha256,
            lifecycle_state=LifecycleState.INGESTING,
            desired_storage_class="ACTIVE",
            observed_storage_class="UNKNOWN",
            ingestion_key=idempotency_key,
            request_fingerprint=fingerprint,
            staging_bucket=settings.s3_staging_bucket,
            staging_key=f"uploads/{object_id}",
        )
        db.add(obj)
        try:
            db.flush()
            audit(db, obj, None, "UPLOAD_INITIATED")
            db.commit()
        except DatabaseConflict as error:
            db.rollback()
            if getattr(error.orig, "sqlstate", None) != "23505":
                raise
            obj = db.scalar(statement)
            if obj is None:
                raise HTTPException(409, "This logical object version already exists") from None
    if obj.request_fingerprint != fingerprint:
        raise HTTPException(409, "Idempotency-Key was used with a different request")
    try:
        upload = (
            None
            if obj.lifecycle_state == LifecycleState.ACTIVE
            else storage.upload_form(
                obj.staging_bucket, obj.staging_key, obj.size_bytes, obj.content_type
            )
        )
    except (BotoCoreError, ClientError) as error:
        raise storage_error(error) from error
    log_event(obj, "upload_session")
    return UploadSession(
        object=ObjectView.model_validate(obj), upload=upload, expires_in=settings.signed_url_seconds
    )


@router.post("/uploads/{object_id}/complete", response_model=ObjectView)
def complete_upload(object_id: uuid.UUID, db: Database, storage: Storage):
    lock_key = int.from_bytes(object_id.bytes[:8], "big", signed=True)
    with db.get_bind().connect() as lock_connection:
        acquired = lock_connection.scalar(
            text("SELECT pg_try_advisory_lock(:key)"), {"key": lock_key}
        )
        lock_connection.commit()
        if not acquired:
            raise HTTPException(409, "Object is being finalized; retry later")
        try:
            obj = find_object(db, object_id)
            if obj.lifecycle_state == LifecycleState.ACTIVE:
                return ObjectView.model_validate(obj)
            if (
                obj.lifecycle_state not in (LifecycleState.INGESTING, LifecycleState.FAILED)
                or not obj.staging_key
            ):
                raise HTTPException(409, "Object cannot be finalized from its current state")
            previous = obj.lifecycle_state
            db.commit()
            try:
                storage.promote(obj)
            except IntegrityError as error:
                obj.lifecycle_state = LifecycleState.FAILED
                audit(db, obj, previous, str(error))
                db.commit()
                log_event(obj, "verification_failed")
                raise storage_error(error) from error
            except (BotoCoreError, ClientError) as error:
                raise storage_error(error) from error
            lock_connection.execute(text("SELECT 1"))
            lock_connection.commit()
            obj.lifecycle_state = LifecycleState.ACTIVE
            obj.observed_storage_class = "ACTIVE"
            audit(db, obj, previous, "INGESTION_VERIFIED")
            db.commit()
            log_event(obj, "ingestion_verified")
            return ObjectView.model_validate(obj)
        finally:
            if not lock_connection.invalidated:
                lock_connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": lock_key})
                lock_connection.commit()


@router.get("/objects/{object_id}", response_model=ObjectView)
def get_object(object_id: uuid.UUID, db: Database):
    return find_object(db, object_id)


@router.get("/objects", response_model=ObjectPage)
def list_objects(
    db: Database,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    cursor: Annotated[str | None, Query(max_length=1024)] = None,
    owner_id: str | None = None,
    dataset_id: str | None = None,
    workflow_state: str | None = None,
    lifecycle_state: LifecycleState | None = None,
    desired_storage_class: str | None = None,
    observed_storage_class: str | None = None,
    created_before: AwareDatetime | None = None,
    created_after: AwareDatetime | None = None,
    last_access_before: AwareDatetime | None = None,
):
    statement = select(StorageObject)
    for field, value in (
        ("owner_id", owner_id),
        ("dataset_id", dataset_id),
        ("workflow_state", workflow_state),
        ("lifecycle_state", lifecycle_state),
        ("desired_storage_class", desired_storage_class),
        ("observed_storage_class", observed_storage_class),
    ):
        if value is not None:
            statement = statement.where(getattr(StorageObject, field) == value)
    if created_before is not None:
        statement = statement.where(StorageObject.created_at < created_before)
    if created_after is not None:
        statement = statement.where(StorageObject.created_at > created_after)
    if last_access_before is not None:
        statement = statement.where(StorageObject.last_accessed_at < last_access_before)
    if cursor:
        try:
            timestamp, object_id = json.loads(
                base64.b64decode(cursor, altchars=b"-_", validate=True)
            )
            created_at = datetime.fromisoformat(timestamp)
            if created_at.tzinfo is None:
                raise ValueError("Timezone required")
            statement = statement.where(
                tuple_(StorageObject.created_at, StorageObject.id)
                > tuple_(created_at, uuid.UUID(object_id))
            )
        except (ValueError, TypeError, UnicodeDecodeError) as error:
            raise HTTPException(422, "Invalid cursor") from error
    rows = list(
        db.scalars(statement.order_by(StorageObject.created_at, StorageObject.id).limit(limit + 1))
    )
    more = len(rows) > limit
    rows = rows[:limit]
    next_cursor = None
    if more:
        next_cursor = base64.urlsafe_b64encode(
            json.dumps([rows[-1].created_at.isoformat(), str(rows[-1].id)]).encode()
        ).decode()
    return ObjectPage(
        items=[ObjectView.model_validate(row) for row in rows], next_cursor=next_cursor
    )


@router.get("/objects/{object_id}/download")
def download_object(object_id: uuid.UUID, db: Database, storage: Storage):
    obj = find_object(db, object_id)
    if obj.lifecycle_state != LifecycleState.ACTIVE or obj.observed_storage_class != "ACTIVE":
        raise HTTPException(409, "Only verified active objects can be downloaded")
    try:
        url = storage.download_url(obj)
    except (BotoCoreError, ClientError, IntegrityError) as error:
        raise storage_error(error) from error
    return {"url": url, "expires_in": get_settings().signed_url_seconds}


@router.post("/objects/{object_id}/access", response_model=ObjectView)
def record_access(object_id: uuid.UUID, db: Database):
    obj = find_object(db, object_id)
    obj.last_accessed_at = datetime.now(UTC)
    obj.access_count += 1
    audit(db, obj, obj.lifecycle_state, "APPLICATION_ACCESS_RECORDED")
    db.commit()
    return obj
