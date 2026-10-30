from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator


class UploadRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    logical_key: str = Field(min_length=1, max_length=512)
    content_version: int = Field(default=1, ge=1)
    owner_id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.-]+$")
    dataset_id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.-]+$")
    size_bytes: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[a-fA-F0-9]{64}$")
    content_type: str = Field(
        default="application/octet-stream", min_length=1, max_length=128, pattern=r"^[\x20-\x7e]+$"
    )
    workflow_state: Literal["IN_PROGRESS", "COMPLETE"] = "IN_PROGRESS"
    legal_hold: bool = False
    retention_until: AwareDatetime
    last_application_access_at: AwareDatetime | None = None

    @field_validator("logical_key")
    @classmethod
    def safe_key(cls, value: str) -> str:
        if (
            any(part in ("", ".", "..") for part in value.split("/"))
            or "\\" in value
            or "%" in value
            or any(ord(character) < 32 or ord(character) == 127 for character in value)
        ):
            raise ValueError("Use a relative logical key without traversal or encoded separators")
        return value

    @field_validator("sha256")
    @classmethod
    def normalized_checksum(cls, value: str) -> str:
        return value.lower()


class ObjectView(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    logical_key: str
    content_version: int
    owner_id: str
    dataset_id: str | None
    size_bytes: int
    checksum_sha256: str | None
    content_type: str
    workflow_state: str | None
    legal_hold: bool
    retention_until: datetime | None
    last_accessed_at: datetime | None
    lifecycle_state: str
    desired_storage_class: str
    observed_storage_class: str
    archive_copy_retained: bool
    ingestion_verified_at: datetime | None
    restored_at: datetime | None
    created_at: datetime


class UploadSession(BaseModel):
    object: ObjectView
    upload: dict | None
    expires_in: int


class ObjectPage(BaseModel):
    items: list[ObjectView]
    next_cursor: str | None


class RetryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: str = Field(min_length=1, max_length=500, pattern=r"^[\x20-\x7e]+$")


class DecisionSummary(BaseModel):
    eligible: bool
    reasons: list[str]
    policy_name: str
    policy_version: int
    evaluated_at: datetime


class AttemptView(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    attempt_number: int
    stage: str
    started_at: datetime
    finished_at: datetime | None
    error_class: str | None
    detail: str | None


class JobView(BaseModel):
    """Job status without storage locations, credentials, or signed URLs."""

    id: UUID
    object_id: UUID | None
    job_type: str
    status: str
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    attempt_count: int
    max_attempts: int
    next_attempt_at: datetime | None
    error_class: str | None
    error_message: str | None
    actor_kind: str | None
    decision: DecisionSummary | None
    retry_of_job_id: UUID | None
    replaced_by_job_id: UUID | None

    @classmethod
    def from_job(cls, job, replaced_by: UUID | None) -> JobView:
        decision = None
        if job.decision:
            decision = DecisionSummary(
                eligible=job.decision["eligible"],
                reasons=job.decision["reasons"],
                policy_name=job.decision["policy"]["name"],
                policy_version=job.decision["policy"]["version"],
                evaluated_at=job.decision["evaluated_at"],
            )
        return cls(
            id=job.id,
            object_id=job.object_id,
            job_type=job.job_type.value,
            status=job.status.value,
            created_at=job.created_at,
            updated_at=job.updated_at,
            started_at=job.started_at,
            finished_at=job.finished_at,
            attempt_count=job.attempt_count,
            max_attempts=job.max_attempts,
            next_attempt_at=job.next_attempt_at,
            error_class=job.error_class,
            error_message=job.error_message,
            actor_kind=job.actor_kind,
            decision=decision,
            retry_of_job_id=job.retry_of_job_id,
            replaced_by_job_id=replaced_by,
        )
