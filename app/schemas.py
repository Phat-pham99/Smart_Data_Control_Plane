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
    created_at: datetime


class UploadSession(BaseModel):
    object: ObjectView
    upload: dict | None
    expires_in: int


class ObjectPage(BaseModel):
    items: list[ObjectView]
    next_cursor: str | None
