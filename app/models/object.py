from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.enums import LifecycleState, StorageClass
from app.models.mixins import (
    OptimisticLockMixin,
    TimestampMixin,
    UUIDPrimaryKeyMixin,
    enum_column,
)


class StorageObject(UUIDPrimaryKeyMixin, TimestampMixin, OptimisticLockMixin, Base):
    """Metadata and lifecycle state for a single S3 object."""

    __tablename__ = "objects"
    __table_args__ = (
        UniqueConstraint("bucket", "key", name="uq_objects_bucket_key"),
        UniqueConstraint("owner_id", "logical_key", "content_version", name="uq_objects_identity"),
        UniqueConstraint("owner_id", "ingestion_key", name="uq_objects_ingestion"),
        CheckConstraint("size_bytes >= 0", name="ck_objects_size"),
        CheckConstraint("content_version > 0", name="ck_objects_content_version"),
        Index("ix_objects_owner_created", "owner_id", "created_at", "id"),
    )

    bucket: Mapped[str] = mapped_column(String(255), nullable=False)
    key: Mapped[str] = mapped_column(String(1024), nullable=False)
    filename: Mapped[str] = mapped_column(String(512), nullable=False)
    logical_key: Mapped[str] = mapped_column(String(512), nullable=False)
    content_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    owner_id: Mapped[str] = mapped_column(String(128), nullable=False)
    dataset_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    workflow_state: Mapped[str | None] = mapped_column(String(32), nullable=True)
    legal_hold: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    retention_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    content_type: Mapped[str] = mapped_column(
        String(128), nullable=False, default="application/octet-stream"
    )
    desired_storage_class: Mapped[str] = mapped_column(String(16), nullable=False, default="ACTIVE")
    observed_storage_class: Mapped[str] = mapped_column(
        String(16), nullable=False, default="UNKNOWN"
    )
    ingestion_key: Mapped[str | None] = mapped_column(String(255), nullable=True)
    request_fingerprint: Mapped[str | None] = mapped_column(String(64), nullable=True)
    staging_bucket: Mapped[str | None] = mapped_column(String(255), nullable=True)
    staging_key: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    checksum_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    storage_class: Mapped[StorageClass] = mapped_column(
        enum_column(StorageClass, "storage_class"),
        nullable=False,
        default=StorageClass.STANDARD,
    )
    lifecycle_state: Mapped[LifecycleState] = mapped_column(
        enum_column(LifecycleState, "lifecycle_state"),
        nullable=False,
        default=LifecycleState.CREATED,
    )
    access_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_accessed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)

    __mapper_args__ = {"version_id_col": version}

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return f"StorageObject(id={self.id!r}, key={self.key!r}, state={self.lifecycle_state!r})"
