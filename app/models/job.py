from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base
from app.enums import JobStatus, JobType
from app.models.mixins import TimestampMixin, UUIDPrimaryKeyMixin, enum_column

NONTERMINAL_SQL = "status IN ('PENDING', 'RUNNING', 'RETRY_SCHEDULED', 'RECOVERY_REQUIRED')"


class Job(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """An asynchronous archive/restore operation for a single object."""

    __tablename__ = "jobs"
    __table_args__ = (
        UniqueConstraint("object_id", "idempotency_key", name="uq_jobs_object_idempotency"),
        UniqueConstraint("retry_of_job_id", name="uq_jobs_retry_of"),
        CheckConstraint("attempt_count >= 0", name="ck_jobs_attempt_count"),
        CheckConstraint("max_attempts BETWEEN 1 AND 20", name="ck_jobs_max_attempts"),
        Index(
            "ux_jobs_one_nonterminal",
            "object_id",
            unique=True,
            postgresql_where=text(NONTERMINAL_SQL),
        ),
        Index(
            "ix_jobs_due",
            "next_attempt_at",
            "created_at",
            "id",
            postgresql_where=text("status IN ('PENDING', 'RETRY_SCHEDULED')"),
        ),
    )

    job_type: Mapped[JobType] = mapped_column(enum_column(JobType, "job_type"), nullable=False)
    status: Mapped[JobStatus] = mapped_column(
        enum_column(JobStatus, "job_status"),
        nullable=False,
        default=JobStatus.PENDING,
    )
    object_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("objects.id", ondelete="CASCADE"), nullable=True
    )
    idempotency_key: Mapped[str | None] = mapped_column(String(255), nullable=True)
    request_fingerprint: Mapped[str | None] = mapped_column(String(64), nullable=True)
    celery_task_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=5)
    error_class: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    content_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    expected_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    expected_size: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    source_bucket: Mapped[str | None] = mapped_column(String(255), nullable=True)
    source_key: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    destination_bucket: Mapped[str | None] = mapped_column(String(255), nullable=True)
    destination_key: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    policy_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("policies.id", name="fk_jobs_policy_id_policies"), nullable=True
    )
    decision: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    actor_kind: Mapped[str | None] = mapped_column(String(32), nullable=True)
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    retry_of_job_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("jobs.id", name="fk_jobs_retry_of_job_id_jobs"), nullable=True
    )

    object = relationship("StorageObject", lazy="joined")

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return f"Job(id={self.id!r}, type={self.job_type!r}, status={self.status!r})"


class JobAttempt(UUIDPrimaryKeyMixin, Base):
    """One execution attempt; completed attempts are never rewritten."""

    __tablename__ = "job_attempts"
    __table_args__ = (UniqueConstraint("job_id", "attempt_number", name="uq_job_attempts_number"),)

    job_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("jobs.id", ondelete="CASCADE", name="fk_job_attempts_job_id_jobs"),
        nullable=False,
    )
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    stage: Mapped[str] = mapped_column(String(32), nullable=False)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_class: Mapped[str | None] = mapped_column(String(64), nullable=True)
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)


class OutboxEvent(UUIDPrimaryKeyMixin, Base):
    """Best-effort notification intent; publication is not delivery."""

    __tablename__ = "outbox_events"
    __table_args__ = (
        Index(
            "ix_outbox_pending",
            "next_publish_at",
            "created_at",
            postgresql_where=text("published_at IS NULL"),
        ),
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    job_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("jobs.id", ondelete="CASCADE", name="fk_outbox_events_job_id_jobs"),
        nullable=False,
    )
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    publish_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    next_publish_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)


class FolderJob(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Aggregate tracking for a prefix-scoped batch operation."""

    __tablename__ = "folder_jobs"

    prefix: Mapped[str] = mapped_column(String(1024), nullable=False)
    status: Mapped[JobStatus] = mapped_column(
        enum_column(JobStatus, "job_status"),
        nullable=False,
        default=JobStatus.PENDING,
    )
    total_objects: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    completed_objects: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_objects: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return f"FolderJob(id={self.id!r}, prefix={self.prefix!r}, status={self.status!r})"
