from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import Boolean, DateTime, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.models.mixins import TimestampMixin, UUIDPrimaryKeyMixin


class Policy(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Versioned lifecycle rule; a database trigger freezes activated versions."""

    __tablename__ = "policies"
    __table_args__ = (UniqueConstraint("name", "version", name="uq_policies_name_version"),)

    name: Mapped[str] = mapped_column(String(128), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    config: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    activated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    superseded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return f"Policy(name={self.name!r}, version={self.version!r})"
