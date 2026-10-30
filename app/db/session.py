from __future__ import annotations

from collections.abc import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.config import get_settings

_settings = get_settings()

engine = create_engine(
    _settings.database_url,
    pool_pre_ping=True,
    pool_timeout=_settings.dependency_timeout_seconds,
    connect_args={
        "connect_timeout": _settings.dependency_timeout_seconds,
        "options": f"-c statement_timeout={_settings.dependency_timeout_seconds * 1000}",
    },
)
SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    expire_on_commit=False,
)


def get_db() -> Generator[Session, None, None]:
    """FastAPI dependency yielding a scoped database session."""

    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
