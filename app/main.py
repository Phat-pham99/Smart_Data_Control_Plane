from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

import redis
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy import text

from app import __version__
from app.config import get_settings
from app.db.session import engine
from app.lifecycle import router as lifecycle_router
from app.objects import router as objects_router
from app.ownership import ObjectBusy, OwnershipLost
from app.storage import get_storage

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


app = FastAPI(
    title=settings.app_name,
    version=__version__,
    description=(
        "Control plane for S3 object lifecycle management: metadata, policy "
        "decisions, and asynchronous job orchestration."
    ),
    lifespan=lifespan,
)

app.include_router(objects_router)
app.include_router(lifecycle_router)


@app.exception_handler(ObjectBusy)
def object_busy(request: Request, error: ObjectBusy) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={"detail": "Object is busy; retry later"},
        headers={"Retry-After": "1"},
    )


@app.exception_handler(OwnershipLost)
def ownership_lost(request: Request, error: OwnershipLost) -> JSONResponse:
    return JSONResponse(
        status_code=503,
        content={"detail": "Object ownership could not be confirmed; retry later"},
    )


def _check_database() -> tuple[bool, str]:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True, "ok"
    except Exception:  # noqa: BLE001
        return False, "unavailable"


def _check_redis() -> tuple[bool, str]:
    try:
        with redis.Redis.from_url(
            settings.redis_url,
            socket_connect_timeout=settings.dependency_timeout_seconds,
            socket_timeout=settings.dependency_timeout_seconds,
        ) as client:
            client.ping()
        return True, "ok"
    except Exception:  # noqa: BLE001
        return False, "unavailable"


def _check_storage() -> tuple[bool, str]:
    try:
        storage = get_storage()
        for bucket in (settings.s3_staging_bucket, settings.s3_bucket):
            storage.client.head_bucket(Bucket=bucket)
        return True, "ok"
    except Exception:  # noqa: BLE001
        return False, "unavailable"


@app.get("/", tags=["meta"])
def root() -> dict[str, str]:
    return {
        "service": settings.app_name,
        "version": __version__,
        "environment": settings.environment,
    }


@app.get("/health", tags=["health"])
def health() -> dict[str, str]:
    """Liveness probe: the process is up."""

    return {"status": "ok"}


@app.get("/health/ready", tags=["health"])
def readiness() -> JSONResponse:
    """Ingestion requires PostgreSQL and storage; Redis is diagnostic only."""

    db_ok, db_detail = _check_database()
    redis_ok, redis_detail = _check_redis()
    storage_ok, storage_detail = _check_storage()
    checks: dict[str, Any] = {
        "database": db_detail,
        "redis": redis_detail,
        "storage": storage_detail,
    }
    healthy = db_ok and storage_ok
    return JSONResponse(
        status_code=200 if healthy else 503,
        content={"status": "ok" if healthy and redis_ok else "degraded", "checks": checks},
    )
