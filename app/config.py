from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration loaded from the environment and ``.env``."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    app_name: str = "Explainable Data Lifecycle Control Plane"
    environment: str = "development"
    debug: bool = False

    database_url: str = "postgresql+psycopg://lifecycle:lifecycle@localhost:5432/lifecycle"
    redis_url: str = "redis://localhost:6379/0"

    celery_broker_url: str | None = None
    celery_result_backend: str | None = None

    s3_region: str = "us-east-1"
    s3_bucket: str = "lifecycle-dev"
    s3_endpoint_url: str | None = None
    s3_public_endpoint_url: str | None = None
    s3_staging_bucket: str = "lifecycle-staging"
    s3_archive_bucket: str = "lifecycle-archive"
    max_upload_bytes: int = Field(default=10 * 1024 * 1024, ge=1, le=1024 * 1024 * 1024)
    signed_url_seconds: int = Field(default=300, ge=30, le=3600)
    dependency_timeout_seconds: int = Field(default=3, ge=1, le=30)
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None

    admin_api_key: str | None = None

    @property
    def broker_url(self) -> str:
        return self.celery_broker_url or self.redis_url

    @property
    def result_backend(self) -> str:
        return self.celery_result_backend or self.redis_url


@lru_cache
def get_settings() -> Settings:
    return Settings()
