import os
import subprocess
import sys

from app import wait_for_deps
from app.config import Settings


def test_defaults_are_usable() -> None:
    settings = Settings(_env_file=None)
    assert settings.database_url.startswith("postgresql+psycopg://")
    assert settings.redis_url.startswith("redis://")
    assert settings.broker_url == settings.redis_url
    assert settings.result_backend == settings.redis_url


def test_celery_overrides_take_precedence() -> None:
    settings = Settings(
        _env_file=None,
        celery_broker_url="redis://broker:6379/1",
        celery_result_backend="redis://backend:6379/2",
    )
    assert settings.broker_url == "redis://broker:6379/1"
    assert settings.result_backend == "redis://backend:6379/2"


def test_migrations_accept_url_encoded_password() -> None:
    environment = dict(
        os.environ,
        DATABASE_URL="postgresql+psycopg://review:synthetic%40password@localhost/review",
    )
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head", "--sql"],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert "CREATE TABLE objects" in result.stdout


def test_entrypoint_waits_before_migrating() -> None:
    environment = dict(
        os.environ, RUN_MIGRATIONS="true", WAIT_FOR_DEPS="true", BOOTSTRAP_STORAGE="false"
    )
    environment["BASH_FUNC_python%%"] = "() { printf 'WAIT\\n'; }"
    environment["BASH_FUNC_alembic%%"] = "() { printf 'MIGRATE\\n'; }"
    result = subprocess.run(
        ["bash", "docker/entrypoint.sh", "true"],
        env=environment,
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )
    assert result.stdout.index("WAIT\n") < result.stdout.index("MIGRATE\n")


def test_startup_requires_database_not_optional_redis(monkeypatch):
    monkeypatch.setattr(wait_for_deps, "_check_database", lambda: True)
    assert wait_for_deps.main() == 0


def test_startup_fails_after_bounded_database_attempts(monkeypatch):
    monkeypatch.setattr(wait_for_deps, "_check_database", lambda: False)
    monkeypatch.setattr(wait_for_deps, "MAX_ATTEMPTS", 1)
    assert wait_for_deps.main() == 1
