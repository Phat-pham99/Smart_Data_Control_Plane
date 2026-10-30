from unittest.mock import MagicMock

import pytest
from app.main import app
from fastapi.testclient import TestClient

client = TestClient(app)


def test_health_is_ok() -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_root_reports_service_metadata() -> None:
    response = client.get("/")
    assert response.status_code == 200
    body = response.json()
    assert body["service"]
    assert body["version"]
    assert body["environment"]


@pytest.mark.parametrize(
    "database,storage,redis_ok,status",
    [
        (True, True, True, 200),
        (True, True, False, 200),
        (False, True, True, 503),
        (True, False, True, 503),
    ],
)
def test_readiness_checks_required_dependencies(monkeypatch, database, storage, redis_ok, status):
    monkeypatch.setattr(
        "app.main._check_database", lambda: (database, "ok" if database else "unavailable")
    )
    monkeypatch.setattr(
        "app.main._check_storage", lambda: (storage, "ok" if storage else "unavailable")
    )
    monkeypatch.setattr(
        "app.main._check_redis", lambda: (redis_ok, "ok" if redis_ok else "unavailable")
    )
    response = client.get("/health/ready")
    assert response.status_code == status
    assert response.json()["checks"]["storage"] == ("ok" if storage else "unavailable")


def test_redis_probe_has_read_timeout_and_sanitizes_errors(monkeypatch):
    from app.main import _check_redis

    factory = MagicMock(side_effect=RuntimeError("sensitive-details"))
    monkeypatch.setattr("app.main.redis.Redis.from_url", factory)
    assert _check_redis() == (False, "unavailable")
    assert factory.call_args.kwargs["socket_timeout"] > 0
