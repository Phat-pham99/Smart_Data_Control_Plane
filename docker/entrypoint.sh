#!/usr/bin/env bash
set -euo pipefail

if [[ "${WAIT_FOR_DEPS:-false}" == "true" ]]; then
    echo "Waiting for dependencies..."
    python -m app.wait_for_deps
fi

if [[ "${RUN_MIGRATIONS:-false}" == "true" ]]; then
    echo "Running database migrations..."
    alembic upgrade head
fi

if [[ "${BOOTSTRAP_STORAGE:-false}" == "true" ]]; then
    python -m app.storage
fi

exec "$@"
