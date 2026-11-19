from __future__ import annotations

import argparse
import json
import logging
import random
import signal
import threading
import time
import uuid

from app.config import get_settings
from app.db.session import SessionLocal
from app.execution import (
    Attestation,
    Clock,
    Jitter,
    RecoveryRefused,
    due_job_ids,
    execute_job,
    quarantine_interrupted,
    utcnow,
)
from app.storage import get_worker_storage

logger = logging.getLogger(__name__)


def run_once(
    storage,
    clock: Clock = utcnow,
    jitter: Jitter = random.random,
    limit: int | None = None,
    stop: threading.Event | None = None,
) -> list[dict]:
    """Poll one bounded batch of due jobs and execute them one at a time."""

    with SessionLocal() as db:
        job_ids = due_job_ids(db, clock(), limit or get_settings().poll_batch_size)
    results = []
    for job_id in job_ids:
        if stop is not None and stop.is_set():
            break
        results.append(
            {"job_id": str(job_id), "outcome": execute_job(job_id, storage, clock, jitter)}
        )
    return results


def run(storage, stop: threading.Event) -> None:
    """Poll PostgreSQL until stopped; correctness never depends on Redis."""

    quarantined = quarantine_interrupted()
    logger.info(json.dumps({"service": "worker", "event": "started", "quarantined": quarantined}))
    interval = get_settings().poll_interval_seconds
    while not stop.is_set():
        started = time.monotonic()
        try:
            run_once(storage, stop=stop)
        except Exception as error:
            logger.error(
                json.dumps(
                    {"service": "worker", "event": "poll_failed", "error": type(error).__name__}
                )
            )
        stop.wait(max(0.0, interval - (time.monotonic() - started)))
    logger.info(json.dumps({"service": "worker", "event": "stopped"}))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Lifecycle worker (PostgreSQL polling)")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("run", help="Poll and execute until SIGTERM")
    once = commands.add_parser("run-once", help="Execute one bounded batch of due jobs")
    once.add_argument("--limit", type=int, default=None)
    recover = commands.add_parser("recover", help="Controlled recovery of one blocked job")
    recover.add_argument("--job-id", type=uuid.UUID, required=True)
    recover.add_argument("--operator", required=True)
    recover.add_argument("--attest-worker-stopped", action="store_true")
    recover.add_argument("--attest-requests-settled", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    storage = get_worker_storage()

    if args.command == "run":
        stop = threading.Event()
        for signum in (signal.SIGTERM, signal.SIGINT):
            signal.signal(signum, lambda *_: stop.set())
        run(storage, stop)
        return 0
    if args.command == "run-once":
        print(json.dumps(run_once(storage, limit=args.limit), indent=2))
        return 0
    attestation = Attestation(
        args.operator, args.attest_worker_stopped, args.attest_requests_settled
    )
    try:
        outcome = execute_job(args.job_id, storage, attestation=attestation)
    except RecoveryRefused as error:
        print(json.dumps({"job_id": str(args.job_id), "refused": str(error)}))
        return 2
    print(json.dumps({"job_id": str(args.job_id), "outcome": outcome}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
