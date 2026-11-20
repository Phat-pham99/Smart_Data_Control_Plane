"""Run a synthetic archive/restore round trip against a running API and lifecycle worker."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
import uuid
from datetime import UTC, datetime, timedelta

import httpx

MIB = 1024 * 1024
SETTLED = {"SUCCEEDED", "FAILED", "RECOVERY_REQUIRED"}


def days(offset: int) -> str:
    return (datetime.now(UTC) + timedelta(days=offset)).isoformat()


def ingest(client: httpx.Client, owner: str, name: str, payload: bytes, **metadata) -> str:
    response = client.post(
        "/v1/uploads/initiate",
        headers={"Idempotency-Key": uuid.uuid4().hex},
        json={
            "logical_key": f"synthetic/{name}.bin",
            "owner_id": owner,
            "dataset_id": "synthetic-lifecycle",
            "size_bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
            "workflow_state": "COMPLETE",
            "retention_until": days(400),
            "last_application_access_at": days(-200),
            **metadata,
        },
    )
    response.raise_for_status()
    session = response.json()
    form = session["upload"]
    httpx.post(
        form["url"], data=form["fields"], files={"file": (f"{name}.bin", payload)}, timeout=30
    ).raise_for_status()
    object_id = session["object"]["id"]
    client.post(f"/v1/uploads/{object_id}/complete").raise_for_status()
    return object_id


def request(client: httpx.Client, object_id: str, operation: str) -> httpx.Response:
    return client.post(
        f"/v1/objects/{object_id}/{operation}", headers={"Idempotency-Key": uuid.uuid4().hex}
    )


def wait(client: httpx.Client, job_id: str, timeout: float) -> tuple[dict, float]:
    started = time.monotonic()
    while True:
        job = client.get(f"/v1/jobs/{job_id}").raise_for_status().json()
        elapsed = time.monotonic() - started
        if job["status"] in SETTLED:
            return job, round(elapsed, 2)
        if elapsed > timeout:
            raise TimeoutError(f"Job {job_id} is still {job['status']} after {timeout}s")
        time.sleep(0.5)


def download_digest(client: httpx.Client, object_id: str) -> str:
    link = client.get(f"/v1/objects/{object_id}/download").raise_for_status().json()
    return hashlib.sha256(httpx.get(link["url"], timeout=30).raise_for_status().content).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api", default=os.getenv("API_BASE_URL", "http://localhost:8000"))
    parser.add_argument("--timeout", type=float, default=60)
    args = parser.parse_args()
    line = b"Synthetic cold engineering artifact. No private data.\n"
    payload = (line * (MIB // len(line) + 1))[:MIB]
    digest = hashlib.sha256(payload).hexdigest()
    owner = f"demo-{uuid.uuid4().hex[:12]}"
    summary: dict = {"owner_id": owner, "sha256": digest, "size_bytes": len(payload)}

    with httpx.Client(base_url=args.api, timeout=30) as client:
        rejected_id = ingest(
            client, owner, "recent", b"small recent artifact", last_application_access_at=days(-5)
        )
        rejection = request(client, rejected_id, "archive")
        if rejection.status_code != 409:
            raise RuntimeError(f"Ineligible object was not rejected: {rejection.status_code}")
        summary["rejected"] = {"object_id": rejected_id, **rejection.json()["detail"]}

        object_id = ingest(client, owner, "cold", payload)
        summary["object_id"] = object_id
        admitted = request(client, object_id, "archive").raise_for_status().json()
        archived, seconds = wait(client, admitted["id"], args.timeout)
        state = client.get(f"/v1/objects/{object_id}").json()
        summary["archive"] = {
            "job_id": admitted["id"],
            "status": archived["status"],
            "decision": admitted["decision"],
            "seconds": seconds,
            "lifecycle_state": state["lifecycle_state"],
            "download_while_archived": client.get(f"/v1/objects/{object_id}/download").status_code,
        }
        if archived["status"] != "SUCCEEDED" or state["lifecycle_state"] != "ARCHIVED":
            raise RuntimeError(f"Archive did not succeed: {json.dumps(summary)}")

        admitted = request(client, object_id, "restore").raise_for_status().json()
        restored, seconds = wait(client, admitted["id"], args.timeout)
        state = client.get(f"/v1/objects/{object_id}").json()
        summary["restore"] = {
            "job_id": admitted["id"],
            "status": restored["status"],
            "seconds": seconds,
            "lifecycle_state": state["lifecycle_state"],
            "archive_copy_retained": state["archive_copy_retained"],
            "download_sha256_matches": download_digest(client, object_id) == digest,
        }
        if restored["status"] != "SUCCEEDED" or not summary["restore"]["download_sha256_matches"]:
            raise RuntimeError(f"Restore did not verify: {json.dumps(summary)}")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
