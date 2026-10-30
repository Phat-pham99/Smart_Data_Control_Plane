"""Seed or verify a synthetic object across a controlled service restart."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("seed", "verify"))
    parser.add_argument("--api", default=os.getenv("API_BASE_URL", "http://localhost:8000"))
    parser.add_argument(
        "--state", type=Path, default=Path(tempfile.gettempdir()) / "lifecycle-restart.json"
    )
    args = parser.parse_args()
    payload = b"Synthetic restart-persistence evidence. No private data."
    checksum = hashlib.sha256(payload).hexdigest()
    with httpx.Client(base_url=args.api, timeout=20) as client:
        if args.mode == "seed":
            response = client.post(
                "/v1/uploads/initiate",
                headers={"Idempotency-Key": uuid.uuid4().hex},
                json={
                    "logical_key": "synthetic/restart.bin",
                    "owner_id": f"restart-{uuid.uuid4().hex}",
                    "dataset_id": "synthetic-restart",
                    "size_bytes": len(payload),
                    "sha256": checksum,
                    "workflow_state": "COMPLETE",
                    "retention_until": (datetime.now(UTC) + timedelta(days=365)).isoformat(),
                },
            )
            response.raise_for_status()
            session = response.json()
            form = session["upload"]
            upload = httpx.post(
                form["url"],
                data=form["fields"],
                files={"file": ("restart.bin", payload)},
                timeout=20,
            )
            upload.raise_for_status()
            object_id = session["object"]["id"]
            client.post(f"/v1/uploads/{object_id}/complete").raise_for_status()
            args.state.write_text(
                json.dumps({"object_id": object_id, "checksum": checksum}), encoding="utf-8"
            )
        else:
            state = json.loads(args.state.read_text(encoding="utf-8"))
            object_id = state["object_id"]
            response = client.get(f"/v1/objects/{object_id}")
            response.raise_for_status()
            catalog = response.json()
            if (
                catalog["lifecycle_state"] != "ACTIVE"
                or catalog["checksum_sha256"] != state["checksum"]
            ):
                raise RuntimeError("Catalog state did not survive restart")
            download = client.get(f"/v1/objects/{object_id}/download")
            download.raise_for_status()
            result = httpx.get(download.json()["url"], timeout=20)
            result.raise_for_status()
            if (
                result.content != payload
                or hashlib.sha256(result.content).hexdigest() != state["checksum"]
            ):
                raise RuntimeError("Object bytes did not survive restart")
        print(f"{args.mode}: verified synthetic object {object_id}")


if __name__ == "__main__":
    main()
