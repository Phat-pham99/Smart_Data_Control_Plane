from __future__ import annotations

import sys
import time

from sqlalchemy import text

from app.db.session import engine

MAX_ATTEMPTS = 30
DELAY_SECONDS = 2


def _check_database() -> bool:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as exc:  # noqa: BLE001
        print(f"database not ready: {type(exc).__name__}")
        return False


def main() -> int:
    for attempt in range(1, MAX_ATTEMPTS + 1):
        if _check_database():
            print("database ready")
            return 0
        print(f"attempt {attempt}/{MAX_ATTEMPTS} failed")
        if attempt < MAX_ATTEMPTS:
            time.sleep(DELAY_SECONDS)
    print("database did not become ready in time")
    return 1


if __name__ == "__main__":
    sys.exit(main())
