from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, text
from sqlalchemy.exc import OperationalError

# lock_not_available / query_canceled raised by lock_timeout or statement_timeout.
BUSY_SQLSTATES = {"55P03", "57014"}
HELD_SQL = text(
    "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND granted "
    "AND pid = pg_backend_pid() AND objsubid = 1 "
    "AND ((classid::bigint << 32) | objid::bigint) = :key)"
)


class ObjectBusy(Exception):
    """Another session owns the object; the caller may retry later."""


class OwnershipLost(Exception):
    """The session lock can no longer be proven; stop further storage mutation."""


def lock_key(object_id: uuid.UUID) -> int:
    return int.from_bytes(object_id.bytes[:8], "big", signed=True)


class ObjectLock:
    def __init__(self, connection, key: int):
        self.connection = connection
        self.key = key

    def verify(self) -> None:
        """Prove the lock is still held on the same physical connection."""

        try:
            held = self.connection.scalar(HELD_SQL, {"key": self.key})
            self.connection.commit()
        except OperationalError as error:
            raise OwnershipLost("Lock connection failed") from error
        if not held:
            raise OwnershipLost("Advisory lock is no longer held")


@contextmanager
def object_lock(
    engine: Engine, object_id: uuid.UUID, wait_seconds: float = 0
) -> Iterator[ObjectLock]:
    """Hold the per-object session advisory lock on a dedicated connection."""

    key = lock_key(object_id)
    with engine.connect() as connection:
        try:
            if wait_seconds > 0:
                connection.execute(
                    text("SELECT set_config('lock_timeout', :timeout, true)"),
                    {"timeout": f"{int(wait_seconds * 1000)}ms"},
                )
                connection.execute(text("SELECT pg_advisory_lock(:key)"), {"key": key})
                acquired = True
            else:
                acquired = connection.scalar(
                    text("SELECT pg_try_advisory_lock(:key)"), {"key": key}
                )
            connection.commit()
        except OperationalError as error:
            if getattr(error.orig, "sqlstate", None) not in BUSY_SQLSTATES:
                raise
            connection.rollback()
            acquired = False
        if not acquired:
            raise ObjectBusy
        try:
            yield ObjectLock(connection, key)
        finally:
            if not connection.invalidated:
                try:
                    connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": key})
                    connection.commit()
                except OperationalError:
                    # PostgreSQL releases session locks when the connection ends.
                    connection.invalidate()
