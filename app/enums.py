from __future__ import annotations

from enum import StrEnum


class StorageClass(StrEnum):
    """S3 storage tiers managed by the control plane."""

    STANDARD = "STANDARD"
    STANDARD_IA = "STANDARD_IA"
    GLACIER_IR = "GLACIER_IR"
    GLACIER = "GLACIER"
    DEEP_ARCHIVE = "DEEP_ARCHIVE"


class LifecycleState(StrEnum):
    """States of an object within the lifecycle state machine."""

    INGESTING = "INGESTING"
    CREATED = "CREATED"
    ACTIVE = "ACTIVE"
    ARCHIVE_REQUESTED = "ARCHIVE_REQUESTED"
    ARCHIVING = "ARCHIVING"
    ARCHIVED = "ARCHIVED"
    RESTORE_REQUESTED = "RESTORE_REQUESTED"
    RESTORING = "RESTORING"
    RESTORED = "RESTORED"
    FAILED = "FAILED"


class JobType(StrEnum):
    ARCHIVE = "ARCHIVE"
    RESTORE = "RESTORE"


class JobStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    RETRY_SCHEDULED = "RETRY_SCHEDULED"
    RECOVERY_REQUIRED = "RECOVERY_REQUIRED"


# RECOVERY_REQUIRED blocks new work but is never picked up by the normal poller.
NONTERMINAL_JOB_STATUSES: frozenset[JobStatus] = frozenset(
    {
        JobStatus.PENDING,
        JobStatus.RUNNING,
        JobStatus.RETRY_SCHEDULED,
        JobStatus.RECOVERY_REQUIRED,
    }
)


# States from which a restore is required to regain access. GLACIER_IR is
# instantly retrievable, so it never enters the RESTORE_* states.
RESTORE_REQUIRED_STATES: frozenset[LifecycleState] = frozenset({LifecycleState.ARCHIVED})

ALLOWED_TRANSITIONS: dict[LifecycleState, frozenset[LifecycleState]] = {
    LifecycleState.INGESTING: frozenset({LifecycleState.ACTIVE, LifecycleState.FAILED}),
    LifecycleState.CREATED: frozenset({LifecycleState.ACTIVE, LifecycleState.FAILED}),
    LifecycleState.ACTIVE: frozenset({LifecycleState.ARCHIVE_REQUESTED, LifecycleState.FAILED}),
    LifecycleState.ARCHIVE_REQUESTED: frozenset({LifecycleState.ARCHIVING, LifecycleState.FAILED}),
    LifecycleState.ARCHIVING: frozenset({LifecycleState.ARCHIVED, LifecycleState.FAILED}),
    LifecycleState.ARCHIVED: frozenset({LifecycleState.RESTORE_REQUESTED, LifecycleState.FAILED}),
    LifecycleState.RESTORE_REQUESTED: frozenset({LifecycleState.RESTORING, LifecycleState.FAILED}),
    LifecycleState.RESTORING: frozenset({LifecycleState.ACTIVE, LifecycleState.FAILED}),
    LifecycleState.RESTORED: frozenset({LifecycleState.ACTIVE, LifecycleState.FAILED}),
    LifecycleState.FAILED: frozenset(
        {
            LifecycleState.ARCHIVE_REQUESTED,
            LifecycleState.RESTORE_REQUESTED,
        }
    ),
}


def can_transition(current: LifecycleState, target: LifecycleState) -> bool:
    """Return True if ``current`` may legally transition to ``target``."""

    return target in ALLOWED_TRANSITIONS.get(current, frozenset())
