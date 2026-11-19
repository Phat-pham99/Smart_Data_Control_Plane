from __future__ import annotations

import argparse
import json
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import and_, exists, or_, select
from sqlalchemy.orm import Session

from app.enums import NONTERMINAL_JOB_STATUSES, LifecycleState
from app.models import Job, Policy, StorageObject


class Conditions(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workflow_state: Literal["COMPLETE"]
    inactive_for_days: int = Field(ge=0, le=36500)
    minimum_remaining_retention_days: int = Field(ge=0, le=36500)
    minimum_size_bytes: int = Field(ge=0)


class Action(BaseModel):
    model_config = ConfigDict(extra="forbid")

    desired_storage_class: Literal["ARCHIVE"]


class Rule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    conditions: Conditions
    action: Action


@dataclass(frozen=True)
class PolicyRef:
    id: uuid.UUID
    name: str
    version: int
    rule: Rule

    @classmethod
    def from_model(cls, policy: Policy) -> PolicyRef:
        return cls(policy.id, policy.name, policy.version, Rule.model_validate(policy.config))


@dataclass(frozen=True)
class ObjectFacts:
    workflow_state: str | None
    last_accessed_at: datetime | None
    retention_until: datetime | None
    size_bytes: int | None
    legal_hold: bool | None
    lifecycle_state: str
    observed_storage_class: str
    ingestion_verified: bool
    active_job_id: uuid.UUID | None = None

    @classmethod
    def from_model(cls, obj: StorageObject, active_job_id: uuid.UUID | None) -> ObjectFacts:
        return cls(
            workflow_state=obj.workflow_state,
            last_accessed_at=obj.last_accessed_at,
            retention_until=obj.retention_until,
            size_bytes=obj.size_bytes,
            legal_hold=obj.legal_hold,
            lifecycle_state=LifecycleState(obj.lifecycle_state).value,
            observed_storage_class=obj.observed_storage_class,
            ingestion_verified=obj.ingestion_verified_at is not None,
            active_job_id=active_job_id,
        )


@dataclass(frozen=True)
class Decision:
    eligible: bool
    reasons: tuple[str, ...]
    checks: tuple[dict[str, Any], ...] = field(default=())
    policy: PolicyRef | None = None
    evaluated_at: datetime | None = None
    inputs: dict[str, Any] = field(default_factory=dict)

    def as_record(self) -> dict[str, Any]:
        return {
            "eligible": self.eligible,
            "reasons": list(self.reasons),
            "checks": list(self.checks),
            "policy": {
                "id": str(self.policy.id),
                "name": self.policy.name,
                "version": self.policy.version,
            },
            "evaluated_at": self.evaluated_at.isoformat(),
            "inputs": self.inputs,
        }


def _aware(value: datetime | None) -> bool:
    return value is not None and value.tzinfo is not None and value.utcoffset() is not None


def evaluate(
    facts: ObjectFacts,
    policy: PolicyRef,
    at: datetime,
    owner_job_id: uuid.UUID | None = None,
) -> Decision:
    """Apply one immutable policy version and the mandatory archive safety guards."""

    if not _aware(at):
        raise ValueError("Evaluation time must be timezone-aware")
    at = at.astimezone(UTC)
    rule = policy.rule.conditions
    checks: list[tuple[str, str | None]] = []

    def check(name: str, failure: str | None) -> None:
        checks.append((name, failure))

    check("ingestion_verified", None if facts.ingestion_verified else "INGESTION_NOT_VERIFIED")
    owns_object = owner_job_id is not None and facts.active_job_id == owner_job_id
    owned_states = {LifecycleState.ARCHIVE_REQUESTED.value, LifecycleState.ARCHIVING.value}
    in_place = facts.lifecycle_state == LifecycleState.ACTIVE.value or (
        owns_object and facts.lifecycle_state in owned_states
    )
    check("lifecycle_state", None if in_place else "NOT_ACTIVE")
    check(
        "observed_placement",
        None if facts.observed_storage_class == "ACTIVE" else "PLACEMENT_NOT_VERIFIED_ACTIVE",
    )
    if facts.legal_hold is None:
        check("legal_hold", "LEGAL_HOLD_UNKNOWN")
    else:
        check("legal_hold", "LEGAL_HOLD" if facts.legal_hold else None)
    conflicting = facts.active_job_id is not None and facts.active_job_id != owner_job_id
    check("in_flight_job", "JOB_IN_PROGRESS" if conflicting else None)

    if facts.workflow_state is None:
        check("workflow_state", "WORKFLOW_UNKNOWN")
    else:
        check(
            "workflow_state",
            None if facts.workflow_state == rule.workflow_state else "WORKFLOW_NOT_COMPLETE",
        )

    if facts.last_accessed_at is None:
        check("inactivity", "LAST_ACCESS_UNKNOWN")
    elif not _aware(facts.last_accessed_at):
        check("inactivity", "LAST_ACCESS_NOT_TIMEZONE_AWARE")
    else:
        idle = at - facts.last_accessed_at
        check(
            "inactivity",
            None if idle >= timedelta(days=rule.inactive_for_days) else "RECENTLY_ACCESSED",
        )

    if facts.retention_until is None:
        check("retention", "RETENTION_UNKNOWN")
    elif not _aware(facts.retention_until):
        check("retention", "RETENTION_NOT_TIMEZONE_AWARE")
    else:
        remaining = facts.retention_until - at
        check(
            "retention",
            None
            if remaining >= timedelta(days=rule.minimum_remaining_retention_days)
            else "RETENTION_TOO_SHORT",
        )

    if facts.size_bytes is None:
        check("size", "SIZE_UNKNOWN")
    else:
        check("size", None if facts.size_bytes >= rule.minimum_size_bytes else "TOO_SMALL")

    reasons = tuple(failure for _, failure in checks if failure)
    return Decision(
        eligible=not reasons,
        reasons=reasons or ("ALL_CONDITIONS_MET",),
        checks=tuple(
            {"check": name, "passed": failure is None, "code": failure or "PASSED"}
            for name, failure in checks
        ),
        policy=policy,
        evaluated_at=at,
        inputs={
            "workflow_state": facts.workflow_state,
            "last_accessed_at": _iso(facts.last_accessed_at),
            "retention_until": _iso(facts.retention_until),
            "size_bytes": facts.size_bytes,
            "legal_hold": facts.legal_hold,
            "lifecycle_state": facts.lifecycle_state,
            "observed_storage_class": facts.observed_storage_class,
        },
    )


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def load_policy(db: Session, name: str) -> Policy | None:
    """Return the highest-precedence activated, unsuperseded version of a policy."""

    return db.scalar(
        select(Policy)
        .where(
            Policy.name == name,
            Policy.is_active.is_(True),
            Policy.activated_at.is_not(None),
            Policy.superseded_at.is_(None),
        )
        .order_by(Policy.priority, Policy.version.desc(), Policy.id)
        .limit(1)
    )


def active_job_id(db: Session, object_id: uuid.UUID) -> uuid.UUID | None:
    return db.scalar(
        select(Job.id).where(Job.object_id == object_id, Job.status.in_(NONTERMINAL_JOB_STATUSES))
    )


def select_candidates(
    db: Session,
    policy: PolicyRef,
    at: datetime,
    limit: int,
    after: tuple[datetime, uuid.UUID] | None = None,
) -> list[StorageObject]:
    """Bounded keyset scan whose predicates are exactly the policy's own conditions."""

    if not _aware(at):
        raise ValueError("Evaluation time must be timezone-aware")
    rule = policy.rule.conditions
    statement = select(StorageObject).where(
        StorageObject.lifecycle_state == LifecycleState.ACTIVE,
        StorageObject.legal_hold.is_(False),
        StorageObject.observed_storage_class == "ACTIVE",
        StorageObject.ingestion_verified_at.is_not(None),
        StorageObject.workflow_state == rule.workflow_state,
        StorageObject.last_accessed_at <= at - timedelta(days=rule.inactive_for_days),
        StorageObject.retention_until >= at + timedelta(days=rule.minimum_remaining_retention_days),
        StorageObject.size_bytes >= rule.minimum_size_bytes,
        ~exists().where(
            Job.object_id == StorageObject.id, Job.status.in_(NONTERMINAL_JOB_STATUSES)
        ),
    )
    if after is not None:
        last_access, object_id = after
        statement = statement.where(
            or_(
                StorageObject.last_accessed_at > last_access,
                and_(
                    StorageObject.last_accessed_at == last_access,
                    StorageObject.id > object_id,
                ),
            )
        )
    statement = statement.order_by(StorageObject.last_accessed_at, StorageObject.id).limit(limit)
    return list(db.scalars(statement))


def main(argv: list[str] | None = None) -> int:
    from app.config import get_settings
    from app.db.session import SessionLocal

    parser = argparse.ArgumentParser(description="Bounded archive policy evaluation")
    commands = parser.add_subparsers(dest="command", required=True)
    evaluate_command = commands.add_parser("evaluate", help="Dry run; never creates work")
    evaluate_command.add_argument("--at", type=datetime.fromisoformat)
    evaluate_command.add_argument("--object-id", type=uuid.UUID, action="append")
    enqueue_command = commands.add_parser(
        "enqueue", help="Admit eligible candidates at server time"
    )
    for command in (evaluate_command, enqueue_command):
        command.add_argument("--limit", type=int, default=20, choices=range(1, 501))
    args = parser.parse_args(argv)

    with SessionLocal() as db:
        policy = load_policy(db, get_settings().archive_policy_name)
        if policy is None:
            parser.error("No activated archive policy is configured")
        ref = PolicyRef.from_model(policy)
        if args.command == "enqueue":
            from app.lifecycle import enqueue_candidates

            results = enqueue_candidates(db, ref, args.limit)
        else:
            at = args.at or datetime.now(UTC)
            if not _aware(at):
                parser.error("--at must include a timezone offset")
            if args.object_id:
                pairs = [
                    (object_id, db.get(StorageObject, object_id)) for object_id in args.object_id
                ]
            else:
                pairs = [(obj.id, obj) for obj in select_candidates(db, ref, at, args.limit)]
            results = []
            for object_id, obj in pairs:
                if obj is None:
                    results.append(
                        {"object_id": str(object_id), "eligible": False, "reasons": ["NOT_FOUND"]}
                    )
                    continue
                facts = ObjectFacts.from_model(obj, active_job_id(db, obj.id))
                results.append(
                    {"object_id": str(object_id), **evaluate(facts, ref, at).as_record()}
                )
    print(json.dumps(results, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
