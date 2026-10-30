import json
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone

import pytest
from app.policies import ObjectFacts, PolicyRef, Rule, evaluate
from pydantic import ValidationError

AT = datetime(2026, 10, 30, 12, 0, tzinfo=UTC)
CONFIG = {
    "conditions": {
        "workflow_state": "COMPLETE",
        "inactive_for_days": 180,
        "minimum_remaining_retention_days": 180,
        "minimum_size_bytes": 1048576,
    },
    "action": {"desired_storage_class": "ARCHIVE"},
}
POLICY = PolicyRef(uuid.uuid4(), "archive-completed-cold-datasets", 1, Rule.model_validate(CONFIG))
BOUNDARY = ObjectFacts(
    workflow_state="COMPLETE",
    last_accessed_at=AT - timedelta(days=180),
    retention_until=AT + timedelta(days=180),
    size_bytes=1048576,
    legal_hold=False,
    lifecycle_state="ACTIVE",
    observed_storage_class="ACTIVE",
    ingestion_verified=True,
)


def test_inclusive_boundaries_are_eligible_with_explanation():
    decision = evaluate(BOUNDARY, POLICY, AT)
    assert decision.eligible
    assert decision.reasons == ("ALL_CONDITIONS_MET",)
    assert all(check["passed"] for check in decision.checks)
    record = json.loads(json.dumps(decision.as_record()))
    assert record["policy"] == {"id": str(POLICY.id), "name": POLICY.name, "version": 1}
    assert record["evaluated_at"] == AT.isoformat()
    assert record["inputs"]["size_bytes"] == 1048576


@pytest.mark.parametrize(
    "changes,reason",
    [
        (
            {"last_accessed_at": AT - timedelta(days=180) + timedelta(seconds=1)},
            "RECENTLY_ACCESSED",
        ),
        (
            {"retention_until": AT + timedelta(days=180) - timedelta(seconds=1)},
            "RETENTION_TOO_SHORT",
        ),
        ({"size_bytes": 1048575}, "TOO_SMALL"),
        ({"workflow_state": "IN_PROGRESS"}, "WORKFLOW_NOT_COMPLETE"),
        ({"legal_hold": True}, "LEGAL_HOLD"),
        ({"lifecycle_state": "ARCHIVED"}, "NOT_ACTIVE"),
        ({"observed_storage_class": "UNKNOWN"}, "PLACEMENT_NOT_VERIFIED_ACTIVE"),
        ({"ingestion_verified": False}, "INGESTION_NOT_VERIFIED"),
        ({"active_job_id": uuid.uuid4()}, "JOB_IN_PROGRESS"),
    ],
)
def test_each_condition_rejects_with_a_stable_reason(changes, reason):
    decision = evaluate(replace(BOUNDARY, **changes), POLICY, AT)
    assert not decision.eligible
    assert decision.reasons == (reason,)


@pytest.mark.parametrize(
    "changes,reason",
    [
        ({"workflow_state": None}, "WORKFLOW_UNKNOWN"),
        ({"last_accessed_at": None}, "LAST_ACCESS_UNKNOWN"),
        ({"retention_until": None}, "RETENTION_UNKNOWN"),
        ({"size_bytes": None}, "SIZE_UNKNOWN"),
        ({"legal_hold": None}, "LEGAL_HOLD_UNKNOWN"),
        ({"last_accessed_at": datetime(2020, 1, 1)}, "LAST_ACCESS_NOT_TIMEZONE_AWARE"),
        ({"retention_until": datetime(2030, 1, 1)}, "RETENTION_NOT_TIMEZONE_AWARE"),
    ],
)
def test_unknown_or_naive_inputs_are_rejected(changes, reason):
    decision = evaluate(replace(BOUNDARY, **changes), POLICY, AT)
    assert decision.reasons == (reason,)


def test_multiple_failures_are_reported_in_stable_order():
    facts = replace(BOUNDARY, legal_hold=True, size_bytes=1, workflow_state="IN_PROGRESS")
    assert evaluate(facts, POLICY, AT).reasons == (
        "LEGAL_HOLD",
        "WORKFLOW_NOT_COMPLETE",
        "TOO_SMALL",
    )


def test_evaluation_time_must_be_aware_and_offsets_are_equivalent():
    with pytest.raises(ValueError):
        evaluate(BOUNDARY, POLICY, datetime(2026, 10, 30, 12, 0))
    shifted = AT.astimezone(timezone(timedelta(hours=7)))
    assert (
        evaluate(BOUNDARY, POLICY, shifted).as_record()
        == evaluate(BOUNDARY, POLICY, AT).as_record()
    )


def test_owning_job_is_not_its_own_conflict():
    job_id = uuid.uuid4()
    facts = replace(BOUNDARY, active_job_id=job_id, lifecycle_state="ARCHIVING")
    assert evaluate(facts, POLICY, AT, owner_job_id=job_id).eligible
    assert evaluate(facts, POLICY, AT, owner_job_id=uuid.uuid4()).reasons == (
        "NOT_ACTIVE",
        "JOB_IN_PROGRESS",
    )


@pytest.mark.parametrize(
    "config",
    [
        {**CONFIG, "unexpected": True},
        {**CONFIG, "conditions": {**CONFIG["conditions"], "inactive_for_days": -1}},
        {**CONFIG, "conditions": {**CONFIG["conditions"], "workflow_state": "IN_PROGRESS"}},
        {**CONFIG, "action": {"desired_storage_class": "DELETE"}},
        {"conditions": CONFIG["conditions"]},
    ],
)
def test_policy_configuration_is_validated(config):
    with pytest.raises(ValidationError):
        Rule.model_validate(config)
