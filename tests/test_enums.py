from app.enums import LifecycleState, can_transition


def test_valid_transitions_are_allowed() -> None:
    assert can_transition(LifecycleState.CREATED, LifecycleState.ACTIVE)
    assert can_transition(LifecycleState.ACTIVE, LifecycleState.ARCHIVE_REQUESTED)
    assert can_transition(LifecycleState.ARCHIVING, LifecycleState.ARCHIVED)
    assert can_transition(LifecycleState.ARCHIVED, LifecycleState.RESTORE_REQUESTED)
    assert can_transition(LifecycleState.RESTORING, LifecycleState.ACTIVE)
    assert can_transition(LifecycleState.RESTORED, LifecycleState.ACTIVE)


def test_restore_completes_directly_to_active() -> None:
    assert not can_transition(LifecycleState.RESTORING, LifecycleState.RESTORED)


def test_invalid_transitions_are_rejected() -> None:
    assert not can_transition(LifecycleState.ACTIVE, LifecycleState.ARCHIVED)
    assert not can_transition(LifecycleState.ARCHIVED, LifecycleState.ACTIVE)
    assert not can_transition(LifecycleState.CREATED, LifecycleState.ARCHIVING)


def test_failed_objects_can_be_retried() -> None:
    assert can_transition(LifecycleState.FAILED, LifecycleState.ARCHIVE_REQUESTED)
    assert can_transition(LifecycleState.FAILED, LifecycleState.RESTORE_REQUESTED)
