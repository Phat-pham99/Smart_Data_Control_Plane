"""Add durable lifecycle jobs, attempts, outbox, immutable policies and archive fields."""

import json

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0003_lifecycle_operations"
down_revision = "0002_verified_ingestion"
branch_labels = None
depends_on = None

NONTERMINAL = "status IN ('PENDING', 'RUNNING', 'RETRY_SCHEDULED', 'RECOVERY_REQUIRED')"
SEED_POLICY = {
    "conditions": {
        "workflow_state": "COMPLETE",
        "inactive_for_days": 180,
        "minimum_remaining_retention_days": 180,
        "minimum_size_bytes": 1048576,
    },
    "action": {"desired_storage_class": "ARCHIVE"},
}


def _rows(sql: str) -> list:
    """Run a preflight query; offline SQL generation cannot inspect data."""

    if op.get_context().as_sql:
        return []
    return list(op.get_bind().execute(sa.text(sql)).scalars())


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("ALTER TYPE job_status ADD VALUE IF NOT EXISTS 'RETRY_SCHEDULED'")
        op.execute("ALTER TYPE job_status ADD VALUE IF NOT EXISTS 'RECOVERY_REQUIRED'")
    conflicts = _rows(
        "SELECT object_id::text FROM jobs WHERE status IN ('PENDING', 'RUNNING') "
        "GROUP BY object_id HAVING count(*) > 1 ORDER BY 1 LIMIT 20"
    )
    if conflicts:
        raise RuntimeError(
            "Objects have multiple nonterminal legacy jobs; settle them before upgrading: "
            + ", ".join(conflicts)
        )

    op.add_column("policies", sa.Column("name", sa.String(128), nullable=True))
    op.add_column(
        "policies", sa.Column("priority", sa.Integer(), server_default="100", nullable=False)
    )
    op.add_column("policies", sa.Column("activated_at", sa.DateTime(timezone=True)))
    op.add_column("policies", sa.Column("superseded_at", sa.DateTime(timezone=True)))
    op.execute("UPDATE policies SET name = 'legacy-' || id::text")
    op.alter_column("policies", "name", nullable=False)
    op.alter_column("policies", "priority", server_default=None)
    op.create_unique_constraint("uq_policies_name_version", "policies", ["name", "version"])
    op.execute(
        """
        CREATE FUNCTION policies_immutable() RETURNS trigger AS $$
        BEGIN
            IF OLD.activated_at IS NOT NULL AND
               (NEW.name, NEW.version, NEW.priority, NEW.config, NEW.activated_at)
               IS DISTINCT FROM
               (OLD.name, OLD.version, OLD.priority, OLD.config, OLD.activated_at) THEN
                RAISE EXCEPTION 'Activated policy versions are immutable'
                    USING ERRCODE = 'check_violation';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
        """
    )
    op.execute(
        "CREATE TRIGGER trg_policies_immutable BEFORE UPDATE ON policies "
        "FOR EACH ROW EXECUTE FUNCTION policies_immutable()"
    )
    op.execute(
        sa.text(
            "INSERT INTO policies (id, name, version, priority, is_active, config, activated_at) "
            "VALUES (gen_random_uuid(), 'archive-completed-cold-datasets', 1, 100, true, "
            "CAST(:config AS jsonb), now())"
        ).bindparams(config=json.dumps(SEED_POLICY))
    )

    for column in (
        sa.Column("archive_bucket", sa.String(255)),
        sa.Column("archive_key", sa.String(1024)),
        sa.Column("archive_copy_retained", sa.Boolean(), server_default=sa.false(), nullable=False),
        sa.Column("restored_at", sa.DateTime(timezone=True)),
        sa.Column("ingestion_verified_at", sa.DateTime(timezone=True)),
    ):
        op.add_column("objects", column)
    op.alter_column("objects", "archive_copy_retained", server_default=None)
    # Only trusted verification audit records establish the timestamp.
    op.execute(
        """
        UPDATE objects SET ingestion_verified_at = verified.first_at
        FROM (
            SELECT object_id, min(created_at) AS first_at FROM audit_events
            WHERE reason = 'INGESTION_VERIFIED' AND object_id IS NOT NULL
            GROUP BY object_id
        ) AS verified
        WHERE verified.object_id = objects.id
        """
    )
    op.create_index(
        "ix_objects_archive_candidates",
        "objects",
        ["last_accessed_at", "id"],
        postgresql_where=sa.text("lifecycle_state = 'ACTIVE' AND legal_hold = false"),
    )

    op.drop_constraint("uq_jobs_idempotency_key", "jobs", type_="unique")
    for column in (
        sa.Column("request_fingerprint", sa.String(64)),
        sa.Column("max_attempts", sa.Integer(), server_default="5", nullable=False),
        sa.Column("error_class", sa.String(64)),
        sa.Column("content_version", sa.Integer()),
        sa.Column("expected_sha256", sa.String(64)),
        sa.Column("expected_size", sa.BigInteger()),
        sa.Column("source_bucket", sa.String(255)),
        sa.Column("source_key", sa.String(1024)),
        sa.Column("destination_bucket", sa.String(255)),
        sa.Column("destination_key", sa.String(1024)),
        sa.Column("policy_id", postgresql.UUID(as_uuid=True)),
        sa.Column("decision", postgresql.JSONB()),
        sa.Column("actor_kind", sa.String(32)),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True)),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("retry_of_job_id", postgresql.UUID(as_uuid=True)),
    ):
        op.add_column("jobs", column)
    op.alter_column("jobs", "max_attempts", server_default=None)
    # Legacy jobs have no immutable binding, so they must never be picked up automatically.
    op.execute(
        "UPDATE jobs SET status = 'RECOVERY_REQUIRED', error_class = 'LEGACY_UNBOUND_JOB' "
        "WHERE status IN ('PENDING', 'RUNNING')"
    )
    op.create_foreign_key("fk_jobs_policy_id_policies", "jobs", "policies", ["policy_id"], ["id"])
    op.create_foreign_key(
        "fk_jobs_retry_of_job_id_jobs", "jobs", "jobs", ["retry_of_job_id"], ["id"]
    )
    op.create_unique_constraint(
        "uq_jobs_object_idempotency", "jobs", ["object_id", "idempotency_key"]
    )
    op.create_unique_constraint("uq_jobs_retry_of", "jobs", ["retry_of_job_id"])
    op.create_check_constraint("ck_jobs_attempt_count", "jobs", "attempt_count >= 0")
    op.create_check_constraint("ck_jobs_max_attempts", "jobs", "max_attempts BETWEEN 1 AND 20")
    op.create_index(
        "ux_jobs_one_nonterminal",
        "jobs",
        ["object_id"],
        unique=True,
        postgresql_where=sa.text(NONTERMINAL),
    )
    op.create_index(
        "ix_jobs_due",
        "jobs",
        ["next_attempt_at", "created_at", "id"],
        postgresql_where=sa.text("status IN ('PENDING', 'RETRY_SCHEDULED')"),
    )

    op.create_table(
        "job_attempts",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("stage", sa.String(32), nullable=False),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("error_class", sa.String(64)),
        sa.Column("detail", sa.Text()),
        sa.ForeignKeyConstraint(
            ["job_id"], ["jobs.id"], name="fk_job_attempts_job_id_jobs", ondelete="CASCADE"
        ),
        sa.UniqueConstraint("job_id", "attempt_number", name="uq_job_attempts_number"),
    )
    op.create_table(
        "outbox_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("publish_attempts", sa.Integer(), nullable=False),
        sa.Column(
            "next_publish_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.Column("last_error", sa.Text()),
        sa.ForeignKeyConstraint(
            ["job_id"], ["jobs.id"], name="fk_outbox_events_job_id_jobs", ondelete="CASCADE"
        ),
    )
    op.create_index(
        "ix_outbox_pending",
        "outbox_events",
        ["next_publish_at", "created_at"],
        postgresql_where=sa.text("published_at IS NULL"),
    )

    for column in (
        sa.Column("event_type", sa.String(64)),
        sa.Column("actor_kind", sa.String(32)),
        sa.Column("policy_id", postgresql.UUID(as_uuid=True)),
        sa.Column("details", postgresql.JSONB()),
    ):
        op.add_column("audit_events", column)
    op.create_foreign_key(
        "fk_audit_events_policy_id_policies",
        "audit_events",
        "policies",
        ["policy_id"],
        ["id"],
        ondelete="SET NULL",
    )


def downgrade() -> None:
    moved = _rows(
        "SELECT count(*) FROM objects WHERE archive_key IS NOT NULL OR restored_at IS NOT NULL "
        "OR lifecycle_state IN ('ARCHIVE_REQUESTED', 'ARCHIVING', 'ARCHIVED', "
        "'RESTORE_REQUESTED', 'RESTORING')"
    )
    if moved and moved[0]:
        raise RuntimeError(
            f"{moved[0]} objects have lifecycle placement data that the previous schema cannot "
            "represent; refusing a destructive downgrade"
        )
    duplicates = _rows(
        "SELECT count(*) FROM (SELECT idempotency_key FROM jobs "
        "WHERE idempotency_key IS NOT NULL GROUP BY 1 HAVING count(*) > 1) AS d"
    )
    if duplicates and duplicates[0]:
        raise RuntimeError(
            f"{duplicates[0]} job idempotency keys are reused across objects; the previous "
            "schema requires global uniqueness"
        )

    op.drop_constraint("fk_audit_events_policy_id_policies", "audit_events", type_="foreignkey")
    for name in ("event_type", "actor_kind", "policy_id", "details"):
        op.drop_column("audit_events", name)

    op.drop_index("ix_outbox_pending", table_name="outbox_events")
    op.drop_table("outbox_events")
    op.drop_table("job_attempts")

    op.drop_index("ix_jobs_due", table_name="jobs")
    op.drop_index("ux_jobs_one_nonterminal", table_name="jobs")
    for name, kind in (
        ("ck_jobs_max_attempts", "check"),
        ("ck_jobs_attempt_count", "check"),
        ("uq_jobs_retry_of", "unique"),
        ("uq_jobs_object_idempotency", "unique"),
        ("fk_jobs_retry_of_job_id_jobs", "foreignkey"),
        ("fk_jobs_policy_id_policies", "foreignkey"),
    ):
        op.drop_constraint(name, "jobs", type_=kind)
    # The previous schema has no retry/recovery states; preserve the rows as failures.
    op.execute(
        "UPDATE jobs SET status = 'FAILED' WHERE status IN ('RETRY_SCHEDULED', 'RECOVERY_REQUIRED')"
    )
    for name in (
        "request_fingerprint",
        "max_attempts",
        "error_class",
        "content_version",
        "expected_sha256",
        "expected_size",
        "source_bucket",
        "source_key",
        "destination_bucket",
        "destination_key",
        "policy_id",
        "decision",
        "actor_kind",
        "next_attempt_at",
        "started_at",
        "finished_at",
        "retry_of_job_id",
    ):
        op.drop_column("jobs", name)
    op.create_unique_constraint("uq_jobs_idempotency_key", "jobs", ["idempotency_key"])

    op.drop_index("ix_objects_archive_candidates", table_name="objects")
    for name in (
        "archive_bucket",
        "archive_key",
        "archive_copy_retained",
        "restored_at",
        "ingestion_verified_at",
    ):
        op.drop_column("objects", name)

    op.execute("DROP TRIGGER trg_policies_immutable ON policies")
    op.execute("DROP FUNCTION policies_immutable()")
    op.execute(
        "DELETE FROM policies WHERE name = 'archive-completed-cold-datasets' AND version = 1"
    )
    op.drop_constraint("uq_policies_name_version", "policies", type_="unique")
    for name in ("name", "priority", "activated_at", "superseded_at"):
        op.drop_column("policies", name)
