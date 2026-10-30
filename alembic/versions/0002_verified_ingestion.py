"""Add immutable ingestion identity and searchable business metadata."""

import sqlalchemy as sa
from alembic import op

revision = "0002_verified_ingestion"
down_revision = "0001_initial_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("ALTER TYPE lifecycle_state ADD VALUE IF NOT EXISTS 'INGESTING'")
    columns = (
        sa.Column("logical_key", sa.String(512), nullable=True),
        sa.Column("content_version", sa.Integer(), server_default="1", nullable=False),
        sa.Column("owner_id", sa.String(128), server_default="legacy", nullable=False),
        sa.Column("dataset_id", sa.String(128), nullable=True),
        sa.Column("workflow_state", sa.String(32), nullable=True),
        sa.Column("legal_hold", sa.Boolean(), server_default=sa.false(), nullable=False),
        sa.Column("retention_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "content_type",
            sa.String(128),
            server_default="application/octet-stream",
            nullable=False,
        ),
        sa.Column("desired_storage_class", sa.String(16), server_default="ACTIVE", nullable=False),
        sa.Column(
            "observed_storage_class", sa.String(16), server_default="UNKNOWN", nullable=False
        ),
        sa.Column("ingestion_key", sa.String(255), nullable=True),
        sa.Column("request_fingerprint", sa.String(64), nullable=True),
        sa.Column("staging_bucket", sa.String(255), nullable=True),
        sa.Column("staging_key", sa.String(1024), nullable=True),
    )
    for column in columns:
        op.add_column("objects", column)
    op.execute("UPDATE objects SET logical_key = id::text")
    op.alter_column("objects", "logical_key", nullable=False)
    for name in (
        "content_version",
        "owner_id",
        "legal_hold",
        "content_type",
        "desired_storage_class",
        "observed_storage_class",
    ):
        op.alter_column("objects", name, server_default=None)
    op.create_unique_constraint(
        "uq_objects_identity", "objects", ["owner_id", "logical_key", "content_version"]
    )
    op.create_unique_constraint("uq_objects_ingestion", "objects", ["owner_id", "ingestion_key"])
    op.create_check_constraint("ck_objects_size", "objects", "size_bytes >= 0")
    op.create_check_constraint("ck_objects_content_version", "objects", "content_version > 0")
    op.create_index("ix_objects_owner_created", "objects", ["owner_id", "created_at", "id"])


def downgrade() -> None:
    op.execute("UPDATE objects SET lifecycle_state = 'CREATED' WHERE lifecycle_state = 'INGESTING'")
    op.drop_index("ix_objects_owner_created", table_name="objects")
    for name, kind in (
        ("uq_objects_identity", "unique"),
        ("uq_objects_ingestion", "unique"),
        ("ck_objects_size", "check"),
        ("ck_objects_content_version", "check"),
    ):
        op.drop_constraint(name, "objects", type_=kind)
    for name in (
        "logical_key",
        "content_version",
        "owner_id",
        "dataset_id",
        "workflow_state",
        "legal_hold",
        "retention_until",
        "content_type",
        "desired_storage_class",
        "observed_storage_class",
        "ingestion_key",
        "request_fingerprint",
        "staging_bucket",
        "staging_key",
    ):
        op.drop_column("objects", name)
