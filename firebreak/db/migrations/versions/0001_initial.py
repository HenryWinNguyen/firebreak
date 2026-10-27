"""initial: alerts, incidents, incident_alerts, evidence, evidence_blobs

Revision ID: 0001
Revises:
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

_TSTZ = sa.DateTime(timezone=True)


def upgrade() -> None:
    op.create_table(
        "alerts",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("idempotency_key", sa.Text(), nullable=False),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("fingerprint", sa.Text(), nullable=False),
        sa.Column("alertname", sa.Text(), nullable=False),
        sa.Column("service", sa.String(), nullable=False),
        sa.Column("severity", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("labels", postgresql.JSONB(), nullable=False),
        sa.Column("started_at", _TSTZ, nullable=False),
        sa.Column("ended_at", _TSTZ, nullable=True),
        sa.Column("created_at", _TSTZ, server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_alerts"),
        sa.UniqueConstraint("idempotency_key", name="uq_alerts_idempotency_key"),
    )
    op.create_table(
        "incidents",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("state", sa.String(), nullable=False),
        sa.Column("severity", sa.String(), nullable=False),
        sa.Column("cluster", sa.String(), nullable=False),
        sa.Column(
            "services",
            postgresql.ARRAY(sa.Text()),
            server_default=sa.text("'{}'::text[]"),
            nullable=False,
        ),
        sa.Column("opened_at", _TSTZ, nullable=False),
        sa.Column("window_closes_at", _TSTZ, nullable=False),
        sa.Column("updated_at", _TSTZ, nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_incidents"),
    )
    op.create_index("ix_incidents_cluster_window", "incidents", ["cluster", "window_closes_at"])
    op.create_table(
        "incident_alerts",
        sa.Column("incident_id", sa.String(), nullable=False),
        sa.Column("alert_id", sa.String(), nullable=False),
        sa.Column("service", sa.String(), nullable=False),
        sa.Column("attached_at", _TSTZ, server_default=sa.func.clock_timestamp(), nullable=False),
        sa.ForeignKeyConstraint(
            ["incident_id"],
            ["incidents.id"],
            name="fk_incident_alerts_incident_id_incidents",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["alert_id"], ["alerts.id"], name="fk_incident_alerts_alert_id_alerts"
        ),
        sa.PrimaryKeyConstraint("incident_id", "alert_id", name="pk_incident_alerts"),
    )
    op.create_table(
        "evidence",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("incident_id", sa.String(), nullable=False),
        sa.Column("tool_call_id", sa.String(), nullable=False),
        sa.Column("kind", sa.String(), nullable=False),
        sa.Column("query", sa.Text(), nullable=False),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("raw_ref", sa.Text(), nullable=False),
        sa.Column("untrusted", sa.Boolean(), nullable=False),
        sa.Column("created_at", _TSTZ, server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(
            ["incident_id"],
            ["incidents.id"],
            name="fk_evidence_incident_id_incidents",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_evidence"),
    )
    op.create_index("ix_evidence_incident_id", "evidence", ["incident_id"])
    op.create_table(
        "evidence_blobs",
        sa.Column("key", sa.Text(), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("created_at", _TSTZ, server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("key", name="pk_evidence_blobs"),
    )


def downgrade() -> None:
    op.drop_table("evidence_blobs")
    op.drop_index("ix_evidence_incident_id", table_name="evidence")
    op.drop_table("evidence")
    op.drop_table("incident_alerts")
    op.drop_index("ix_incidents_cluster_window", table_name="incidents")
    op.drop_table("incidents")
    op.drop_table("alerts")
