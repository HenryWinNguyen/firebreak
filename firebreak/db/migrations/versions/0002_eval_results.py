"""eval_runs and eval_results

Revision ID: 0002
Revises: 0001
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None

_TSTZ = sa.DateTime(timezone=True)


def upgrade() -> None:
    op.create_table(
        "eval_runs",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("started_at", _TSTZ, nullable=False),
        sa.Column("mode", sa.String(), nullable=False),
        sa.Column("agent", sa.String(), nullable=False),
        sa.Column("git_sha", sa.String(), nullable=True),
        sa.Column("prompt_sha256", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_eval_runs"),
    )
    op.create_index("ix_eval_runs_agent_started_at", "eval_runs", ["agent", "started_at"])
    op.create_table(
        "eval_results",
        sa.Column("run_id", sa.String(), nullable=False),
        sa.Column("scenario_id", sa.String(), nullable=False),
        sa.Column("top1", sa.Boolean(), nullable=False),
        sa.Column("top3", sa.Boolean(), nullable=False),
        sa.Column("cited_kinds_ok", sa.Boolean(), nullable=False),
        sa.Column("report_valid", sa.Boolean(), nullable=False),
        sa.Column("cost_usd", sa.Numeric(12, 6, asdecimal=False), nullable=False),
        sa.Column("latency_s", sa.Float(), nullable=False),
        sa.Column("steps", sa.Integer(), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("report", postgresql.JSONB(), nullable=True),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["eval_runs.id"],
            name="fk_eval_results_run_id_eval_runs",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("run_id", "scenario_id", name="pk_eval_results"),
    )


def downgrade() -> None:
    op.drop_table("eval_results")
    op.drop_index("ix_eval_runs_agent_started_at", table_name="eval_runs")
    op.drop_table("eval_runs")
