"""SQLAlchemy tables. Rows map 1:1 to the contracts in ``firebreak/models/``."""

from datetime import datetime
from typing import Any

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    Numeric,
    String,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

_NAMING = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

TSTZ = DateTime(timezone=True)


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=_NAMING)


class AlertRow(Base):
    __tablename__ = "alerts"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    idempotency_key: Mapped[str] = mapped_column(Text, unique=True)
    source: Mapped[str] = mapped_column(String)
    fingerprint: Mapped[str] = mapped_column(Text)
    alertname: Mapped[str] = mapped_column(Text)
    service: Mapped[str] = mapped_column(String)
    severity: Mapped[str] = mapped_column(String)
    status: Mapped[str] = mapped_column(String)
    summary: Mapped[str] = mapped_column(Text)
    labels: Mapped[dict[str, str]] = mapped_column(JSONB)
    started_at: Mapped[datetime] = mapped_column(TSTZ)
    ended_at: Mapped[datetime | None] = mapped_column(TSTZ, nullable=True)
    created_at: Mapped[datetime] = mapped_column(TSTZ, server_default=func.now())


class IncidentRow(Base):
    __tablename__ = "incidents"
    __table_args__ = (Index("ix_incidents_cluster_window", "cluster", "window_closes_at"),)

    id: Mapped[str] = mapped_column(String, primary_key=True)
    state: Mapped[str] = mapped_column(String)
    severity: Mapped[str] = mapped_column(String)
    cluster: Mapped[str] = mapped_column(String)
    services: Mapped[list[str]] = mapped_column(ARRAY(Text), server_default=text("'{}'::text[]"))
    opened_at: Mapped[datetime] = mapped_column(TSTZ)
    window_closes_at: Mapped[datetime] = mapped_column(TSTZ)
    updated_at: Mapped[datetime] = mapped_column(TSTZ)


class IncidentAlertRow(Base):
    __tablename__ = "incident_alerts"

    incident_id: Mapped[str] = mapped_column(
        ForeignKey("incidents.id", ondelete="CASCADE"), primary_key=True
    )
    alert_id: Mapped[str] = mapped_column(ForeignKey("alerts.id"), primary_key=True)
    service: Mapped[str] = mapped_column(String)
    attached_at: Mapped[datetime] = mapped_column(TSTZ, server_default=func.clock_timestamp())


class EvidenceRow(Base):
    __tablename__ = "evidence"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    incident_id: Mapped[str] = mapped_column(
        ForeignKey("incidents.id", ondelete="CASCADE"), index=True
    )
    tool_call_id: Mapped[str] = mapped_column(String)
    kind: Mapped[str] = mapped_column(String)
    query: Mapped[str] = mapped_column(Text)
    summary: Mapped[str] = mapped_column(Text)
    raw_ref: Mapped[str] = mapped_column(Text)
    untrusted: Mapped[bool] = mapped_column()
    created_at: Mapped[datetime] = mapped_column(TSTZ, server_default=func.now())


class EvidenceBlobRow(Base):
    """Full payload for an evidence row, keyed by ``Evidence.raw_ref``."""

    __tablename__ = "evidence_blobs"

    key: Mapped[str] = mapped_column(Text, primary_key=True)
    content: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(TSTZ, server_default=func.now())


class EvalRunRow(Base):
    """One invocation of the eval runner (ARCH §9, §13)."""

    __tablename__ = "eval_runs"
    __table_args__ = (Index("ix_eval_runs_agent_started_at", "agent", "started_at"),)

    id: Mapped[str] = mapped_column(String, primary_key=True)
    started_at: Mapped[datetime] = mapped_column(TSTZ)
    mode: Mapped[str] = mapped_column(String)
    agent: Mapped[str] = mapped_column(String)
    git_sha: Mapped[str | None] = mapped_column(String, nullable=True)
    prompt_sha256: Mapped[str | None] = mapped_column(String, nullable=True)


class EvalResultRow(Base):
    """One graded scenario inside an eval run."""

    __tablename__ = "eval_results"

    run_id: Mapped[str] = mapped_column(
        ForeignKey("eval_runs.id", ondelete="CASCADE"), primary_key=True
    )
    scenario_id: Mapped[str] = mapped_column(String, primary_key=True)
    top1: Mapped[bool] = mapped_column(Boolean)
    top3: Mapped[bool] = mapped_column(Boolean)
    cited_kinds_ok: Mapped[bool] = mapped_column(Boolean)
    report_valid: Mapped[bool] = mapped_column(Boolean)
    cost_usd: Mapped[float] = mapped_column(Numeric(12, 6, asdecimal=False))
    latency_s: Mapped[float] = mapped_column(Float)
    steps: Mapped[int] = mapped_column(Integer)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    report: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
