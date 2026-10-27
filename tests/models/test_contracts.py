from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from firebreak.models.alert import Alert, AlertSource
from firebreak.models.common import Severity
from firebreak.models.evidence import Finding
from firebreak.models.llm import ToolSpec, Usage
from firebreak.models.reasoning import ActionType, ProposedAction

T0 = datetime(2026, 10, 27, 12, 0, tzinfo=UTC)


def _alert(**kw: object) -> Alert:
    base: dict[str, object] = {
        "source": AlertSource.ALERTMANAGER,
        "fingerprint": "abc123",
        "alertname": "OrdersHighErrorRate",
        "service": "orders",
        "severity": Severity.CRITICAL,
        "summary": "5xx > 5%",
        "started_at": T0,
    }
    base.update(kw)
    return Alert.model_validate(base)


def test_alert_idempotency_key_ignores_generated_id() -> None:
    a, b = _alert(), _alert()
    assert a.id != b.id
    assert a.idempotency_key == b.idempotency_key


def test_alert_is_frozen() -> None:
    with pytest.raises(ValidationError):
        _alert().service = "gateway"  # type: ignore[misc]


def test_finding_requires_evidence() -> None:
    with pytest.raises(ValidationError):
        Finding(claim="db is slow", evidence_ids=[], confidence=0.5, agent="metrics")


def test_finding_confidence_bounds() -> None:
    with pytest.raises(ValidationError):
        Finding(claim="x", evidence_ids=["ev_1"], confidence=1.5, agent="metrics")


def test_proposed_action_risk_tier_unset_by_default() -> None:
    act = ProposedAction(
        action=ActionType.ROLLBACK, target="orders", rationale="bad deploy", hypothesis_id="hy_1"
    )
    assert act.risk_tier is None


def test_tool_name_pattern() -> None:
    with pytest.raises(ValidationError):
        ToolSpec(name="Run Shell", description="x", input_schema={})


def test_usage_add() -> None:
    total = Usage(input_tokens=10, cost_usd=0.1) + Usage(input_tokens=5, cost_usd=0.05)
    assert total.input_tokens == 15
    assert total.cost_usd == pytest.approx(0.15)


def _report_kwargs(**over: object) -> dict[str, object]:
    from firebreak.models.evidence import Finding
    from firebreak.models.reasoning import Hypothesis, RootCauseCategory

    f = Finding(
        claim="5xx spike after deploy", evidence_ids=["ev_1"], confidence=0.8, agent="baseline"
    )
    h = Hypothesis(
        root_cause_category=RootCauseCategory.BAD_DEPLOY,
        explanation="v2 deploy",
        supporting_findings=["f0"],
        confidence=0.8,
        rank=1,
    )
    kw: dict[str, object] = {"findings": [f], "hypotheses": [h]}
    kw.update(over)
    return kw


def test_investigation_report_valid() -> None:
    from firebreak.models.reasoning import InvestigationReport, finding_ref

    r = InvestigationReport(**_report_kwargs())
    assert r.hypotheses[0].supporting_findings == [finding_ref(0)]


def test_investigation_report_rejects_unknown_ref() -> None:
    import pytest
    from pydantic import ValidationError

    from firebreak.models.reasoning import Hypothesis, InvestigationReport, RootCauseCategory

    bad = Hypothesis(
        root_cause_category=RootCauseCategory.BAD_DEPLOY,
        explanation="x",
        supporting_findings=["f3"],
        confidence=0.5,
        rank=1,
    )
    with pytest.raises(ValidationError, match="unknown findings"):
        InvestigationReport(**_report_kwargs(hypotheses=[bad]))


def test_investigation_report_rejects_bad_ranks() -> None:
    import pytest
    from pydantic import ValidationError

    from firebreak.models.reasoning import Hypothesis, InvestigationReport, RootCauseCategory

    hs = [
        Hypothesis(
            root_cause_category=RootCauseCategory.BAD_DEPLOY,
            explanation="x",
            supporting_findings=["f0"],
            confidence=0.5,
            rank=r,
        )
        for r in (1, 3)
    ]
    with pytest.raises(ValidationError, match="ranks"):
        InvestigationReport(**_report_kwargs(hypotheses=hs))
