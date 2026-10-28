import pytest

from firebreak.evals.grader import Grade, grade, summarize
from firebreak.evals.scenario import Scenario
from firebreak.models.evidence import EvidenceKind, Finding
from firebreak.models.reasoning import Hypothesis, InvestigationReport, RootCauseCategory

C = RootCauseCategory
KINDS = {"e1": EvidenceKind.DEPLOY, "e2": EvidenceKind.METRIC_SERIES, "e3": EvidenceKind.LOG_LINES}


def scenario() -> Scenario:
    return Scenario.model_validate(
        {
            "id": "S1",
            "title": "t",
            "setup": {},
            "load": {"rps": 1, "duration_s": 1},
            "expect": {
                "alerts": [{"alertname": "A", "service": "orders"}],
                "root_cause_category": "bad_deploy",
                "must_cite_kinds": ["deploy", "metric_series"],
            },
        }
    )


def report(cats: list[C], support: list[str] | None = None) -> InvestigationReport:
    findings = [
        Finding(claim="a", evidence_ids=["e1"], confidence=0.9, agent="x"),
        Finding(claim="b", evidence_ids=["e2"], confidence=0.9, agent="x"),
        Finding(claim="c", evidence_ids=["e3"], confidence=0.9, agent="x"),
    ]
    hyps = [
        Hypothesis(
            root_cause_category=c,
            explanation="e",
            supporting_findings=support if (i == 0 and support) else ["f0", "f1"],
            confidence=0.5,
            rank=i + 1,
        )
        for i, c in enumerate(cats)
    ]
    return InvestigationReport(findings=findings, hypotheses=hyps)


def run(r: InvestigationReport | None, error: str | None = None) -> Grade:
    return grade(scenario(), r, evidence_kinds=KINDS, usage_usd=0.1, latency_s=2.0, error=error)


def test_top1_hit() -> None:
    g = run(report([C.BAD_DEPLOY, C.CAPACITY]))
    assert g.top1 and g.top3 and g.report_valid and g.cited_kinds_ok
    assert g.cost_usd == 0.1 and g.latency_s == 2.0


def test_top3_only() -> None:
    g = run(report([C.CAPACITY, C.SLOW_QUERY, C.BAD_DEPLOY]))
    assert not g.top1 and g.top3


def test_miss() -> None:
    g = run(report([C.CAPACITY, C.SLOW_QUERY]))
    assert not g.top1 and not g.top3 and g.report_valid


def test_unknown_category_is_miss() -> None:
    g = run(report([C.UNKNOWN]))
    assert not g.top1 and not g.top3


def test_missing_report_keeps_error() -> None:
    g = run(None, error="boom")
    assert not (g.top1 or g.top3 or g.cited_kinds_ok or g.report_valid)
    assert g.error == "boom"


def test_cited_kinds_not_satisfied() -> None:
    g = run(report([C.BAD_DEPLOY], support=["f0", "f2"]))
    assert g.top1 and not g.cited_kinds_ok


def test_cited_kinds_satisfied_by_supporting_findings() -> None:
    g = run(report([C.BAD_DEPLOY], support=["f0", "f1", "f2"]))
    assert g.cited_kinds_ok


def test_unmapped_evidence_not_counted() -> None:
    g = grade(
        scenario(),
        report([C.BAD_DEPLOY]),
        evidence_kinds={"e1": EvidenceKind.DEPLOY},
        usage_usd=0,
        latency_s=0,
        error=None,
    )
    assert not g.cited_kinds_ok


def test_summary_percentiles() -> None:
    grades = [
        Grade(
            top1=i < 7,
            top3=i < 9,
            latency_s=float(i + 1),
            cost_usd=(i + 1) / 10,
            error="x" if i == 9 else None,
        )
        for i in range(10)
    ]
    s = summarize(grades)
    assert s.n == 10
    assert s.top1_accuracy == 0.7 and s.top3_accuracy == 0.9
    assert s.latency_p50_s == 5.0 and s.latency_p95_s == 10.0
    assert s.cost_p50_usd == pytest.approx(0.5) and s.cost_p95_usd == pytest.approx(1.0)
    assert s.error_count == 1


def test_summary_empty_raises() -> None:
    with pytest.raises(ValueError):
        summarize([])
