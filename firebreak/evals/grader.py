"""Deterministic grader (ARCH §9): scores an InvestigationReport against a Scenario."""

import math
from collections.abc import Mapping, Sequence

from pydantic import BaseModel

from firebreak.evals.scenario import Scenario
from firebreak.models.evidence import EvidenceKind
from firebreak.models.reasoning import InvestigationReport


class Grade(BaseModel):
    top1: bool = False
    top3: bool = False
    cited_kinds_ok: bool = False
    report_valid: bool = False
    cost_usd: float = 0.0
    latency_s: float = 0.0
    error: str | None = None


class Summary(BaseModel):
    n: int
    top1_accuracy: float
    top3_accuracy: float
    latency_p50_s: float
    latency_p95_s: float
    cost_p50_usd: float
    cost_p95_usd: float
    error_count: int


def grade(
    scenario: Scenario,
    report: InvestigationReport | None,
    *,
    evidence_kinds: Mapping[str, EvidenceKind],
    usage_usd: float,
    latency_s: float,
    error: str | None,
) -> Grade:
    """Grade one run; never raises.

    A missing report is all-false with the error kept (``"no report produced"`` if none given).
    A malformed report (e.g. built with ``model_construct``) is all-false with an error.
    Errored runs still count in the accuracy denominators and the percentiles.
    """
    if report is None:
        return Grade(cost_usd=usage_usd, latency_s=latency_s, error=error or "no report produced")
    try:
        return _grade_report(scenario, report, evidence_kinds, usage_usd, latency_s, error)
    except (AttributeError, IndexError, ValueError, TypeError, KeyError) as exc:
        return Grade(
            cost_usd=usage_usd,
            latency_s=latency_s,
            error=f"malformed report: {type(exc).__name__}: {exc}",
        )


def _grade_report(
    scenario: Scenario,
    report: InvestigationReport,
    evidence_kinds: Mapping[str, EvidenceKind],
    usage_usd: float,
    latency_s: float,
    error: str | None,
) -> Grade:
    expected = scenario.expect.root_cause_category
    ranked = sorted(report.hypotheses, key=lambda h: h.rank)
    top = ranked[0]
    top1 = top.root_cause_category == expected
    top3 = any(h.root_cause_category == expected for h in ranked[:3])

    cited: set[EvidenceKind] = set()
    for ref in top.supporting_findings:
        # refs are "f<index>", validated by InvestigationReport
        finding = report.findings[int(ref[1:])]
        for evidence_id in finding.evidence_ids:
            kind = evidence_kinds.get(evidence_id)
            if kind is not None:
                cited.add(kind)
    cited_ok = all(k in cited for k in scenario.expect.must_cite_kinds)

    return Grade(
        top1=top1,
        top3=top3,
        cited_kinds_ok=cited_ok,
        report_valid=True,
        cost_usd=usage_usd,
        latency_s=latency_s,
        error=error,
    )


def _percentile(values: Sequence[float], pct: float) -> float:
    """Nearest-rank percentile."""
    ordered = sorted(values)
    rank = max(1, math.ceil(pct / 100 * len(ordered)))
    return ordered[rank - 1]


def summarize(grades: Sequence[Grade]) -> Summary:
    """Errored runs count in the accuracy denominators and in the percentiles."""
    if not grades:
        raise ValueError("cannot summarize zero grades")
    n = len(grades)
    latencies = [g.latency_s for g in grades]
    costs = [g.cost_usd for g in grades]
    return Summary(
        n=n,
        top1_accuracy=sum(g.top1 for g in grades) / n,
        top3_accuracy=sum(g.top3 for g in grades) / n,
        latency_p50_s=_percentile(latencies, 50),
        latency_p95_s=_percentile(latencies, 95),
        cost_p50_usd=_percentile(costs, 50),
        cost_p95_usd=_percentile(costs, 95),
        error_count=sum(g.error is not None for g in grades),
    )
