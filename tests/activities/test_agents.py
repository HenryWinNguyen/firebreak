"""T-065: the roster configs and the ``run_agent`` activity (fake LLM, fake tools, no network)."""

import json
from typing import Any
from unittest.mock import AsyncMock

import pytest
from temporalio.exceptions import ApplicationError

from firebreak.activities.agents import AgentActivities, build_agent_task_text
from firebreak.agents.config import CONFIGS_DIR, load_agent_configs, resolve_output_schema
from firebreak.agents.settings import AgentSettings
from firebreak.models.common import Severity
from firebreak.models.evidence import Finding, SpecialistReport
from firebreak.models.llm import LLMRequest
from firebreak.models.orchestration import (
    AgentTask,
    IncidentContext,
    Plan,
    SpecialistTask,
    TriageResult,
    TriageVerdict,
)
from firebreak.tools.settings import ToolSettings
from tests.agents.test_baseline import fake_read_registry
from tests.tools.fakes import (
    FakeEvidenceSink,
    ScriptedLLM,
    evidence_ids_in,
    text_response,
    tool_response,
)

PROFILE = CONFIGS_DIR / "multi-local"
SPECIALISTS = {"metrics", "logs", "deploys"}
EXPECTED_TOOLS = {
    "triage": [],
    "supervisor": ["service_graph"],
    "metrics": ["promql_query", "promql_range", "list_metrics"],
    "logs": ["logql_query"],
    "deploys": ["recent_deploys", "service_graph"],
    "synthesizer": [],
}
EXPECTED_SCHEMA = {
    "triage": "TriageResult",
    "supervisor": "Plan",
    "metrics": "SpecialistReport",
    "logs": "SpecialistReport",
    "deploys": "SpecialistReport",
    "synthesizer": "InvestigationReport",
}

SUMMARY = (
    "incident inc_1 severity high\n"
    '<untrusted-data source="alert_annotations" nonce="n1">orders is slow'
    '</untrusted-data nonce="n1">'
)


def _report(agent: str, evidence_id: str) -> SpecialistReport:
    return SpecialistReport(
        agent=agent,
        question=f"question for {agent}",
        findings=[
            Finding(
                claim=f"{agent} saw a problem",
                evidence_ids=[evidence_id],
                confidence=0.7,
                agent=agent,
            )
        ],
    )


def _ctx(**kw: Any) -> IncidentContext:
    base: dict[str, Any] = {
        "incident_id": "inc_1",
        "summary": SUMMARY,
        "services": ["gateway", "orders", "inventory"],
    }
    base.update(kw)
    return IncidentContext(**base)


def _triage() -> TriageResult:
    return TriageResult(
        verdict=TriageVerdict.INVESTIGATE,
        severity=Severity.CRITICAL,
        affected_services=["orders"],
        rationale="orders latency alert",
    )


def _activities(llm: ScriptedLLM, sink: FakeEvidenceSink | None = None) -> AgentActivities:
    sink = sink or FakeEvidenceSink()
    return AgentActivities(
        llm=llm,
        sessions=AsyncMock(),
        tools_settings=ToolSettings(),
        settings=AgentSettings(),
        registry_factory=lambda: (fake_read_registry(), None),
        sink_factory=lambda: sink,
    )


# --- configs ----------------------------------------------------------------------------


def test_every_config_loads_and_resolves_schema_and_tools() -> None:
    cfgs = load_agent_configs(fake_read_registry(), configs_dir=PROFILE)
    assert set(cfgs) == set(EXPECTED_TOOLS)
    for name, cfg in cfgs.items():
        assert cfg.name == name
        assert cfg.model == "qwen3:8b" and cfg.fallback_model is None
        assert cfg.tools == EXPECTED_TOOLS[name]
        assert cfg.output_schema == EXPECTED_SCHEMA[name]
        resolve_output_schema(cfg.output_schema)


def test_supervisor_prompt_lists_the_specialists_that_exist() -> None:
    text = (CONFIGS_DIR.parent / "prompts" / "multi" / "supervisor.md").read_text()
    for name in SPECIALISTS:
        assert f"`{name}`" in text


def test_prompts_are_short() -> None:
    for p in (CONFIGS_DIR.parent / "prompts" / "multi").glob("*.md"):
        assert len(p.read_text()) < 3000, p.name


# --- slices -----------------------------------------------------------------------------


def test_triage_sees_only_the_summary() -> None:
    ctx = _ctx(triage=_triage(), reports=[_report("metrics", "ev_a")])
    text = build_agent_task_text(AgentTask(agent="triage", incident_id="inc_1", context=ctx))
    assert SUMMARY in text
    assert "orders latency alert" not in text and "ev_a" not in text


def test_supervisor_sees_summary_and_triage() -> None:
    ctx = _ctx(triage=_triage(), reports=[_report("metrics", "ev_a")])
    text = build_agent_task_text(AgentTask(agent="supervisor", incident_id="inc_1", context=ctx))
    assert SUMMARY in text and "orders latency alert" in text
    assert "ev_a" not in text


def test_specialist_sees_question_and_services_but_not_other_reports() -> None:
    ctx = _ctx(
        triage=_triage(),
        plan=Plan(tasks=[SpecialistTask(agent="logs", question="q")], rationale="r"),
        reports=[_report("metrics", "ev_metrics"), _report("deploys", "ev_deploys")],
    )
    task = AgentTask(
        agent="logs", incident_id="inc_1", question="Any errors in orders?", context=ctx
    )
    text = build_agent_task_text(task)
    assert SUMMARY in text and "Any errors in orders?" in text
    assert "Affected services: orders" in text
    assert "ev_metrics" not in text and "ev_deploys" not in text
    assert "orders latency alert" not in text  # no triage rationale


def test_specialist_falls_back_to_incident_services_without_triage() -> None:
    task = AgentTask(agent="metrics", incident_id="inc_1", question="q", context=_ctx())
    assert "Affected services: gateway, orders, inventory" in build_agent_task_text(task)


def test_synthesizer_sees_reports_and_degraded_sources_wrapped() -> None:
    ctx = _ctx(
        triage=_triage(),
        reports=[_report("metrics", "ev_metrics"), _report("deploys", "ev_deploys")],
        degraded_sources=["logs"],
    )
    text = build_agent_task_text(AgentTask(agent="synthesizer", incident_id="inc_1", context=ctx))
    assert "ev_metrics" in text and "ev_deploys" in text
    assert "Degraded sources (failed or timed out): logs" in text
    assert '<untrusted-data source="specialist_reports"' in text
    assert SUMMARY in text  # the alert wrapping is passed through untouched


def test_specialist_without_question_is_non_retryable() -> None:
    with pytest.raises(ApplicationError) as e:
        build_agent_task_text(AgentTask(agent="metrics", incident_id="inc_1", context=_ctx()))
    assert e.value.non_retryable


# --- run_agent --------------------------------------------------------------------------


def _specialist_final(req: LLMRequest) -> Any:
    ids = evidence_ids_in(req)
    return text_response(
        json.dumps(
            {
                "agent": "metrics",
                "question": "q",
                "findings": [
                    {
                        "claim": "latency up",
                        "evidence_ids": ids,
                        "confidence": 0.8,
                        "agent": "metrics",
                    }
                ],
            }
        )
    )


async def test_run_agent_returns_validated_specialist_report() -> None:
    llm = ScriptedLLM(
        [
            tool_response(("t1", "promql_query", {"query": "up"})),
            _specialist_final,
        ]
    )
    sink = FakeEvidenceSink()
    task = AgentTask(agent="metrics", incident_id="inc_1", question="q", context=_ctx())
    result = await _activities(llm, sink).run_agent(task)
    assert result.stopped_reason == "final_output" and result.agent == "metrics"
    report = SpecialistReport.model_validate(result.output)
    assert report.findings[0].evidence_ids[0] in sink.stored
    assert [r.model for r in llm.requests] == ["qwen3:8b", "qwen3:8b"]
    assert "Your question: q" in str(llm.requests[0].messages[0]["content"])


async def test_run_agent_triage_has_no_tools() -> None:
    out = {
        "verdict": "investigate",
        "severity": "critical",
        "affected_services": ["orders"],
        "rationale": "real",
    }
    llm = ScriptedLLM([text_response(json.dumps(out))])
    task = AgentTask(agent="triage", incident_id="inc_1", context=_ctx())
    result = await _activities(llm).run_agent(task)
    assert TriageResult.model_validate(result.output).verdict is TriageVerdict.INVESTIGATE
    assert llm.requests[0].tools == []


async def test_invalid_output_is_returned_not_raised() -> None:
    llm = ScriptedLLM([text_response("not json"), text_response("still not json")])
    task = AgentTask(agent="triage", incident_id="inc_1", context=_ctx())
    result = await _activities(llm).run_agent(task)
    assert result.stopped_reason == "error"
    assert result.output == {} and result.error


async def test_unknown_agent_is_non_retryable() -> None:
    task = AgentTask(agent="nope", incident_id="inc_1", context=_ctx())
    with pytest.raises(ApplicationError) as e:
        await _activities(ScriptedLLM([])).run_agent(task)
    assert e.value.non_retryable


async def test_path_traversal_agent_name_is_rejected() -> None:
    task = AgentTask(agent="../baseline", incident_id="inc_1", context=_ctx())
    with pytest.raises(ApplicationError) as e:
        await _activities(ScriptedLLM([])).run_agent(task)
    assert e.value.non_retryable


async def test_unknown_profile_is_non_retryable() -> None:
    acts = _activities(ScriptedLLM([]))
    acts.settings = AgentSettings(profile="does-not-exist")
    task = AgentTask(agent="triage", incident_id="inc_1", context=_ctx())
    with pytest.raises(ApplicationError) as e:
        await acts.run_agent(task)
    assert e.value.non_retryable


async def test_adapters_are_closed_after_each_call() -> None:
    from firebreak.tools.read_tools import ReadAdapters

    adapters = AsyncMock(spec=ReadAdapters)
    llm = ScriptedLLM([text_response("x"), text_response("y")])
    acts = _activities(llm)
    acts.registry_factory = lambda: (fake_read_registry(), adapters)
    await acts.run_agent(AgentTask(agent="triage", incident_id="inc_1", context=_ctx()))
    adapters.aclose.assert_awaited_once()
