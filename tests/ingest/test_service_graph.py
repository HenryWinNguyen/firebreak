import logging
from pathlib import Path

import pytest

from firebreak.ingest.service_graph import ServiceGraph, ServiceGraphError


def test_loads_repo_config() -> None:
    g = ServiceGraph.load()
    for svc in ("gateway", "orders", "inventory", "postgres", "redis"):
        assert g.cluster_of(svc) == "shopfloor"


def test_unknown_service_is_singleton_with_warning(caplog: pytest.LogCaptureFixture) -> None:
    g = ServiceGraph.load()
    with caplog.at_level(logging.WARNING):
        assert g.cluster_of("mystery") == "singleton:mystery"
    assert "mystery" in caplog.text


@pytest.mark.parametrize(
    "data",
    [
        [],
        {"clusters": []},
        {"clusters": {"a": {"services": "x"}}},
        {"clusters": {"a": {"services": ["x"]}, "b": {"services": ["x"]}}},
        {"clusters": {"a": {"services": ["x"]}}, "edges": [["x", "y"]]},
        {"clusters": {"a": {"services": ["x"]}, "b": {"services": ["y"]}}, "edges": [["x", "y"]]},
        {"clusters": {"a": {"services": ["x"]}}, "edges": [["x"]]},
    ],
)
def test_invalid_graph_rejected(data: object) -> None:
    with pytest.raises(ServiceGraphError):
        ServiceGraph.from_dict(data)


def test_missing_and_malformed_file(tmp_path: Path) -> None:
    with pytest.raises(ServiceGraphError):
        ServiceGraph.load(tmp_path / "nope.yaml")
    bad = tmp_path / "bad.yaml"
    bad.write_text("clusters: [unclosed")
    with pytest.raises(ServiceGraphError):
        ServiceGraph.load(bad)


def test_unknown_service_warns_once_per_service(caplog: pytest.LogCaptureFixture) -> None:
    g = ServiceGraph.load()
    with caplog.at_level(logging.WARNING):
        for _ in range(3):
            g.cluster_of("mystery")
        g.cluster_of("other")
    assert caplog.text.count("mystery") == 1
    assert caplog.text.count("other") == 1


def test_warn_once_set_is_capped(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    import firebreak.ingest.service_graph as sg

    monkeypatch.setattr(sg, "MAX_WARNED_SERVICES", 2)
    g = ServiceGraph.load()
    with caplog.at_level(logging.WARNING):
        for name in ("a", "b", "c"):
            g.cluster_of(name)
        assert len(g._warned) == 2
        caplog.clear()
        g.cluster_of("c")
        g.cluster_of("c")
    # Past the cap: still warns every time, without growing the set.
    assert caplog.text.count("'c'") == 2
    assert len(g._warned) == 2
