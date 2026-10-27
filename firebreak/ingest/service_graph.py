"""Service dependency graph loader (ARCH §3, config/service_graph.yaml)."""

import logging
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

DEFAULT_PATH = Path(__file__).resolve().parents[2] / "config" / "service_graph.yaml"


class ServiceGraphError(Exception):
    """The service graph file is missing or invalid."""


class ServiceGraph:
    """Maps a service to its dependency cluster.

    A service not in any cluster becomes its own singleton cluster
    (``singleton:<service>``) and a warning is logged.
    """

    def __init__(self, clusters: dict[str, list[str]], edges: list[tuple[str, str]]) -> None:
        self._service_to_cluster: dict[str, str] = {}
        for name, services in clusters.items():
            for svc in services:
                if svc in self._service_to_cluster:
                    raise ServiceGraphError(
                        f"service {svc!r} is in clusters {self._service_to_cluster[svc]!r} "
                        f"and {name!r}"
                    )
                self._service_to_cluster[svc] = name
        for caller, callee in edges:
            for svc in (caller, callee):
                if svc not in self._service_to_cluster:
                    raise ServiceGraphError(f"edge {caller}->{callee}: unknown service {svc!r}")
            if self._service_to_cluster[caller] != self._service_to_cluster[callee]:
                raise ServiceGraphError(
                    f"edge {caller}->{callee} crosses clusters "
                    f"{self._service_to_cluster[caller]!r} and {self._service_to_cluster[callee]!r}"
                )
        self.clusters = {k: list(v) for k, v in clusters.items()}
        self.edges = list(edges)

    @classmethod
    def from_dict(cls, data: Any) -> "ServiceGraph":
        if not isinstance(data, dict) or not isinstance(data.get("clusters"), dict):
            raise ServiceGraphError("service graph must be a mapping with a 'clusters' mapping")
        clusters: dict[str, list[str]] = {}
        for name, body in data["clusters"].items():
            services = body.get("services") if isinstance(body, dict) else None
            if not isinstance(services, list) or not all(isinstance(s, str) for s in services):
                raise ServiceGraphError(f"cluster {name!r} needs a 'services' list of strings")
            clusters[str(name)] = services
        edges: list[tuple[str, str]] = []
        for edge in data.get("edges") or []:
            if not (
                isinstance(edge, list) and len(edge) == 2 and all(isinstance(s, str) for s in edge)
            ):
                raise ServiceGraphError(f"edge must be a [caller, callee] pair, got {edge!r}")
            edges.append((edge[0], edge[1]))
        return cls(clusters, edges)

    @classmethod
    def load(cls, path: Path | str = DEFAULT_PATH) -> "ServiceGraph":
        p = Path(path)
        try:
            data = yaml.safe_load(p.read_text())
        except (OSError, yaml.YAMLError) as exc:
            raise ServiceGraphError(f"cannot load service graph {p}: {exc}") from exc
        return cls.from_dict(data)

    def cluster_of(self, service: str) -> str:
        cluster = self._service_to_cluster.get(service)
        if cluster is None:
            logger.warning("unknown service %r; using its own singleton cluster", service)
            return f"singleton:{service}"
        return cluster
