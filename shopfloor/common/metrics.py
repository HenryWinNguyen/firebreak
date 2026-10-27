"""Prometheus metrics with the exact names and labels from CONTRACTS 2.4."""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, ProcessCollector

from common.pool import BoundedPool


class Metrics:
    def __init__(self, service: str, version: str) -> None:
        self.service = service
        self.registry = CollectorRegistry()
        ProcessCollector(registry=self.registry)
        self.requests = Counter(
            "http_requests_total",
            "HTTP requests",
            ["service", "route", "method", "status"],
            registry=self.registry,
        )
        self.duration = Histogram(
            "http_request_duration_seconds",
            "HTTP request latency",
            ["service", "route"],
            registry=self.registry,
        )
        self.pool_in_use = Gauge(
            "db_pool_in_use", "DB connections in use", ["service"], registry=self.registry
        )
        self.pool_size = Gauge("db_pool_size", "DB pool size", ["service"], registry=self.registry)
        self.pool_in_use.labels(service).set(0)
        self.pool_size.labels(service).set(0)
        info = Gauge(
            "app_info", "Service build info", ["service", "version"], registry=self.registry
        )
        info.labels(service, version).set(1)

    def bind_pool(self, pool: BoundedPool) -> None:
        self.pool_in_use.labels(self.service).set_function(lambda: pool.in_use)
        self.pool_size.labels(self.service).set_function(lambda: pool.size)

    def observe(self, route: str, method: str, status: int, seconds: float) -> None:
        self.requests.labels(self.service, route, method, str(status)).inc()
        self.duration.labels(self.service, route).observe(seconds)
