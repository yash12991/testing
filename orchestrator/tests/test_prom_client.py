"""Tests for orchestrator.prom_client — the real Prometheus HTTP client.

`httpx.get` is monkeypatched to a fake so these tests never make a real
network call. They pin the request shape (URL, query param) and the
response-parsing contract (empty result -> 0.0; non-empty -> the first
series' instant value, as a float).
"""

from __future__ import annotations

from orchestrator import prom_client


class FakeResponse:
    def __init__(self, json_body):
        self._json_body = json_body

    def raise_for_status(self):
        pass

    def json(self):
        return self._json_body


def test_query_returns_float_value_from_first_result(monkeypatch):
    captured = {}

    def fake_get(url, params=None, timeout=None):
        captured["url"] = url
        captured["params"] = params
        return FakeResponse({"data": {"result": [{"value": [1700000000, "0.42"]}]}})

    monkeypatch.setattr(prom_client.httpx, "get", fake_get)

    client = prom_client.PrometheusClient("http://prometheus.monitoring.svc:9090")
    value = client.query('up{job="x"}')

    assert value == 0.42
    assert captured["url"] == "http://prometheus.monitoring.svc:9090/api/v1/query"
    assert captured["params"] == {"query": 'up{job="x"}'}


def test_query_returns_zero_when_result_empty(monkeypatch):
    def fake_get(url, params=None, timeout=None):
        return FakeResponse({"data": {"result": []}})

    monkeypatch.setattr(prom_client.httpx, "get", fake_get)

    client = prom_client.PrometheusClient("http://prometheus.monitoring.svc:9090")
    assert client.query('up{job="nonexistent"}') == 0.0


def test_base_url_trailing_slash_is_stripped(monkeypatch):
    captured = {}

    def fake_get(url, params=None, timeout=None):
        captured["url"] = url
        return FakeResponse({"data": {"result": []}})

    monkeypatch.setattr(prom_client.httpx, "get", fake_get)

    client = prom_client.PrometheusClient("http://prometheus.monitoring.svc:9090/")
    client.query("up")
    assert captured["url"] == "http://prometheus.monitoring.svc:9090/api/v1/query"
