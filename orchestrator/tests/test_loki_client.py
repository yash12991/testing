"""Tests for orchestrator.loki_client — the real Loki HTTP client.

`httpx.get` is monkeypatched to a fake so these tests never make a real
network call. The main thing under test (Task 13's review flagged this as
a gap): `query_range` must send explicit `start`/`end` query params
computed from `window_seconds`, rather than omitting them and relying on
Loki's implicit last-one-hour default.
"""

from __future__ import annotations

from orchestrator import loki_client


class FakeResponse:
    def __init__(self, json_body):
        self._json_body = json_body

    def raise_for_status(self):
        pass

    def json(self):
        return self._json_body


def test_query_range_sends_explicit_start_and_end_params(monkeypatch):
    captured = {}

    def fake_get(url, params=None, timeout=None):
        captured["url"] = url
        captured["params"] = params
        return FakeResponse({"data": {"result": []}})

    monkeypatch.setattr(loki_client.httpx, "get", fake_get)
    fixed_now_ns = 1_700_000_000 * 1_000_000_000
    monkeypatch.setattr(loki_client.time, "time", lambda: fixed_now_ns / 1_000_000_000)

    client = loki_client.LokiClient("http://loki.monitoring.svc:3100")
    client.query_range('{pod=~"cartservice.*"}', window_seconds=300)

    assert captured["url"] == "http://loki.monitoring.svc:3100/loki/api/v1/query_range"
    assert captured["params"]["query"] == '{pod=~"cartservice.*"}'
    assert captured["params"]["end"] == fixed_now_ns
    assert captured["params"]["start"] == fixed_now_ns - 300 * 1_000_000_000
    # Never silently fall back to Loki's implicit last-hour default.
    assert captured["params"]["start"] != captured["params"]["end"]


def test_query_range_window_affects_start_param(monkeypatch):
    seen_params = []

    def fake_get(url, params=None, timeout=None):
        seen_params.append(params)
        return FakeResponse({"data": {"result": []}})

    monkeypatch.setattr(loki_client.httpx, "get", fake_get)

    client = loki_client.LokiClient("http://loki.monitoring.svc:3100")
    client.query_range('{pod=~"x.*"}', window_seconds=60)
    client.query_range('{pod=~"x.*"}', window_seconds=3600)

    short_window_span = seen_params[0]["end"] - seen_params[0]["start"]
    long_window_span = seen_params[1]["end"] - seen_params[1]["start"]
    assert short_window_span == 60 * 1_000_000_000
    assert long_window_span == 3600 * 1_000_000_000


def test_query_range_flattens_streams_into_log_lines(monkeypatch):
    def fake_get(url, params=None, timeout=None):
        return FakeResponse(
            {
                "data": {
                    "result": [
                        {"values": [["1700000000000000000", "line one"], ["1700000001000000000", "line two"]]},
                        {"values": [["1700000002000000000", "line three"]]},
                    ]
                }
            }
        )

    monkeypatch.setattr(loki_client.httpx, "get", fake_get)

    client = loki_client.LokiClient("http://loki.monitoring.svc:3100")
    lines = client.query_range('{pod=~"x.*"}', window_seconds=300)

    assert lines == ["line one", "line two", "line three"]
