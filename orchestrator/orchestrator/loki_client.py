"""Real Loki HTTP API client satisfying observer.LokiClient.

Originally written inline in main.py as a Task 13 stopgap (see
prom_client.py's docstring for the same history). Extracted into its own
module here per the plan's original file list for this task, and hardened
per Task 13's own review feedback: `query_range` now takes an explicit
`window_seconds` and sends `start`/`end` query params computed from it,
rather than omitting them and relying on Loki's implicit "last one hour"
default — a silent correctness gap for any caller that actually wants a
window other than exactly the last hour (the Observer itself stopped
relying on Loki for oom_killed detection in Task 16 since OOMKilled is a
container termination reason, never a log line Loki could search for, but
this client is kept and hardened for future log-based signals).
"""

from __future__ import annotations

import time

import httpx


class LokiClient:
    """Minimal real Loki HTTP API client satisfying observer.LokiClient."""

    def __init__(self, base_url: str):
        self._base_url = base_url.rstrip("/")

    def query_range(self, logql: str, window_seconds: int) -> list[str]:
        """Return every log line matching `logql` within the trailing
        `window_seconds` ending now.

        `start`/`end` are sent explicitly (nanosecond-precision Unix epoch
        timestamps, the form Loki's HTTP API accepts) rather than left
        unset, so the query is always scoped to the caller's actual window
        instead of silently falling back to Loki's implicit last-hour
        default.
        """
        end_ns = int(time.time() * 1_000_000_000)
        start_ns = end_ns - int(window_seconds * 1_000_000_000)
        response = httpx.get(
            f"{self._base_url}/loki/api/v1/query_range",
            params={"query": logql, "start": start_ns, "end": end_ns},
            timeout=10.0,
        )
        response.raise_for_status()
        streams = response.json()["data"]["result"]
        return [value[1] for stream in streams for value in stream.get("values", [])]
