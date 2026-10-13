"""Real Prometheus HTTP API client satisfying observer.PromClient.

Originally written inline in main.py as a Task 13 stopgap so `main()` was
runnable end-to-end before the real Observer PromQL bugs (container
filtering, OOM detection, the restart-window fix) were found and fixed by
live testing in Tasks 15/16. Extracted into its own module here per the
plan's original file list for this task; behavior is unchanged from the
Task 13 version other than this docstring.
"""

from __future__ import annotations

import httpx


class PrometheusClient:
    """Minimal real Prometheus HTTP API client satisfying observer.PromClient."""

    def __init__(self, base_url: str):
        self._base_url = base_url.rstrip("/")

    def query(self, promql: str) -> float:
        response = httpx.get(f"{self._base_url}/api/v1/query", params={"query": promql}, timeout=10.0)
        response.raise_for_status()
        result = response.json()["data"]["result"]
        if not result:
            return 0.0
        return float(result[0]["value"][1])
