"""Environment-driven configuration for the orchestrator's main loop.

`main.py`'s `main()` is the only consumer of this module — every pipeline
stage module (`observer`, `diagnoser`, `approver`, `fixer`, `verifier`) and
`main.run_once` itself take their inputs as explicit function parameters, not
via this module, so they stay testable without any env vars set.

Connection strings (`DATABASE_URL`, `PROMETHEUS_URL`, `LOKI_URL`) have no
sensible default — they are deployment-specific and a wrong default would
fail silently against the wrong service, so a missing one raises immediately
(via `os.environ[...]`) rather than falling back to something plausible but
wrong. `CONFIDENCE_THRESHOLD` and `POLL_INTERVAL_SECONDS` do have defaults,
fixed by the plan's Global Constraints (0.8 and 15s respectively).
"""

from __future__ import annotations

import os
from dataclasses import dataclass

DEFAULT_CONFIDENCE_THRESHOLD = 0.8
DEFAULT_POLL_INTERVAL_SECONDS = 15
DEFAULT_MIN_REPLICAS = 1
DEFAULT_MAX_REPLICAS = 5
DEFAULT_SCALE_REPLICAS = 3
DEFAULT_VERIFY_SAMPLE_COUNT = 1
DEFAULT_VERIFY_SAMPLE_INTERVAL_SECONDS = 5.0
DEFAULT_MAX_RETRIES = 2
DEFAULT_INCIDENT_TIMEOUT_SECONDS = 600


@dataclass(frozen=True)
class Config:
    database_url: str
    prometheus_url: str
    loki_url: str
    confidence_threshold: float
    poll_interval_seconds: int
    min_replicas: int = DEFAULT_MIN_REPLICAS
    max_replicas: int = DEFAULT_MAX_REPLICAS
    verify_sample_count: int = DEFAULT_VERIFY_SAMPLE_COUNT
    verify_sample_interval_seconds: float = DEFAULT_VERIFY_SAMPLE_INTERVAL_SECONDS
    max_retries: int = DEFAULT_MAX_RETRIES
    incident_timeout_seconds: int = DEFAULT_INCIDENT_TIMEOUT_SECONDS


def load_config() -> Config:
    """Read the main loop's configuration from environment variables."""
    return Config(
        database_url=os.environ["DATABASE_URL"],
        prometheus_url=os.environ["PROMETHEUS_URL"],
        loki_url=os.environ["LOKI_URL"],
        confidence_threshold=float(
            os.environ.get("CONFIDENCE_THRESHOLD", str(DEFAULT_CONFIDENCE_THRESHOLD))
        ),
        poll_interval_seconds=int(
            os.environ.get("POLL_INTERVAL_SECONDS", str(DEFAULT_POLL_INTERVAL_SECONDS))
        ),
        min_replicas=int(os.environ.get("MIN_REPLICAS", str(DEFAULT_MIN_REPLICAS))),
        max_replicas=int(os.environ.get("MAX_REPLICAS", str(DEFAULT_MAX_REPLICAS))),
        verify_sample_count=int(
            os.environ.get("VERIFY_SAMPLE_COUNT", str(DEFAULT_VERIFY_SAMPLE_COUNT))
        ),
        verify_sample_interval_seconds=float(
            os.environ.get(
                "VERIFY_SAMPLE_INTERVAL_SECONDS", str(DEFAULT_VERIFY_SAMPLE_INTERVAL_SECONDS)
            )
        ),
        max_retries=int(os.environ.get("MAX_RETRIES", str(DEFAULT_MAX_RETRIES))),
        incident_timeout_seconds=int(
            os.environ.get("INCIDENT_TIMEOUT_SECONDS", str(DEFAULT_INCIDENT_TIMEOUT_SECONDS))
        ),
    )
