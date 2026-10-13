# Rule-Based Chaos-to-Remediation Loop Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and deploy, on Minikube, the full inject→detect→rule-based-diagnose→approve→fix→verify loop for 3 scenarios against Google Online Boutique, with every run recorded in Postgres and summarized by an evaluation harness.

**Architecture:** One Python orchestrator Deployment running five pipeline stages as internal modules (Observer, Diagnoser, Approver, Fixer, Verifier). Stages never call each other directly — each reads the incident's existing rows in Postgres and, if its own stage's row is missing, computes it and appends it. This makes the loop resumable/replayable and keeps a clean seam for later splitting stages into separate services.

**Tech Stack:** Python 3.12, `kubernetes` client, `psycopg` (Postgres), Prometheus HTTP API, Loki HTTP API (LogQL), Chaos Mesh CRDs, Helm, Minikube, pytest.

**Spec:** [docs/superpowers/specs/2026-10-13-second-review-pipeline-design.md](../specs/2026-10-13-second-review-pipeline-design.md)

## Global Constraints

- Namespace `online-boutique` hosts the demo app, the orchestrator Deployment, and Postgres. Observability (`monitoring` namespace: kube-prometheus-stack + Loki) and Chaos Mesh (`chaos-mesh` namespace) are separate namespaces reachable over the network; the orchestrator needs no RBAC on them.
- Orchestrator ServiceAccount `orchestrator-sa` lives in `online-boutique` and is bound to a Role scoped to that namespace only, verbs: `get`,`list`,`watch` on `pods`,`services`; `patch` on `deployments`; `delete` on `pods`. No `exec`, no wildcard verbs/resources.
- Whitelisted Fixer actions: `restart_pod`, `scale_replicas`, `rollback_deployment` (constant `WHITELISTED_ACTIONS` in `orchestrator/approver.py`).
- Scenario → target service: `pod_crash` → `cartservice`; `cpu_stress` → `recommendationservice`; `memory_pressure` → `productcatalogservice`. Pods are selected via label `app=<target>`.
- Per-rule fixed confidence constants in `orchestrator/diagnoser.py`: `transient_pod_failure=0.95`, `resource_exhaustion=0.9`, `undersized_limit_or_leak=0.85`, `unknown=0.0`.
- Approver `confidence_threshold` default = `0.8` (env-overridable). An `unknown` cause (confidence 0.0, action `None`) always escalates.
- Postgres table `incident_events(id serial PK, incident_id uuid, scenario text, stage text, ts timestamptz, payload jsonb, confidence float null, latency_ms int null)`. Valid `stage` values: `anomaly_detected`, `diagnosis`, `approval`, `fix_executed`, `verification`.
- MTTR = `verification.ts − anomaly_detected.ts` for a resolved incident (per spec: injection-to-recovery, approximated here by detection timestamp since the harness separately records true injection time — the harness computes the injection-to-recovery number; the DB captures detection-to-recovery as a secondary figure).
- Verifier backoff: fixed 30s wait before re-checking (env-overridable `VERIFY_BACKOFF_SECONDS`).

## Review Focus

- **Unrecognized evidence pattern:** evidence matching none of the three rules must diagnose as `unknown` (confidence 0.0, action `None`) rather than raising or defaulting to a guessed cause — pinned in Task 9's tests.
- **Confidence exactly at threshold:** `confidence == confidence_threshold` must be treated as approved (inclusive `>=`), not rejected — pinned in Task 10's tests.
- **Fixer API failure:** a Kubernetes API error (pod already deleted, patch conflict, deployment not found) during `execute()` must produce a `FixResult(success=False, detail=...)` and a written `fix_executed` event, not an unhandled exception that kills the orchestrator loop — pinned in Task 11's tests.
- **Verifier non-recovery:** if the metric is still breaching threshold after the backoff, Verifier must write `recovered: false` and stop (no infinite retry inside `verify()`) — pinned in Task 12's tests.
- **Duplicate anomalies on an in-flight incident:** while an incident for a given `(scenario, target)` has no `verification` row yet, Observer must not create a second incident for the same `(scenario, target)` on the next poll — pinned in Task 8's tests via `get_open_incident`.

---

## File Structure

```
btp_project/
  infra/
    minikube/bootstrap.sh
    helm-values/{prometheus-values.yaml, loki-values.yaml, chaos-mesh-values.yaml, postgres-values.yaml}
    manifests/
      namespaces.yaml
      rbac/{service-account.yaml, role.yaml, role-binding.yaml}
      orchestrator-deployment.yaml
      chaos-experiments/{pod-crash.yaml, cpu-stress.yaml, memory-pressure.yaml}
  db/schema.sql
  orchestrator/
    pyproject.toml
    orchestrator/
      __init__.py
      config.py
      models.py
      db.py
      observer.py
      diagnoser.py
      approver.py
      fixer.py
      verifier.py
      main.py
    Dockerfile
    tests/{test_db.py, test_observer.py, test_diagnoser.py, test_approver.py, test_fixer.py, test_verifier.py, test_main_integration.py}
  harness/
    pyproject.toml
    harness/{run_experiments.py, report.py}
    tests/test_report.py
```

---

### Task 1: Minikube cluster bootstrap

**Files:**
- Create: `infra/minikube/bootstrap.sh`
- Create: `infra/manifests/namespaces.yaml`

**Interfaces:**
- Produces: a running Minikube cluster with namespaces `online-boutique`, `monitoring`, `chaos-mesh` created, and `kubectl`/`helm` on PATH as prerequisites documented in the script's header comment.

- [ ] **Step 1: Write `infra/minikube/bootstrap.sh`**

Starts Minikube with enough resources for the whole stack (`minikube start --cpus=4 --memory=8192 --driver=docker`), then applies `infra/manifests/namespaces.yaml`. Script uses `set -euo pipefail`.

- [ ] **Step 2: Write `infra/manifests/namespaces.yaml`**

Three `Namespace` objects: `online-boutique`, `monitoring`, `chaos-mesh`.

- [ ] **Step 3: Run the bootstrap script**

Run: `bash infra/minikube/bootstrap.sh`
Expected: `minikube status` reports `Running`; `kubectl get ns` lists all three namespaces.

- [ ] **Step 4: Commit**

```bash
git add infra/minikube/bootstrap.sh infra/manifests/namespaces.yaml
git commit -m "infra: bootstrap minikube cluster and namespaces"
```

---

### Task 2: Deploy Google Online Boutique

**Files:**
- Create: `infra/manifests/online-boutique/` (downloaded/vendored upstream `release/kubernetes-manifests.yaml`, committed as-is)
- Create: `infra/minikube/deploy_online_boutique.sh`

**Interfaces:**
- Produces: all Online Boutique Deployments/Services running in namespace `online-boutique`, including `cartservice`, `recommendationservice`, `productcatalogservice` (names must match `app=<target>` labels used by Global Constraints).

- [ ] **Step 1: Vendor the upstream manifest**

Download the official Online Boutique `release/kubernetes-manifests.yaml` into `infra/manifests/online-boutique/kubernetes-manifests.yaml`.

- [ ] **Step 2: Write `infra/minikube/deploy_online_boutique.sh`**

`kubectl apply -n online-boutique -f infra/manifests/online-boutique/kubernetes-manifests.yaml`.

- [ ] **Step 3: Run it and verify**

Run: `bash infra/minikube/deploy_online_boutique.sh && kubectl get pods -n online-boutique`
Expected: all pods reach `Running`, including pods labeled `app=cartservice`, `app=recommendationservice`, `app=productcatalogservice`.

- [ ] **Step 4: Commit**

```bash
git add infra/manifests/online-boutique infra/minikube/deploy_online_boutique.sh
git commit -m "infra: deploy online boutique demo app"
```

---

### Task 3: Deploy observability stack (Prometheus + Loki)

**Files:**
- Create: `infra/helm-values/prometheus-values.yaml`
- Create: `infra/helm-values/loki-values.yaml`
- Create: `infra/minikube/deploy_observability.sh`

**Interfaces:**
- Produces: Prometheus reachable at `http://prometheus-kube-prometheus-prometheus.monitoring.svc.cluster.local:9090` (or the chart's actual service name — record the real name in this script's comments once installed) scraping all namespaces; Loki reachable at `http://loki.monitoring.svc.cluster.local:3100` collecting Online Boutique pod logs.

- [ ] **Step 1: Write `infra/helm-values/prometheus-values.yaml`**

Minimal overrides for `prometheus-community/kube-prometheus-stack`: disable Grafana and Alertmanager (not needed for this phase) to save resources on Minikube, keep `prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues: false` so it scrapes all namespaces.

- [ ] **Step 2: Write `infra/helm-values/loki-values.yaml`**

Minimal single-binary Loki values for `grafana/loki-stack`, with Promtail enabled to ship pod logs cluster-wide.

- [ ] **Step 3: Write `infra/minikube/deploy_observability.sh`**

Adds the `prometheus-community` and `grafana` Helm repos, `helm upgrade --install` both charts into namespace `monitoring` using the two values files above.

- [ ] **Step 4: Run it and verify**

Run: `bash infra/minikube/deploy_observability.sh && kubectl get pods -n monitoring`
Expected: Prometheus and Loki/Promtail pods `Running`. Then `kubectl port-forward -n monitoring svc/<prometheus-service> 9090:9090` and `curl localhost:9090/api/v1/query --data-urlencode 'query=up'` returns a JSON result with `status: success`.

- [ ] **Step 5: Commit**

```bash
git add infra/helm-values/prometheus-values.yaml infra/helm-values/loki-values.yaml infra/minikube/deploy_observability.sh
git commit -m "infra: deploy prometheus and loki observability stack"
```

---

### Task 4: Deploy Chaos Mesh

**Files:**
- Create: `infra/helm-values/chaos-mesh-values.yaml`
- Create: `infra/minikube/deploy_chaos_mesh.sh`

**Interfaces:**
- Produces: Chaos Mesh controller running in `chaos-mesh` namespace, able to apply `PodChaos`/`StressChaos` CRDs targeting pods in `online-boutique`.

- [ ] **Step 1: Write `infra/helm-values/chaos-mesh-values.yaml`**

Sets `chaosDaemon.runtime: docker` (Minikube's default container runtime) and restricts the webhook's namespace selector to allow `online-boutique`.

- [ ] **Step 2: Write `infra/minikube/deploy_chaos_mesh.sh`**

Adds the `chaos-mesh` Helm repo, `helm upgrade --install chaos-mesh chaos-mesh/chaos-mesh -n chaos-mesh -f infra/helm-values/chaos-mesh-values.yaml`.

- [ ] **Step 3: Run it and verify**

Run: `bash infra/minikube/deploy_chaos_mesh.sh && kubectl get pods -n chaos-mesh`
Expected: all Chaos Mesh pods `Running`; `kubectl get crds | grep chaos-mesh` lists `podchaos.chaos-mesh.org` and `stresschaos.chaos-mesh.org`.

- [ ] **Step 4: Commit**

```bash
git add infra/helm-values/chaos-mesh-values.yaml infra/minikube/deploy_chaos_mesh.sh
git commit -m "infra: deploy chaos mesh"
```

---

### Task 5: Deploy Postgres and incident schema

**Files:**
- Create: `infra/helm-values/postgres-values.yaml`
- Create: `infra/minikube/deploy_postgres.sh`
- Create: `db/schema.sql`

**Interfaces:**
- Produces: Postgres reachable at `postgres.online-boutique.svc.cluster.local:5432`, database `incidents`, with table `incident_events` matching the Global Constraints schema. Later tasks' `db.py` connects using a `DATABASE_URL` env var pointing here (or at a local test Postgres in unit tests).

- [ ] **Step 1: Write `infra/helm-values/postgres-values.yaml`**

Overrides for `bitnami/postgresql`: sets `auth.database: incidents`, a fixed `auth.username`/`auth.password` (values read from a `.env`-style file, not committed — document this in the script), single replica, small persistence size (1Gi) appropriate for ~150+ incident rows.

- [ ] **Step 2: Write `db/schema.sql`**

```sql
CREATE TABLE IF NOT EXISTS incident_events (
    id SERIAL PRIMARY KEY,
    incident_id UUID NOT NULL,
    scenario TEXT NOT NULL,
    stage TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload JSONB NOT NULL,
    confidence DOUBLE PRECISION,
    latency_ms INTEGER
);
CREATE INDEX IF NOT EXISTS idx_incident_events_incident_id ON incident_events (incident_id);
```

- [ ] **Step 3: Write `infra/minikube/deploy_postgres.sh`**

Adds the `bitnami` Helm repo, installs into `online-boutique` namespace with the values file, waits for the pod to be ready, then `kubectl exec`s into the pod to run `db/schema.sql` via `psql`.

- [ ] **Step 4: Run it and verify**

Run: `bash infra/minikube/deploy_postgres.sh`
Expected: `kubectl exec -n online-boutique <postgres-pod> -- psql -U <user> -d incidents -c '\dt'` lists `incident_events`.

- [ ] **Step 5: Commit**

```bash
git add infra/helm-values/postgres-values.yaml infra/minikube/deploy_postgres.sh db/schema.sql
git commit -m "infra: deploy postgres and create incident_events schema"
```

---

### Task 6: Orchestrator RBAC

**Files:**
- Create: `infra/manifests/rbac/service-account.yaml`
- Create: `infra/manifests/rbac/role.yaml`
- Create: `infra/manifests/rbac/role-binding.yaml`

**Interfaces:**
- Produces: ServiceAccount `orchestrator-sa` in `online-boutique`, bound to a Role granting exactly the verbs listed in Global Constraints.

- [ ] **Step 1: Write the three manifests**

`ServiceAccount orchestrator-sa`; `Role orchestrator-role` with rules `{apiGroups: [""], resources: ["pods","services"], verbs: ["get","list","watch"]}`, `{apiGroups: [""], resources: ["pods"], verbs: ["delete"]}`, `{apiGroups: ["apps"], resources: ["deployments"], verbs: ["get","list","watch","patch"]}`; `RoleBinding` binding the SA to the Role, both in namespace `online-boutique`.

- [ ] **Step 2: Apply and verify**

Run: `kubectl apply -f infra/manifests/rbac/ && kubectl auth can-i delete pods --as=system:serviceaccount:online-boutique:orchestrator-sa -n online-boutique`
Expected: `yes`. Also run `kubectl auth can-i delete pods --as=system:serviceaccount:online-boutique:orchestrator-sa -n kube-system` and expect `no`.

- [ ] **Step 3: Commit**

```bash
git add infra/manifests/rbac/
git commit -m "infra: scope orchestrator RBAC to online-boutique namespace"
```

---

### Task 7: Data models and Postgres repository layer

**Files:**
- Create: `orchestrator/pyproject.toml`
- Create: `orchestrator/orchestrator/models.py`
- Create: `orchestrator/orchestrator/db.py`
- Test: `orchestrator/tests/test_db.py`

**Interfaces:**
- Produces:
  - `models.py`: dataclasses `EvidenceSummary`, `Diagnosis(cause: str, confidence: float, evidence_cited: list[str], action: str | None)`, `ApprovalDecision(action: str | None, approved: bool, reason: str)`, `FixResult(action: str, target: str, success: bool, detail: str)`, `VerificationResult(recovered: bool, checked_at: datetime)`.
  - `db.py`: `create_incident(conn, scenario: str, target: str) -> uuid.UUID`; `append_event(conn, incident_id: uuid.UUID, stage: str, payload: dict, confidence: float | None = None, latency_ms: int | None = None) -> None`; `get_events(conn, incident_id: uuid.UUID) -> list[EventRow]` where `EventRow` is a dataclass `(id, incident_id, scenario, stage, ts, payload, confidence, latency_ms)`; `get_open_incident(conn, scenario: str, target: str) -> uuid.UUID | None` (returns an incident id that has an `anomaly_detected` row but no `verification` row yet, else `None`); `get_incomplete_incidents(conn) -> list[uuid.UUID]` (all distinct `incident_id`s with an `anomaly_detected` row but no `verification` row yet, across all scenarios — used by Task 13's main loop to find incidents needing their next stage).
- Consumes: a `psycopg.Connection` passed in by the caller (tests create their own against `TEST_DATABASE_URL`).

- [ ] **Step 1: Write `orchestrator/pyproject.toml`**

Dependencies: `psycopg[binary]`, `kubernetes`, `httpx`, pytest as dev dependency.

- [ ] **Step 2: Write `orchestrator/orchestrator/models.py`** per the dataclasses above.

- [ ] **Step 3: Write the failing tests in `orchestrator/tests/test_db.py`**

```python
def test_create_incident_and_append_events_round_trip(db_conn):
    incident_id = db.create_incident(db_conn, "pod_crash", "cartservice")
    db.append_event(db_conn, incident_id, "anomaly_detected", {"metric": "restart_count"}, latency_ms=12)
    events = db.get_events(db_conn, incident_id)
    assert [e.stage for e in events] == ["anomaly_detected"]
    assert events[0].payload["metric"] == "restart_count"

def test_get_open_incident_returns_none_when_resolved(db_conn):
    incident_id = db.create_incident(db_conn, "cpu_stress", "recommendationservice")
    db.append_event(db_conn, incident_id, "anomaly_detected", {})
    db.append_event(db_conn, incident_id, "verification", {"recovered": True})
    assert db.get_open_incident(db_conn, "cpu_stress", "recommendationservice") is None

def test_get_open_incident_returns_id_when_unresolved(db_conn):
    incident_id = db.create_incident(db_conn, "memory_pressure", "productcatalogservice")
    db.append_event(db_conn, incident_id, "anomaly_detected", {})
    assert db.get_open_incident(db_conn, "memory_pressure", "productcatalogservice") == incident_id

def test_get_incomplete_incidents_excludes_verified_ones(db_conn):
    open_id = db.create_incident(db_conn, "pod_crash", "cartservice")
    db.append_event(db_conn, open_id, "anomaly_detected", {})
    closed_id = db.create_incident(db_conn, "cpu_stress", "recommendationservice")
    db.append_event(db_conn, closed_id, "anomaly_detected", {})
    db.append_event(db_conn, closed_id, "verification", {"recovered": True})
    assert db.get_incomplete_incidents(db_conn) == [open_id]
```

A `db_conn` pytest fixture (in `orchestrator/tests/conftest.py`) connects to `os.environ["TEST_DATABASE_URL"]`, applies `db/schema.sql`, yields the connection, and truncates `incident_events` on teardown. Document in the test file's header that `TEST_DATABASE_URL` must point at a scratch Postgres (e.g. `docker run -e POSTGRES_PASSWORD=test -p 5433:5432 postgres:16`, then `postgresql://postgres:test@localhost:5433/postgres`).

- [ ] **Step 4: Run tests to verify they fail**

Run: `pytest orchestrator/tests/test_db.py -v`
Expected: FAIL with `ModuleNotFoundError` or `AttributeError` for `db`.

- [ ] **Step 5: Implement `db.py`** to satisfy the three functions above using `psycopg`.

- [ ] **Step 6: Run tests to verify they pass**

Run: `pytest orchestrator/tests/test_db.py -v`
Expected: PASS (4 passed)

- [ ] **Step 7: Commit**

```bash
git add orchestrator/pyproject.toml orchestrator/orchestrator/models.py orchestrator/orchestrator/db.py orchestrator/tests/test_db.py orchestrator/tests/conftest.py
git commit -m "feat(orchestrator): incident data models and postgres repository layer"
```

---

### Task 8: Observer — evidence gathering, threshold checks, dedup

**Files:**
- Create: `orchestrator/orchestrator/observer.py`
- Test: `orchestrator/tests/test_observer.py`

**Interfaces:**
- Consumes: `models.EvidenceSummary`; `db.get_open_incident`, `db.create_incident`, `db.append_event` (Task 7).
- Produces: `build_evidence(prom_client, loki_client, scenario: str, target: str, window_seconds: int) -> EvidenceSummary`; `check_threshold(scenario: str, evidence: EvidenceSummary) -> bool`; `poll_once(prom_client, loki_client, conn, scenarios: list[tuple[str, str]]) -> None` — for each `(scenario, target)`, skips if `db.get_open_incident` returns non-`None`; else builds evidence, and if `check_threshold` is true, calls `db.create_incident` + `db.append_event(..., "anomaly_detected", {...})`.
- `prom_client`/`loki_client` are thin wrapper objects with methods `query(promql: str) -> float` / `query_range(logql: str) -> list[str]` respectively — tests use fakes implementing the same interface, no real HTTP calls.

- [ ] **Step 1: Write failing tests in `test_observer.py`**

```python
def test_check_threshold_pod_crash_breaches_on_restart_delta():
    evidence = EvidenceSummary(restart_count_delta=2, error_rate=0.0, cpu_ratio=0.1,
                                p99_latency_ms=50, p99_latency_rising=False,
                                oom_killed=False, mem_ratio=0.1, target="cartservice")
    assert observer.check_threshold("pod_crash", evidence) is True

def test_check_threshold_cpu_stress_requires_both_signals():
    evidence = EvidenceSummary(restart_count_delta=0, error_rate=0.0, cpu_ratio=0.95,
                                p99_latency_ms=50, p99_latency_rising=False,
                                oom_killed=False, mem_ratio=0.1, target="recommendationservice")
    assert observer.check_threshold("cpu_stress", evidence) is False  # latency not rising yet

def test_poll_once_skips_scenario_with_open_incident(db_conn):
    db.create_incident(db_conn, "pod_crash", "cartservice")
    db.append_event(db_conn, _, "anomaly_detected", {})  # existing open incident
    fake_prom = FakePromClient(restart_count_delta=5)  # would otherwise breach
    observer.poll_once(fake_prom, FakeLokiClient(), db_conn, [("pod_crash", "cartservice")])
    rows = db.get_events(db_conn, <that incident id>)
    assert len(rows) == 1  # no second anomaly_detected row created

def test_poll_once_creates_incident_on_breach(db_conn):
    fake_prom = FakePromClient(restart_count_delta=3)
    observer.poll_once(fake_prom, FakeLokiClient(), db_conn, [("pod_crash", "cartservice")])
    incident_id = db.get_open_incident(db_conn, "pod_crash", "cartservice")
    assert incident_id is not None
```

(`FakePromClient`/`FakeLokiClient` are small test doubles defined in the test file returning fixed values per the metric name requested.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest orchestrator/tests/test_observer.py -v`
Expected: FAIL (`observer` module not found)

- [ ] **Step 3: Implement `observer.py`**

`check_threshold` branches: `pod_crash` → `restart_count_delta > 0 or error_rate > 0.1`; `cpu_stress` → `cpu_ratio > 0.85 and p99_latency_rising`; `memory_pressure` → `oom_killed or mem_ratio > 0.9`. `build_evidence` issues the PromQL/LogQL queries via the client objects and assembles an `EvidenceSummary`; exact PromQL strings are an implementation detail not pinned by tests here — write them against the real Prometheus in Task 16.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest orchestrator/tests/test_observer.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add orchestrator/orchestrator/observer.py orchestrator/tests/test_observer.py
git commit -m "feat(orchestrator): observer evidence gathering and dedup"
```

---

### Task 9: Diagnoser — rule-based decision table

**Files:**
- Create: `orchestrator/orchestrator/diagnoser.py`
- Test: `orchestrator/tests/test_diagnoser.py`

**Interfaces:**
- Consumes: `models.EvidenceSummary`, `models.Diagnosis`.
- Produces: `diagnose(evidence: EvidenceSummary) -> Diagnosis`.

- [ ] **Step 1: Write failing tests**

```python
def test_diagnose_pod_crash_evidence_yields_transient_pod_failure():
    evidence = EvidenceSummary(restart_count_delta=2, error_rate=0.2, cpu_ratio=0.1,
                                p99_latency_ms=50, p99_latency_rising=False,
                                oom_killed=False, mem_ratio=0.1, target="cartservice")
    d = diagnoser.diagnose(evidence)
    assert d.cause == "transient_pod_failure"
    assert d.confidence == 0.95
    assert d.action == "restart_pod"

def test_diagnose_cpu_stress_evidence_yields_resource_exhaustion():
    evidence = EvidenceSummary(restart_count_delta=0, error_rate=0.0, cpu_ratio=0.9,
                                p99_latency_ms=800, p99_latency_rising=True,
                                oom_killed=False, mem_ratio=0.1, target="recommendationservice")
    d = diagnoser.diagnose(evidence)
    assert d.cause == "resource_exhaustion"
    assert d.confidence == 0.9
    assert d.action == "scale_replicas"

def test_diagnose_memory_pressure_evidence_yields_undersized_limit_or_leak():
    evidence = EvidenceSummary(restart_count_delta=0, error_rate=0.0, cpu_ratio=0.1,
                                p99_latency_ms=50, p99_latency_rising=False,
                                oom_killed=True, mem_ratio=0.95, target="productcatalogservice")
    d = diagnoser.diagnose(evidence)
    assert d.cause == "undersized_limit_or_leak"
    assert d.confidence == 0.85
    assert d.action == "restart_pod"

def test_diagnose_unrecognized_evidence_yields_unknown():
    evidence = EvidenceSummary(restart_count_delta=0, error_rate=0.0, cpu_ratio=0.1,
                                p99_latency_ms=50, p99_latency_rising=False,
                                oom_killed=False, mem_ratio=0.1, target="loadgenerator")
    d = diagnoser.diagnose(evidence)
    assert d.cause == "unknown"
    assert d.confidence == 0.0
    assert d.action is None
    assert d.evidence_cited == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest orchestrator/tests/test_diagnoser.py -v`
Expected: FAIL

- [ ] **Step 3: Implement `diagnose()`** checking the three rules in order (pod crash, then CPU stress, then memory pressure — each test's evidence only satisfies one rule, so order does not matter for these cases but must be documented), falling through to `unknown` with the confidence constants from Global Constraints. `evidence_cited` lists the field names that triggered the match (e.g. `["restart_count_delta", "error_rate"]`), empty list for `unknown`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest orchestrator/tests/test_diagnoser.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add orchestrator/orchestrator/diagnoser.py orchestrator/tests/test_diagnoser.py
git commit -m "feat(orchestrator): rule-based diagnoser decision table"
```

---

### Task 10: Approver — policy gate

**Files:**
- Create: `orchestrator/orchestrator/approver.py`
- Test: `orchestrator/tests/test_approver.py`

**Interfaces:**
- Consumes: `models.Diagnosis`, `models.ApprovalDecision`.
- Produces: `WHITELISTED_ACTIONS: frozenset[str] = frozenset({"restart_pod", "scale_replicas", "rollback_deployment"})`; `approve(diagnosis: Diagnosis, confidence_threshold: float = 0.8) -> ApprovalDecision`.

- [ ] **Step 1: Write failing tests**

```python
def test_approve_accepts_known_cause_above_threshold():
    d = Diagnosis(cause="transient_pod_failure", confidence=0.95, evidence_cited=[], action="restart_pod")
    result = approver.approve(d, confidence_threshold=0.8)
    assert result == ApprovalDecision(action="restart_pod", approved=True, reason="")

def test_approve_accepts_confidence_exactly_at_threshold():
    d = Diagnosis(cause="undersized_limit_or_leak", confidence=0.8, evidence_cited=[], action="restart_pod")
    result = approver.approve(d, confidence_threshold=0.8)
    assert result.approved is True

def test_approve_escalates_unknown_cause():
    d = Diagnosis(cause="unknown", confidence=0.0, evidence_cited=[], action=None)
    result = approver.approve(d, confidence_threshold=0.8)
    assert result.approved is False
    assert result.action is None

def test_approve_escalates_below_threshold():
    d = Diagnosis(cause="undersized_limit_or_leak", confidence=0.5, evidence_cited=[], action="restart_pod")
    result = approver.approve(d, confidence_threshold=0.8)
    assert result.approved is False

def test_approve_escalates_non_whitelisted_action():
    d = Diagnosis(cause="service_down", confidence=0.99, evidence_cited=[], action="reroute_traffic")
    result = approver.approve(d, confidence_threshold=0.8)
    assert result.approved is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest orchestrator/tests/test_approver.py -v`
Expected: FAIL

- [ ] **Step 3: Implement `approve()`**: `approved = diagnosis.action is not None and diagnosis.confidence >= confidence_threshold and diagnosis.action in WHITELISTED_ACTIONS`; `reason` is `""` when approved, else a short string naming which check failed.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest orchestrator/tests/test_approver.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add orchestrator/orchestrator/approver.py orchestrator/tests/test_approver.py
git commit -m "feat(orchestrator): approver policy gate"
```

---

### Task 11: Fixer — constrained remediation executor

**Files:**
- Create: `orchestrator/orchestrator/fixer.py`
- Test: `orchestrator/tests/test_fixer.py`

**Interfaces:**
- Consumes: `models.FixResult`; a `kubernetes.client.CoreV1Api`/`AppsV1Api` pair, injected by the caller (tests pass `unittest.mock.Mock` instances).
- Produces: `restart_pod(core_v1, namespace: str, target: str) -> FixResult`; `scale_replicas(apps_v1, namespace: str, target: str, replicas: int) -> FixResult`; `rollback_deployment(apps_v1, namespace: str, target: str) -> FixResult`; `execute(action: str, core_v1, apps_v1, namespace: str, target: str) -> FixResult` (dispatches to the three above by `action` name).

- [ ] **Step 1: Write failing tests**

```python
def test_restart_pod_deletes_matching_pod():
    core_v1 = Mock()
    core_v1.list_namespaced_pod.return_value = Mock(items=[Mock(metadata=Mock(name="cartservice-abc123"))])
    result = fixer.restart_pod(core_v1, "online-boutique", "cartservice")
    core_v1.delete_namespaced_pod.assert_called_once_with(name="cartservice-abc123", namespace="online-boutique")
    assert result.success is True

def test_scale_replicas_patches_deployment():
    apps_v1 = Mock()
    result = fixer.scale_replicas(apps_v1, "online-boutique", "recommendationservice", 3)
    apps_v1.patch_namespaced_deployment_scale.assert_called_once()
    assert result.success is True

def test_execute_dispatches_by_action_name():
    core_v1, apps_v1 = Mock(), Mock()
    core_v1.list_namespaced_pod.return_value = Mock(items=[Mock(metadata=Mock(name="cartservice-abc123"))])
    result = fixer.execute("restart_pod", core_v1, apps_v1, "online-boutique", "cartservice")
    assert result.action == "restart_pod"

def test_execute_catches_k8s_api_exception_and_returns_failure():
    core_v1, apps_v1 = Mock(), Mock()
    core_v1.list_namespaced_pod.side_effect = ApiException(status=404, reason="Not Found")
    result = fixer.execute("restart_pod", core_v1, apps_v1, "online-boutique", "cartservice")
    assert result.success is False
    assert "Not Found" in result.detail
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest orchestrator/tests/test_fixer.py -v`
Expected: FAIL

- [ ] **Step 3: Implement the four functions.** Pods are found via `core_v1.list_namespaced_pod(namespace, label_selector=f"app={target}")` and the first match is deleted. `execute()` wraps its dispatch in `try/except kubernetes.client.exceptions.ApiException` and returns `FixResult(action, target, success=False, detail=str(exc))` on failure.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest orchestrator/tests/test_fixer.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add orchestrator/orchestrator/fixer.py orchestrator/tests/test_fixer.py
git commit -m "feat(orchestrator): fixer remediation executor with safe failure handling"
```

---

### Task 12: Verifier — recovery check

**Files:**
- Create: `orchestrator/orchestrator/verifier.py`
- Test: `orchestrator/tests/test_verifier.py`

**Interfaces:**
- Consumes: `models.VerificationResult`; a `build_evidence_fn: Callable[[], EvidenceSummary]` and `check_threshold_fn: Callable[[str, EvidenceSummary], bool]` (tests inject fakes; production wires in `observer.build_evidence`/`observer.check_threshold` partially applied).
- Produces: `verify(scenario: str, build_evidence_fn, check_threshold_fn, sleep_fn=time.sleep, backoff_seconds: int = 30) -> VerificationResult`.

- [ ] **Step 1: Write failing tests**

```python
def test_verify_recovered_when_no_longer_breaching():
    result = verifier.verify("pod_crash", lambda: SOME_HEALTHY_EVIDENCE,
                              lambda scenario, ev: False, sleep_fn=lambda s: None, backoff_seconds=0)
    assert result.recovered is True

def test_verify_not_recovered_when_still_breaching():
    result = verifier.verify("cpu_stress", lambda: SOME_BREACHING_EVIDENCE,
                              lambda scenario, ev: True, sleep_fn=lambda s: None, backoff_seconds=0)
    assert result.recovered is False

def test_verify_sleeps_for_backoff_before_checking():
    calls = []
    verifier.verify("memory_pressure", lambda: SOME_HEALTHY_EVIDENCE,
                     lambda scenario, ev: False, sleep_fn=calls.append, backoff_seconds=30)
    assert calls == [30]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest orchestrator/tests/test_verifier.py -v`
Expected: FAIL

- [ ] **Step 3: Implement `verify()`**: call `sleep_fn(backoff_seconds)` once, then `evidence = build_evidence_fn()`, `recovered = not check_threshold_fn(scenario, evidence)`, return `VerificationResult(recovered, checked_at=datetime.now(timezone.utc))`. No retry loop inside this function — one check only, per Review Focus.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest orchestrator/tests/test_verifier.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add orchestrator/orchestrator/verifier.py orchestrator/tests/test_verifier.py
git commit -m "feat(orchestrator): verifier recovery check"
```

---

### Task 13: Main loop — wiring the pipeline through Postgres state

**Files:**
- Create: `orchestrator/orchestrator/config.py`
- Create: `orchestrator/orchestrator/main.py`
- Test: `orchestrator/tests/test_main_integration.py`

**Interfaces:**
- Consumes: everything from Tasks 7-12 (`db`, `observer`, `diagnoser`, `approver`, `fixer`, `verifier`), including `db.get_incomplete_incidents(conn) -> list[uuid.UUID]` defined in Task 7.
- Produces: `run_once(conn, prom_client, loki_client, core_v1, apps_v1, scenarios: list[tuple[str, str]], confidence_threshold: float) -> None` — runs one full pass: `observer.poll_once` for new anomalies, then for every incident missing its next expected stage row (in order `anomaly_detected → diagnosis → approval → fix_executed → verification`), computes and appends exactly that next row. `main()` reads config from env (`config.py`: `DATABASE_URL`, `PROMETHEUS_URL`, `LOKI_URL`, `CONFIDENCE_THRESHOLD`, `POLL_INTERVAL_SECONDS`) and loops `run_once` forever with `time.sleep(POLL_INTERVAL_SECONDS)`.

- [ ] **Step 1: Write the failing integration test**

```python
def test_run_once_drives_one_incident_from_anomaly_to_verified_recovery(db_conn):
    fake_prom = FakePromClient(breach_then_healthy_sequence=True)  # breaches once, then reports healthy
    core_v1, apps_v1 = Mock(), Mock()
    core_v1.list_namespaced_pod.return_value = Mock(items=[Mock(metadata=Mock(name="cartservice-abc"))])

    main.run_once(db_conn, fake_prom, FakeLokiClient(), core_v1, apps_v1,
                   scenarios=[("pod_crash", "cartservice")], confidence_threshold=0.8)
    incident_id = db.get_open_incident(db_conn, "pod_crash", "cartservice")
    assert incident_id is not None  # anomaly created, not yet resolved

    main.run_once(db_conn, fake_prom, FakeLokiClient(), core_v1, apps_v1,
                   scenarios=[("pod_crash", "cartservice")], confidence_threshold=0.8)
    # second pass: diagnosis + approval + fix_executed should now exist
    stages = [e.stage for e in db.get_events(db_conn, incident_id)]
    assert "fix_executed" in stages

    main.run_once(db_conn, fake_prom, FakeLokiClient(), core_v1, apps_v1,
                   scenarios=[("pod_crash", "cartservice")], confidence_threshold=0.8)
    stages = [e.stage for e in db.get_events(db_conn, incident_id)]
    assert stages == ["anomaly_detected", "diagnosis", "approval", "fix_executed", "verification"]
    assert db.get_open_incident(db_conn, "pod_crash", "cartservice") is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest orchestrator/tests/test_main_integration.py -v`
Expected: FAIL

- [ ] **Step 3: Implement `config.py` and `main.py`.** `run_once` logic: call `observer.poll_once(...)` first; then for each incident id returned by `db.get_incomplete_incidents(conn)`, look at its existing stages (via `db.get_events`) and append the single next one, timing each stage's wall-clock duration into `latency_ms`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest orchestrator/tests/test_main_integration.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add orchestrator/orchestrator/config.py orchestrator/orchestrator/main.py orchestrator/tests/test_main_integration.py
git commit -m "feat(orchestrator): wire pipeline stages through postgres-driven main loop"
```

---

### Task 14: Containerize and deploy the orchestrator

**Files:**
- Create: `orchestrator/Dockerfile`
- Create: `infra/manifests/orchestrator-deployment.yaml`
- Create: `infra/minikube/deploy_orchestrator.sh`

**Interfaces:**
- Produces: the orchestrator image built and loaded into Minikube, running as a Deployment in `online-boutique` using `orchestrator-sa` (Task 6), with env vars from `config.py` set via the Deployment spec (pointing at the in-cluster Postgres/Prometheus/Loki service DNS names from Tasks 3 and 5).

- [ ] **Step 1: Write `orchestrator/Dockerfile`**

`python:3.12-slim` base, installs `orchestrator/pyproject.toml` dependencies, copies the package, `CMD ["python", "-m", "orchestrator.main"]`.

- [ ] **Step 2: Write `infra/manifests/orchestrator-deployment.yaml`**

One replica, `serviceAccountName: orchestrator-sa`, env vars `DATABASE_URL`, `PROMETHEUS_URL`, `LOKI_URL`, `CONFIDENCE_THRESHOLD=0.8`, `POLL_INTERVAL_SECONDS=15`.

- [ ] **Step 3: Write `infra/minikube/deploy_orchestrator.sh`**

`eval $(minikube docker-env) && docker build -t orchestrator:dev orchestrator/ && kubectl apply -f infra/manifests/orchestrator-deployment.yaml`.

- [ ] **Step 4: Run it and verify**

Run: `bash infra/minikube/deploy_orchestrator.sh && kubectl get pods -n online-boutique -l app=orchestrator`
Expected: pod `Running`; `kubectl logs -n online-boutique -l app=orchestrator` shows poll-loop log lines with no crash/traceback over at least 2 poll intervals.

- [ ] **Step 5: Commit**

```bash
git add orchestrator/Dockerfile infra/manifests/orchestrator-deployment.yaml infra/minikube/deploy_orchestrator.sh
git commit -m "infra: containerize and deploy orchestrator"
```

---

### Task 15: Chaos experiment manifests

**Files:**
- Create: `infra/manifests/chaos-experiments/pod-crash.yaml`
- Create: `infra/manifests/chaos-experiments/cpu-stress.yaml`
- Create: `infra/manifests/chaos-experiments/memory-pressure.yaml`

**Interfaces:**
- Produces: three Chaos Mesh CRD manifests, each selecting pods by `app=<target>` per Global Constraints, targeting the `online-boutique` namespace.

- [ ] **Step 1: Write `pod-crash.yaml`**: `PodChaos` kind `pod-kill`, selector `app: cartservice`, namespace `online-boutique`.

- [ ] **Step 2: Write `cpu-stress.yaml`**: `StressChaos` with `stressors.cpu.workers`/`load`, selector `app: recommendationservice`, `duration` long enough for Observer to detect it (at least 2x `POLL_INTERVAL_SECONDS`).

- [ ] **Step 3: Write `memory-pressure.yaml`**: `StressChaos` with `stressors.memory.workers`/`size` tuned to exceed `productcatalogservice`'s memory limit and trigger an OOMKill, selector `app: productcatalogservice`.

- [ ] **Step 4: Apply each manually and verify**

Run: `kubectl apply -f infra/manifests/chaos-experiments/pod-crash.yaml` (repeat per file)
Expected: `kubectl get podchaos,stresschaos -n online-boutique` shows each experiment `Running`/`Finished`, and the targeted pod shows the expected symptom (`kubectl get pods -n online-boutique` shows a restart for pod-crash; `kubectl top pod` shows elevated CPU/mem for the other two, resources permitting on Minikube).

- [ ] **Step 5: Commit**

```bash
git add infra/manifests/chaos-experiments/
git commit -m "infra: add chaos mesh experiments for the three second-review scenarios"
```

---

### Task 16: End-to-end validation per scenario

**Files:**
- Test: `orchestrator/tests/test_e2e_live_cluster.py`

**Interfaces:**
- Consumes: a live Minikube cluster from Tasks 1-15; real `psycopg` connection, real Prometheus/Loki HTTP clients (small wrapper classes implementing the `prom_client`/`loki_client` interface from Task 8, built here since this is the first task that needs them for real), real `kubernetes.client` API objects.
- Produces: `pytest.mark.integration`-marked tests, one per scenario, each: applies the matching chaos manifest via `kubectl apply` (via `subprocess.run`), polls the live `incident_events` table until a `verification` row appears for that `(scenario, target)` or a timeout elapses, then asserts `recovered is True` and deletes the chaos manifest (via `kubectl delete`) in a `finally` block.

- [ ] **Step 1: Write the real `PrometheusClient`/`LokiClient` wrapper classes** (`orchestrator/orchestrator/prom_client.py`, `orchestrator/orchestrator/loki_client.py`) implementing `query(promql) -> float` / `query_range(logql) -> list[str]` over `httpx`, matching the fake interface Tasks 8/12 already tested against.

- [ ] **Step 2: Write the three live-cluster tests** (pod_crash, cpu_stress, memory_pressure), each parameterized with its scenario/target/manifest path.

- [ ] **Step 3: Run against the live cluster**

Run: `pytest orchestrator/tests/test_e2e_live_cluster.py -v -m integration` (with the orchestrator Deployment from Task 14 running, and port-forwards or in-cluster DNS for Postgres/Prometheus reachable from the test runner)
Expected: 3 passed. If a test times out, tune `check_threshold`'s numeric thresholds (Task 8) or the chaos manifests' `duration`/`stressors` (Task 15) rather than the pipeline logic — this is the tuning step the spec flags as the main risk.

- [ ] **Step 4: Commit**

```bash
git add orchestrator/tests/test_e2e_live_cluster.py orchestrator/orchestrator/prom_client.py orchestrator/orchestrator/loki_client.py
git commit -m "test: end-to-end validation of all three scenarios on live cluster"
```

---

### Task 17: Evaluation harness and results report

**Files:**
- Create: `harness/pyproject.toml`
- Create: `harness/harness/run_experiments.py`
- Create: `harness/harness/report.py`
- Test: `harness/tests/test_report.py`

**Interfaces:**
- Consumes: `EventRow` shape from Task 7's `db.get_events` (a list of `(incident_id, scenario, stage, ts, payload, confidence, latency_ms)`).
- Produces: `report.aggregate_results(rows: list[EventRow]) -> dict[str, ScenarioStats]` where `ScenarioStats` is a dataclass `(scenario: str, runs: int, detection_times_s: list[float], diagnosis_correct: int, remediation_success: int, mttr_s: list[float], escalations: int)`, pure and independent of any cluster; `run_experiments.py` is a script (not unit tested) that for each scenario repeats N times: records the real injection timestamp, `kubectl apply`s the chaos manifest, polls Postgres for resolution, `kubectl delete`s the manifest, then at the end calls `report.aggregate_results` and writes a markdown table to `harness/results/<timestamp>.md`.

- [ ] **Step 1: Write failing tests for `aggregate_results`**

```python
def test_aggregate_results_computes_per_scenario_stats():
    rows = [
        EventRow(incident_id=ID1, scenario="pod_crash", stage="anomaly_detected", ts=T0, payload={}, confidence=None, latency_ms=None),
        EventRow(incident_id=ID1, scenario="pod_crash", stage="diagnosis", ts=T1, payload={"cause": "transient_pod_failure"}, confidence=0.95, latency_ms=50),
        EventRow(incident_id=ID1, scenario="pod_crash", stage="verification", ts=T2, payload={"recovered": True}, confidence=None, latency_ms=None),
    ]
    stats = report.aggregate_results(rows)
    assert stats["pod_crash"].runs == 1
    assert stats["pod_crash"].remediation_success == 1
    assert stats["pod_crash"].mttr_s == [(T2 - T0).total_seconds()]

def test_aggregate_results_counts_escalation_when_no_fix_executed():
    rows = [
        EventRow(incident_id=ID2, scenario="pod_crash", stage="anomaly_detected", ts=T0, payload={}, confidence=None, latency_ms=None),
        EventRow(incident_id=ID2, scenario="pod_crash", stage="diagnosis", ts=T1, payload={"cause": "unknown"}, confidence=0.0, latency_ms=50),
        EventRow(incident_id=ID2, scenario="pod_crash", stage="approval", ts=T1, payload={"approved": False}, confidence=None, latency_ms=None),
    ]
    stats = report.aggregate_results(rows)
    assert stats["pod_crash"].escalations == 1
    assert stats["pod_crash"].remediation_success == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest harness/tests/test_report.py -v`
Expected: FAIL

- [ ] **Step 3: Implement `aggregate_results`** grouping rows by `incident_id`, deriving one `ScenarioStats` entry per `scenario` by folding each incident's stage list into the counts/lists above.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest harness/tests/test_report.py -v`
Expected: PASS

- [ ] **Step 5: Implement `run_experiments.py`** using the real `db` module (Task 7) against the live cluster's Postgres (reached via `kubectl port-forward` or in-cluster DNS if run as a Job) and `subprocess.run(["kubectl", "apply"/"delete", "-f", ...])` for chaos manifests from Task 15.

- [ ] **Step 6: Run the full harness for real**

Run: `python -m harness.run_experiments --runs-per-scenario 10`
Expected: `harness/results/<timestamp>.md` produced, Postgres `incident_events` populated with ≥30 incidents across the 3 scenarios, report shows non-zero `remediation_success` for each scenario.

- [ ] **Step 7: Commit**

```bash
git add harness/
git commit -m "feat(harness): repeated-trial evaluation harness and results report"
```
