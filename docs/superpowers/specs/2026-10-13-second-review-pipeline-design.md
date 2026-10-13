# Second-Review Build: Rule-Based Chaos-to-Remediation Loop

## Status
Approved for implementation planning. LLM-based diagnosis is explicitly out of scope for this phase (deck timeline: weeks 14-17); this spec covers only the deterministic path.

## Goal
Demonstrate, live and reproducibly, the full loop — inject → detect → rule-based diagnose → approve → fix → verify — on 3 scenarios, with every run persisted to an incident database, in time for the second BTP review (2-3 weeks from 2026-10-13).

## Scope

### In scope
- Minikube single-node cluster
- Google Online Boutique demo app as the target system
- Chaos Mesh for fault injection (`PodChaos`, `StressChaos`)
- Prometheus + Loki for metrics/logs
- Postgres (in-cluster) as the incident database
- One Python orchestrator Deployment implementing all 5 pipeline stages as internal modules (Observer, Diagnoser, Approver, Fixer, Verifier), chained only through Postgres rows — not direct function calls between stage modules in different processes
- Rule-based diagnosis only (fixed decision table)
- 3 scenarios: **pod crash**, **CPU stress**, **memory pressure**
- RBAC-scoped ServiceAccount (namespace-scoped, verb-limited, no exec)
- An evaluation harness that triggers repeated chaos experiments and reads back results
- Manual baseline timing (done by hand/stopwatch during trial runs, not automated)

### Out of scope (deferred to post-review phases per deck timeline)
- LLM-based Diagnoser
- Network latency and service-unavailable scenarios (require topology-aware / two-sided correlation and traffic rerouting)
- The "reroute traffic" whitelisted action
- Splitting the 5 stages into separate deployments/CronJobs
- Multi-node scale testing

## Architecture

```
Chaos Mesh --injects fault--> Online Boutique (Minikube)
                                      |
                         Prometheus + Loki (observability)
                                      |
                              Orchestrator Pod
   ┌─────────────────────────────────────────────────────────┐
   │ Observer -> Diagnoser(rule) -> Approver -> Fixer -> Verifier │
   │   each stage reads prior row(s), writes its own row      │
   └─────────────────────────────────────────────────────────┘
                                      |
                              Postgres (incident DB)
                                      |
                        Evaluation harness (outside cluster)
```

Stages are logically separate Python modules inside one Deployment, not separate services. They communicate only by reading/writing Postgres rows for a given `incident_id` — no stage calls another stage's code directly. This keeps the implementation inside the 2-3 week budget while preserving the auditability/replayability property (every stage transition is a DB row) that the architecture requires, and keeps a clean seam for later splitting the Diagnoser out into its own service when the LLM path is added.

## Data model

Single table `incident_events`:

| column | type | notes |
|---|---|---|
| id | serial PK | |
| incident_id | uuid | shared across all rows for one fault injection |
| scenario | text | `pod_crash` \| `cpu_stress` \| `memory_pressure` |
| stage | text | `anomaly_detected` \| `diagnosis` \| `approval` \| `fix_executed` \| `verification` |
| ts | timestamptz | |
| payload | jsonb | stage-specific structured content, see below |
| confidence | float, nullable | set by diagnosis stage only |
| latency_ms | int, nullable | time spent in that stage |

Payload shapes per stage (informal, enforced in code not DB):
- `anomaly_detected`: `{metric, window_start, window_end, observed_value, threshold}`
- `diagnosis`: `{cause, confidence, evidence_cited, strategy: "rule_based"}`
- `approval`: `{action, approved: bool, reason}` (reason captures why escalated if not approved)
- `fix_executed`: `{action, target, result}`
- `verification`: `{metric, recovered: bool, checked_at}`

A fully resolved incident has exactly one row per stage, in order. MTTR = `verification.ts - anomaly_detected.ts` (or, if we want fault-to-recovery rather than detection-to-recovery, `verification.ts - injection timestamp recorded by the harness`). We use **injection-to-recovery** for MTTR since that matches "MTTR measured from fault injection to verified healthy state" on slide 7.

## Components

### Observer
Polls Prometheus (RED + resource metrics) and Loki (crash/OOMKilled logs) for the Online Boutique namespace on a fixed interval. Applies a **fixed threshold** per scenario (deliberately simple/deterministic, per the architecture's explicit stance that detection stays non-AI):
- Pod crash: restart count delta > 0 in window, or error-rate spike
- CPU stress: CPU usage / limit > threshold AND P99 latency rising
- Memory pressure: OOMKilled event present in Loki, or memory usage / limit > threshold

On breach, creates a new `incident_id`, writes `anomaly_detected`.

### Diagnoser (rule-based)
Pure function: evidence summary → `{cause, confidence, evidence_cited}`. Fixed decision table matching slide 11 exactly (one branch per scenario). No ML, no LLM. Confidence is a fixed constant per rule (since rules are deterministic, not probabilistic) unless we want to vary it to feed RQ4 later — for this phase a fixed high confidence is fine since RQ4 (confidence-threshold trade-off) is explicitly an LLM-path question.

### Approver
Checks: `confidence >= threshold` AND `action in {restart_pod, scale_replicas, rollback_deployment}`. If both pass, approve; else write `approval` with `approved: false` and a reason, and the incident stops there (counted as a manual escalation in the harness's results).

### Fixer
Executes via the Kubernetes Python client (no shell, no generated YAML):
- `restart_pod` → delete the pod (K8s reschedules it)
- `scale_replicas` → patch Deployment `spec.replicas`
- `rollback_deployment` (available but not needed for these 3 scenarios — kept implemented since slide 10 lists it as whitelisted)

### Verifier
Waits a fixed backoff, re-queries the same metric Observer used, marks `recovered: true/false`. This closes the loop and produces the MTTR number.

## Safety boundary
- ServiceAccount + Role scoped to the Online Boutique namespace only
- Verbs: `get/list/watch` on pods/services, `patch` on deployments, `delete` on pods — nothing else
- No `exec`, no dynamic YAML generation
- Malformed diagnosis output (fails schema validation) is treated as a failed diagnosis, not retried silently

## Evaluation harness
A script outside the cluster that, per scenario: triggers the matching Chaos Mesh experiment, waits for the orchestrator to produce a resolved (or escalated) incident, then repeats N times (target ≥10 per scenario, more if time allows). Reads Postgres at the end to compute: detection time, diagnosis accuracy (against the known injected fault — this is ground truth by construction), remediation success rate, MTTR, manual escalation count. Manual-baseline numbers are collected separately by a team member timing themselves resolving the same injected faults by hand, not by this harness.

## Testing strategy
- Unit tests on the Diagnoser decision table: synthetic evidence in, expected `{cause, confidence}` out — no cluster needed
- Unit tests on Approver policy logic
- One integration test per scenario: trigger the real chaos experiment once, assert the incident reaches `verification.recovered = true` within a timeout
- The harness itself, run for real, is the final acceptance check (populated incident DB, ≥3 scenarios, repeated runs)

## Risks
- Prometheus/Loki query correctness (PromQL/LogQL against the actual Online Boutique metric names) is the most likely source of debugging time, not the orchestration logic itself
- Minikube resource sizing with Online Boutique + Prometheus + Loki + Chaos Mesh + Postgres running concurrently on one node — may need to trim optional components (e.g., Grafana UI) if resources are tight
- Chaos Mesh experiment timing vs. Observer polling interval needs tuning so detection reliably fires before the harness times out
