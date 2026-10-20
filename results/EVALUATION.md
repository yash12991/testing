# Evaluation Results

Real data from the live cluster. Raw files are in [`harness/results/`](../harness/results/) — this page is the honest summary, with the same caveats the raw files themselves carry, pulled up front.

## The numbers (5 repeats per scenario, live cluster, 2026-10-13)

From [`harness/results/20261013T213848Z.md`](../harness/results/20261013T213848Z.md) — the corrected report (see "Why there are two timestamped files for this run" below).

| scenario | attempted | detected | diagnosed correctly | remediation succeeded | mean detection | mean MTTR (injection→recovery) |
|---|---|---|---|---|---|---|
| **pod_crash** | 5 | 5/5 | 5/5 | 5/5 | 34.3s | 79.6s |
| **cpu_stress** | 5 | 3/5 | 3/3 of detected | *see caveat below* | 278.3s | *see caveat below* |
| **memory_pressure** | 5 | 5/5 | *see caveat below* | 5/5 | 38.5s | 83.8s |

Zero escalations, zero timeouts across all 15 trials.

## Three things in this table that need honest context, not just the number

### Update (2026-10-20): the 0/3 gap below is now fixed, live-verified

After this page was first written, a "Rule Book" document (produced by the team) proposed exactly the right fix: distinguish a CPU **hotspot** (one specific pod saturated, the rest fine — restart it) from genuine **load saturation** (all replicas equally loaded — scaling actually helps). `cpu_stress`'s only remediation had always been `scale_replicas`, which is the *load-saturation* answer, applied unconditionally even though Chaos Mesh's `mode: one` injection is always a single-pod hotspot by construction — exactly why it measured 0/3 below.

Before changing any code, we verified live that restarting the specific stressed pod actually ends the injected CPU load (it doesn't just move elsewhere): deleted the hotspot pod while `cpu_ratio=0.87`, watched the replacement pod hold steady at `~0.10-0.12` for 30 seconds, and confirmed Chaos Mesh's `StressChaos` experiment stayed locked onto the now-deleted pod rather than re-selecting the new one. Only after that did we add a `cpu_hotspot` signal (worst-pod ratio vs. fleet-average ratio, or a single replica) and have the Diagnoser pick `restart_pod` for a hotspot instead of always `scale_replicas`.

**Real result from the live cluster, same day:** a genuine CPU-stress trial now produces `cpu_hotspot: true` → `action: restart_pod` → the stressed pod is deleted → **`recovered: true`**. This is the first time `cpu_stress` has resolved for a real reason rather than a measurement artifact. The 3/5 detection rate and the 0/3 remediation number below are the honest historical record of what shipped for the second review; they are superseded in behavior (not in the raw data, which is kept as-is) by this fix. A fresh batch run with `--runs-per-scenario 10` would give a current, single-source-of-truth table.

### 1. `cpu_stress`'s remediation_success/MTTR in this file predate a correctness fix — the real answer is 0/3, not 3/3 (now fixed, see "Update" above)

The committed file above shows `remediation_success: 3/3`. That number was measured with a bug: the `cpu_ratio` metric averaged across all matching pods, so once the fix (`scale_replicas`) added two idle replicas, the *cluster-wide average* dropped below threshold even though the single pod Chaos Mesh was actually stressing stayed fully saturated the whole time. The system was declaring victory because the average got diluted, not because anything recovered.

This was caught by the final whole-branch review and fixed (`cpu_ratio`'s query changed from `avg(...)` to `max(...)`, i.e. "the worst matching pod," not "the average"). **After the fix, a live confirming trial showed detection still firing correctly (`cpu_ratio=0.917`) but verification now honestly reporting `recovered=False`** — the stressed pod is still stressed, because `scale_replicas` never actually relieves it. That confirming trial is documented in [`BUILD_LOG.md`](BUILD_LOG.md); a fresh full 5-repeat batch run with the corrected query was not re-executed as part of this build (see "Suggested next step" below).

**The honest conclusion:** with the current action set, `cpu_stress`'s real remediation success rate is **0/N** — this is a legitimate, presentable finding about where the current rule-based remediation falls short, not a measurement artifact to hide.

### 2. `memory_pressure`'s `diagnosis_correct` in this file reads 0/5 — also expected, also not current

The data behind this number was diagnosed by a pre-fix version of the Diagnoser (it checked the `pod_crash` rule before the `memory_pressure` rule, so every real OOM — which, after an earlier fix, always also shows a container restart — was being labeled `transient_pod_failure` instead of `undersized_limit_or_leak`). That ordering bug was found and fixed. **A live confirming trial after the fix showed a real OOM-with-restart now correctly diagnosed as `undersized_limit_or_leak`** (see `BUILD_LOG.md`). The 0/5 here is a property of the old data being re-scored honestly against the corrected criterion, not a regression.

### 3. `pod_crash`'s remediation_success may partly reflect Kubernetes' own self-healing

`pod-crash.yaml` uses Chaos Mesh's `container-kill` action, which causes the kubelet to restart the killed container in place within seconds — independent of whether the orchestrator's own `restart_pod` fix (which deletes the pod so it gets rescheduled) ever ran. Verification happens within the ~60-second window after the fix, by which point Kubernetes may have already self-healed regardless. This is a known, honestly-documented limitation of the current chaos-injection timing, not something the numbers above correct for.

## Why there are two timestamped result files for the same run

`20261013T193732Z.md` is the original harness output. `20261013T213848Z.md` re-aggregates the *same raw incident data* (same 15 trials, same `incident_id`s) through a corrected MTTR calculation — the original file's "MTTR" column was actually `detection→recovery` (dominated by the Verifier's fixed ~30s backoff, reading ~45s for every scenario regardless of what actually happened), not the spec's `injection→recovery`. The corrected file adds the real metric: for `cpu_stress`, true MTTR is **323.6 seconds**, not 45.3 — a 7x difference, because true repair time includes the ~278s detection latency the old column didn't count at all. Both files are kept; the corrected one is authoritative for MTTR.

## What's genuinely solid in these numbers

- **pod_crash and memory_pressure: 5/5 detected, 5/5 remediated, zero escalations, zero timeouts.** Detection in the 34–39 second range, full injection-to-recovery in the 80–84 second range. This is real, repeatable, live-cluster behavior.
- **The pipeline never silently failed.** Every one of 15 trials reached a terminal state (resolved or honestly not_detected) — no incident got stuck, no escalation went unhandled, no process crashed mid-run.
- **cpu_stress's 3/5 → 5/5 detection gap is itself a real, reproducible characteristic**, not noise: detection for this scenario averages 278s against a 420s timeout, a thin margin driven by the underlying metric's rate-window dynamics on this hardware.

## Suggested next step before presenting

Two things would make this data airtight for a review panel, both cheap and already fully supported by the existing tooling — no code changes needed:

```bash
# A fresh 10-repeat run with all final-review fixes in place (current code,
# not the historical pre-fix data above):
kubectl port-forward -n online-boutique svc/postgres 5432:5432 &
export DATABASE_URL="postgresql://incident_app:<password>@localhost:5432/incidents"
cd harness && python3 harness/run_experiments.py --runs-per-scenario 10
```

This would give a current, complete, single-source-of-truth table (instead of this page's "here's the old number, here's why it's wrong, here's the one confirming trial for the real behavior" structure) and a tighter confidence interval than n=5, especially for `cpu_stress`.
