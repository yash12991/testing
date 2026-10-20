# Build Log — Second-Review Pipeline (17 Tasks)

This is the full, honest record of how the second-review build happened: what each task delivered, how it was verified, and — where it matters — what went wrong and how it was found and fixed. Nothing here is smoothed over; several real bugs were only caught because every task's live claims were independently re-verified against the actual running cluster, not just trusted from a report.

Every task went through: implementation → independent task-level code review → (if findings) a fix round → re-review, before being marked complete. The whole branch then went through one additional whole-branch review and one final fix wave before merge. Full technical detail (exact queries, exact file:line references) lives in the git history (`git log`) and in the code's own comments/docstrings at each decision point — this document is the narrative.

## Infrastructure (Tasks 1–6)

**Task 1 — Minikube cluster bootstrap.** Brought up Minikube on rootless Podman (this build machine has no Docker). Discovered live: minikube needs `--rootless` passed explicitly to `start`, not just `minikube config set rootless true`, or it tries `sudo podman` and fails non-interactively. Also discovered `minikube status` is unreliable under rootless Podman regardless — health checks use `kubectl get nodes` instead.

**Task 2 — Google Online Boutique deployed.** Vendored the upstream manifest, deployed all 12 services. Hit a real host resource limit here: `cartservice`'s .NET runtime crash-looped on `fs.inotify.max_user_instances` exhaustion (Fedora default of 128, shared across kubelet/containerd/podman/editor). Worked around live with a `kubectl set env` patch (not committed to the vendored manifest) while flagging the durable fix (raising the sysctl) for later.

**Task 3 — Prometheus + Loki observability stack.** Deployed kube-prometheus-stack + Loki. The same inotify limit then hit Prometheus and Promtail themselves — this was now blocking core infrastructure, not just one app pod, so the sysctl fix was applied for real (`sudo sysctl -w fs.inotify.max_user_instances=1024`, persisted). Once applied, Prometheus came up clean and its `up` query returned real scrape results.

**Task 4 — Chaos Mesh deployed.** Two of the brief's assumptions turned out wrong and were caught via live testing rather than assumed: the chaos-daemon's configured container runtime had to be `containerd` (not `docker`, since this cluster has no Docker), and the plan's suggested RBAC-style namespace restriction (`clusterScoped: false`) turned out to break ALL chaos reconciliation cluster-wide (an unrelated controller's own RBAC failure stalls the whole manager). Reverted to the chart default and enforced namespace scoping by convention instead (every chaos manifest explicitly sets `selector.namespaces`). Proven working with a real pod-kill test before moving on.

**Task 5 — Postgres deployed, `incident_events` schema created.** Credentials never committed — a gitignored `.env.postgres` file holds the real password, referenced via Kubernetes Secret at deploy time.

**Task 6 — Orchestrator RBAC.** ServiceAccount scoped to exactly the verbs each Fixer action needs, verified live with `kubectl auth can-i`, including explicit negative checks (no `exec`, no cluster-wide access, no access to `kube-system`).

## The pipeline itself (Tasks 7–14)

**Task 7 — Data models + Postgres repository layer.** One real plan defect found and fixed here: the original single-table design (`incident_events` only) couldn't actually support the planned function signatures — there was nowhere to persist an incident's `target`, and `append_event` had no way to learn an incident's `scenario` for a `NOT NULL` column. Resolved with a second header table (`incidents`), which is the correct minimal fix, not a deviation.

**Task 8 — Observer** (evidence gathering, threshold checks, incident dedup). Fully unit-tested against fakes.

**Task 9 — Diagnoser** (rule-based decision table). Pure function, deterministic — this is the one stage an LLM would eventually replace; right now it's a fixed if/elif table with fixed confidence constants.

**Task 10 — Approver** (policy gate: confidence threshold + action whitelist).

**Task 11 — Fixer** (executes `restart_pod` / `scale_replicas` / `rollback_deployment` via the Kubernetes API, converts any cluster-side API error into a recorded failure rather than crashing).

**Task 12 — Verifier** (one-shot recovery check after a fixed backoff — explicitly no internal retry loop, so a still-breaching metric is recorded as `recovered: false` and the loop moves on rather than hanging).

**Task 13 — Main loop**, wiring all five stages together through Postgres state only. The riskiest integration task: found and fixed a real production bug where an unhandled exception in any one incident would crash the entire poll cycle (and the process); found and fixed a verification-window bug where re-checking recovery against the same 300-second window used for detection would have falsely reported "not recovered" on a live cluster, because stale pre-fix signal was still inside the window.

**Task 14 — Orchestrator containerized and deployed to the live cluster.** The literal `eval $(minikube docker-env) && docker build` approach in the original plan doesn't work here (no Docker); `minikube image build`/`podman-env`/`ssh` all turned out broken too. Built via plain `podman build` + `podman save` + `podman cp` + `ctr images import` directly into the node's containerd store instead. **At the end of this task the orchestrator was genuinely running in the cluster, making real Prometheus/Loki queries every 15 seconds** — the first point at which the system was "alive," even before any scenario had been proven to fire.

## Proving the system actually works (Tasks 15–16)

This is where the build earned its result. Writing the chaos experiment manifests (Task 15) and then testing them against the live, already-deployed orchestrator (which Task 14 had just stood up) surfaced that **the Observer, as merged, could not detect any of the three scenarios end to end** — exactly the "Prometheus/Loki query correctness" risk the original design spec called out as the most likely source of trouble.

**Task 15 — Chaos experiment manifests.** All three manifests were proven to produce real, confirmed effects (pod genuinely killed, CPU genuinely saturated, memory genuinely OOM-killed). But testing them against the live Observer found:
1. `cpu_ratio`/`mem_ratio` PromQL had no container filter, diluting the real ratio with the near-zero "pause" container.
2. Online Boutique exports no `http_requests_total` metric at all — `error_rate`/`p99_latency_ms` were structurally always zero, not just buggy.
3. Loki only ships container stdout/stderr, never Kubernetes Events — searching it for "OOMKilled" could never match.
4. Chaos Mesh's `pod-kill` action (as originally specified) replaces the whole Pod object, so it never increments the restart counter `pod_crash` detection depends on.

Item 4 was fixed within this task (switched to `container-kill`, which restarts in place) — and with that one fix, **the full pipeline fired end to end for the first time**: a real fault injection led to a real detection, diagnosis, approval, fix, and verified recovery, recorded in Postgres. Items 1–3 required changes to already-merged Observer code and were carried forward.

**A decision point here, put to the project owner directly rather than decided unilaterally:** since Online Boutique has no HTTP-level metrics without a service mesh, `error_rate` and `p99_latency_rising` are not just buggy but *unmeasurable* as designed. The choice was between adding Istio (real RED metrics, but new infrastructure and real integration risk this close to the deadline) or simplifying the detection thresholds to rely only on what's actually measurable (resource and object-level signals). **The project owner chose to simplify** — `pod_crash` now triggers on `restart_count_delta > 0` alone, `cpu_stress` on `cpu_ratio > 0.85` alone. This is reflected throughout the code and is why the live detection logic looks slightly different from the original design slide.

**Task 16 — The largest task in the plan.** Fixed items 1–3 above (container-filtered PromQL, the threshold simplification, OOM detection switched from Loki to Prometheus's `kube_pod_container_status_last_terminated_reason`), extracted the real Prometheus/Loki HTTP clients into their own modules, and — critically — **live-validated all three scenarios end to end against the real cluster**, not just unit tests. This surfaced a seventh cross-cutting bug: the orchestrator's RBAC role granted `patch` on `deployments` but not the `deployments/scale` *subresource* that `scale_replicas` actually needs, which a real `cpu_stress` incident hit live (a 403, correctly recorded as a failed fix). Fixed and re-verified.

A fix-round review then caught two more real latent bugs before they could bite: the new OOM-via-Prometheus signal had no time window (a pod that OOMed hours ago would trigger forever), and the restart-count/OOM queries read an arbitrary pod's value when more than one pod matched (a real situation, since a successful `cpu_stress` fix leaves 3 replicas). Both fixed and live-verified, including a real test against live Prometheus data proving a stale OOM-with-no-recent-restart correctly no longer fires.

**By the end of Task 16, all three scenarios were proven working end-to-end against the live cluster:** 6 pod_crash, 3 cpu_stress, and 1 memory_pressure incident, every one reaching `anomaly_detected → diagnosis → approval → fix_executed → verification`.

## Evaluation (Task 17)

Built the repeated-trial harness: a pure, TDD'd result-aggregation function plus a live-cluster trial runner that injects a fault, polls Postgres for resolution, cleans up, and repeats. Two real workarounds were needed and are documented in the code: an escalated incident permanently blocks future detection for its (scenario, target) pair (a known limitation from Task 13), so the harness manually closes stuck incidents to let detection resume; and `scale_replicas` permanently leaves `recommendationservice` at 3 replicas after a successful fix, which the harness resets before every `cpu_stress` trial.

Ran a real 5-repeat trial across all 3 scenarios against the live cluster — see [`EVALUATION.md`](EVALUATION.md) for the actual numbers and what they honestly mean.

## The final whole-branch review

Every task above was reviewed individually, but a dedicated whole-branch review (reading the entire ~230KB, 24-commit diff against the spec) caught something no single-task review could: **the evaluation numbers themselves were methodologically inflated.**

1. **MTTR was mislabeled.** The reported figure was detection-to-recovery (dominated by a fixed 30-second verifier backoff, reading ~45s for every scenario), not the spec-required injection-to-recovery (which for `cpu_stress` was actually ~323 seconds — a 7x understatement).
2. **`memory_pressure`'s "5/5 diagnosis accuracy" was correct only by an accommodation that hid a dead code path.** Task 16's OOM-gate fix meant every real OOM incident now also has a restart; the Diagnoser checked the `pod_crash` rule first, so every real OOM was being labeled `transient_pod_failure` — and the harness was separately configured to accept that label as "correct" for `memory_pressure`, masking that the actual OOM-diagnosis rule could never fire in production.
3. **`cpu_stress`'s "100% remediation success" was a metric-averaging artifact**, not genuine recovery: `scale_replicas` adds idle replicas that dilute the *cluster-wide average* CPU ratio below threshold, while the one actually-stressed pod (Chaos Mesh only stresses one) stays fully saturated the whole time.
4. The orchestrator could hang silently forever after a Postgres disconnect (no reconnect logic, no liveness probe).
5. A deploy script's own comment made a false claim about Chaos Mesh's namespace restriction being enforced when it was only a convention.

All five were fixed in one consolidated fix wave, each verified: the diagnoser rule order was corrected and live-re-verified (a real OOM-with-restart now correctly diagnoses as `undersized_limit_or_leak`); `cpu_ratio`'s query was changed from `avg` to `max` and live-re-verified — **which correctly flipped `cpu_stress`'s reported remediation success to 0/3**, proving the fix rather than causing a regression, and surfacing a genuinely useful, presentable finding: the current `scale_replicas` action does not actually relieve a single artificially-stressed pod. MTTR was recomputed from the same real Postgres data (no new trials needed) to show the true, honest number. Two more small gaps were caught by the re-review of that fix wave (an unrecovered incident would have still gotten a fake "repair time" in a future run, and the historical results file needed an explicit caveat) and fixed in one final, narrowly-scoped pass.

**84 tests pass** (72 orchestrator + 12 harness). The branch was merged to `main` and pushed only after every one of these was independently verified — not merely reported — against the actual running system.

## Post-merge: fixing the known cpu_stress gap (2026-10-20)

A rule-book document written by the team proposed a fix for the `cpu_stress` weakness documented above: distinguish a CPU **hotspot** (one specific pod saturated, others fine) from genuine **load saturation** (all replicas equally loaded), and restart the hotspot pod instead of always scaling out.

Before writing any code, this was verified live rather than assumed: injected real CPU stress on `recommendationservice`, confirmed `cpu_ratio=0.87` on the targeted pod, deleted that specific pod, and watched its replacement hold steady at `~0.10-0.12` CPU for 30 seconds straight — confirming Chaos Mesh's `StressChaos` stayed locked onto the deleted pod rather than re-attacking the new one, so restarting the hotspot pod genuinely ends the injected load rather than just displacing it.

With that confirmed, a `cpu_hotspot` signal was added to the Observer (worst-pod CPU ratio vs. the fleet average, or a single replica with no other pod to compare against), and the Diagnoser now picks `restart_pod` for a hotspot instead of unconditionally `scale_replicas`. 5 new tests added (77 passing total), deployed to the live cluster, and confirmed with a real trial: `cpu_hotspot: true` → `restart_pod` → the stressed pod deleted → **`recovered: true`** — the first time `cpu_stress` has resolved for a real reason rather than the earlier averaging artifact.
