# Autonomous Cloud-Native Chaos Engineering & Auto-Remediation Agent

BTP project (IIIT Kottayam) building a bounded, reproducible Kubernetes chaos-engineering and auto-remediation pipeline. A chaos engine injects a labelled fault into a demo microservice application; a deterministic Observer detects it from Prometheus/Loki signals; a rule-based Diagnoser identifies the probable cause; a deterministic Approver gates the proposed fix against a confidence threshold and a whitelist of actions; a deterministic Fixer executes the fix against the Kubernetes API; a deterministic Verifier checks recovery. Every stage communicates only by reading and writing rows in Postgres — never by calling another stage's code directly — so every incident is independently replayable and auditable.

This README covers the **second-review build**: the rule-based diagnosis path, fully working end-to-end against a live cluster for three scenarios (pod crash, CPU stress, memory pressure). LLM-based diagnosis, the network-latency and service-unavailable scenarios, and the full RQ1–RQ4 comparative evaluation are later-phase work per the project's own timeline (see `docs/superpowers/specs/`).

For the full build history — every task, what was found, what was fixed, and why — see [`results/BUILD_LOG.md`](results/BUILD_LOG.md). For the live evaluation numbers, see [`results/EVALUATION.md`](results/EVALUATION.md).

## Architecture

```
Chaos Mesh --injects fault--> Online Boutique (Minikube, namespace: online-boutique)
                                        |
                           Prometheus + Loki (namespace: monitoring)
                                        |
                                Orchestrator Pod
   +-----------------------------------------------------------------+
   | Observer -> Diagnoser(rule-based) -> Approver -> Fixer -> Verifier|
   |   each stage reads prior row(s) from Postgres, writes its own    |
   +-----------------------------------------------------------------+
                                        |
                          Postgres (incident_events, incidents)
                                        |
                       harness/ (repeated-trial evaluation runner)
```

- **Observer** (`orchestrator/orchestrator/observer.py`): polls Prometheus/Loki on a fixed interval, applies fixed thresholds per scenario, and on breach creates a new incident + `anomaly_detected` row.
- **Diagnoser** (`orchestrator/orchestrator/diagnoser.py`): pure rule-based decision table — the *only* place an LLM would plug in later; currently fully deterministic.
- **Approver** (`orchestrator/orchestrator/approver.py`): checks confidence ≥ threshold and that the proposed action is one of three whitelisted actions.
- **Fixer** (`orchestrator/orchestrator/fixer.py`): executes exactly one of `restart_pod`, `scale_replicas`, `rollback_deployment` via the Kubernetes API — no shell, no `exec`, no dynamically generated YAML.
- **Verifier** (`orchestrator/orchestrator/verifier.py`): re-checks the triggering metric after a backoff and records whether the system actually recovered.
- **main.py**: the poll loop. Each `run_once()` call advances every incomplete incident by exactly the stages it's ready for, driven purely by what's already in Postgres (so a crashed/restarted orchestrator resumes correctly with no lost state).

Safety boundary: the orchestrator's ServiceAccount (`orchestrator-sa`) is RBAC-scoped to the `online-boutique` namespace only, with exactly the verbs each Fixer action needs — no cluster-wide grants, no `exec`.

## Prerequisites

- A Linux host with either **Docker** or **rootless Podman** (this project was built and tested entirely on rootless Podman — no Docker — see the "Podman-specific notes" section below if that's your situation too).
- `python3` (3.10+) and `pip`.
- Internet access to pull Helm charts and container images on first run.
- **No `kubectl`/`minikube`/`helm` installation is assumed** — install instructions are below. If you already have them, skip to [Bring up the cluster](#1-bring-up-the-cluster).

### Installing kubectl, minikube, helm (no sudo required)

```bash
mkdir -p ~/.local/bin

# kubectl
KVER=$(curl -Ls https://dl.k8s.io/release/stable.txt)
curl -Lo ~/.local/bin/kubectl "https://dl.k8s.io/release/${KVER}/bin/linux/amd64/kubectl"
chmod +x ~/.local/bin/kubectl

# minikube
curl -Lo ~/.local/bin/minikube https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
chmod +x ~/.local/bin/minikube

# helm
curl -Lo /tmp/helm.tar.gz "https://get.helm.sh/helm-v3.16.4-linux-amd64.tar.gz"
tar -xzf /tmp/helm.tar.gz -C /tmp
mv /tmp/linux-amd64/helm ~/.local/bin/helm

# make sure ~/.local/bin is on PATH
export PATH="$HOME/.local/bin:$PATH"
```

### Podman-specific notes (no Docker daemon)

If your host has no Docker but has **rootless Podman** configured (check with `podman info` — look for `rootless: true`), everything in this project works, but with two things minikube itself gets wrong that you need to know about:

1. **`minikube start` needs `--rootless` passed explicitly** — `minikube config set rootless true` alone is not enough; minikube will still try `sudo podman` and fail non-interactively. `infra/minikube/bootstrap.sh` already does this correctly for you (it also falls back to the Docker driver automatically if a Docker daemon *is* present).
2. **`minikube status`, `minikube image build`, `minikube podman-env`, and `minikube ssh` are all unreliable or outright broken** under rootless Podman on at least this host — they shell out to `sudo podman ...` and fail even when the cluster is perfectly healthy and reachable as the invoking user. The scripts in this repo work around this entirely: health checks use `kubectl get nodes` instead of `minikube status`, and the orchestrator's image is built/loaded via plain `podman build` + `podman save` + `podman cp` + `ctr images import` directly into the node's containerd store (see the top of `infra/minikube/deploy_orchestrator.sh` for the full explanation).

If your Fedora/RHEL-family host has a `fs.inotify.max_user_instances` sysctl default of 128, you may see pods crash-loop under moderate load (kubelet + containerd + podman + your editor all compete for the same budget). Raise it before you start:

```bash
sudo sysctl -w fs.inotify.max_user_instances=1024
echo 'fs.inotify.max_user_instances=1024' | sudo tee /etc/sysctl.d/99-minikube-inotify.conf
```

## 1. Bring up the cluster

Run each of these in order. They're all idempotent — safe to re-run.

```bash
# 1a. Minikube cluster + namespaces (online-boutique, monitoring, chaos-mesh)
bash infra/minikube/bootstrap.sh

# 1b. Google Online Boutique demo app (12 microservices)
bash infra/minikube/deploy_online_boutique.sh

# 1c. Prometheus + Loki observability stack
bash infra/minikube/deploy_observability.sh

# 1d. Chaos Mesh (fault injection)
bash infra/minikube/deploy_chaos_mesh.sh

# 1e. Postgres (incident database)
#     First: cp infra/minikube/.env.postgres.example infra/minikube/.env.postgres
#     and fill in a real generated password (see the comment in that file).
cp infra/minikube/.env.postgres.example infra/minikube/.env.postgres
# edit infra/minikube/.env.postgres — set POSTGRES_PASSWORD to a real value, e.g.:
#   openssl rand -base64 24 | tr -d '/+=' | head -c 32
bash infra/minikube/deploy_postgres.sh

# 1f. RBAC for the orchestrator's ServiceAccount
kubectl apply -f infra/manifests/rbac/
```

Verify everything is healthy:

```bash
kubectl get nodes                      # should show Ready
kubectl get pods -n online-boutique    # 12+ services, all Running
kubectl get pods -n monitoring         # prometheus + loki + promtail, all Running
kubectl get pods -n chaos-mesh         # controller-manager + daemon, all Running
```

## 2. Build and deploy the orchestrator

```bash
bash infra/minikube/deploy_orchestrator.sh
```

This builds the orchestrator's container image, loads it into the cluster, creates a Kubernetes Secret for the Postgres credentials (read from your `.env.postgres`, never committed or passed via process arguments), and deploys it as a Deployment under the `orchestrator-sa` ServiceAccount. It forces a rollout restart every time it's run, so re-running it after a code change always picks up the new image.

Watch it running:

```bash
kubectl get pods -n online-boutique -l app=orchestrator
kubectl logs -n online-boutique -l app=orchestrator -f
```

You should see a `poll cycle complete, sleeping 15s` log line every 15 seconds, with real Prometheus/Loki query results above it.

## 3. Trigger a chaos experiment manually

Three ready-made Chaos Mesh experiments live in `infra/manifests/chaos-experiments/`:

| File | Scenario | Target | Chaos Mesh action |
|---|---|---|---|
| `pod-crash.yaml` | pod_crash | `cartservice` | `container-kill` (in-place SIGKILL, increments the pod's restart count) |
| `cpu-stress.yaml` | cpu_stress | `recommendationservice` | `StressChaos`, CPU load |
| `memory-pressure.yaml` | memory_pressure | `productcatalogservice` | `StressChaos`, memory load (triggers a real OOM kill) |

```bash
kubectl apply -f infra/manifests/chaos-experiments/pod-crash.yaml
# wait ~30-90s, watch the orchestrator logs or query Postgres (below)
kubectl delete -f infra/manifests/chaos-experiments/pod-crash.yaml
```

To see what happened, query Postgres directly:

```bash
kubectl exec -n online-boutique postgres-0 -- env PGPASSWORD="$(grep POSTGRES_PASSWORD infra/minikube/.env.postgres | cut -d= -f2)" \
  psql -U incident_app -d incidents -c \
  "SELECT scenario, stage, ts, payload FROM incident_events ORDER BY ts DESC LIMIT 10;"
```

## 4. Run the evaluation harness

The harness (`harness/`) automates the above: trigger a chaos experiment, poll Postgres until the incident resolves (or times out / escalates), clean up, repeat N times per scenario, and write a markdown results report.

```bash
# Port-forward Postgres so the harness (running outside the cluster) can reach it
kubectl port-forward -n online-boutique svc/postgres 5432:5432 &

export DATABASE_URL="postgresql://incident_app:$(grep POSTGRES_PASSWORD infra/minikube/.env.postgres | cut -d= -f2)@localhost:5432/incidents"

cd harness
python3 -m pip install -e .
python3 harness/run_experiments.py --runs-per-scenario 10
```

Useful flags: `--scenarios pod_crash cpu_stress` (subset), `--out-dir <path>` (default `harness/results/`). Results land in `harness/results/<timestamp>.md` — see [`results/EVALUATION.md`](results/EVALUATION.md) for how to read one, and what the honest caveats are (especially for `cpu_stress`).

## 5. Running the tests

```bash
# Orchestrator unit/integration tests need a scratch Postgres (not the cluster's):
podman run -d --name pg-test -e POSTGRES_PASSWORD=test -p 5433:5432 postgres:16
cd orchestrator
python3 -m venv .venv && .venv/bin/pip install -e .
export TEST_DATABASE_URL="postgresql://postgres:test@localhost:5433/postgres"
.venv/bin/python -m pytest -v        # 72 tests, no cluster needed

# Harness tests (pure functions only, no DB/cluster needed)
cd ../harness
python3 -m pytest -v                 # 12 tests
```

## Project structure

```
db/schema.sql                        Postgres schema (incidents, incident_events)
infra/
  minikube/*.sh                      One idempotent script per infra component (run in order, see above)
  helm-values/*.yaml                 Helm values for Prometheus/Loki/Chaos Mesh/Postgres
  manifests/
    namespaces.yaml                  online-boutique / monitoring / chaos-mesh namespaces
    online-boutique/                 Vendored upstream Online Boutique manifest
    rbac/                            ServiceAccount + Role + RoleBinding for the orchestrator
    orchestrator-deployment.yaml     The orchestrator's own Deployment + Secret wiring
    chaos-experiments/               The three second-review chaos experiments
orchestrator/
  orchestrator/                      The pipeline itself (models, db, observer, diagnoser,
                                      approver, fixer, verifier, main, prom_client, loki_client)
  tests/                             pytest suite (72 tests)
  Dockerfile
harness/
  harness/
    run_experiments.py               Live-cluster trial runner
    report.py                        Pure result-aggregation (TDD'd, no cluster dependency)
  tests/                             pytest suite (12 tests)
  results/                           Real evaluation run outputs (timestamped markdown)
docs/superpowers/
  specs/                             Design spec this build implements
  plans/                             Implementation plan (17 tasks)
results/                             Full build log + evaluation summary (this review's deliverable)
```

## Known limitations (stated honestly — see `results/BUILD_LOG.md` and `results/EVALUATION.md` for the full story)

- **`cpu_stress`'s only remediation (`scale_replicas`) does not actually relieve a single stressed pod.** This was discovered and is measured honestly in the evaluation data — see `results/EVALUATION.md`. It's a real finding about the limits of the current action set, not a bug in the measurement.
- **`pod_crash`'s measured recovery may partly reflect Kubernetes' own self-healing** (the `container-kill` chaos action causes an in-place kubelet restart within seconds, independent of the Fixer's own `restart_pod` action).
- Online Boutique exports no HTTP-level (request/error/latency) metrics without a service-mesh sidecar, so detection relies on resource and object-level signals only (CPU/memory ratios, restart counts, OOM reason) — `error_rate` and `p99_latency_ms` fields exist in the data model for forward-compatibility but aren't currently populated.
- The orchestrator's image lives only in this node's local container store (no registry) — rebuilding the cluster from scratch requires re-running `deploy_orchestrator.sh`.
- No resource limits or liveness probes on the orchestrator Deployment (fine for a dev/demo cluster, not production-hardened).

None of these block the second-review deliverable (inject → detect → rule-based diagnose → remediate → verify, working end-to-end on 3 scenarios with the incident database populated by real runs) — they're documented so they're never a surprise in front of a review panel.
