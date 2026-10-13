# Dockerfile - Containerizes the auto-remediation orchestrator (Task 13's
# main.py loop) for deployment into the Minikube cluster's `online-boutique`
# namespace (Task 14).
#
# Build context is the `orchestrator/` directory (see
# infra/minikube/deploy_orchestrator.sh), so paths below are relative to it.
FROM python:3.12-slim

WORKDIR /app

# Install dependencies from pyproject.toml first (better layer caching),
# then copy the package source and install it. The package has no compiled
# extensions beyond psycopg[binary], which ships prebuilt wheels, so no
# extra build toolchain is needed.
COPY pyproject.toml ./
COPY orchestrator ./orchestrator

RUN pip install --no-cache-dir .

CMD ["python", "-m", "orchestrator.main"]
