#!/usr/bin/env bash
set -euo pipefail
command -v kind >/dev/null
command -v kubectl >/dev/null
docker compose build gateway ordering inventory payments worker control dashboard
kind get clusters | rg -qx reliability || kind create cluster --name reliability --config deploy/kubernetes/kind.yaml
# Tag frontend with the stable kind image name.
docker tag reliability-dashboard reliability-dashboard:local
kind load docker-image --name reliability reliability-gateway:local reliability-ordering:local reliability-inventory:local reliability-payments:local reliability-worker:local reliability-control:local reliability-dashboard:local
kubectl --context kind-reliability apply -f deploy/kubernetes/platform.json
kubectl --context kind-reliability -n reliability wait --for=condition=complete job/tox-init --timeout=180s
kubectl --context kind-reliability -n reliability rollout status deployment/ordering --timeout=180s
printf '%s\n' 'Run: kubectl --context kind-reliability -n reliability port-forward service/dashboard 5174:80'
