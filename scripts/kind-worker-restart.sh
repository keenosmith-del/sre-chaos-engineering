#!/usr/bin/env bash
set -euo pipefail
# Fixed context, namespace and selector; no arbitrary frontend arguments.
kubectl --context kind-reliability -n reliability delete pod -l app=worker --wait=true
kubectl --context kind-reliability -n reliability rollout status deployment/worker --timeout=120s
