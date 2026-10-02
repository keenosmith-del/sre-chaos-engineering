# Secondary kind deployment

Prerequisites: kind, kubectl, Docker Desktop, and rg. `make kind-up` builds local service images, creates a fixed reliability cluster if absent, loads images and applies the generated Kubernetes List in deploy/kubernetes/platform.json. Resources include namespace, ConfigMaps, local Secret template, Deployments/Services, startup checks, liveness/readiness, requests/limits, rolling application updates and local PVCs for PostgreSQL/Redis/RabbitMQ. Stateful single replicas use Recreate to avoid overlapping writers. Secrets contain public demo-only values and must be replaced for other environments.

```sh
make kind-up
kubectl --context kind-reliability -n reliability port-forward service/dashboard 5174:80
bash scripts/kind-worker-restart.sh
bash scripts/kind-network-fault.sh inventory
```

The worker script deletes only worker-selected pods in the fixed namespace and waits for Deployment self-healing. The network script disables one allowlisted dependency proxy for ten seconds and installs an EXIT/INT/TERM cleanup trap. It runs ephemeral curl pods rather than exposing Docker control or mounting a socket. These are manual Kubernetes-native experiments; their results are visible in application/Prometheus telemetry but are not orchestrated/persisted as dashboard experiment reports.

The primary Compose runner cannot operate against Kubernetes resources. Dashboard experiment/load/backup commands in kind return an explicit unsupported error from an adapter. PostgreSQL backup/restore automation, full experiment state-machine runs, and Grafana provisioning are Compose-only in this version. Jaeger/Prometheus can be port-forwarded; frontend telemetry links default to Compose ports and need equivalent forwards. Prometheus storage and traces are ephemeral in kind; local business PVCs use the kind storage class. kind has not been runtime-validated unless recorded in docs/validation.md. This path is a deployment/self-healing demonstration, not feature parity with Compose.
