#!/usr/bin/env bash
set -euo pipefail
case "${1:-}" in inventory|payments|redis|rabbitmq|postgres) target="$1" ;; *) printf '%s\n' 'Choose inventory, payments, redis, rabbitmq or postgres'; exit 1;; esac
# An ephemeral namespaced curl pod invokes only the selected fixed proxy.
name="fault-$target"
cleanup(){ kubectl --context kind-reliability -n reliability run "$name-cleanup" --image=curlimages/curl:8.12.1 --restart=Never --rm -i --quiet -- curl -fsS -X POST -H 'Content-Type: application/json' -d '{"enabled":true}' "http://toxiproxy:8474/proxies/$target"; }
trap cleanup EXIT INT TERM
kubectl --context kind-reliability -n reliability run "$name" --image=curlimages/curl:8.12.1 --restart=Never --rm -i --quiet -- curl -fsS -X POST -H 'Content-Type: application/json' -d '{"enabled":false}' "http://toxiproxy:8474/proxies/$target"
sleep 10
