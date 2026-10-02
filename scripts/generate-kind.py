from pathlib import Path
root=Path(__file__).resolve().parents[1]
def put(path,text):
    target=root/path
    target.parent.mkdir(parents=True,exist_ok=True)
    target.write_text(text)
import json
# Secondary path is application + telemetry. Privileged Docker operations intentionally absent.
put('deploy/kubernetes/kind.yaml','''kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
''')
resources=[{'apiVersion':'v1','kind':'Namespace','metadata':{'name':'reliability'}}]
def resource(kind,name,**kwargs):
 return {'apiVersion':'v1','kind':kind,'metadata':{'name':name,'namespace':'reliability'},**kwargs}
config_files={'database-init':{'001.sql':(root/'db/migrations/001_init.sql').read_text()},'toxiproxy-config':{'proxies.json':(root/'chaos/toxiproxy/proxies.json').read_text()},'otel-config':{'config.yaml':(root/'observability/otel/config.yaml').read_text()},'prometheus-config':{'prometheus.yml':(root/'observability/prometheus/prometheus.yml').read_text()},'rabbitmq-config':{'rabbitmq.conf':(root/'observability/rabbitmq.conf').read_text(),'enabled_plugins':(root/'observability/enabled_plugins').read_text()}}
for name,data in config_files.items():resources.append(resource('ConfigMap',name,data=data))
resources.append(resource('Secret','local-demo',type='Opaque',stringData={'POSTGRES_PASSWORD':'local_admin','RABBITMQ_DEFAULT_USER':'demo','RABBITMQ_DEFAULT_PASS':'local_rabbit',**{s+'_DATABASE_URL':f'postgres://{s}:local_{s}@toxiproxy:5432/commerce' for s in ['ordering','inventory','payments','worker','control']}}))
def deploy(name,image,ports=[8080],env=None,volumes=None,mounts=None,args=None,probe=True,command=None,persistent=False):
 container={'name':name,'image':image,'imagePullPolicy':'IfNotPresent','ports':[{'containerPort':p} for p in ports],'resources':{'requests':{'cpu':'50m','memory':'64Mi'},'limits':{'cpu':'1','memory':'512Mi'}}}
 if env:container['env']=env
 if mounts:container['volumeMounts']=mounts
 if args:container['args']=args
 if command:container['command']=command
 if probe:container.update(readinessProbe={'httpGet':{'path':'/health/ready','port':8080},'initialDelaySeconds':5,'periodSeconds':5},livenessProbe={'httpGet':{'path':'/health/live','port':8080},'initialDelaySeconds':15,'periodSeconds':10})
 else:container.update(readinessProbe={'tcpSocket':{'port':ports[0]},'initialDelaySeconds':8,'periodSeconds':5},livenessProbe={'tcpSocket':{'port':ports[0]},'initialDelaySeconds':30,'periodSeconds':10})
 spec={'containers':[container],'automountServiceAccountToken':False}
 if volumes:spec['volumes']=volumes
 resources.append({'apiVersion':'apps/v1','kind':'Deployment','metadata':{'name':name,'namespace':'reliability'},'spec':{'replicas':1,'strategy':{'type':'Recreate'} if persistent else {'type':'RollingUpdate','rollingUpdate':{'maxSurge':1,'maxUnavailable':0}},'selector':{'matchLabels':{'app':name}},'template':{'metadata':{'labels':{'app':name}},'spec':spec}}})
 resources.append(resource('Service',name,spec={'selector':{'app':name},'ports':[{'name':'p'+str(p),'port':p,'targetPort':p} for p in ports]}))
def E(k,v):return {'name':k,'value':v}
def secret(k,key):return {'name':k,'valueFrom':{'secretKeyRef':{'name':'local-demo','key':key}}}
def cm(name):return {'name':name,'configMap':{'name':name}}
def mount(name,path):return {'name':name,'mountPath':path}
for name in ['postgres','redis','rabbitmq']:
 resources.append(resource('PersistentVolumeClaim',name,spec={'accessModes':['ReadWriteOnce'],'resources':{'requests':{'storage':'1Gi'}}}))
def pvc(name):return {'name':name,'persistentVolumeClaim':{'claimName':name}}
deploy('postgres','postgres:16.8-alpine',[5432],env=[secret('POSTGRES_PASSWORD','POSTGRES_PASSWORD'),E('POSTGRES_DB','commerce')],volumes=[pvc('postgres'),cm('database-init')],mounts=[mount('postgres','/var/lib/postgresql/data'),mount('database-init','/docker-entrypoint-initdb.d')],probe=False,persistent=True)
deploy('redis','redis:7.4.2-alpine',[6379],volumes=[pvc('redis')],mounts=[mount('redis','/data')],args=['redis-server','--appendonly','yes'],probe=False,persistent=True)
deploy('rabbitmq','rabbitmq:3.13.7-management-alpine',[5672,15672,15692],env=[secret('RABBITMQ_DEFAULT_USER','RABBITMQ_DEFAULT_USER'),secret('RABBITMQ_DEFAULT_PASS','RABBITMQ_DEFAULT_PASS')],volumes=[pvc('rabbitmq'),cm('rabbitmq-config')],mounts=[mount('rabbitmq','/var/lib/rabbitmq'),{'name':'rabbitmq-config','mountPath':'/etc/rabbitmq/rabbitmq.conf','subPath':'rabbitmq.conf'},{'name':'rabbitmq-config','mountPath':'/etc/rabbitmq/enabled_plugins','subPath':'enabled_plugins'}],probe=False,persistent=True)
deploy('toxiproxy','ghcr.io/shopify/toxiproxy:2.12.0',[8474,8081,8082,5432,5672,6379],probe=False)
resources.append({'apiVersion':'batch/v1','kind':'Job','metadata':{'name':'tox-init','namespace':'reliability'},'spec':{'backoffLimit':10,'template':{'spec':{'restartPolicy':'OnFailure','automountServiceAccountToken':False,'volumes':[cm('toxiproxy-config')],'containers':[{'name':'init','image':'curlimages/curl:8.12.1','volumeMounts':[mount('toxiproxy-config','/config')],'command':['/bin/sh','-c','until curl -fsS http://toxiproxy:8474/version; do sleep 2; done; curl -fsS -X POST -H "Content-Type: application/json" --data-binary @/config/proxies.json http://toxiproxy:8474/populate'],'resources':{'requests':{'cpu':'20m','memory':'16Mi'},'limits':{'cpu':'100m','memory':'32Mi'}}}]}}}})
for service in ['gateway','ordering','inventory','payments','worker','control']:
 env=[E('OTEL_ENDPOINT','otel:4318'),E('INVENTORY_URL','http://toxiproxy:8081'),E('PAYMENTS_URL','http://toxiproxy:8082'),E('AMQP_URL','amqp://demo:local_rabbit@toxiproxy:5672/'),E('REDIS_ADDR','toxiproxy:6379')]
 if service!='gateway':env.append(secret('DATABASE_URL',service+'_DATABASE_URL'))
 if service=='control':env+=[E('RUNNER_URL','http://unsupported-runner:8080'),E('PROMETHEUS_URL','http://prometheus:9090')]
 deploy(service,'reliability-'+service+':local',env=env)
deploy('dashboard','reliability-dashboard:local',[80],probe=False)
deploy('otel','otel/opentelemetry-collector-contrib:0.123.0',[4318,4317],volumes=[cm('otel-config')],mounts=[mount('otel-config','/etc/otel')],args=['--config=/etc/otel/config.yaml'],probe=False)
deploy('prometheus','prom/prometheus:v3.2.1',[9090],volumes=[cm('prometheus-config')],mounts=[mount('prometheus-config','/etc/prometheus')],args=['--config.file=/etc/prometheus/prometheus.yml'],probe=False)
deploy('jaeger','jaegertracing/all-in-one:1.67.0',[16686,4317],env=[E('COLLECTOR_OTLP_ENABLED','true')],probe=False)
# Explicit adapter returning unsupported avoids a fake Docker-backed control path.
put('deploy/kubernetes/unsupported-runner.py','''from http.server import BaseHTTPRequestHandler,HTTPServer
class H(BaseHTTPRequestHandler):
 def do_POST(self):
  self.send_response(501);self.send_header('Content-Type','application/json');self.end_headers();self.wfile.write(b'{"error":"Compose runner operations unavailable in kind. Use documented namespace-scoped kubectl experiments."}')
HTTPServer(('0.0.0.0',8080),H).serve_forever()
''')
resources.append(resource('ConfigMap','unsupported-runner-code',data={'server.py':(root/'deploy/kubernetes/unsupported-runner.py').read_text()}))
deploy('unsupported-runner','python:3.12.9-alpine',[8080],volumes=[cm('unsupported-runner-code')],mounts=[mount('unsupported-runner-code','/app')],command=['python','/app/server.py'],probe=False)
put('deploy/kubernetes/platform.json',json.dumps({'apiVersion':'v1','kind':'List','items':resources},indent=2))
put('scripts/kind-up.sh','''#!/usr/bin/env bash
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
printf '%s\\n' 'Run: kubectl --context kind-reliability -n reliability port-forward service/dashboard 5174:80'
''')
put('scripts/kind-worker-restart.sh','''#!/usr/bin/env bash
set -euo pipefail
# Fixed context, namespace and selector; no arbitrary frontend arguments.
kubectl --context kind-reliability -n reliability delete pod -l app=worker --wait=true
kubectl --context kind-reliability -n reliability rollout status deployment/worker --timeout=120s
''')
put('scripts/kind-network-fault.sh','''#!/usr/bin/env bash
set -euo pipefail
case "${1:-}" in inventory|payments|redis|rabbitmq|postgres) target="$1" ;; *) printf '%s\\n' 'Choose inventory, payments, redis, rabbitmq or postgres'; exit 1;; esac
# An ephemeral namespaced curl pod invokes only the selected fixed proxy.
name="fault-$target"
cleanup(){ kubectl --context kind-reliability -n reliability run "$name-cleanup" --image=curlimages/curl:8.12.1 --restart=Never --rm -i --quiet -- curl -fsS -X POST -H 'Content-Type: application/json' -d '{"enabled":true}' "http://toxiproxy:8474/proxies/$target"; }
trap cleanup EXIT INT TERM
kubectl --context kind-reliability -n reliability run "$name" --image=curlimages/curl:8.12.1 --restart=Never --rm -i --quiet -- curl -fsS -X POST -H 'Content-Type: application/json' -d '{"enabled":false}' "http://toxiproxy:8474/proxies/$target"
sleep 10
''')
