#!/usr/bin/env python3
"""Privileged local adapter. No host port; fixed operations and Compose label targets only."""
import hashlib,json,os,subprocess,threading,time,urllib.request
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
PROJECT='reliability'
LOCK=threading.RLock()
BACKUPS=Path('/backups');BACKUPS.mkdir(exist_ok=True)
FAULTS={'inventory_unavailable':'inventory','payment_latency':'payments','redis_outage':'redis','rabbitmq_interruption':'rabbitmq','postgres_interruption':'postgres','worker_restart':'worker'}
PROFILES={'baseline','concurrent','sustained','burst','recovery'}
GENERATION=0
LOAD=None
VERIFY_SQL="""SELECT json_build_object('orders',(SELECT count(*) FROM ordering.orders),'confirmed',(SELECT count(*) FROM ordering.orders WHERE state='CONFIRMED'),'cancelled',(SELECT count(*) FROM ordering.orders WHERE state='CANCELLED'),'outstanding',(SELECT count(*) FROM ordering.orders WHERE state NOT IN ('CONFIRMED','CANCELLED','FAILED')),'watermark',(SELECT coalesce(max(created_at)::text,'') FROM ordering.orders),'invariant_violations',(SELECT count(*) FROM ordering.orders o LEFT JOIN inventory.reservations i ON i.id=o.id LEFT JOIN payments.authorizations p ON p.id=o.id WHERE (o.state='CONFIRMED' AND (i.active IS DISTINCT FROM true OR p.status IS DISTINCT FROM 'AUTHORIZED' OR p.amount IS DISTINCT FROM o.amount OR i.product IS DISTINCT FROM o.product OR i.quantity IS DISTINCT FROM o.quantity)) OR (o.state='CANCELLED' AND (i.active IS DISTINCT FROM false OR p.status IS DISTINCT FROM 'DECLINED'))),'missing_deliveries',(SELECT count(*) FROM ordering.orders o LEFT JOIN worker.deliveries d ON d.id=o.id WHERE o.state='CONFIRMED' AND d.id IS NULL),'orphan_records',(SELECT count(*) FROM inventory.reservations i LEFT JOIN ordering.orders o ON o.id=i.id WHERE o.id IS NULL AND i.active),'released_diagnostic_reservations',(SELECT count(*) FROM inventory.reservations i LEFT JOIN ordering.orders o ON o.id=i.id WHERE o.id IS NULL AND NOT i.active),'negative_stock',(SELECT count(*) FROM inventory.products WHERE stock<0),'pending_events',(SELECT count(*) FROM ordering.outbox WHERE published_at IS NULL),'worker_deliveries',(SELECT count(*) FROM worker.deliveries))"""
def run(args,data=None,timeout=90):
 p=subprocess.run(args,input=data,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=timeout)
 if p.returncode: raise RuntimeError(p.stderr.decode()[-2000:])
 return p.stdout

def container(service):
 out=run(['docker','ps','-a','--filter','label=com.docker.compose.project='+PROJECT,'--filter','label=com.docker.compose.service='+service,'--format','{{.ID}}']).decode().split()
 if len(out)!=1:raise RuntimeError('expected exactly one managed '+service+' container')
 return out[0]

def tox(path,body=None,method='POST'):
 req=urllib.request.Request('http://toxiproxy:8474'+path,data=json.dumps(body).encode() if body is not None else None,method=method,headers={'Content-Type':'application/json'})
 with urllib.request.urlopen(req,timeout=4) as r:return json.load(r) if r.status!=204 else None

def cleanup():
 global GENERATION
 GENERATION+=1
 for name in ['inventory','payments','redis','rabbitmq','postgres']:
  try:tox('/proxies/'+name,{'enabled':True})
  except urllib.error.HTTPError as e:
   if e.code!=404:raise
  try:
   proxy=tox('/proxies/'+name,method='GET')
   for toxic in proxy.get('toxics',[]):tox('/proxies/'+name+'/toxics/'+toxic['name'],method='DELETE')
  except urllib.error.HTTPError as e:
   if e.code!=404:raise
 run(['docker','start',container('worker')])
 return {'cleaned':True}

def inject(v):
 global GENERATION
 fault=v.get('fault');m=v.get('magnitude',0);duration=v.get('duration',0)
 if fault not in FAULTS or type(m)!=int or not 0<=m<=3000 or type(duration)!=int or not 5<=duration<=120:raise ValueError('fault parameters outside allowlist')
 cleanup();GENERATION+=1;generation=GENERATION
 if fault=='worker_restart':run(['docker','stop','--time','3',container('worker')])
 elif fault=='payment_latency':tox('/proxies/payments/toxics',{'name':'experiment_latency','type':'latency','stream':'downstream','toxicity':1,'attributes':{'latency':m,'jitter':0}})
 else:tox('/proxies/'+FAULTS[fault],{'enabled':False})
 def expire():
  time.sleep(duration+5)
  with LOCK:
   if generation==GENERATION:cleanup()
 threading.Thread(target=expire,daemon=True).start()
 if fault=='worker_restart':
  observed=json.loads(run(['docker','inspect',container('worker')]))[0]['State']['Running'] is False
 else:
  proxy=tox('/proxies/'+FAULTS[fault],method='GET')
  observed=any(t['name']=='experiment_latency' and t['attributes']['latency']==m for t in proxy.get('toxics',[])) if fault=='payment_latency' else proxy['enabled'] is False
 if not observed:raise RuntimeError('fault configuration was not observed')
 return {'injected':fault,'configuration_verified':observed,'cleanup_deadline_seconds':duration+5}

def sql(service,query,database='commerce'):
 return run(['docker','exec',container(service),'psql','-U','postgres','-d',database,'-At','-c',query]).decode().strip()

def state(service='postgres',database='commerce'):
 result=json.loads(sql(service,VERIFY_SQL,database));result['consistent']=result['invariant_violations']==0 and result['negative_stock']==0 and result['outstanding']==0 and result['pending_events']==0 and result['missing_deliveries']==0 and result['orphan_records']==0
 return result

def backup():
 # Export a repeatable-read snapshot and query its watermark before pg_dump imports it.
 cid=container('postgres')
 p=subprocess.Popen(['docker','exec','-i',cid,'psql','-U','postgres','-d','commerce','-At'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
 try:
  p.stdin.write(b"BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT pg_export_snapshot();\n");p.stdin.flush()
  line=p.stdout.readline()
  if line.strip()==b'BEGIN':line=p.stdout.readline()
  snapshot=line.decode().strip()
  if not snapshot or not all(c in '0123456789ABCDEF-' for c in snapshot):raise RuntimeError('could not export snapshot')
  p.stdin.write((VERIFY_SQL+';\n').encode());p.stdin.flush();watermark=json.loads(p.stdout.readline())
  data=run(['docker','exec',cid,'pg_dump','-U','postgres','-d','commerce','-Fc','--snapshot='+snapshot])
 finally:
  p.stdin.close();p.wait(timeout=10)
 artifact=BACKUPS/'latest.dump';artifact.write_bytes(data)
 record={'action':'backup','timestamp':time.time(),'database':'commerce','format':'pg_dump custom','checksum':hashlib.sha256(data).hexdigest(),'size_bytes':len(data),'snapshot':snapshot,'watermark':watermark}
 (BACKUPS/'latest.json').write_text(json.dumps(record));return record

def verify():
 started=time.monotonic();declared=time.time();record=json.loads((BACKUPS/'latest.json').read_text());artifact=BACKUPS/'latest.dump';data=artifact.read_bytes()
 if hashlib.sha256(data).hexdigest()!=record['checksum']:raise RuntimeError('backup checksum mismatch')
 run(['docker','exec',container('recovery-postgres'),'dropdb','-U','postgres','--if-exists','recovered'])
 run(['docker','exec',container('recovery-postgres'),'createdb','-U','postgres','recovered'])
 run(['docker','exec','-i',container('recovery-postgres'),'pg_restore','-U','postgres','-d','recovered','--no-owner','--no-privileges','--exit-on-error'],data)
 restored=state('recovery-postgres','recovered');live=state();expected=record['watermark'];matches=all(restored[k]==expected[k] for k in ['orders','confirmed','cancelled','watermark','pending_events','worker_deliveries'])
 rto=time.monotonic()-started
 from datetime import datetime
 def epoch(s):return datetime.fromisoformat(s).timestamp() if s else 0
 rpo=max(0,epoch(live['watermark'])-epoch(restored['watermark']));rto_target=int(os.getenv('RTO_TARGET_SECONDS','120'));rpo_target=int(os.getenv('RPO_TARGET_SECONDS','300'))
 return {'action':'restore_verify','declared_disruption_at':declared,'verified_at':time.time(),'isolated_database':'recovered','checksum_verified':True,'snapshot_matches':matches,'restored':restored,'live':live,'rto_seconds':rto,'rpo_seconds':rpo,'lost_order_count':max(0,live['orders']-restored['orders']),'rto_target_seconds':rto_target,'rpo_target_seconds':rpo_target,'objective_met':matches and restored['consistent'] and rto<=rto_target and rpo<=rpo_target,'scope':'isolated PostgreSQL restore drill; elapsed restore time, not a production outage RTO; PostgreSQL backup excludes RabbitMQ'}

def load_start(v):
 global LOAD
 profile=v.get('profile')
 if profile not in PROFILES:raise ValueError('unknown load profile')
 load_stop({})
 name='reliability-approved-load'
 run(['docker','run','-d','--name',name,'--label','reliability.runner=load','--network','reliability_application','--memory','192m','--cpus','0.5','-e','PROFILE='+profile,'-v',os.environ['LOAD_DIRECTORY']+':/scripts:ro','grafana/k6:0.57.0','run','/scripts/transactions.js'])
 LOAD=name;return {'state':'running','profile':profile}

def load_status():
 ids=run(['docker','ps','-aq','--filter','label=reliability.runner=load']).decode().split()
 if not ids:return {'running':False,'exit_code':0}
 details=json.loads(run(['docker','inspect',ids[0]]))[0]['State']
 return {'running':details['Running'],'exit_code':details['ExitCode']}

def load_stop(v):
 global LOAD
 ids=run(['docker','ps','-aq','--filter','label=reliability.runner=load']).decode().split()
 for cid in ids:run(['docker','rm','-f',cid])
 LOAD=None;return {'state':'stopped'}

class Handler(BaseHTTPRequestHandler):
 def do_GET(self):
  if self.path!='/health/live':self.send_error(404);return
  self.send_response(200);self.end_headers();self.wfile.write(b'ok')
 def do_POST(self):
  try:
   length=int(self.headers.get('Content-Length','0'))
   if length>4096:raise ValueError('body too large')
   v=json.loads(self.rfile.read(length) or b'{}') or {}
   with LOCK:
    functions={'/inject':lambda:inject(v),'/cleanup':cleanup,'/backup':backup,'/verify':verify,'/consistency':state,'/load/start':lambda:load_start(v),'/load/stop':lambda:load_stop(v),'/load/status':load_status}
    if self.path not in functions:raise ValueError('operation not allowlisted')
    result=functions[self.path]()
   data=json.dumps(result).encode();self.send_response(200);self.send_header('Content-Type','application/json');self.end_headers();self.wfile.write(data)
  except Exception as e:
   self.send_response(400 if isinstance(e,ValueError) else 503);self.send_header('Content-Type','application/json');self.end_headers();self.wfile.write(json.dumps({'error':str(e)}).encode())
ThreadingHTTPServer(('0.0.0.0',8080),Handler).serve_forever()
