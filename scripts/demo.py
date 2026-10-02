#!/usr/bin/env python3
import concurrent.futures,json,time,urllib.request,urllib.error,uuid
from pathlib import Path
BASE='http://127.0.0.1:8080/api'
def call(path,body=None,key=None):
 headers={'Content-Type':'application/json'}
 if key:headers['Idempotency-Key']=key
 req=urllib.request.Request(BASE+path,data=json.dumps(body).encode() if body is not None else None,headers=headers)
 with urllib.request.urlopen(req,timeout=180) as r:return json.load(r)
def wait_order(oid,terminal):
 deadline=time.monotonic()+30
 while time.monotonic()<deadline:
  o=call('/orders/'+oid)
  if o['state']==terminal:return o
  time.sleep(1)
 raise AssertionError('order did not reach '+terminal)
def main():
 key='demo-'+str(uuid.uuid4());body={'product':'mouse','quantity':1,'decline':False}
 with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:results=list(pool.map(lambda _:call('/orders',body,key),range(4)))
 assert len({r['id'] for r in results})==1,'duplicate orders'
 order=wait_order(results[0]['id'],'CONFIRMED');print('Confirmed order:',order['id'])
 try:call('/orders',{**body,'quantity':2},key);raise AssertionError('payload conflict not rejected')
 except urllib.error.HTTPError as e:assert e.code==409
 before=next(p['stock'] for p in call('/products') if p['id']=='mouse')
 cancelled=call('/orders',{**body,'decline':True},'decline-'+str(uuid.uuid4()));wait_order(cancelled['id'],'CANCELLED')
 detail=call('/transactions/'+cancelled['id']);assert detail['inventory']['active'] is False,'reservation retained'
 after=next(p['stock'] for p in call('/products') if p['id']=='mouse');assert after==before,'compensation failed'
 print('Concurrent idempotency, payload conflict and compensation verified.')
 run=call('/control/experiments',{'fault':'inventory_unavailable','target':'inventory','magnitude':0,'duration':12,'baseline':15,'recovery':15,'stop_error_rate':1})
 deadline=time.monotonic()+150
 while time.monotonic()<deadline:
  report=call('/control/experiments/'+run['id']);print('Experiment:',report['state'])
  if report['state'] in ('COMPLETED','FAILED','ABORTED'):break
  time.sleep(4)
 assert report['state']=='COMPLETED',json.dumps(report)
 assert any(p.get('injection',{}).get('configuration_verified') for p in report['evidence']),'fault configuration not verified'
 phases={p['phase']:p for p in report['evidence'] if 'phase' in p};assert set(phases)=={'BASELINE','FAILURE','RECOVERY'}
 assert phases['FAILURE']['error_rate']['available'],'fault telemetry missing'
 assert phases['FAILURE']['error_rate']['value']>0,'fault had no measured effect'
 assert report['evidence'][-1]['verification']['consistent'],'consistency verification failed'
 backup=call('/control/recovery/backup',{});verification=call('/control/recovery/verify',{})
 assert verification['checksum_verified'] and verification['snapshot_matches'] and verification['restored']['consistent'],verification
 Path('artifacts').mkdir(exist_ok=True)
 Path('artifacts/experiment.json').write_text(json.dumps(call('/control/reports/'+run['id']),indent=2))
 Path('artifacts/recovery.json').write_text(json.dumps(verification,indent=2))
 print('Measured fault, automatic recovery, report and isolated restore verified.')
 print('Dashboard: http://localhost:5175')
if __name__=='__main__':main()
