"""Optional bounded smoke run of the five remaining allowlisted presets."""
import json,time,argparse
from pathlib import Path
from demo import call
presets={'payment_latency':'payments','redis_outage':'redis','rabbitmq_interruption':'rabbitmq','worker_restart':'worker','postgres_interruption':'postgres'}
parser=argparse.ArgumentParser();parser.add_argument('faults',nargs='*',choices=list(presets));args=parser.parse_args();selected=args.faults or list(presets)
Path('artifacts').mkdir(exist_ok=True)
for fault in selected:
    target=presets[fault]
    created=call('/control/experiments',{'fault':fault,'target':target,'magnitude':1500 if target=='payments' else 0,'duration':10 if fault=='worker_restart' else 5,'baseline':10,'recovery':10,'stop_error_rate':1})
    deadline=time.monotonic()+90
    while time.monotonic()<deadline:
        report=call('/control/experiments/'+created['id'])
        if report['state'] in ('COMPLETED','FAILED','ABORTED'):
            break
        time.sleep(2)
    Path('artifacts/'+fault+'.json').write_text(json.dumps(report,indent=2))
    assert report['state']=='COMPLETED',json.dumps(report)
    assert report['evidence'][-1]['verification']['consistent'],report
    print(fault,report['state'],flush=True)
