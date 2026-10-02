#!/usr/bin/env python3
import sys,json,urllib.request
op={'backup':'backup','restore-verify':'verify'}[sys.argv[1]]
req=urllib.request.Request('http://127.0.0.1:8080/api/control/recovery/'+op,data=b'{}',headers={'Content-Type':'application/json'})
with urllib.request.urlopen(req,timeout=180) as r:print(json.dumps(json.load(r),indent=2))
