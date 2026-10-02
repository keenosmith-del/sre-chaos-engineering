import json,urllib.request
req=urllib.request.Request('http://127.0.0.1:8080/api/control/deadletters/replay',data=b'{}',headers={'Content-Type':'application/json'})
with urllib.request.urlopen(req,timeout=30) as r:print(json.dumps(json.load(r),indent=2))
