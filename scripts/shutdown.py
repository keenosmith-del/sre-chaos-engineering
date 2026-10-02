"""Remove only the fixed approved load before Compose removes its network."""
import subprocess,urllib.request
try:
    req=urllib.request.Request('http://127.0.0.1:8080/api/control/load/stop',data=b'{}',headers={'Content-Type':'application/json'})
    with urllib.request.urlopen(req,timeout=10) as response:
        response.read()
except Exception:
    # Control may already be unavailable. The resource remains strictly scoped.
    ids=subprocess.check_output(['docker','ps','-aq','--filter','label=reliability.runner=load','--filter','name=^/reliability-approved-load$'],text=True).split()
    for container_id in ids:
        subprocess.run(['docker','rm','-f',container_id],check=True)
