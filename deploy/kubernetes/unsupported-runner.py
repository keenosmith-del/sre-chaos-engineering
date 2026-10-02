from http.server import BaseHTTPRequestHandler,HTTPServer
class H(BaseHTTPRequestHandler):
 def do_POST(self):
  self.send_response(501);self.send_header('Content-Type','application/json');self.end_headers();self.wfile.write(b'{"error":"Compose runner operations unavailable in kind. Use documented namespace-scoped kubectl experiments."}')
HTTPServer(('0.0.0.0',8080),H).serve_forever()
