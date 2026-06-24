import os, json
from http.server import SimpleHTTPRequestHandler, HTTPServer

class H(SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/list':
            files = sorted(f[:-4] for f in os.listdir('.') if f.endswith('.csv') and not f.endswith('_ma.csv'))
            b = json.dumps(files).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', len(b))
            self.end_headers()
            self.wfile.write(b)
        else:
            super().do_GET()
    def log_message(self, *a): pass

HTTPServer(('', 8000), H).serve_forever()