import os, json
from http.server import SimpleHTTPRequestHandler, HTTPServer

class H(SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/list':
            symbols = {}
            for f in os.listdir('.'):
                if f.endswith('_daily.csv'):
                    sym = f[:-10]
                    if '_ma' not in sym.lower():
                        symbols.setdefault(sym, []).append('daily')
                elif f.endswith('_minute.csv'):
                    sym = f[:-11]
                    if '_ma' not in sym.lower():
                        symbols.setdefault(sym, []).append('minute')
            result = [
                {'symbol': sym, 'periods': sorted(periods)}
                for sym, periods in sorted(symbols.items())
            ]
            b = json.dumps(result).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', len(b))
            self.end_headers()
            self.wfile.write(b)
        else:
            super().do_GET()
    def log_message(self, *a): pass

HTTPServer(('', 8000), H).serve_forever()