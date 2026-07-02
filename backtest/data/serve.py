import os, json, re
from urllib.parse import unquote
from http.server import SimpleHTTPRequestHandler, HTTPServer

# File naming: <SYM>_daily_<YEAR>.csv  /  <SYM>_minute_<YEAR>.csv
#              <SYM>_ma_<YEAR>.csv      /  <SYM>_ma_minute_<YEAR>.csv

YEAR_RE = re.compile(r'^\d{4}$')
FILE_RE = re.compile(r'^(.+)_(daily|minute)_(\d{4})\.csv$')
MA_RE   = re.compile(r'^(.+)_(ma(?:_minute)?)_(\d{4})\.csv$')

def scan():
    symbols = {}
    for entry in sorted(os.scandir('.'), key=lambda e: e.name):
        if not entry.is_dir() or not YEAR_RE.match(entry.name):
            continue
        year = entry.name
        for f in sorted(os.scandir(entry.path), key=lambda e: e.name):
            m = FILE_RE.match(f.name)
            if not m:
                continue
            sym, period = m.group(1), m.group(2)
            if '_ma' in sym.lower():
                continue
            path = f'{year}/{f.name}'
            symbols.setdefault(sym, {}).setdefault(period, []).append(path)
    return symbols

def concat_files(paths):
    chunks = []
    header = None
    for path in paths:
        if not re.match(r'^\d{4}/[\w_]+\.csv$', path):
            continue
        try:
            with open(path, 'r') as f:
                lines = f.read().splitlines()
            if not lines:
                continue
            if header is None:
                header = lines[0]
                chunks.append(lines[0])
            chunks.extend(lines[1:])
        except OSError:
            continue
    return '\n'.join(chunks).encode()

class H(SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/list':
            raw = scan()
            result = [
                {'symbol': sym, 'periods': sorted(periods.keys()), 'files': periods}
                for sym, periods in sorted(raw.items())
            ]
            b = json.dumps(result).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', len(b))
            self.end_headers()
            self.wfile.write(b)

        elif self.path.startswith('/concat?'):
            qs = self.path.split('?', 1)[1]
            params = dict(p.split('=', 1) for p in qs.split('&') if '=' in p)
            files = unquote(params.get('files', '')).split(',')
            body = concat_files(files)
            self.send_response(200)
            self.send_header('Content-Type', 'text/csv')
            self.send_header('Content-Length', len(body))
            self.end_headers()
            self.wfile.write(body)

        elif self.path.startswith('/ma?'):
            # /ma?sym=AMZN&period=daily|minute
            qs = self.path.split('?', 1)[1]
            params = dict(p.split('=', 1) for p in qs.split('&') if '=' in p)
            sym    = unquote(params.get('sym', ''))
            period = unquote(params.get('period', 'daily'))
            # daily → <SYM>_ma_<YEAR>.csv, minute → <SYM>_ma_minute_<YEAR>.csv
            suffix = 'ma_minute' if period == 'minute' else 'ma'
            paths = []
            for entry in sorted(os.scandir('.'), key=lambda e: e.name):
                if not entry.is_dir() or not YEAR_RE.match(entry.name):
                    continue
                year  = entry.name
                fname = f'{sym}_{suffix}_{year}.csv'
                path  = f'{year}/{fname}'
                if os.path.exists(path):
                    paths.append(path)
            body = concat_files(paths)
            self.send_response(200)
            self.send_header('Content-Type', 'text/csv')
            self.send_header('Content-Length', len(body))
            self.end_headers()
            self.wfile.write(body)

        else:
            super().do_GET()

    def log_message(self, *a): pass

HTTPServer(('', 8000), H).serve_forever()