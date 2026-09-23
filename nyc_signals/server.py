from __future__ import annotations

import io
import json
import mimetypes
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse, unquote

from .core import DB, ROOT, FEEDS, connect, get_state
from .pipeline import filtered, load_projects, write_csv
from .quality import report

class Handler(BaseHTTPRequestHandler):
    database=DB

    def send(self,body,status=200,ctype='application/json; charset=utf-8',attachment=None):
        if isinstance(body,(dict,list)):body=json.dumps(body,ensure_ascii=False).encode()
        if isinstance(body,str):body=body.encode()
        self.send_response(status)
        self.send_header('Content-Type',ctype)
        self.send_header('Content-Length',str(len(body)))
        self.send_header('X-Content-Type-Options','nosniff')
        self.send_header('Referrer-Policy','no-referrer')
        self.send_header('Cache-Control','no-store')
        self.send_header('Content-Security-Policy',"default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
        if attachment:self.send_header('Content-Disposition',f'attachment; filename="{attachment}"')
        self.end_headers();self.wfile.write(body)

    def do_GET(self):
        # Read-only, localhost application. No browser-triggered ingestion or paid calls.
        parsed=urlparse(self.path)
        params={k:v[-1] for k,v in parse_qs(parsed.query).items()}
        path=unquote(parsed.path)
        if path.startswith('/api/'):
            conn=connect(self.database)
            try:
                if path=='/api/health':return self.send({'ok':True,'mode':'local research preview'})
                if path=='/api/quality':return self.send(report(conn))
                if path=='/api/sources':return self.send(get_state(conn))
                projects=load_projects(conn)
                if path=='/api/summary':
                    return self.send({'projects':len(projects),'feeds':{k:sum(k in p['feeds'] for p in projects) for k in FEEDS},
                        'boroughs':{b:sum(p['borough']==b for p in projects) for b in ['Manhattan','Brooklyn','Queens','Bronx','Staten Island']},
                        'sources':get_state(conn),'reviewed':sum(p['review_status']=='Reviewed' for p in projects)})
                if path=='/api/projects':
                    matches=filtered(projects,params)
                    limit=min(max(int(params.get('limit',50)),1),200)
                    offset=max(int(params.get('offset',0)),0)
                    return self.send({'total':len(matches),'offset':offset,'limit':limit,'projects':matches[offset:offset+limit]})
                if path=='/api/leads':
                    # Lead candidates are evidence-prioritized research work, not confirmed prospects.
                    params.setdefault('priority','high')
                    matches=filtered(projects,params)
                    limit=min(max(int(params.get('limit',50)),1),200)
                    offset=max(int(params.get('offset',0)),0)
                    return self.send({'purpose':'Evidence-prioritized research candidates; verify before outreach.',
                        'total':len(matches),'offset':offset,'limit':limit,'projects':matches[offset:offset+limit]})
                if path.startswith('/api/projects/'):
                    pid=path[len('/api/projects/'):]
                    project=next((p for p in projects if p['id']==pid),None)
                    return self.send(project if project else {'error':'Project not found'},200 if project else 404)
                if path=='/api/export.csv':
                    matches=filtered(projects,params)
                    if params.get('ids'):
                        ids=set(params['ids'].split(','));matches=[p for p in matches if p['id'] in ids]
                    stream=io.StringIO(newline='');write_csv(matches,stream)
                    return self.send('\ufeff'+stream.getvalue(),ctype='text/csv; charset=utf-8',attachment='nyc-project-signals.csv')
                return self.send({'error':'Not found'},404)
            except ValueError:
                return self.send({'error':'Invalid filter or pagination value'},400)
            finally:conn.close()
        if path not in ('/','/index.html','/app.js','/style.css','/favicon.svg'):
            return self.send({'error':'Not found'},404)
        file=ROOT/'web'/('index.html' if path=='/' else path[1:])
        if not file.is_file():return self.send({'error':'Not found'},404)
        return self.send(file.read_bytes(),ctype=(mimetypes.guess_type(file.name)[0] or 'application/octet-stream')+'; charset=utf-8')

def serve(database=DB,port=8765):
    Handler.database=database
    server=ThreadingHTTPServer(('127.0.0.1',port),Handler)
    print(f'NYC Project Signals: http://127.0.0.1:{port}',flush=True)
    try:server.serve_forever()
    except KeyboardInterrupt:pass
    finally:server.server_close()
