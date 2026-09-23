import argparse
import json
import sys

from .core import DB, ROOT, connect
from .pipeline import ingest, enrich_properties, export, rebuild
from .quality import sample, import_reviews, write_report
from .sources import BY_NAME

def main():
    parser=argparse.ArgumentParser(description='NYC Project Signals — public-record research pipeline')
    parser.add_argument('--db',default=str(DB))
    subs=parser.add_subparsers(dest='command',required=True)
    p=subs.add_parser('ingest');p.add_argument('--days',type=int,default=90);p.add_argument('--limit',type=int,default=10000)
    p.add_argument('--sources',nargs='+',choices=list(BY_NAME));p.add_argument('--force',action='store_true')
    p=subs.add_parser('enrich');p.add_argument('--limit',type=int,default=500)
    p=subs.add_parser('export');p.add_argument('--output',default=str(ROOT/'exports'/'latest'))
    p=subs.add_parser('sample');p.add_argument('--size',type=int,default=100);p.add_argument('--output',default=str(ROOT/'exports'/'review-sample.csv'))
    p=subs.add_parser('review-import');p.add_argument('path')
    p=subs.add_parser('quality');p.add_argument('--output',default=str(ROOT/'exports'/'quality-report.json'))
    p=subs.add_parser('azure-review');p.add_argument('--limit',type=int,default=20)
    p=subs.add_parser('serve');p.add_argument('--port',type=int,default=8765)
    p=subs.add_parser('mcp');p.add_argument('--transport',choices=['stdio','streamable-http','sse'],default='stdio')
    p.add_argument('--host',default='127.0.0.1');p.add_argument('--port',type=int,default=8000)
    p.add_argument('--no-refresh',action='store_true',help='Do not refresh public sources when data is older than 24 hours')
    subs.add_parser('rebuild')
    args=parser.parse_args()
    if args.command=='serve':
        from .server import serve
        serve(args.db,args.port);return
    if args.command=='mcp':
        from .mcp_server import run
        run(args.db,args.transport,args.host,args.port,not args.no_refresh);return
    conn=connect(args.db)
    try:
        if args.command=='ingest':
            if not 1<=args.days<=3660 or not 1<=args.limit<=100000:raise ValueError('Days or source limit outside safe bounds.')
            result=ingest(conn,args.days,args.limit,args.sources,args.force)
            failures=[n for n,s in result['sources'].items() if s.get('status')=='error']
            print(json.dumps({'run':result['started_at'],'failed_sources':failures,'projects':conn.execute('SELECT count(*) FROM projects').fetchone()[0]},indent=2))
            if failures:sys.exit(2)
        elif args.command=='enrich':print(json.dumps({'properties':enrich_properties(conn,args.limit)}))
        elif args.command=='export':print(json.dumps(export(conn,args.output),indent=2))
        elif args.command=='rebuild':print(json.dumps({'projects':rebuild(conn)}))
        elif args.command=='sample':print(json.dumps({'sampled':sample(conn,args.output,args.size),'path':args.output}))
        elif args.command=='review-import':print(json.dumps({'imported':import_reviews(conn,args.path)}))
        elif args.command=='quality':
            result=write_report(conn,args.output)
            print(json.dumps({k:v for k,v in result.items() if k!='sources'},indent=2))
        elif args.command=='azure-review':
            from .azure import review
            print(json.dumps(review(conn,args.limit)))
    except (ValueError,RuntimeError) as error:
        print(str(error),file=sys.stderr);sys.exit(1)
    finally:conn.close()

if __name__=='__main__':main()
