from __future__ import annotations

import csv
import json
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .core import ROOT, business, borough, classify, date, digest, dumps, get_state, job_root, lead_priority, now, put_state, text, address_key
from .sources import SOURCES, BY_NAME, PLUTO, metadata, query, source_record_url, window_filter

def store_record(conn, source, row, observed):
    key = '|'.join(str(row.get(k,'')) for k in source.id_fields)
    if not key.strip('|'):
        key = digest(row)
    payload, hashed = dumps(row), digest(row)
    old = conn.execute('SELECT hash FROM raw_records WHERE source=? AND record_key=?',(source.name,key)).fetchone()
    conn.execute('''INSERT INTO raw_records VALUES (?,?,?,?,?,?) ON CONFLICT(source,record_key)
      DO UPDATE SET payload=excluded.payload, hash=excluded.hash, last_seen=excluded.last_seen''',
      (source.name,key,payload,hashed,observed,observed))
    if not old or old['hash'] != hashed:
        conn.execute('INSERT OR IGNORE INTO record_versions VALUES (?,?,?,?,?)',(source.name,key,hashed,observed,payload))
        return 1
    return 0

def ingest(conn, days=90, limit=10000, names=None, force=False):
    since = (datetime.now(timezone.utc)-timedelta(days=days)).date().isoformat()
    run = {'started_at':now(),'since':since,'limit_per_source':limit,'sources':{}}
    old_states = get_state(conn)
    for source in SOURCES:
        if names and source.name not in names:
            continue
        start = time.monotonic()
        prior = old_states.get(source.name,{})
        state = {**prior,'name':source.label,'url':source.url,'dataset':source.dataset,
                 'last_attempt':now(),'window_start':since,'row_limit':limit}
        print(f'Fetching {source.label}…',flush=True)
        try:
            meta = metadata(source)
            state['metadata'] = meta
            required={source.date_field,*source.id_fields}
            if not required <= set(meta['fields']):
                raise RuntimeError('Publisher schema changed; missing fields: '+', '.join(sorted(required-set(meta['fields']))))
            if (not force and prior.get('status')=='ok' and prior.get('metadata',{}).get('rows_updated_at')==meta['rows_updated_at']
                and prior.get('window_start','9999') <= since and prior.get('complete') and prior.get('last_success')):
                state['status']='ok'
                state['checked_at']=now()
                run['sources'][source.name]={'skipped_unchanged':True}
                put_state(conn,source.name,state)
                continue
            where = window_filter(source,since)
            count = int(query(source,**{'$select':'count(*) as n','$where':where})[0]['n'])
            observed = now()
            read = changed = 0
            order = f'{source.date_expression} DESC, :id ASC'
            while read < min(limit,count):
                rows = query(source,**{'$where':where,'$order':order,'$limit':min(1000,limit-read),'$offset':read})
                if not rows:
                    break
                for row in rows:
                    changed += store_record(conn,source,row,observed)
                read += len(rows)
                conn.commit()
            state.update(status='ok',last_success=observed,checked_at=now(),source_count=count,
                         fetched=read,changed=changed,complete=read>=count,error=None,
                         duration_seconds=round(time.monotonic()-start,2))
            # Reconcile current-pending membership only after a complete snapshot.
            if source.snapshot and state['complete']:
                state['snapshot_at']=observed
            print(f'  {read:,}/{count:,} records; {changed:,} new or changed',flush=True)
        except Exception as error:
            conn.rollback()
            state.update(status='error',error=str(error)[:700],complete=False)
            print(f'  Source failed: {error}',flush=True)
        put_state(conn,source.name,state)
        run['sources'][source.name]=state
    run['finished_at']=now()
    conn.execute('INSERT INTO runs(started_at,finished_at,payload) VALUES (?,?,?)',(run['started_at'],run['finished_at'],dumps(run)))
    conn.commit()
    rebuild(conn)
    return run

def normalized(source, row, record):
    license_record = source.name=='liquor'
    boro = borough(row.get('premises_county') if license_record else row.get('borough'))
    address = text(row.get('actual_address_of_premises')) if license_record else text(f"{row.get('house_no',row.get('house__',''))} {row.get('street_name','')}")
    job = job_root(row.get('job_filing_number') or row.get('job__') or row.get('job_filing_name'))
    project_id = 'dob:'+job if job else source.name+':'+digest(record['record_key'])[:20]
    description = text(row.get('job_description') or (row.get('description') if license_record else ''))
    work = text(row.get('work_type') or row.get('job_type'))
    status = text(row.get('filing_status') if source.name=='dob_applications' else
                  row.get('job_status_descrp') if source.name=='legacy_applications' else
                  row.get('c_of_o_status') if source.name=='occupancy' else
                  row.get('status') if license_record else row.get('permit_status'))
    event_date = date(row.get('c_of_o_issuance_date')) if source.name=='occupancy' else date(row.get(source.date_field))
    event_date = event_date or date(row.get(source.date_field))
    event_type = {'dob_applications':'Application status','legacy_applications':'Application status',
                  'dob_permits':'Permit issuance','legacy_permits':'Permit issuance',
                  'occupancy':'Occupancy record','liquor':'License application received'}[source.name]
    roles = []
    def role(name,value):
        value = business(value)
        if value:
            roles.append({'role':name,'name':value})
    if license_record:
        role('License applicant',row.get('legalname'))
        role('Trading name',row.get('dba'))
    else:
        role('Recorded owner business',row.get('owner_s_business_name') or row.get('owner_business_name'))
        role('Permittee business' if source.name in ('dob_permits','legacy_permits') else 'Filing applicant business',
             row.get('permittee_s_business_name') or row.get('applicant_business_name'))
        role('Filing representative',row.get('filing_representative_business_name'))
    bbl = str(row.get('bbl','')).split('.')[0]
    if len(bbl)!=10 or not bbl.isdigit():
        bbl=''
        code = {'Manhattan':'1','Bronx':'2','Brooklyn':'3','Queens':'4','Staten Island':'5'}.get(boro)
        if code and str(row.get('block','')).isdigit() and str(row.get('lot','')).isdigit():
            bbl=code+str(row['block']).zfill(5)+str(row['lot']).zfill(4)
    flags=[]
    if license_record:
        flags.append('A pending license is not a confirmed new opening.')
    if 'renew' in (str(row.get('c_of_o_filing_type',''))+' '+str(row.get('filing_reason',''))).lower():
        flags.append('Renewal or reissuance; not a new project signal.')
    if source.name=='occupancy':
        flags.append('Occupancy approval does not establish tenant opening or purchasing dates.')
    if not event_date:
        flags.append('Source event date is missing or could not be parsed.')
    return {'project_id':project_id,'job':job,'borough':boro,'address':address,'bbl':bbl,
            'bin':text(row.get('bin') or row.get('bin__')),'description':description,'work_type':work,
            'status':status,'date':event_date,'type':event_type,'source':source.name,'source_label':source.label,
            'source_url':source_record_url(source,row),'dataset_url':source.url,
            'source_record_key':record['record_key'],'observed_at':record['last_seen'],
            'businesses':roles,'flags':flags,'is_license':license_record,
            'building_type':text(row.get('building_type')),'filing_date':date(row.get('filing_date') or row.get('pre__filing_date')),
            'cost':text(row.get('initial_cost') or row.get('estimated_job_costs')),
            'occupancy_filing_type':text(row.get('c_of_o_filing_type'))}

def rebuild(conn):
    states=get_state(conn)
    grouped={}
    for record in conn.execute('SELECT * FROM raw_records ORDER BY source,record_key'):
        source=BY_NAME[record['source']]
        row=json.loads(record['payload'])
        if source.snapshot:
            snapshot=states.get(source.name,{}).get('snapshot_at')
            if snapshot and record['last_seen'] < snapshot:
                continue
        event=normalized(source,row,record)
        grouped.setdefault(event['project_id'],[]).append(event)
    projects=[]
    for pid,events in grouped.items():
        events.sort(key=lambda e:(e['date'],e['observed_at']),reverse=True)
        primary=events[0]
        feeds=set();trades=set();reasons=[];flags=[];roles=[]
        for event in events:
            c=classify(event['description'],event['work_type'],event['building_type'],event['is_license'])
            feeds.update(c['feeds']);trades.update(c['trades']);reasons.extend(c['reasons']);flags.extend(event['flags'])
            for role in event['businesses']:
                item={**role,'source_url':event['source_url']}
                if not any(x['role']==item['role'] and x['name']==item['name'] for x in roles):roles.append(item)
        description=next((e['description'] for e in events if e['description']),'No project description supplied in these records.')
        if not roles:flags.append('No business identity supplied in the ingested records.')
        flags.append('Current buying intent, opening date, and supplier availability are unverified.')
        if any(re.search(r'withdraw|revok|cancel|disapprov',e['status'],re.I) for e in events[:1]):
            flags.append('Latest source status indicates withdrawal, cancellation or rejection.')
        p={'id':pid,'job':primary['job'],'address':primary['address'],'address_key':address_key(primary['address']),
           'borough':primary['borough'],'bin':next((e['bin'] for e in events if e['bin']),''),
           'bbl':next((e['bbl'] for e in events if e['bbl']),''),'description':description,
           'latest_date':primary['date'],'status':primary['status'] or 'Status not supplied',
           'event_type':primary['type'],'feeds':sorted(feeds),'trades':sorted(trades),
           'reasons':list(dict.fromkeys(reasons)),'flags':list(dict.fromkeys(flags)),
           'businesses':roles,'events':events,'classification_method':'rules-v1','review_status':'Unreviewed',
           'source_count':len(set(e['source'] for e in events)),'source_names':sorted(set(e['source'] for e in events)),
           'source_errors':[s for s in set(e['source'] for e in events) if states.get(s,{}).get('status')=='error']}
        p['evidence_hash']=digest({'events':[{k:v for k,v in e.items() if k!='observed_at'} for e in events], 'feeds':p['feeds']})
        prop=conn.execute('SELECT payload,fetched_at FROM properties WHERE bbl=?',(p['bbl'],)).fetchone()
        p['property']=json.loads(prop['payload']) if prop else None
        if p['property']:
            p['property']['fetched_at']=prop['fetched_at']
        p.update(lead_priority(p))
        projects.append(p)
    # Same-site links are research hints; never merge projects merely by their address or building.
    sites={}
    for p in projects:
        key=(p['borough'],p['bin'] or p['address_key'])
        if key[1]:sites.setdefault(key,[]).append(p['id'])
    conn.execute('DELETE FROM projects')
    for p in projects:
        p['related_projects']=[x for x in sites.get((p['borough'],p['bin'] or p['address_key']),[]) if x!=p['id']][:20]
        conn.execute('INSERT INTO projects VALUES (?,?)',(p['id'],dumps(p)))
    conn.commit()
    return len(projects)

def rekey_records(conn):
    """Rebuild logical keys from retained versions when a source has multi-work-type rows."""
    versions=list(conn.execute('SELECT * FROM record_versions ORDER BY observed_at'))
    seen={(r['source'],r['hash']):r['last_seen'] for r in conn.execute('SELECT source,hash,last_seen FROM raw_records')}
    conn.execute('DELETE FROM raw_records')
    for record in versions:
        observed=seen.get((record['source'],record['hash']),record['observed_at'])
        store_record(conn,BY_NAME[record['source']],json.loads(record['payload']),observed)
    conn.commit()
    return rebuild(conn)

def enrich_properties(conn, limit=500):
    candidates=[]
    for r in conn.execute('SELECT payload FROM projects'):
        p=json.loads(r[0])
        if p['bbl'] and ('commercial' in p['feeds'] or 'restaurant' in p['feeds']) and not p['property']:
            candidates.append(p['bbl'])
    wanted=sorted(set(candidates))[:limit]
    count=0
    for start in range(0,len(wanted),75):
        batch=wanted[start:start+75]
        rows=query(PLUTO,**{'$where':"bbl in ("+','.join(batch)+')',
          '$select':'bbl,address,borough,landuse,bldgclass,bldgarea,comarea,retailarea,officearea,yearbuilt,ownername,version', '$limit':1000})
        for row in rows:
            row['source_url']=source_record_url(PLUTO,row)
            conn.execute('INSERT OR REPLACE INTO properties VALUES (?,?,?)',(str(row['bbl']).split('.')[0],dumps(row),now()))
            count+=1
        conn.commit()
    put_state(conn,'pluto',{'name':PLUTO.label,'url':PLUTO.url,'status':'ok','last_success':now(),
                          'fetched':count,'coverage':'Targeted property matches; not a citywide download.'})
    rebuild(conn)
    return count

def load_projects(conn):
    reviews={r['project_id']:dict(r) for r in conn.execute('SELECT * FROM reviews')}
    result=[]
    for row in conn.execute('SELECT payload FROM projects'):
        p=json.loads(row[0]);r=reviews.get(p['id'])
        if r and r['project_hash']==p['evidence_hash']:
            p['review_status']='Reviewed' if r['verdict']=='correct' else 'Needs correction'
            p['review']=r
        elif r:
            p['review_status']='Evidence changed'
        result.append(p)
    return sorted(result,key=lambda p:(p['latest_date'],p['id']),reverse=True)

def filtered(projects, params):
    search=text(params.get('q','')).casefold()
    feed=params.get('feed','all');boro=params.get('borough','all');review=params.get('review','all');priority=params.get('priority','all');service=text(params.get('service','all')).casefold()
    since=params.get('since','');until=params.get('until','')
    items=[]
    for p in projects:
        if feed!='all' and feed not in p['feeds']:continue
        if boro!='all' and boro!=p['borough']:continue
        if review!='all' and review!=p['review_status']:continue
        if priority!='all' and priority!=p.get('priority_band','low'):continue
        if service!='all' and service not in {text(t).casefold() for t in p.get('trades',[])}:continue
        if since and p['latest_date']<since:continue
        if until and p['latest_date']>until:continue
        if search and search not in (p['address']+' '+p['description']+' '+p['job']+' '+' '.join(x['name'] for x in p['businesses'])).casefold():continue
        items.append(p)
    return items

def csv_safe(value):
    value=str(value or '')
    return "'"+value if value.lstrip().startswith(('=','+','-','@','\t','\r')) else value

def write_csv(projects, stream):
    fields=['project_id','address','borough','latest_source_date','recorded_status','feeds','business_roles',
            'priority_band','priority_score','priority_reasons','recommended_action','description','review_status','limitations','source_urls']
    writer=csv.writer(stream);writer.writerow(fields)
    for p in projects:
        writer.writerow([csv_safe(v) for v in [p['id'],p['address'],p['borough'],p['latest_date'],p['status'],
          '; '.join(p['feeds']),'; '.join(x['role']+': '+x['name'] for x in p['businesses']),p['priority_band'],p['priority_score'],
          '; '.join(p['priority_reasons']),p['recommended_action'],p['description'],
          p['review_status'],'; '.join(p['flags']),'; '.join(dict.fromkeys(e['source_url'] for e in p['events']))]])

def export(conn, target):
    target=Path(target);target.mkdir(parents=True,exist_ok=True)
    projects=load_projects(conn)
    for feed in ('all','restaurant','commercial','building'):
        items=projects if feed=='all' else [p for p in projects if feed in p['feeds']]
        with (target/f'{feed}.csv').open('w',newline='') as stream:write_csv(items,stream)
    with (target/'leads.csv').open('w',newline='') as stream:
        write_csv([p for p in projects if p.get('priority_band')=='high'],stream)
    (target/'projects.json').write_text(dumps(projects))
    (target/'sources.json').write_text(json.dumps(get_state(conn),indent=2))
    import sqlite3
    destination=sqlite3.connect(target/'signals.sqlite3')
    with destination:conn.backup(destination)
    destination.close()
    manifest={'created_at':now(),'projects':len(projects),'files':{}}
    for path in sorted(target.iterdir()):
        if path.is_file() and path.name!='manifest.json':
            import hashlib
            manifest['files'][path.name]={'bytes':path.stat().st_size,'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
    (target/'manifest.json').write_text(json.dumps(manifest,indent=2))
    return manifest
