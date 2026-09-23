from __future__ import annotations

import csv
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .core import FEEDS, digest, dumps, get_state, now
from .pipeline import load_projects

def sample(conn, target, size=100):
    projects=load_projects(conn)
    # Stable sample across all three published feeds, not just easy positives.
    population=[p for p in projects if p['feeds']]
    rng=random.Random(20260923)
    chosen=[];seen=set()
    for feed in FEEDS:
        pool=[p for p in population if feed in p['feeds'] and p['id'] not in seen]
        part=rng.sample(pool,min(size//3,len(pool)))
        chosen.extend(part);seen.update(p['id'] for p in part)
    remaining=[p for p in population if p['id'] not in seen]
    chosen.extend(rng.sample(remaining,min(max(0,size-len(chosen)),len(remaining))))
    path=Path(target);path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('w',newline='') as out:
        writer=csv.DictWriter(out,fieldnames=['project_id','project_hash','address','description','predicted_feeds','source_urls',
            'verdict','expected_feeds','reviewer','kind','notes'])
        writer.writeheader()
        for p in chosen:
            writer.writerow({'project_id':p['id'],'project_hash':p['evidence_hash'],'address':p['address'],
              'description':p['description'],'predicted_feeds':';'.join(p['feeds']),
              'source_urls':'\n'.join(dict.fromkeys(e['source_url'] for e in p['events'])),'kind':'human'})
    return len(chosen)

def import_reviews(conn, path):
    projects={p['id']:p for p in load_projects(conn)}
    entries=[]
    with open(path,newline='') as stream:
        for row in csv.DictReader(stream):
            pid=row['project_id'];p=projects.get(pid)
            if not p or row['project_hash']!=p['evidence_hash']:
                raise ValueError(f'Stale or unknown project: {pid}; generate a fresh review sample.')
            if row['verdict'] not in ('correct','incorrect') or row['kind'] not in ('human','agent') or not row['reviewer'].strip():
                raise ValueError(f'{pid}: verdict, reviewer and kind are required.')
            expected=sorted(set(x.strip() for x in row['expected_feeds'].split(';') if x.strip()))
            if not set(expected)<=set(FEEDS):raise ValueError(f'{pid}: invalid expected feed.')
            if (row['verdict']=='correct') != (expected==sorted(p['feeds'])):
                raise ValueError(f'{pid}: verdict disagrees with expected feeds.')
            entries.append((pid,p['evidence_hash'],row['reviewer'],row['kind'],row['verdict'],dumps(expected),row['notes'],now()))
    with conn:conn.executemany('INSERT OR REPLACE INTO reviews VALUES (?,?,?,?,?,?,?,?)',entries)
    return len(entries)

def report(conn):
    projects=load_projects(conn);states=get_state(conn)
    recent=(datetime.now(timezone.utc)-timedelta(days=30)).date().isoformat()
    reviewed=[p for p in projects if p.get('review')]
    human=[p for p in reviewed if p['review']['kind']=='human']
    correct=sum(p['review']['verdict']=='correct' for p in human)
    accuracy=correct/len(human) if human else None
    evidence_issues=[p['id'] for p in projects if not p['events'] or any(not e.get('source_url') for e in p['events'])]
    stale=[]
    for name,state in states.items():
        updated=state.get('metadata',{}).get('rows_updated_at')
        if state.get('status')!='ok' or (updated and (datetime.now(timezone.utc).timestamp()-updated)>7*86400):stale.append(name)
    feeds={}
    for name in FEEDS:
        items=[p for p in projects if name in p['feeds']]
        humans=[p for p in items if p.get('review',{}).get('kind')=='human']
        feed_accuracy=sum(p['review']['verdict']=='correct' for p in humans)/len(humans) if humans else None
        fresh=sum(p['latest_date']>=recent for p in items)
        reasons=[]
        if len(human)<100:reasons.append('100 current human-reviewed projects required across the product.')
        if len(humans)<30:reasons.append('30 reviewed examples required in this feed.')
        if feed_accuracy is None or feed_accuracy<.9:reasons.append('Measured human-review accuracy must reach 90%.')
        if fresh<30:reasons.append('Fewer than 30 distinct projects with source events in the last 30 days.')
        if stale:reasons.append('A source is stale or failed; confirm coverage before charging.')
        reasons.append('Payment-provider onboarding and source-reuse review are not yet completed.')
        feeds[name]={'label':FEEDS[name],'projects':len(items),'recent_projects':fresh,'reviewed':len(humans),
                     'accuracy':feed_accuracy,'launch_ready':False,'gates':reasons}
    return {'generated_at':now(),'projects':len(projects),'raw_records':conn.execute('SELECT count(*) FROM raw_records').fetchone()[0],
        'record_versions':conn.execute('SELECT count(*) FROM record_versions').fetchone()[0],
        'duplicate_project_ids':len(projects)-len(set(p['id'] for p in projects)),
        'projects_missing_business':sum(not p['businesses'] for p in projects),
        'human_reviews':len(human),'agent_reviews':len(reviewed)-len(human),'human_accuracy':accuracy,
        'evidence_issues':evidence_issues,'stale_or_failed_sources':stale,
        'feeds':feeds,'sources':states,'status':'Research preview',
        'limitations':['Rule-based feed labels are candidates, not confirmed purchasing needs.',
          'Completeness applies to the configured query window and row limit, not all NYC projects.',
          'Electrical, elevator and LAA specialist feeds and legacy occupancy data are not ingested.',
          'DOB status-change dates and permit issue dates represent different types of activity.',
          'Pending license records disappearing from the source are archived, not assumed approved.',
          'Business roles are recorded as published; they are not independently verified contacts.']}

def write_report(conn, path):
    result=report(conn)
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(result,indent=2))
    return result

