from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / 'data' / 'signals.sqlite3'
FEEDS = {'restaurant': 'Restaurant activity', 'commercial': 'Commercial renovations', 'building': 'Building activity'}
BOROUGHS = {'1': 'Manhattan', '2': 'Bronx', '3': 'Brooklyn', '4': 'Queens', '5': 'Staten Island',
            'MN': 'Manhattan', 'BX': 'Bronx', 'BK': 'Brooklyn', 'QN': 'Queens', 'SI': 'Staten Island',
            'NEW YORK': 'Manhattan', 'KINGS': 'Brooklyn', 'RICHMOND': 'Staten Island'}

def now():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')

def dumps(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'))

def digest(value):
    return hashlib.sha256(dumps(value).encode()).hexdigest()

def date(value):
    value = str(value or '').strip()
    if not value:
        return ''
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00')).date().isoformat()
    except ValueError:
        for fmt in ('%m/%d/%Y', '%m/%d/%y %I:%M:%S %p', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%y'):
            try:
                return datetime.strptime(value, fmt).date().isoformat()
            except ValueError:
                pass
    return ''

def borough(value):
    value = str(value or '').strip().upper()
    return BOROUGHS.get(value, value.title() if value in ('MANHATTAN','BRONX','BROOKLYN','QUEENS','STATEN ISLAND') else '')

def text(value):
    return re.sub(r'\s+', ' ', str(value or '')).strip()

def business(value):
    value = text(value)
    return '' if value.upper() in ('', 'N/A', 'NA', 'NONE', 'PR', 'PRIVATE', 'INDIVIDUAL', 'N.A.', 'HOMEOWNER', 'OWNER') else value

def job_root(value):
    value = text(value).upper()
    m = re.match(r'^([MBQXS]\d{8}|\d{9})(?:-|$)', value)
    return m.group(1) if m else ''

def address_key(value):
    value = re.sub(r'[^A-Z0-9\s-]', '', text(value).upper())
    for full, short in [('STREET','ST'),('AVENUE','AVE'),('BOULEVARD','BLVD'),('ROAD','RD'),('PLACE','PL')]:
        value = re.sub(r'\b'+full+r'\b',short,value)
    return value

def connect(path=DB):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA foreign_keys=ON')
    conn.executescript('''
    CREATE TABLE IF NOT EXISTS raw_records (
      source TEXT NOT NULL, record_key TEXT NOT NULL, payload TEXT NOT NULL,
      hash TEXT NOT NULL, first_seen TEXT NOT NULL, last_seen TEXT NOT NULL,
      PRIMARY KEY(source,record_key));
    CREATE TABLE IF NOT EXISTS record_versions (
      source TEXT, record_key TEXT, hash TEXT, observed_at TEXT, payload TEXT,
      PRIMARY KEY(source,record_key,hash));
    CREATE TABLE IF NOT EXISTS source_state (source TEXT PRIMARY KEY, payload TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS projects (id TEXT PRIMARY KEY, payload TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS properties (bbl TEXT PRIMARY KEY, payload TEXT NOT NULL, fetched_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS reviews (
      project_id TEXT PRIMARY KEY, project_hash TEXT NOT NULL, reviewer TEXT NOT NULL,
      kind TEXT NOT NULL, verdict TEXT NOT NULL, expected_feeds TEXT NOT NULL,
      notes TEXT NOT NULL, reviewed_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS ai_reviews (
      project_id TEXT, project_hash TEXT, payload TEXT, created_at TEXT,
      PRIMARY KEY(project_id,project_hash));
    CREATE TABLE IF NOT EXISTS ai_calls (
      id INTEGER PRIMARY KEY, created_at TEXT NOT NULL, status TEXT NOT NULL,
      reserved_usd REAL NOT NULL, actual_usd REAL, input_tokens INTEGER, output_tokens INTEGER);
    CREATE TABLE IF NOT EXISTS runs (id INTEGER PRIMARY KEY, started_at TEXT, finished_at TEXT, payload TEXT);
    ''')
    return conn

def get_state(conn):
    return {r['source']: json.loads(r['payload']) for r in conn.execute('SELECT * FROM source_state')}

def put_state(conn, name, value):
    conn.execute('INSERT OR REPLACE INTO source_state VALUES (?,?)', (name,dumps(value)))
    conn.commit()

def classify(description, work_type='', building_type='', is_license=False):
    """Conservative, explainable candidate classification; never a buying-intent prediction."""
    value = text(description).lower()
    evidence = []
    restaurant = re.search(r'\b(restaurant|cafe|cafeteria|coffee shop|bar|tavern|pizzeria|bakery|food service|commercial kitchen|eating and drinking)\b',value)
    commercial = re.search(r'\b(retail|storefront|store|commercial|office|restaurant|cafe|hotel|warehouse|medical|clinic|school|salon|gym|daycare|bank|supermarket|grocery)\b',value)
    activity = re.search(r'\b(renovat\w*|alteration\w*|fit[ -]?out|build[ -]?out|interior|install\w*|replac\w*|remodel\w*)\b',value)
    sign = re.search(r'\b(signage|illuminated sign|storefront sign|awning|business sign)\b',value)
    negative = re.search(r'\b(no|not|without)\s+(?:new\s+)?(?:restaurant|commercial|retail|office)\b',value)
    feeds = []
    if not is_license:
        feeds.append('building')
        evidence.append('DOB filing or permit records building-related activity; current purchasing needs are unknown.')
    if restaurant and not negative:
        feeds.append('restaurant')
        evidence.append(f'Source description mentions “{restaurant.group()}”; this may describe an existing venue.')
    if commercial and activity and not negative and not is_license:
        feeds.append('commercial')
        evidence.append(f'Description combines “{commercial.group()}” with “{activity.group()}”.')
    trades = []
    if 'commercial' in feeds:
        trades.append('Commercial cleaning')
    if sign or work_type.lower() == 'sign':
        trades.append('Signage')
    if 'restaurant' in feeds:
        trades.extend(['Restaurant equipment','Hospitality services'])
    if 'building' in feeds:
        trades.append('Building services')
    return {'feeds':feeds,'reasons':evidence,'trades':sorted(set(trades)), 'method':'rules-v1',
            'classification_status':'Candidate — review required'}

def lead_priority(project):
    """Rank research work by observable evidence; this is not a conversion probability."""
    score = 0
    reasons = []
    feeds = set(project.get('feeds', []))
    text_value = text(project.get('description', '')).lower()
    if 'restaurant' in feeds:
        score += 25; reasons.append('Restaurant-related activity is explicitly mentioned.')
    if 'commercial' in feeds:
        score += 20; reasons.append('Commercial renovation or fit-out activity is present.')
    if re.search(r'\b(proposed new|new restaurant|new retail|fit[ -]?out|build[ -]?out|tenant retrofit|change use)\b', text_value):
        score += 20; reasons.append('Description contains a proposed, new-use, fit-out, or tenant-retrofit signal.')
    if project.get('businesses'):
        score += 15; reasons.append('A published business role is attached to the record.')
    if project.get('property'):
        score += 10; reasons.append('Property context is available for the tax lot.')
    if project.get('latest_date'):
        try:
            age=(datetime.now(timezone.utc).date()-datetime.fromisoformat(project['latest_date']).date()).days
            if age <= 30:
                score += 10; reasons.append('Latest source event is within 30 days.')
            elif age > 90:
                score -= 10; reasons.append('Latest source event is older than 90 days.')
        except ValueError:
            pass
    if project.get('source_names') == ['liquor'] or (project.get('events') and all(e.get('is_license') for e in project['events'])):
        score -= 25; reasons.append('License-only evidence is lower priority until construction evidence appears.')
    if not project.get('businesses'):
        score -= 10; reasons.append('No business identity is published in the records.')
    if re.search(r'withdraw|revok|cancel|disapprov', str(project.get('status','')), re.I):
        score -= 25; reasons.append('Latest status indicates withdrawal, cancellation, or rejection.')
    score=max(0,min(100,score))
    band='high' if score>=60 else 'medium' if score>=35 else 'low'
    action='Verify business and project status before outreach.' if band!='low' else 'Monitor for a stronger or newer signal.'
    return {'priority_score':score,'priority_band':band,'priority_reasons':reasons,'recommended_action':action}
