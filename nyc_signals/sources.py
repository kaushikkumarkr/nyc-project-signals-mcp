from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass

@dataclass(frozen=True)
class Source:
    name: str
    label: str
    host: str
    dataset: str
    date_field: str
    id_fields: tuple
    legacy_date: bool = False
    snapshot: bool = False

    @property
    def url(self):
        return f'https://{self.host}/d/{self.dataset}'

    @property
    def date_expression(self):
        f = self.date_field
        if self.legacy_date:
            return f"substring({f},7,4) || '-' || substring({f},1,2) || '-' || substring({f},4,2)"
        return f

SOURCES = [
    Source('dob_applications','DOB NOW applications','data.cityofnewyork.us','w9ak-ipjd','current_status_date',('job_filing_number',)),
    Source('dob_permits','DOB NOW permits','data.cityofnewyork.us','rbx6-tga4','issued_date',('tracking_number','work_permit','sequence_number','work_type','issued_date')),
    Source('legacy_applications','Legacy DOB applications','data.cityofnewyork.us','ic3t-wcy2','latest_action_date',('job_s1_no',),True),
    Source('legacy_permits','Legacy DOB permits','data.cityofnewyork.us','ipu4-2q9a','issuance_date',('permit_si_no',),True),
    Source('occupancy','Certificates of occupancy','data.cityofnewyork.us','pkdm-hqz6','submitted_date',('application_number','c_of_o_sequence')),
    Source('liquor','Pending liquor applications','data.ny.gov','f8i8-k2gm','received_date',('application_id',),snapshot=True),
]
BY_NAME = {s.name:s for s in SOURCES}
PLUTO = Source('pluto','PLUTO property context','data.cityofnewyork.us','64uk-42ks','',('bbl',))

def fetch_json(url, *, timeout=35, attempts=3):
    headers = {'User-Agent':'NYCProjectSignals/1.0 (public-record research)', 'Accept':'application/json'}
    token = os.environ.get('SOCRATA_APP_TOKEN')
    if token:
        headers['X-App-Token'] = token
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(urllib.request.Request(url,headers=headers),timeout=timeout) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            if error.code not in (429,500,502,503,504) or attempt == attempts-1:
                # Never include request credentials in errors.
                detail = error.read(600).decode(errors='replace')
                raise RuntimeError(f'HTTP {error.code}: {detail}') from None
            retry = error.headers.get('Retry-After','')
            time.sleep(min(int(retry) if retry.isdigit() else 2**attempt, 15))
        except (urllib.error.URLError,TimeoutError):
            if attempt == attempts-1:
                raise
            time.sleep(2**attempt)

def query(source, **params):
    url = f'https://{source.host}/resource/{source.dataset}.json?'+urllib.parse.urlencode(params)
    return fetch_json(url)

def metadata(source):
    raw = fetch_json(f'https://{source.host}/api/views/{source.dataset}.json')
    return {'name':raw.get('name'), 'rows_updated_at':raw.get('rowsUpdatedAt'),
            'attribution':raw.get('attribution'), 'license_id':raw.get('licenseId'),
            'fields':{c['fieldName']:c.get('dataTypeName') for c in raw.get('columns',[]) if not c['fieldName'].startswith(':')}}

def window_filter(source, since):
    if source.snapshot:
        return "premises_county in ('New York','Kings','Queens','Bronx','Richmond')"
    return f"{source.date_expression} >= '{since}'"

def source_record_url(source, row):
    clauses = []
    for field in source.id_fields:
        if row.get(field):
            value = str(row[field]).replace("'","''")
            clauses.append(f"{field} = '{value}'")
    if not clauses:
        return source.url
    return f'https://{source.host}/resource/{source.dataset}.json?'+urllib.parse.urlencode({'$where':' AND '.join(clauses)})
