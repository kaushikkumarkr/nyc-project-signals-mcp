import csv
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from nyc_signals.core import connect, date, job_root, classify, dumps, now, put_state, lead_priority
from nyc_signals.sources import BY_NAME, window_filter
from nyc_signals.pipeline import store_record, rebuild, load_projects, write_csv, export, normalized, filtered, ingest
from nyc_signals.quality import report, sample, import_reviews

class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.conn=connect(Path(self.temp.name)/'test.sqlite3')
    def tearDown(self):
        self.conn.close();self.temp.cleanup()
    def add(self,name,row,observed=None):
        source=BY_NAME[name]
        store_record(self.conn,source,row,observed or now());self.conn.commit()
    def application(self,job='M00123456-I1',description='Interior renovation of existing restaurant.'):
        return {'job_filing_number':job,'house_no':'100','street_name':'BROADWAY','borough':'MANHATTAN',
                'job_description':description,'current_status_date':'2026-09-20T00:00:00.000','bin':'1234567',
                'block':'1','lot':'20','applicant_business_name':'Example Architecture LLC','filing_status':'Approved'}
    def test_dates_and_identifiers(self):
        self.assertEqual(date('09/20/2026'),'2026-09-20')
        self.assertEqual(date('02/15/22 11:08:46 AM'),'2022-02-15')
        self.assertEqual(date('invalid'),'')
        self.assertEqual(job_root('M00123456-I1-GC'),'M00123456')
        self.assertEqual(job_root('Permit is not issued'),'')
        self.assertIn('substring(issuance_date,7,4)',window_filter(BY_NAME['legacy_permits'],'2026-06-01'))
    def test_idempotence_and_version_history(self):
        row=self.application();self.add('dob_applications',row);self.add('dob_applications',row)
        self.assertEqual(self.conn.execute('SELECT count(*) FROM raw_records').fetchone()[0],1)
        self.assertEqual(self.conn.execute('SELECT count(*) FROM record_versions').fetchone()[0],1)
        row['filing_status']='Permit Issued';self.add('dob_applications',row)
        self.assertEqual(self.conn.execute('SELECT count(*) FROM record_versions').fetchone()[0],2)
    def test_multiple_work_types_preserved(self):
        base={'job_filing_number':'M00123456-I1','tracking_number':'42','work_permit':'M00123456-I1-GC',
              'sequence_number':'1','issued_date':'2026-09-20T00:00:00.000'}
        self.add('dob_permits',{**base,'work_type':'General Construction'})
        self.add('dob_permits',{**base,'work_type':'Mechanical Systems'})
        self.assertEqual(self.conn.execute('SELECT count(*) FROM raw_records').fetchone()[0],2)
        rebuild(self.conn);self.assertEqual(len(load_projects(self.conn)[0]['events']),2)
    def test_projects_join_by_job_not_address(self):
        self.add('dob_applications',self.application())
        self.add('dob_applications',self.application('M00123456-I2'))
        self.add('dob_applications',self.application('M00999999-I1'))
        rebuild(self.conn);projects=load_projects(self.conn)
        self.assertEqual(len(projects),2)
        self.assertEqual(len(projects[0]['related_projects']),1)
        self.assertEqual(sorted(len(p['events']) for p in projects),[1,2])
    def test_business_roles_and_bbl(self):
        row=self.application();row['owner_s_business_name']='N/A';row['owner_first_name']='Private person'
        self.add('dob_applications',row);rebuild(self.conn);p=load_projects(self.conn)[0]
        self.assertEqual(p['bbl'],'1000010020')
        self.assertEqual(p['businesses'][0]['role'],'Filing applicant business')
        self.assertNotIn('Private person',dumps(p))
    def test_classification_does_not_invent_openings(self):
        c=classify('Renovation of an existing restaurant. No change in use.')
        self.assertEqual(set(c['feeds']),{'restaurant','commercial','building'})
        self.assertIn('existing venue',' '.join(c['reasons']))
        self.assertEqual(classify('Replace kitchen fixtures in a one family home.')['feeds'],['building'])
        self.assertEqual(classify('Grocery Store',is_license=True)['feeds'],[])
        self.assertEqual(classify('Restaurant Wine',is_license=True)['feeds'],['restaurant'])
    def test_pending_snapshot_reconciliation(self):
        row={'application_id':'OLD','premises_county':'Kings','received_date':'2026-09-01','description':'Restaurant'}
        self.add('liquor',row,'2026-09-01T00:00:00+00:00')
        put_state(self.conn,'liquor',{'snapshot_at':'2026-09-01T00:00:00+00:00','status':'error'})
        rebuild(self.conn);self.assertEqual(len(load_projects(self.conn)),1)
        put_state(self.conn,'liquor',{'snapshot_at':'2026-09-02T00:00:00+00:00','status':'ok'})
        rebuild(self.conn);self.assertEqual(len(load_projects(self.conn)),0)
        self.assertEqual(self.conn.execute('SELECT count(*) FROM raw_records').fetchone()[0],1)
    def test_failed_source_keeps_previous_data(self):
        self.add('dob_applications',self.application())
        with patch('nyc_signals.pipeline.metadata',side_effect=RuntimeError('Network unavailable')):
            result=ingest(self.conn,names=['dob_applications'])
        self.assertEqual(result['sources']['dob_applications']['status'],'error')
        self.assertEqual(len(load_projects(self.conn)),1)
    def test_csv_formula_injection_and_restore(self):
        row=self.application();row['house_no']='=HYPERLINK("evil")';self.add('dob_applications',row);rebuild(self.conn)
        stream=io.StringIO();write_csv(load_projects(self.conn),stream)
        values=list(csv.reader(io.StringIO(stream.getvalue())))
        self.assertTrue(values[1][1].startswith("'="))
        out=Path(self.temp.name)/'export';export(self.conn,out)
        restored=connect(out/'signals.sqlite3')
        self.assertEqual(len(load_projects(restored)),1);restored.close()
        self.assertTrue((out/'manifest.json').exists())
    def test_filters(self):
        self.add('dob_applications',self.application());rebuild(self.conn)
        projects=load_projects(self.conn)
        self.assertEqual(len(filtered(projects,{'feed':'restaurant','borough':'Manhattan','q':'broadway'})),1)
        self.assertEqual(len(filtered(projects,{'borough':'Queens'})),0)
    def test_quality_cannot_claim_unmeasured_accuracy(self):
        self.add('dob_applications',self.application());rebuild(self.conn)
        q=report(self.conn);self.assertIsNone(q['human_accuracy'])
        self.assertFalse(q['feeds']['restaurant']['launch_ready'])

    def test_lead_priority_is_explainable_and_not_probability(self):
        project={'feeds':['restaurant','commercial'],'description':'Proposed new restaurant fit-out.',
                 'businesses':[{'role':'Filing applicant business','name':'Example LLC'}],
                 'property':{'bldgarea':'1000'},'latest_date':'2026-09-20','source_names':['dob_applications'],
                 'status':'Approved'}
        result=lead_priority(project)
        self.assertEqual(result['priority_band'],'high')
        self.assertIn('fit-out', ' '.join(result['priority_reasons']))
        self.assertNotIn('probability', result['recommended_action'].lower())
    def test_reviews_invalidated_after_source_change(self):
        self.add('dob_applications',self.application());rebuild(self.conn)
        path=Path(self.temp.name)/'review.csv';sample(self.conn,path,1)
        with path.open() as f:rows=list(csv.DictReader(f))
        rows[0].update(verdict='correct',expected_feeds=rows[0]['predicted_feeds'],reviewer='QA fixture',kind='agent')
        with path.open('w',newline='') as f:
            writer=csv.DictWriter(f,fieldnames=rows[0]);writer.writeheader();writer.writerows(rows)
        import_reviews(self.conn,path)
        self.assertEqual(load_projects(self.conn)[0]['review_status'],'Reviewed')
        self.assertEqual(report(self.conn)['human_reviews'],0)
        row=self.application();row['job_description']='Interior apartment renovation';self.add('dob_applications',row);rebuild(self.conn)
        self.assertEqual(load_projects(self.conn)[0]['review_status'],'Evidence changed')

if __name__=='__main__':unittest.main()
