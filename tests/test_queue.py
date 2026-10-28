import os
import unittest
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
import db
from extraction import FIELDS, MODEL, VERSION


@unittest.skipUnless(os.getenv('QUEUE_INTEGRATION_TEST') == 'disposable-mysql', 'Needs disposable MySQL')
class QueueTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.environ['DB_HOST'] != 'mysql-test':
            raise RuntimeError('Tests only run against mysql-test')
        with db.transaction() as cur:
            cur.execute('CREATE DATABASE IF NOT EXISTS ai')
            cur.execute(f'''CREATE TABLE IF NOT EXISTS {db.TABLE} (
                id VARCHAR(768) PRIMARY KEY, s3_uri TEXT, model VARCHAR(255),
                original_answer VARCHAR(255)) ENGINE=InnoDB''')
        db.initialize()
        db.initialize()

    def setUp(self):
        with db.transaction() as cur:
            cur.execute('DROP TRIGGER IF EXISTS ai.reject_success')
            cur.execute(f'DELETE FROM {db.QUEUE}')
            cur.execute(f'DELETE FROM {db.TABLE}')

    def add(self, name, partial=False, model=MODEL, completed=False):
        with db.transaction() as cur:
            cur.execute(f'''INSERT INTO {db.TABLE}
                (id,s3_uri,model,original_answer,uniondues2_ai,uniondues_version,
                 uniondues_model,text_char_count,uniondues_no_reason_ai)
                VALUES (%s,%s,%s,'preserve',%s,%s,%s,%s,%s)''',
                (name, name+'.pdf', model, 'No' if partial else None,
                 VERSION if completed else ('union-dues-v1' if partial else None),
                 MODEL if completed else None, 42 if completed else None, 'NA' if completed else None))

    def test_seed_scope_priority_and_idempotency(self):
        self.add('partial', partial=True)
        self.add('new')
        self.add('qwen', model='qwen/other')
        self.add('done', completed=True)
        self.assertEqual(db.seed(), 2)
        self.assertEqual(db.seed(), 0)
        self.assertEqual(db.claim('worker', 300, 3)['target_id'], 'partial')
        self.assertEqual(db.snapshot()['counts'], {'pending':1,'running':1})
        self.assertEqual(db.snapshot()['unqueued'], 0)

    def test_concurrent_claims(self):
        for i in range(12): self.add(str(i))
        db.seed()
        barrier = Barrier(6)
        def claim(_):
            barrier.wait(timeout=10)
            return db.claim('worker',300,3)
        with ThreadPoolExecutor(max_workers=6) as pool:
            jobs = list(pool.map(claim,range(6)))
        self.assertTrue(all(jobs))
        self.assertEqual(len({j['job_id'] for j in jobs}),6)
        self.assertEqual(db.snapshot()['counts'],{'pending':6,'running':6})

    def test_stale_worker_is_fenced(self):
        self.add('one'); db.seed()
        old = db.claim('old',300,3)
        with db.transaction() as cur:
            cur.execute(f"UPDATE {db.QUEUE} SET lease_until=CURRENT_TIMESTAMP(6)-INTERVAL 1 SECOND")
        db.reap(3)
        new = db.claim('new',300,3)
        self.assertNotEqual(old['claim_token'],new['claim_token'])
        for op in (lambda: db.complete(old,dict.fromkeys(FIELDS,'NA'),10),
                   lambda: db.fail(old,'error',10,3,10), lambda: db.renew(old,300)):
            with self.assertRaises(db.LeaseLost): op()
        db.complete(new,dict.fromkeys(FIELDS,'NA'),20)
        with db.transaction() as cur:
            cur.execute(f'SELECT text_char_count,original_answer FROM {db.TABLE}')
            self.assertEqual(cur.fetchone(),dict(text_char_count=20,original_answer='preserve'))
        self.assertEqual(db.snapshot()['counts'],{'succeeded':1})

    def test_atomic_commit(self):
        self.add('one'); db.seed()
        job=db.claim('worker',300,3)
        with db.transaction() as cur:
            cur.execute(f"""CREATE TRIGGER ai.reject_success BEFORE UPDATE ON {db.QUEUE}
                FOR EACH ROW BEGIN IF NEW.status='succeeded' THEN
                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='injected failure';
                END IF; END""")
        with self.assertRaises(Exception): db.complete(job,dict.fromkeys(FIELDS,'NA'),50)
        with db.transaction() as cur:
            cur.execute(f'SELECT text_char_count FROM {db.TABLE}')
            self.assertIsNone(cur.fetchone()['text_char_count'])
            cur.execute('DROP TRIGGER ai.reject_success')
        self.assertEqual(db.snapshot()['counts'],{'running':1})
        db.complete(job,dict.fromkeys(FIELDS,'NA'),50)

    def test_retry_limit_and_manual_retry(self):
        self.add('one'); db.seed()
        job=db.claim('worker',300,1)
        db.fail(job,'InvalidObjectState',100,1,300)
        self.assertIsNone(db.claim('other',300,1))
        self.assertEqual(db.snapshot()['counts'],{'failed':1})
        self.assertEqual(db.retry_failed(),1)
        self.assertEqual(db.claim('other',300,1)['attempts'],1)

    def test_heartbeat_delayed_retry(self):
        self.add('one'); db.seed()
        job=db.claim('worker',300,3)
        db.renew(job,600)
        db.fail(job,'RateLimitError',None,3,300)
        self.assertIsNone(db.claim('other',300,3))
        with db.transaction() as cur:
            cur.execute(f"UPDATE {db.QUEUE} SET available_at=CURRENT_TIMESTAMP(6)-INTERVAL 1 SECOND")
        self.assertEqual(db.claim('other',300,3)['attempts'],2)

    def test_required_tls(self):
        from contextlib import closing
        from unittest.mock import patch
        with patch.dict(os.environ, {'DB_SSLMODE': 'require'}):
            with closing(db.connect()) as conn:
                with conn.cursor() as cur:
                    cur.execute("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
                    self.assertTrue(cur.fetchone()['Value'])

    def test_expired_claim_cannot_renew_or_complete(self):
        self.add('expired'); db.seed()
        job = db.claim('worker', 300, 3)
        with db.transaction() as cur:
            cur.execute(f"UPDATE {db.QUEUE} SET lease_until=CURRENT_TIMESTAMP(6)-INTERVAL 1 SECOND")
        with self.assertRaises(db.LeaseLost):
            db.renew(job, 300)
        with self.assertRaises(db.LeaseLost):
            db.complete(job, dict.fromkeys(FIELDS, 'NA'), 50)

    def test_long_source_id(self):
        target = 'x' * 768
        self.add(target)
        self.assertEqual(db.seed(), 1)
        self.assertEqual(db.seed(), 0)
        job = db.claim('worker', 300, 3)
        self.assertEqual(job['target_id'], target)
        db.complete(job, dict.fromkeys(FIELDS, 'NA'), 123)
        self.assertEqual(db.snapshot()['counts'], {'succeeded': 1})

    def test_seed_batch_progress_after_commit(self):
        for i in range(5):
            self.add('batch-' + str(i))
        events = []
        def progress(**values):
            events.append(values)
            self.assertEqual(db.snapshot()['counts']['pending'], values['inserted'])
        self.assertEqual(db.seed(page_size=2, progress=progress), 5)
        self.assertEqual(events, [
            {'scanned': 2, 'inserted': 2},
            {'scanned': 4, 'inserted': 4},
            {'scanned': 5, 'inserted': 5}])
        self.assertEqual(db.seed(page_size=2), 0)
