"""MySQL job queue: short claims, renewable leases and fenced commits."""
from contextlib import contextmanager, closing
import hashlib
import os
import ssl
from urllib.parse import urlsplit, unquote
import uuid

from extraction import TABLE, MODEL, VERSION, FIELDS

QUEUE = 'ai.union_dues_jobs'
COLUMNS = {**{k: 'varchar(64)' for k in FIELDS}, 'uniondues_no_reason_ai': 'text',
    'text_char_count': 'bigint', 'uniondues_model': 'varchar(255)',
    'uniondues_version': 'varchar(64)', 'uniondues_updated_at': 'datetime(6)',
    'uniondues_status': 'varchar(16)', 'uniondues_error': 'varchar(255)',
    'uniondues_attempted_at': 'datetime(6)'}
PENDING = """(uniondues_version IS NULL OR uniondues_version <> %s
    OR uniondues_model IS NULL OR uniondues_model <> %s
    OR text_char_count IS NULL OR uniondues_no_reason_ai IS NULL)"""
DDL = f'''CREATE TABLE IF NOT EXISTS {QUEUE} (
    job_id char(64) CHARACTER SET ascii PRIMARY KEY, target_id varchar(768) COLLATE utf8mb4_bin NOT NULL, s3_uri text NOT NULL,
    version varchar(64) NOT NULL, priority smallint NOT NULL,
    status varchar(16) NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending','running','succeeded','failed')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    owner varchar(128), claim_token char(32) CHARACTER SET ascii, lease_until datetime(6),
    available_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    last_error varchar(255), created_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), completed_at datetime(6),
    INDEX union_dues_claim_idx (version,status,priority,available_at,job_id),
    INDEX union_dues_lease_idx (version,status,lease_until)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin'''


class ConfigurationError(RuntimeError):
    """A credential-free setup error safe to display to the operator."""
    pass


class LeaseLost(RuntimeError):
    pass


def connect():
    import pymysql
    options = dict(host=os.getenv('DB_HOST'), port=int(os.getenv('DB_PORT', '3306')),
        database=os.getenv('DB_NAME'), user=os.getenv('DB_USER'), password=os.getenv('DB_PASSWORD'))
    if os.getenv('DATABASE_URL'):
        url = urlsplit(os.environ['DATABASE_URL'])
        if url.scheme not in ('mysql', 'mysql+pymysql') or url.query or not url.hostname:
            raise ValueError('DATABASE_URL must be mysql://user:password@host:port/database without query options')
        options.update(host=url.hostname, port=url.port or 3306,
            user=unquote(url.username or ''), password=unquote(url.password or ''),
            database=unquote(url.path.lstrip('/')) or options['database'])
    mode = os.getenv('DB_SSLMODE', 'require').lower()
    if mode not in ('disable', 'require', 'verify-full'):
        raise ValueError('DB_SSLMODE must be disable, require, or verify-full')
    tls = None
    if mode != 'disable':
        tls = ssl.create_default_context(cafile=os.getenv('DB_SSL_CA'))
        if mode == 'require':
            tls.check_hostname = False
            tls.verify_mode = ssl.CERT_NONE
    conn = pymysql.connect(**options, charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor, autocommit=False,
        connect_timeout=15, read_timeout=30, write_timeout=30,
        ssl=tls, ssl_disabled=mode == 'disable')
    try:
        with conn.cursor() as cur:
            if mode != 'disable':
                cur.execute("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
                if not cur.fetchone()['Value']:
                    raise ConfigurationError('MySQL connection requires TLS')
            cur.execute("SET SESSION time_zone = '+00:00'")
            cur.execute('SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED')
            cur.execute('SET SESSION innodb_lock_wait_timeout = 10')
        return conn
    except BaseException:
        conn.close()
        raise


@contextmanager
def transaction():
    with closing(connect()) as conn:
        try:
            with conn.cursor() as cur:
                yield cur
            conn.commit()
        except BaseException:
            conn.rollback()
            raise


def preflight(conn):
    with conn.cursor() as cur:
        cur.execute('SELECT VERSION() AS version')
        version = cur.fetchone()['version']
        if 'mariadb' in version.lower() or int(version.split('.')[0]) < 8:
            raise ConfigurationError('MySQL 8.0+ is required')
        cur.execute("""SELECT ENGINE AS engine FROM information_schema.tables
            WHERE table_schema='ai' AND table_name='contratos_colectivos_v2'""")
        row = cur.fetchone()
        if not row or row['engine'] != 'InnoDB':
            raise ConfigurationError('ai.contratos_colectivos_v2 must exist and use InnoDB')
        cur.execute("""SELECT INDEX_NAME FROM information_schema.statistics
            WHERE table_schema='ai' AND table_name='contratos_colectivos_v2'
            AND non_unique=0 GROUP BY INDEX_NAME
            HAVING COUNT(*)=1 AND MAX(column_name)='id' AND MAX(sub_part) IS NULL""")
        if not cur.fetchone():
            raise ConfigurationError('Result table needs a full unique index on id')
        cur.execute("""SELECT DATA_TYPE AS kind, CHARACTER_MAXIMUM_LENGTH AS size
            FROM information_schema.columns WHERE table_schema='ai'
            AND table_name='contratos_colectivos_v2' AND column_name='id'""")
        row = cur.fetchone()
        if row['kind'] not in ('varchar', 'char') or row['size'] > 768:
            raise ConfigurationError('Result id must be CHAR/VARCHAR with at most 768 characters')


def check_schema(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COLUMN_NAME AS name FROM information_schema.columns WHERE table_schema='ai' AND table_name='contratos_colectivos_v2'")
        missing = set(COLUMNS) - {r['name'] for r in cur.fetchall()}
        if missing:
            raise ConfigurationError('Missing enrichment columns; run init: ' + ', '.join(sorted(missing)))
        cur.execute("SELECT ENGINE AS engine FROM information_schema.tables WHERE table_schema='ai' AND table_name='union_dues_jobs'")
        row = cur.fetchone()
        if not row or row['engine'] != 'InnoDB':
            raise ConfigurationError('InnoDB queue table is missing; run init')


def initialize():
    # MySQL DDL implicitly commits. Run once before starting workers.
    with closing(connect()) as conn:
        preflight(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT COLUMN_NAME AS name FROM information_schema.columns WHERE table_schema='ai' AND table_name='contratos_colectivos_v2'")
            existing = {r['name'] for r in cur.fetchall()}
            for name, kind in COLUMNS.items():
                if name not in existing:
                    cur.execute(f'ALTER TABLE {TABLE} ADD COLUMN {name} {kind}')
            cur.execute(DDL)
        check_schema(conn)


def job_key(target_id):
    return hashlib.sha256((VERSION + '\0' + MODEL + '\0' + target_id).encode()).hexdigest()


def seed(page_size=500, progress=None):
    after = None
    inserted = 0
    scanned = 0
    touched = ' OR '.join(f'{c} IS NOT NULL' for c in
        (*FIELDS, 'text_char_count', 'uniondues_version', 'uniondues_attempted_at', 'uniondues_status'))
    while True:
        with transaction() as cur:
            extra = 'AND id > %s' if after is not None else ''
            params = [MODEL, VERSION, MODEL]
            if after is not None:
                params.append(after)
            params.append(page_size)
            cur.execute(f'''SELECT id,s3_uri,CASE WHEN ({touched}) THEN 0 ELSE 1 END AS priority
                FROM {TABLE} WHERE model=%s AND s3_uri IS NOT NULL AND s3_uri <> ''
                AND {PENDING} {extra} ORDER BY id LIMIT %s''', params)
            rows = cur.fetchall()
            if rows:
                cur.executemany(f"""INSERT INTO {QUEUE} (job_id,target_id,s3_uri,version,priority)
                    VALUES (%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE job_id=job_id""",
                    [(job_key(row['id']), row['id'], row['s3_uri'], VERSION, row['priority'])
                     for row in rows])
                inserted += cur.rowcount
        scanned += len(rows)
        if rows and progress:
            progress(scanned=scanned, inserted=inserted)
        if not rows:
            return inserted
        after = rows[-1]['id']


def reap(max_attempts):
    with transaction() as cur:
        cur.execute(f'''SELECT job_id,attempts FROM {QUEUE} WHERE version=%s
            AND status='running' AND lease_until < CURRENT_TIMESTAMP(6)
            LIMIT 100 FOR UPDATE SKIP LOCKED''', (VERSION,))
        for row in cur.fetchall():
            status = 'failed' if row['attempts'] >= max_attempts else 'pending'
            cur.execute(f'''UPDATE {QUEUE} SET status=%s,owner=NULL,claim_token=NULL,
                lease_until=NULL,available_at=CURRENT_TIMESTAMP(6),updated_at=CURRENT_TIMESTAMP(6),
                last_error='LeaseExpired' WHERE job_id=%s''', (status,row['job_id']))


def claim(owner, lease_seconds, max_attempts):
    with transaction() as cur:
        cur.execute(f'''SELECT * FROM {QUEUE} WHERE version=%s AND status='pending'
            AND available_at <= CURRENT_TIMESTAMP(6) AND attempts < %s
            ORDER BY priority,available_at,job_id LIMIT 1 FOR UPDATE SKIP LOCKED''',
            (VERSION,max_attempts))
        job = cur.fetchone()
        if not job:
            return None
        token = uuid.uuid4().hex
        cur.execute(f'''UPDATE {QUEUE} SET status='running',owner=%s,claim_token=%s,
            attempts=attempts+1,lease_until=CURRENT_TIMESTAMP(6) + INTERVAL %s SECOND,
            updated_at=CURRENT_TIMESTAMP(6) WHERE job_id=%s''',
            (owner,token,lease_seconds,job['job_id']))
        job.update(owner=owner,claim_token=token,attempts=job['attempts']+1)
        return job


def renew(job, seconds):
    with transaction() as cur:
        lock_owned(cur, job)
        cur.execute(f'''UPDATE {QUEUE} SET lease_until=CURRENT_TIMESTAMP(6) + INTERVAL %s SECOND,
            updated_at=CURRENT_TIMESTAMP(6) WHERE job_id=%s AND claim_token=%s
            AND status='running' AND lease_until > CURRENT_TIMESTAMP(6)''',
            (seconds,job['job_id'],job['claim_token']))
        if cur.rowcount != 1:
            raise LeaseLost('Lease no longer owned')


def lock_owned(cur, job):
    cur.execute(f'SELECT job_id FROM {QUEUE} WHERE job_id=%s FOR UPDATE', (job['job_id'],))
    cur.fetchone()
    cur.execute(f"""SELECT job_id FROM {QUEUE} WHERE job_id=%s AND claim_token=%s
        AND status='running' AND lease_until > CURRENT_TIMESTAMP(6)""",
        (job['job_id'],job['claim_token']))
    if not cur.fetchone():
        raise LeaseLost('Stale worker cannot commit')


def complete(job, result, char_count):
    with transaction() as cur:
        lock_owned(cur,job)
        cur.execute(f'SELECT id FROM {TABLE} WHERE id=%s AND model=%s FOR UPDATE', (job['target_id'],MODEL))
        if not cur.fetchone():
            raise ValueError('Target row missing or model changed')
        assignments = ','.join(f'{key}=%s' for key in FIELDS)
        cur.execute(f'''UPDATE {TABLE} SET {assignments},text_char_count=%s,
            uniondues_model=%s,uniondues_version=%s,uniondues_updated_at=CURRENT_TIMESTAMP(6),
            uniondues_status='success',uniondues_error=NULL,uniondues_attempted_at=CURRENT_TIMESTAMP(6)
            WHERE id=%s AND model=%s''',
            (*[result[k] for k in FIELDS],char_count,MODEL,VERSION,job['target_id'],MODEL))
        cur.execute(f'''UPDATE {QUEUE} SET status='succeeded',completed_at=CURRENT_TIMESTAMP(6),
            updated_at=CURRENT_TIMESTAMP(6),lease_until=NULL,claim_token=NULL,last_error=NULL
            WHERE job_id=%s''', (job['job_id'],))


def fail(job, error, char_count, max_attempts, delay):
    with transaction() as cur:
        lock_owned(cur,job)
        status = 'failed' if job['attempts'] >= max_attempts else 'pending'
        cur.execute(f'''UPDATE {QUEUE} SET status=%s,last_error=%s,owner=NULL,
            claim_token=NULL,lease_until=NULL,available_at=CURRENT_TIMESTAMP(6) + INTERVAL %s SECOND,
            updated_at=CURRENT_TIMESTAMP(6) WHERE job_id=%s''', (status,error[:255],delay,job['job_id']))
        cur.execute(f'''UPDATE {TABLE} SET uniondues_status='error',uniondues_error=%s,
            uniondues_attempted_at=CURRENT_TIMESTAMP(6),text_char_count=COALESCE(%s,text_char_count)
            WHERE id=%s AND model=%s''', (error[:255],char_count,job['target_id'],MODEL))


def snapshot():
    with transaction() as cur:
        cur.execute(f'SELECT status,COUNT(*) AS count FROM {QUEUE} WHERE version=%s GROUP BY status', (VERSION,))
        counts = {r['status']:r['count'] for r in cur.fetchall()}
        cur.execute(f'''SELECT owner,target_id,attempts,lease_until,lease_until < CURRENT_TIMESTAMP(6) AS expired
            FROM {QUEUE} WHERE version=%s AND status='running' ORDER BY lease_until LIMIT 50''', (VERSION,))
        running = cur.fetchall()
        cur.execute(f'''SELECT COUNT(*) AS count FROM {TABLE} r WHERE model=%s
            AND s3_uri IS NOT NULL AND s3_uri <> '' AND {PENDING}
            AND NOT EXISTS (SELECT 1 FROM {QUEUE} j WHERE j.version=%s AND j.target_id=r.id)''',
            (MODEL,VERSION,MODEL,VERSION))
        unqueued = cur.fetchone()['count']
        cur.execute(f"SELECT COUNT(*) AS count FROM {TABLE} WHERE model=%s AND (s3_uri IS NULL OR s3_uri='')", (MODEL,))
        missing_uri = cur.fetchone()['count']
    return dict(version=VERSION,counts=counts,running=running,unqueued=unqueued,missing_s3_uri=missing_uri)


def retry_failed():
    with transaction() as cur:
        cur.execute(f'''UPDATE {QUEUE} SET status='pending',attempts=0,
            available_at=CURRENT_TIMESTAMP(6),updated_at=CURRENT_TIMESTAMP(6)
            WHERE version=%s AND status='failed' ''', (VERSION,))
        return cur.rowcount
