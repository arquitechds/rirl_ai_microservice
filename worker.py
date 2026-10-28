"""One independently scalable worker process per container."""
import argparse
from contextlib import closing
import json
import os
import signal
import socket
import threading
import uuid

import db
from extraction import MODEL, VERSION, PROMPT, download_text, parse_response, count_characters


def log(event, **values):
    print(json.dumps({'event': event, **values}, default=str, ensure_ascii=False), flush=True)


def positive(value):
    value = int(value)
    if value <= 0:
        raise argparse.ArgumentTypeError('Must be positive')
    return value


def error_code(exc):
    aws_code = getattr(exc, 'response', {}).get('Error', {}).get('Code') if isinstance(getattr(exc, 'response', None), dict) else None
    number = exc.args[0] if exc.args and isinstance(exc.args[0], int) else None
    return ':'.join(str(v) for v in (type(exc).__name__, aws_code or getattr(exc, 'sqlstate', None) or number) if v is not None)


class Heartbeat:
    def __init__(self, job, seconds):
        self.job, self.seconds = job, seconds
        self.stop = threading.Event()
        self.lost = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)

    def run(self):
        while not self.stop.wait(self.seconds / 3):
            try:
                db.renew(self.job, self.seconds)
            except Exception as exc:
                # Fail closed: this process must not write after uncertain ownership.
                self.lost.set()
                log('lease_lost', job_id=self.job['job_id'], error=error_code(exc))
                return

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.stop.set()
        self.thread.join()

    def assert_owned(self):
        if self.lost.is_set():
            raise db.LeaseLost('Heartbeat failed')


def process(job, client, s3, args):
    chars = None
    with Heartbeat(job, args.lease_seconds) as heartbeat:
        try:
            text = download_text(s3, job['s3_uri'], os.getenv('S3_BUCKET', 'rirl-documents'),
                                 os.getenv('S3_TEXT_SUFFIX', '.json.json'), args.max_text_bytes)
            chars = count_characters(text)
            heartbeat.assert_owned()
            response = client.chat.completions.create(model=MODEL,
                messages=[{'role': 'system', 'content': PROMPT}, {'role': 'user', 'content': text}],
                response_format={'type': 'json_object'})
            choice = response.choices[0]
            if choice.finish_reason != 'stop':
                raise ValueError('Incomplete model response')
            result = parse_response(choice.message.content)
            heartbeat.assert_owned()
            db.complete(job, result, chars)
            log('succeeded', job_id=job['job_id'], text_char_count=chars)
        except db.LeaseLost:
            log('discarded_stale_result', job_id=job['job_id'])
        except Exception as exc:
            heartbeat.assert_owned()
            # Commit errors can be ambiguous: fail() is also fenced and cannot undo
            # a transaction that already committed success.
            try:
                db.fail(job, error_code(exc), chars, args.max_attempts,
                        min(args.retry_seconds * 2 ** (job['attempts'] - 1), 3600))
            except db.LeaseLost:
                log('ownership_changed', job_id=job['job_id'])
                return
            log('attempt_failed', job_id=job['job_id'], error=error_code(exc), attempt=job['attempts'])


def active_counts():
    with db.transaction() as cur:
        cur.execute(f'''SELECT status,COUNT(*) AS n FROM {db.QUEUE}
            WHERE version=%s GROUP BY status''', (VERSION,))
        return {row['status']: row['n'] for row in cur.fetchall()}


def work(args):
    import boto3
    from botocore.config import Config
    from openai import OpenAI
    if not os.getenv('OPENROUTER_API_KEY'):
        raise ValueError('OPENROUTER_API_KEY is required')
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    owner = f'{socket.gethostname()}-{uuid.uuid4().hex}'[:128]
    credentials = {}
    if not os.getenv('AWS_ACCESS_KEY_ID') and os.getenv('AWS_ACCESS') and os.getenv('AWS_SECRET'):
        credentials = dict(aws_access_key_id=os.environ['AWS_ACCESS'], aws_secret_access_key=os.environ['AWS_SECRET'])
    s3 = boto3.client('s3', region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-2'),
        config=Config(connect_timeout=15, read_timeout=60,
                      retries={'mode': 'standard', 'max_attempts': 3}), **credentials)
    processed = 0
    with OpenAI(api_key=os.environ['OPENROUTER_API_KEY'], base_url='https://openrouter.ai/api/v1',
                timeout=180, max_retries=2) as client:
        while not stop.is_set() and (args.max_jobs is None or processed < args.max_jobs):
            db.reap(args.max_attempts)
            job = db.claim(owner, args.lease_seconds, args.max_attempts)
            if not job:
                counts = active_counts()
                if not counts.get('pending') and not counts.get('running'):
                    log('queue_drained', counts=counts)
                    return 1 if counts.get('failed') else 0
                stop.wait(args.poll_seconds)
                continue
            log('claimed', job_id=job['job_id'], owner=owner, priority=job['priority'], attempt=job['attempts'])
            process(job, client, s3, args)
            processed += 1
    log('worker_stopped', processed=processed)
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['init', 'seed', 'status', 'retry-failed', 'work'])
    parser.add_argument('--max-jobs', type=positive, help='Stop after this many claims in this container')
    parser.add_argument('--lease-seconds', type=positive, default=300)
    parser.add_argument('--max-attempts', type=positive, default=3)
    parser.add_argument('--retry-seconds', type=positive, default=300)
    parser.add_argument('--poll-seconds', type=positive, default=15)
    parser.add_argument('--max-text-bytes', type=positive, default=2_000_000)
    args = parser.parse_args()
    if args.lease_seconds < 90:
        parser.error('--lease-seconds must be at least 90')
    if args.command == 'init':
        db.initialize()
        log('initialized')
        return 0
    with closing(db.connect()) as conn:
        db.preflight(conn)
        db.check_schema(conn)
    if args.command == 'seed':
        log('seeding_started')
        log('seeded', inserted=db.seed(progress=lambda **values: log('seed_progress', **values)))
    elif args.command == 'status':
        log('status', **db.snapshot())
    elif args.command == 'retry-failed':
        log('requeued', count=db.retry_failed())
    else:
        return work(args)
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except Exception as exc:
        log('fatal', error=error_code(exc),
            **({'message': str(exc)} if isinstance(exc, db.ConfigurationError) else {}))
        raise SystemExit(1)
