# Parallel MySQL union-dues worker

Standalone MySQL → S3 → OpenRouter → MySQL pipeline. Run the **same image as multiple ECS/Fargate tasks**. Only original `ai.contratos_colectivos_v2.model = 'openai/gpt-oss-20b'` rows are eligible; inference also uses that model. No imports from the parent repository, scraping, PDF/OCR tools, pandas, or web server.

Outputs: the four union-dues answers, `uniondues_no_reason_ai`, `text_char_count = len(full_decoded_text)`, and metadata. Version is `union-dues-v2`; already-complete rows with that model/version and both new columns are skipped. Other models and unrelated columns are preserved.

## Parallel control

`ai.union_dues_jobs` stores one job per target row/version:

| Status | Meaning |
| --- | --- |
| pending | Unclaimed or waiting for a retry at `available_at` |
| running | Claimed by `owner`, with a `claim_token` and `lease_until` |
| succeeded | Answers and completion committed together |
| failed | Attempt limit reached; inspect `last_error` before retrying |

Claims use MySQL `FOR UPDATE SKIP LOCKED` in short transactions; database locks are not held during external API requests. A heartbeat renews the five-minute lease every 100 seconds. Active workers reclaim expired leases. Each completion/failure locks and validates the unexpired token, so stale workers cannot overwrite new owners' results. Answers and completion commit atomically.

Partial/previously attempted rows have priority 0; untouched rows priority 1. Priority applies among available unlocked jobs, so untouched jobs can proceed while partial ones run or await retry. Defaults: 3 attempts and 5/10-minute retry delays. Keep settings identical across replicas. Restore archived S3 texts before scaling; use `retry-failed` after correcting/restoring failures.

**At-least-once processing, not exactly-once API billing:** a crash after an API response but before commit can cause another paid call. Lease expiration may permit overlapping external calls, though only the valid owner can commit. Stop old AI/enrichment runners before launching this queue; they do not honor leases.

## MySQL requirements

MySQL 8.0+ with an existing InnoDB table `ai.contratos_colectivos_v2` and a full unique index on its CHAR/VARCHAR `id` (at most 768 characters). Initialization verifies these requirements and adds missing enrichment columns plus the InnoDB queue and indexes. Run initialization once before workers: MySQL DDL implicitly commits and may take metadata locks. If interrupted, rerun init; it skips existing columns. Existing column types are preserved.

In MySQL, `ai` is a database. The worker always addresses `ai.contratos_colectivos_v2` and `ai.union_dues_jobs`, even if `DB_NAME=metadata` selects a different default database on the same server. The `ai` database must already exist.

The initialization account needs ALTER on the result table and CREATE on `ai`. Runtime needs SELECT/UPDATE on the result table and SELECT/INSERT/UPDATE on the queue. No credentials are included in the image.

## Build and initialize

From this folder, with Docker available:

```sh
docker build -t rirl-union-dues:local .
# Only if .env does not already exist:
cp .env.example .env
# Fill MySQL host/port/database/user/password and API/AWS credentials.
docker run --rm --env-file .env rirl-union-dues:local init
docker run --rm --env-file .env rirl-union-dues:local seed
docker run --rm --env-file .env rirl-union-dues:local status
# One paid API call/job attempt and database write:
docker run --rm --env-file .env rirl-union-dues:local work --max-jobs 1
```

Use `DB_HOST`, `DB_PORT=3306`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`, and `DB_SSLMODE=require`. The existing `DB_SSLMODE` name is retained: `require` requires encryption; `verify-full` also validates the certificate and hostname; `disable` is for disposable local tests. With `verify-full`, mount the trusted CA and set `DB_SSL_CA` to its container path.

Alternatively, `DATABASE_URL=mysql://user:password@host:3306/database` overrides connection fields. Percent-encode special characters in URL credentials. SQL-client URLs containing UI query options are rejected; use the DB_* settings instead. TLS settings still come from `DB_SSLMODE` and `DB_SSL_CA`. Docker env files need `KEY=value` without spaces around `=` or surrounding quotes.

Docker injects .env; Python does not load it automatically. Use ECS secrets and IAM task roles in production. Standard AWS credentials and legacy `AWS_ACCESS`/`AWS_SECRET` are supported locally. S3 mapping is `.pdf` → `.json.json` (configurable suffix). Objects over 2 MB fail rather than truncate; override with `work --max-text-bytes N`, keeping model context limits in mind. Counts include all decoded characters, including whitespace; counts are also saved after successful download if inference fails.

## Push to ECR

```sh
export AWS_DEFAULT_REGION=us-east-2
export IMAGE_TAG=union-dues-v2-001
# Once, if repository doesn't exist:
aws ecr create-repository --repository-name rirl-union-dues --image-tag-mutability IMMUTABLE
bash deploy/push-ecr.sh
```

The script authenticates with AWS CLI, builds Linux/amd64, and pushes a versioned image. Use a new tag per release. It stops on repository lookup errors.

## Deploy ECS/Fargate tasks

1. Create/use an ECS cluster. The **task role** needs S3 GetObject on text prefixes (and KMS decrypt if needed). The **execution role** needs ECR pull, CloudWatch logs, and access to injected Secrets Manager secrets.
2. Create CloudWatch log group `/ecs/rirl-union-dues`. Copy `deploy/task-definition.example.json` to `deploy/task-definition.generated.json` and replace all placeholders: image/account/tag, roles, MySQL host/database/user, and secrets. The example secrets contain individual plaintext values, not JSON objects.
3. Copy `deploy/network.example.json` to `deploy/network.generated.json`. Set subnets/security groups. Allow MySQL port 3306 and outbound HTTPS to S3/OpenRouter. Private subnets need NAT for OpenRouter; AWS endpoints alone aren't enough. Public subnets require an internet gateway route and `assignPublicIp: ENABLED`. No inbound container port is needed.
4. Run `init` then `seed` once from a machine with DB access. Alternatively run ECS tasks with command overrides `["init"]` then `["seed"]`, waiting for successful completion before launching workers.
5. Register and launch:

```sh
aws ecs register-task-definition --cli-input-json file://deploy/task-definition.generated.json
export ECS_CLUSTER=YOUR_CLUSTER
export TASK_DEFINITION=rirl-union-dues:REVISION
export NETWORK_CONFIG="$PWD/deploy/network.generated.json"
export TASK_COUNT=5
bash deploy/run-tasks.sh
```

Launcher batches at most 10 tasks per API request. If a later batch fails, earlier tasks remain running; inspect before repeating. Start with 5 tasks; monitor provider rate limits, MySQL connections, throughput, and spend before scaling. Each task processes one contract at a time.

Use standalone tasks, not a service that continually restarts completed work. Tasks exit when pending/running work drains, returning exit code 1 if terminal failures remain. Idle tasks poll while other jobs run or await retry. SIGTERM stops new claims; ECS can kill an unfinished request after its 120-second stop timeout, leaving a recoverable lease. If all workers stop, launch replacements to reclaim expired leases.

## Monitor and recover

```sh
docker run --rm --env-file .env rirl-union-dues:local status
# After fixing causes of failures:
docker run --rm --env-file .env rirl-union-dues:local retry-failed
```

`status`: counts, up to 50 current owners/leases, eligible-but-unqueued rows, and missing S3 URIs. `seed` inserts batches of 500 and emits `seed_progress` after each committed batch, with cumulative scanned and inserted counts for this run. It is idempotent and discovers new eligible rows without resetting jobs; interrupted runs can be restarted safely. `retry-failed` resets terminal failures and attempt counts; then launch workers. Bump VERSION for future prompt changes, stop old workers, and seed again. Do not manually edit completed results during processing.

```sql
SELECT status,count(*) FROM ai.union_dues_jobs
WHERE version='union-dues-v2' GROUP BY status;

SELECT owner,target_id,attempts,lease_until FROM ai.union_dues_jobs
WHERE version='union-dues-v2' AND status='running';

SELECT last_error,count(*) FROM ai.union_dues_jobs
WHERE version='union-dues-v2' AND status='failed' GROUP BY last_error;
```

The queue stores latest state/error and attempt count; JSON CloudWatch events preserve individual attempts. Logs exclude raw texts, responses, and secrets. Numeric MySQL error codes identify database failures. A DB failure may terminate a worker; its lease permits recovery. API retries and queue retries are bounded but may repeat paid calls.

## Tests

```sh
python3 -m unittest discover -s tests -v
# Actual MySQL concurrency/fencing/rollback tests, isolated from AWS:
docker compose -p rirl-queue-mysql-test -f compose.test.yaml up --abort-on-container-exit --exit-code-from tests
docker compose -p rirl-queue-mysql-test -f compose.test.yaml down
```

Tests use a disposable MySQL database in tmpfs on an internal Docker network. They refuse any host other than `mysql-test`; offline runs skip these integration tests. The production schema and live model still need a one-job smoke test.

References: [MySQL locking](https://dev.mysql.com/doc/refman/8.4/en/innodb-locking-reads.html), [ECS task roles](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html), [execution roles](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task_execution_IAM_role.html).
