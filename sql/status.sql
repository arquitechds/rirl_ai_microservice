-- Read-only checks for this worker version.
SET @worker_version = 'union-dues-v2';
SET @worker_model = 'openai/gpt-oss-20b';

-- Queue totals. Expired running jobs need a worker to reclaim them.
SELECT status, COUNT(*) AS jobs,
       SUM(attempts) AS attempts,
       SUM(CASE WHEN status='running' AND lease_until < CURRENT_TIMESTAMP(6)
                THEN 1 ELSE 0 END) AS expired_leases
FROM ai.union_dues_jobs
WHERE version=@worker_version
GROUP BY status;

-- Latest error categories; errors on pending/running jobs are previous failures.
SELECT status, last_error, COUNT(*) AS jobs
FROM ai.union_dues_jobs
WHERE version=@worker_version AND last_error IS NOT NULL
GROUP BY status, last_error
ORDER BY jobs DESC;

-- Contracts still incomplete, including whether they have a queue entry.
SELECT r.id, j.status, j.attempts, j.last_error,
       CASE WHEN r.s3_uri IS NULL OR r.s3_uri='' THEN 'missing S3 URI'
            WHEN j.job_id IS NULL THEN 'not queued'
            ELSE 'queued but incomplete' END AS remaining_reason
FROM ai.contratos_colectivos_v2 r
LEFT JOIN ai.union_dues_jobs j ON j.target_id=r.id AND j.version=@worker_version
WHERE r.model=@worker_model
  AND (r.uniondues_version IS NULL OR r.uniondues_version<>@worker_version
       OR r.uniondues_model IS NULL OR r.uniondues_model<>@worker_model
       OR r.text_char_count IS NULL OR r.uniondues_no_reason_ai IS NULL)
ORDER BY j.status, r.id
LIMIT 100;

-- Detect missing fields even when a row carries the current version/model.
SELECT COUNT(*) AS incomplete_current_version_rows
FROM ai.contratos_colectivos_v2
WHERE model=@worker_model AND uniondues_model=@worker_model
  AND uniondues_version=@worker_version
  AND (uniondues2_ai IS NULL OR uniondues_basis_ai IS NULL
       OR uniondues_amount_ai IS NULL OR uniondues_period_ai IS NULL
       OR uniondues_no_reason_ai IS NULL OR text_char_count IS NULL);

-- Inspect stalled claims.
SELECT target_id, attempts, owner, lease_until,
       lease_until < CURRENT_TIMESTAMP(6) AS expired
FROM ai.union_dues_jobs
WHERE version=@worker_version AND status='running'
ORDER BY lease_until;
