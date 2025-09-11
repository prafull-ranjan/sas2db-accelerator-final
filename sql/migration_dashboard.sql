-- Migration Progress Dashboard SQL Scripts

-- 1. Migration Summary View
CREATE OR REPLACE VIEW migration.migration_summary AS
SELECT
  COUNT(DISTINCT artifact_id) AS total_artifacts,
  SUM(CASE WHEN status='PENDING' THEN 1 ELSE 0 END) AS pending_artifacts,
  SUM(CASE WHEN status='CONVERTED' THEN 1 ELSE 0 END) AS converted_artifacts,
  SUM(CASE WHEN status='ERROR' THEN 1 ELSE 0 END) AS error_artifacts
FROM migration.sas_blocks;

-- 2. Conversion Status Breakdown
SELECT status, COUNT(*) AS block_count
FROM migration.sas_blocks
GROUP BY status;

-- 3. Artifacts Conversion Progress
SELECT
  a.filename,
  SUM(CASE WHEN b.status='CONVERTED' THEN 1 ELSE 0 END) AS converted_blocks,
  COUNT(b.block_id) AS total_blocks
FROM migration.sas_artifacts a
LEFT JOIN migration.sas_blocks b
  ON a.artifact_id = b.artifact_id
GROUP BY a.filename;

-- 4. Validation Results
SELECT
  check_name AS metric,
  COUNT(*) AS checks,
  SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) AS passed_checks,
  SUM(CASE WHEN status!='PASS' THEN 1 ELSE 0 END) AS failed_checks
FROM migration.validation_results
GROUP BY check_name;
