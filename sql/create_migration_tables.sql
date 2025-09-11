CREATE SCHEMA IF NOT EXISTS migration;

CREATE TABLE IF NOT EXISTS migration.sas_artifacts (
  artifact_id STRING,
  filename STRING,
  path STRING,
  uploaded_by STRING,
  created_at TIMESTAMP,
  status STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS migration.sas_blocks (
  block_id STRING,
  artifact_id STRING,
  block_type STRING,
  code STRING,
  ast STRING,
  complexity STRING,
  converted_code STRING,
  converter_used STRING,
  status STRING,
  error_message STRING,
  created_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS migration.sas_to_pyspark_map (
  sas_pattern STRING,
  replacement STRING,
  notes STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS migration.validation_results (
  result_id STRING,
  block_id STRING,
  artifact_id STRING,
  check_name STRING,
  status STRING,
  message STRING,
  checked_at TIMESTAMP
) USING DELTA;
