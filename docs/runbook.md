# Runbook - SAS to Databricks Accelerator (Volumes / Free Workspace)

Generated: 2025-09-11T15:01:25.922282

1. In Databricks SQL, run:
   CREATE CATALOG IF NOT EXISTS migration;
   CREATE SCHEMA IF NOT EXISTS migration.default;
   CREATE VOLUME IF NOT EXISTS migration.default.sas_artifacts;
   CREATE VOLUME IF NOT EXISTS migration.default.sas_outputs;
   CREATE VOLUME IF NOT EXISTS migration.default.migration_results;

2. Upload SAS files (samples provided) into Data -> Volumes -> migration.default.sas_artifacts.

3. Import this repo ZIP into Repos (Add Repo -> Upload ZIP).

4. In Databricks SQL, run: sql/create_migration_tables.sql

5. In Repos, open notebooks/99_demo_run.py and click 'Run All'. Monitor progress cell-by-cell.

6. After run, check Migration tables:
   SELECT * FROM migration.sas_artifacts;
   SELECT * FROM migration.sas_blocks;
   SELECT * FROM migration.validation_results;

7. Create the dashboard: run sql/migration_dashboard.sql to create view, then use Databricks SQL Dashboards to add tiles (see instructions).

Notes:
- AI step requires a secret scope 'llm' with key 'openai-key' (optional).
- For production, replace regex tokenizer with ANTLR-generated parser in antlr/ (README included).
