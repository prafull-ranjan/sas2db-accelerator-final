# 99_demo_run.py - demo orchestrator
from datetime import datetime
print('Starting SAS->Databricks demo at', datetime.utcnow())
steps = [
 '00_ingest_sas_files',
 '01_tokenize_ast_parser',
 '02_rule_based_converter',
 '03_ai_assisted_converter',
 '04_validation_framework',
 '05_code_quality_checks',
 '06_deploy_to_unity_catalog'
]
for s in steps:
    try:
        print('Running', s)
        dbutils.notebook.run(f'./{s}', 0)
    except Exception as e:
        print('Step failed or skipped:', s, e)
print('Demo run finished at', datetime.utcnow())