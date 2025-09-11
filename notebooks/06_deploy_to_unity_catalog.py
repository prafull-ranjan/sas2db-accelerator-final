# 06_deploy_to_unity_catalog.py
import os
REPO_BASE = '/Repos/sas2db-accelerator-deployed'
validated = spark.table('migration.sas_blocks').filter("status='VALIDATED'").collect()
for b in validated:
    target = f"/Repos/sas2db-accelerator-deployed/{b.artifact_id}_{b.block_id}.py"
    try:
        # write to DBFS path under Repos area (workspace API preferred)
        dbutils.fs.put(target, b.converted_code or '', True)
        spark.sql(f"UPDATE migration.sas_blocks SET status='DEPLOYED', error_message=NULL WHERE block_id='{b.block_id}'")
    except Exception as e:
        spark.sql(f"UPDATE migration.sas_blocks SET status='ERROR', error_message={repr(str(e))} WHERE block_id='{b.block_id}'")
print('Deploy complete')