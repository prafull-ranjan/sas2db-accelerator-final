# 00_ingest_sas_files.py
import os, uuid
from datetime import datetime
from pyspark.sql import Row

# Use Volumes path for Databricks Free / Unity Catalog volumes
SAS_ROOT = '/Volumes/migration/default/sas_artifacts'

owner = 'unknown'
try:
    owner = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get('user').get()
except:
    pass

files = []
for root, dirs, filenames in os.walk(SAS_ROOT):
    for f in filenames:
        if f.lower().endswith(('.sas', '.egp')):
            full = os.path.join(root, f)
            files.append((str(uuid.uuid4()), f, full, owner, datetime.utcnow()))

rows = [Row(artifact_id=r[0], filename=r[1], path=r[2], uploaded_by=r[3], created_at=r[4], status='PENDING') for r in files]
if rows:
    spark.createDataFrame(rows).write.format('delta').mode('append').saveAsTable('migration.sas_artifacts')
print(f"Registered {len(rows)} files into migration.sas_artifacts")