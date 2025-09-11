# 04_validation_framework.py
from pyspark.sql.functions import sha2, concat_ws
import uuid
from datetime import datetime

SAS_OUTPUTS = '/Volumes/migration/default/sas_outputs'
MIGRATION_RESULTS = '/Volumes/migration/default/migration_results'

def exec_converted(code_str):
    local_ns = {}
    global_ns = {'spark': spark}
    exec(code_str, global_ns, local_ns)
    for candidate in ('out_df','df_out','out'):
        if candidate in local_ns:
            return local_ns[candidate]
        if candidate in global_ns:
            return global_ns[candidate]
    raise Exception('No out_df produced by converted code')

converted_blocks = spark.table('migration.sas_blocks').filter("status='CONVERTED'").collect()

for b in converted_blocks:
    try:
        out_df = exec_converted(b.converted_code)
        target_path = f"{MIGRATION_RESULTS}/{b.artifact_id}/{b.block_id}"
        out_df.write.format('delta').mode('overwrite').save(target_path)
        sas_path = f"{SAS_OUTPUTS}/{b.artifact_id}/{b.block_id}"
        try:
            dbutils.fs.ls(sas_path)
            baseline_exists = True
        except Exception:
            baseline_exists = False
        if baseline_exists:
            sas_df = spark.read.format('delta').load(sas_path)
            out_df_read = spark.read.format('delta').load(target_path)
            rc_sas = sas_df.count(); rc_out = out_df_read.count()
            now = datetime.utcnow()
            if rc_sas != rc_out:
                recs = [(str(uuid.uuid4()), b.block_id, b.artifact_id, 'rowcount', 'FAIL', f'sas={rc_sas},out={rc_out}', now)]
                spark.createDataFrame(recs, schema=['result_id','block_id','artifact_id','check_name','status','message','checked_at']).write.format('delta').mode('append').saveAsTable('migration.validation_results')
                spark.sql(f"UPDATE migration.sas_blocks SET status='VALIDATION_FAILED', error_message='Rowcount mismatch' WHERE block_id='{b.block_id}'")
                continue
            cols = sorted(list(set(sas_df.columns).intersection(set(out_df_read.columns))))
            if not cols:
                recs = [(str(uuid.uuid4()), b.block_id, b.artifact_id, 'schema', 'FAIL', 'No overlapping columns', now)]
                spark.createDataFrame(recs, schema=['result_id','block_id','artifact_id','check_name','status','message','checked_at']).write.format('delta').mode('append').saveAsTable('migration.validation_results')
                spark.sql(f"UPDATE migration.sas_blocks SET status='VALIDATION_FAILED', error_message='No overlapping columns' WHERE block_id='{b.block_id}'")
                continue
            sas_hash = sas_df.withColumn('_h', sha2(concat_ws('||', *cols),256)).select('_h')
            out_hash = out_df_read.withColumn('_h', sha2(concat_ws('||', *cols),256)).select('_h')
            cnt_sas = sas_hash.select('_h').distinct().count()
            cnt_out = out_hash.select('_h').distinct().count()
            if cnt_sas != cnt_out:
                recs = [(str(uuid.uuid4()), b.block_id, b.artifact_id, 'hash', 'FAIL', f'hashes sas={cnt_sas},out={cnt_out}', now)]
                spark.createDataFrame(recs, schema=['result_id','block_id','artifact_id','check_name','status','message','checked_at']).write.format('delta').mode('append').saveAsTable('migration.validation_results')
                spark.sql(f"UPDATE migration.sas_blocks SET status='VALIDATION_FAILED', error_message='Hash mismatch' WHERE block_id='{b.block_id}'")
                continue
            # pass
            recs = [(str(uuid.uuid4()), b.block_id, b.artifact_id, 'all', 'PASS', 'Validation OK', now)]
            spark.createDataFrame(recs, schema=['result_id','block_id','artifact_id','check_name','status','message','checked_at']).write.format('delta').mode('append').saveAsTable('migration.validation_results')
            spark.sql(f"UPDATE migration.sas_blocks SET status='VALIDATED' WHERE block_id='{b.block_id}'")
        else:
            recs = [(str(uuid.uuid4()), b.block_id, b.artifact_id, 'baseline', 'SKIP', 'No baseline', datetime.utcnow())]
            spark.createDataFrame(recs, schema=['result_id','block_id','artifact_id','check_name','status','message','checked_at']).write.format('delta').mode('append').saveAsTable('migration.validation_results')
            spark.sql(f"UPDATE migration.sas_blocks SET status='NO_BASELINE' WHERE block_id='{b.block_id}'")
    except Exception as e:
        spark.sql(f"UPDATE migration.sas_blocks SET status='ERROR', error_message={repr(str(e))} WHERE block_id='{b.block_id}'")
print('Validation complete')