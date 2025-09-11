# 05_code_quality_checks.py
blocks = spark.table('migration.sas_blocks').filter("status='VALIDATED'").collect()
for b in blocks:
    issues = []
    code = b.converted_code or ''
    if 'collect(' in code.lower():
        issues.append('collect() detected')
    if 'toPandas' in code:
        issues.append('toPandas detected')
    if issues:
        spark.sql(f"UPDATE migration.sas_blocks SET status='QUALITY_REVIEW', error_message={repr('; '.join(issues))} WHERE block_id='{b.block_id}'")
print('Quality checks done')