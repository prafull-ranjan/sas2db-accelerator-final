# 02_rule_based_converter.py
import re
from pyspark.sql import functions as F

# Load mappings
try:
    mappings_df = spark.table('migration.sas_to_pyspark_map')
    mappings = [(r.sas_pattern, r.replacement) for r in mappings_df.collect()]
except Exception:
    # fallback load from file in repo (if table not loaded)
    import json, os
    repo_path = '/Workspace/Repos/'+dbutils.notebook.entry_point.getDbutils().notebook().getContext().repositoryPath().get().split('/')[-1]
    with open(repo_path + '/mappings/sas_to_pyspark.json','r') as fh:
        mappings = [(m['sas_pattern'], m['replacement']) for m in json.load(fh)]

blocks = spark.table('migration.sas_blocks').filter("status='PENDING' AND complexity in ('SIMPLE','MEDIUM')").collect()

def apply_mappings(code):
    out = code
    for pat, repl in mappings:
        try:
            out = re.sub(pat, repl, out, flags=re.I|re.M)
        except Exception:
            pass
    return out

for b in blocks:
    converted = apply_mappings(b.code)
    if re.search(r'\bdata\b|%macro|proc\s+sql', converted, re.I):
        status = 'REVIEW'
        converter_used = None
    else:
        status = 'CONVERTED'
        converter_used = 'RULE'
    spark.sql(f"UPDATE migration.sas_blocks SET converted_code = {repr(converted)}, status = '{status}', converter_used = {repr(converter_used)} WHERE block_id = '{b.block_id}'")
print('Rule-based conversion complete')