# 01_tokenize_ast_parser.py
import re, uuid
from datetime import datetime
from pyspark.sql import Row

# Simple tokenizer using regex for DATAs and PROCs. Switch to ANTLR for production.
def split_into_blocks(raw_text):
    pattern = re.compile(r'(?=^\s*(proc\s+\w+|data\s+[^;]+))', re.I|re.M)
    parts = [p for p in pattern.split(raw_text) if p and not p.isspace()]
    blocks = []
    temp = ''
    for p in parts:
        if re.match(r'^\s*(proc\s+\w+|data\s+)', p, re.I):
            if temp:
                blocks.append(temp)
            temp = p
        else:
            temp += '\n' + p
    if temp:
        blocks.append(temp)
    return blocks

artifacts = spark.table('migration.sas_artifacts').filter("status='PENDING'").collect()
rows = []
for a in artifacts:
    try:
        with open(a.path, 'r') as fh:
            raw = fh.read()
    except Exception as e:
        spark.sql(f"UPDATE migration.sas_artifacts SET status='FAILED', error_message={repr(str(e))} WHERE artifact_id='{a.artifact_id}'")
        continue
    blocks = split_into_blocks(raw)
    if not blocks:
        blocks = [raw]
    for b in blocks:
        block_type = 'OTHER'
        if re.search(r'proc\s+sql', b, re.I):
            block_type = 'PROC_SQL'
        elif re.search(r'^\s*data\s+', b, re.I|re.M):
            block_type = 'DATA_STEP'
        elif re.search(r'%macro', b, re.I):
            block_type = 'MACRO'
        complexity = 'COMPLEX' if (block_type=='MACRO' or len(b) > 3000 or ' do ' in b.lower()) else 'SIMPLE'
        rows.append(Row(block_id=str(uuid.uuid4()), artifact_id=a.artifact_id, code=b, ast=None, block_type=block_type, complexity=complexity, converted_code=None, converter_used=None, status='PENDING', error_message=None, created_at=datetime.utcnow()))
    spark.createDataFrame(rows).write.format('delta').mode('append').saveAsTable('migration.sas_blocks')
    spark.sql(f"UPDATE migration.sas_artifacts SET status='PARSED' WHERE artifact_id='{a.artifact_id}'")
print(f'Created {len(rows)} blocks')