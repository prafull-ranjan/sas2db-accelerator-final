%pip install openai==0.28


# 03_ai_assisted_converter.py
import time, uuid
USE_AI = True
try:
    OPENAI_KEY = dbutils.secrets.get(scope='llm', key='openai-key')
except Exception:
    OPENAI_KEY = None
    USE_AI = False

if not USE_AI:
    print('AI disabled (no secret found)')

# Minimal OpenAI call wrapper — adapt to your environment
def call_openai(prompt):
    try:
        import openai
        openai.api_key = OPENAI_KEY
        resp = openai.Completion.create(engine='text-davinci-003', prompt=prompt, max_tokens=1500)
        return resp.choices[0].text
    except Exception as e:
        raise

blocks = spark.table("migration.sas_blocks").filter("status in ('PENDING','REVIEW')").collect()
prompt_template = """You are an expert translator. Convert the SAS code to production PySpark DataFrame code. Return only Python code that produces a DataFrame named out_df."""

for b in blocks:
    if not USE_AI:
        break
    if b.complexity not in ('COMPLEX','MEDIUM') and b.status=='REVIEW':
        continue
    prompt = prompt_template + "\n\nSAS CODE:\n" + b.code
    try:
        converted = call_openai(prompt)
        if converted and 'out_df' in converted:
            spark.sql(f"UPDATE migration.sas_blocks SET converted_code = {repr(converted)}, status='CONVERTED', converter_used='AI' WHERE block_id='{b.block_id}'")
        else:
            spark.sql(f"UPDATE migration.sas_blocks SET status='REVIEW', converter_used='AI' WHERE block_id='{b.block_id}'")
    except Exception as e:
        spark.sql(f"UPDATE migration.sas_blocks SET status='REVIEW', converter_used='AI', error_message={repr(str(e))} WHERE block_id='{b.block_id}'")
    time.sleep(0.5)
print('AI conversion attempted')
