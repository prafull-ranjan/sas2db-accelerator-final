%pip install openai==0.28


# 03_ai_assisted_converter.py
# 03_ai_assisted_converter.py
import time, uuid

# ========================
# 🔑 API Key Handling
# ========================
OPENAI_KEY = None

# Try Databricks secrets first
try:
    OPENAI_KEY = dbutils.secrets.get(scope="llm", key="openai-key")
except Exception:
    # If no secret scope (e.g. Community Edition), paste your key inline:
    OPENAI_KEY = ""  # TODO: replace with your key

# ========================
# 🧩 OpenAI Client Setup
# ========================
USE_AI = True
try:
    from openai import OpenAI
    client = OpenAI(api_key=OPENAI_KEY)
except Exception as e:
    print("⚠️ OpenAI client not available:", e)
    USE_AI = False

# ========================
# 🚀 Conversion Function
# ========================
def call_openai(prompt: str) -> str:
    if not USE_AI or not OPENAI_KEY:
        return None
    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",   # change to "gpt-4o" for higher quality
            messages=[
                {"role": "system", "content": "You are an expert SAS to PySpark converter. Return only runnable PySpark code."},
                {"role": "user", "content": prompt}
            ],
            temperature=0
        )
        return resp.choices[0].message.content
    except Exception as e:
        print("❌ OpenAI call failed:", e)
        return None

# ========================
# 🔄 Conversion Loop
# ========================
prompt_template = """Convert the following SAS code into PySpark DataFrame code.
The output must define a DataFrame called `out_df`.

SAS CODE:
{code}
"""

blocks = spark.table("migration.sas_blocks").filter("status IN ('PENDING','REVIEW')").collect()

for b in blocks:
    prompt = prompt_template.format(code=b.code)
    converted = call_openai(prompt)

    if converted and "out_df" in converted:
        # Successful conversion
        spark.sql(f"""
            UPDATE migration.sas_blocks
            SET converted_code = {repr(converted)},
                status = 'CONVERTED',
                converter_used = 'AI'
            WHERE block_id = '{b.block_id}'
        """)
    else:
        # Fallback to review
        spark.sql(f"""
            UPDATE migration.sas_blocks
            SET status = 'REVIEW',
                converter_used = 'AI',
                error_message = 'AI unavailable or no valid code produced'
            WHERE block_id = '{b.block_id}'
        """)

    time.sleep(0.3)

print("✅ AI-assisted conversion complete (with fallbacks).")

print('AI conversion attempted')
