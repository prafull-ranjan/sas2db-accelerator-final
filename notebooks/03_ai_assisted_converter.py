# 03_ai_assisted_converter.py
import time, uuid, sys, subprocess

# ========================
# 🔑 API Key Handling
# ========================
OPENAI_KEY = None

# Try Databricks secrets first
try:
    OPENAI_KEY = dbutils.secrets.get(scope="llm", key="openai-key")
except Exception:
    # For Community Edition, paste your key below:
    OPENAI_KEY = "<your_openai_api_key_here>"  # TODO: replace with your key

# ========================
# 📦 Ensure OpenAI Installed
# ========================
try:
    from openai import OpenAI
except ImportError:
    print("⚠️ openai package not found. Installing...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "openai"])
    from openai import OpenAI

# ========================
# 🧩 OpenAI Client Setup
# ========================
client = None
if OPENAI_KEY and OPENAI_KEY != "<your_openai_api_key_here>":
    try:
        client = OpenAI(api_key=OPENAI_KEY)
    except Exception as e:
        print("⚠️ Could not initialize OpenAI client:", e)

# ========================
# 🚀 Conversion Function
# ========================
def call_openai(prompt: str) -> str:
    if not client:
        return None
    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",   # Or "gpt-4o" for better quality
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
        spark.sql(f"""
            UPDATE migration.sas_blocks
            SET converted_code = {repr(converted)},
                status = 'CONVERTED',
                converter_used = 'AI'
            WHERE block_id = '{b.block_id}'
        """)
    else:
        spark.sql(f"""
            UPDATE migration.sas_blocks
            SET status = 'REVIEW',
                converter_used = 'AI',
                error_message = 'AI unavailable or no valid code produced'
            WHERE block_id = '{b.block_id}'
        """)

    time.sleep(0.3)

print("✅ AI-assisted conversion complete (with fallbacks).")
