import pandas as pd
import math
from supabase import create_client

SUPABASE_URL = "https://xhtpmskqxuhqnlnfdaqb.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InhodHBtc2txeHVocW5sbmZkYXFiIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2ODcxODE4MywiZXhwIjoyMDg0Mjk0MTgzfQ.RDNyewkc0GAMuvzr09cNjrPD95fgkOEB4NInHgVaRAo"

LOG_FILE = r"D:\L.Y\Eigth Sem\Major\prediction_pipeline_log.csv"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ✅ Read CSV
df = pd.read_csv(LOG_FILE)

# -------------------------------------------------
# ✅ FORCE TYPE FIXES BEFORE JSON CONVERSION
# -------------------------------------------------

# records column → force integer-safe
if "records" in df.columns:
    df["records"] = pd.to_numeric(df["records"], errors="coerce")
    df["records"] = df["records"].astype("Int64")  # nullable integer

# replace NaN / Inf
df = df.replace([float("inf"), float("-inf")], None)
df = df.where(pd.notnull(df), None)

# -------------------------------------------------
# ✅ Convert to records
# -------------------------------------------------

records = df.to_dict(orient="records")

# -------------------------------------------------
# ✅ Final JSON safety clean
# -------------------------------------------------

def clean_row(row):
    out = {}
    for k, v in row.items():

        if isinstance(v, float):
            if math.isnan(v) or math.isinf(v):
                out[k] = None
            else:
                out[k] = v

        elif hasattr(v, "item"):   # numpy types
            out[k] = v.item()

        else:
            out[k] = v

    return out

records = [clean_row(r) for r in records]

# -------------------------------------------------
# ✅ Upload
# -------------------------------------------------

resp = supabase.table("prediction_pipeline_logs") \
    .insert(records) \
    .execute()

print("✅ Logs uploaded:", len(records))