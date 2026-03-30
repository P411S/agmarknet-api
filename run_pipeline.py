raise Exception("Test email alert")
import time
import os
import csv
import pandas as pd
import math
from supabase import create_client
from datetime import datetime

from extract_data import extract_all_states
from cleaned_data import clean_all_states
from upload_data import upload_all_states
from hybrid_predict_prices import run_price_prediction_for_all_states
from upload_predictions import upload_all_predictions

# ✅ LOGGER
LOG_FILE = r"D:\L.Y\Eigth Sem\Major\prediction_pipeline_log.csv"

def write_log(stage="", state="", commodity="", model="", records="", metrics="", message=""):
    file_exists = os.path.exists(LOG_FILE)
    with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow(["timestamp","stage","state","commodity","model","records","metrics_status","message"])
        w.writerow([datetime.now(), stage, state, commodity, model, records, metrics, message])


print("\n" + "="*70)
print("🚀 AGMARKNET PIPELINE STARTED (CLEAN + PREDICT)")
print("="*70)

write_log(stage="PIPELINE_START", message="Full pipeline started")

pipeline_start = time.time()

t = time.time()
write_log(stage="STEP_EXTRACT_START")
extract_all_states()
write_log(stage="STEP_EXTRACT_DONE", message=f"{int(time.time()-t)} sec")
print("⏱️ Extract:", int(time.time()-t), "sec\n")

t = time.time()
write_log(stage="STEP_CLEAN_START")
clean_all_states()
write_log(stage="STEP_CLEAN_DONE", message=f"{int(time.time()-t)} sec")
print("⏱️ Clean:", int(time.time()-t), "sec\n")

t = time.time()
write_log(stage="STEP_UPLOAD_CLEAN_START")
upload_all_states()
write_log(stage="STEP_UPLOAD_CLEAN_DONE", message=f"{int(time.time()-t)} sec")
print("⏱️ Upload:", int(time.time()-t), "sec\n")

t = time.time()
write_log(stage="STEP_PREDICT_START")
run_price_prediction_for_all_states()
write_log(stage="STEP_PREDICT_DONE", message=f"{int(time.time()-t)} sec")
print("⏱️ Predict:", int(time.time()-t), "sec\n")

t = time.time()
write_log(stage="STEP_UPLOAD_PRED_START")
upload_all_predictions()
write_log(stage="STEP_UPLOAD_PRED_DONE", message=f"{int(time.time()-t)} sec")
print("⏱️ Upload Predictions:", int(time.time()-t), "sec\n")

# ================================
# ✅ AUTO UPLOAD PIPELINE LOGS
# ================================
print("\n📤 Uploading pipeline logs to Supabase...")


SUPABASE_URL = "https://xhtpmskqxuhqnlnfdaqb.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InhodHBtc2txeHVocW5sbmZkYXFiIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2ODcxODE4MywiZXhwIjoyMDg0Mjk0MTgzfQ.RDNyewkc0GAMuvzr09cNjrPD95fgkOEB4NInHgVaRAo"

LOG_FILE = r"D:\L.Y\Eigth Sem\Major\prediction_pipeline_log.csv"

try:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    df = pd.read_csv(LOG_FILE)

    if "records" in df.columns:
        df["records"] = pd.to_numeric(df["records"], errors="coerce")
        df["records"] = df["records"].astype("Int64")

    df = df.replace([float("inf"), float("-inf")], None)
    df = df.where(pd.notnull(df), None)

    records = df.to_dict(orient="records")

    def clean_row(row):
        out = {}
        for k, v in row.items():
            if isinstance(v, float):
                if math.isnan(v) or math.isinf(v):
                    out[k] = None
                else:
                    out[k] = v
            elif hasattr(v, "item"):
                out[k] = v.item()
            else:
                out[k] = v
        return out

    records = [clean_row(r) for r in records]

    supabase.table("prediction_pipeline_logs") \
        .insert(records) \
        .execute()

    print("✅ Logs uploaded:", len(records))

except Exception as e:
    print("⚠️ Log upload failed:", str(e))

write_log(stage="PIPELINE_DONE", message=f"Total {int(time.time()-pipeline_start)} sec")

print("="*70)
print("✅ PIPELINE COMPLETED")
print("Total:", int(time.time()-pipeline_start), "sec")
print("="*70)