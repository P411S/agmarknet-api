def upload_all_predictions():

    import os
    import pandas as pd
    import numpy as np
    import math
    from datetime import date, datetime
    from supabase import create_client, Client
    import csv

    # ✅ LOGGER
    LOG_FILE = r"D:\L.Y\Eigth Sem\Major\prediction_pipeline_log.csv"
    def write_log(stage="", state="", commodity="", model="", records="", metrics="", message=""):
        file_exists = os.path.exists(LOG_FILE)
        with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if not file_exists:
                w.writerow(["timestamp","stage","state","commodity","model","records","metrics_status","message"])
            w.writerow([datetime.now(), stage, state, commodity, model, records, metrics, message])

    print("\n☁️ Uploading PREDICTION data to Supabase...\n")
    write_log(stage="PRED_UPLOAD_START", message="Prediction upload started")

    SUPABASE_URL = "https://xhtpmskqxuhqnlnfdaqb.supabase.co"
    SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InhodHBtc2txeHVocW5sbmZkYXFiIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2ODcxODE4MywiZXhwIjoyMDg0Mjk0MTgzfQ.RDNyewkc0GAMuvzr09cNjrPD95fgkOEB4NInHgVaRAo"
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    BASE_PATH = r"D:\L.Y\Eigth Sem\Major\Data\PredictedData"

    def latest_csv(folder):
        files = [f for f in os.listdir(folder) if f.endswith(".csv")]
        if not files:
            return None
        files.sort(key=lambda x: os.path.getmtime(os.path.join(folder, x)), reverse=True)
        return os.path.join(folder, files[0])

    def clean_value(v):
        if v is None: return None
        if isinstance(v, (date, datetime)): return v.isoformat()
        if isinstance(v, float):
            if math.isnan(v) or math.isinf(v): return None
        return v

    total_rows = 0

    for state in os.listdir(BASE_PATH):

        state_path = os.path.join(BASE_PATH, state)
        if not os.path.isdir(state_path):
            continue

        csv_file = latest_csv(state_path)
        if not csv_file:
            continue

        df = pd.read_csv(csv_file)
        if "price_date" in df.columns:
            df["price_date"] = pd.to_datetime(df["price_date"], errors="coerce").dt.date

        df = df.replace([np.inf, -np.inf], np.nan)

        raw_records = df.to_dict(orient="records")
        records = [{k: clean_value(v) for k,v in row.items()} for row in raw_records]

        if not records:
            continue

        write_log(stage="PRED_UPLOAD_STATE", state=state,
                  records=len(records), message="Uploading predictions")

        try:
            supabase.table("agmarknet_price_predictions").upsert(
                records,
                on_conflict="state,commodity,price_date"
            ).execute()

            total_rows += len(records)
            write_log(stage="PRED_UPLOAD_OK", state=state,
                      records=len(records), message="Upload success")

        except Exception as e:
            write_log(stage="PRED_UPLOAD_FAIL", state=state, message=str(e))

    write_log(stage="PRED_UPLOAD_DONE", records=total_rows,
              message="Prediction upload finished")

    print(f"\n🎯 Prediction upload completed → {total_rows} rows\n")
