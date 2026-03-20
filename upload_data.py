def upload_all_states():
    import os
    import pandas as pd
    from supabase import create_client
    import csv
    from datetime import datetime

    # ✅ LOGGER
    LOG_FILE = r"D:\L.Y\Eigth Sem\Major\prediction_pipeline_log.csv"
    def write_log(stage="", state="", commodity="", model="", records="", metrics="", message=""):
        file_exists = os.path.exists(LOG_FILE)
        with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if not file_exists:
                w.writerow(["timestamp","stage","state","commodity","model","records","metrics_status","message"])
            w.writerow([datetime.now(), stage, state, commodity, model, records, metrics, message])

    print("\n☁️ Uploading CLEAN data to Supabase...\n")
    write_log(stage="CLEAN_UPLOAD_START", message="Clean upload started")

    SUPABASE_URL = "https://xhtpmskqxuhqnlnfdaqb.supabase.co"
    SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InhodHBtc2txeHVocW5sbmZkYXFiIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2ODcxODE4MywiZXhwIjoyMDg0Mjk0MTgzfQ.RDNyewkc0GAMuvzr09cNjrPD95fgkOEB4NInHgVaRAo"

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    BASE = r"D:\L.Y\Eigth Sem\Major\Data\ProcessedData"

    def latest_csv(folder):
        files = [f for f in os.listdir(folder) if f.endswith(".csv")]
        if not files:
            return None
        files.sort(key=lambda x: os.path.getmtime(os.path.join(folder, x)), reverse=True)
        return os.path.join(folder, files[0])

    total = 0

    for state in os.listdir(BASE):
        state_path = os.path.join(BASE, state)
        if not os.path.isdir(state_path):
            continue

        file = latest_csv(state_path)
        if not file:
            continue

        df = pd.read_csv(file)
        cols = ["commodity_group","commodity","state","source_date","price_date","price"]
        df = df[cols]
        records = df.to_dict(orient="records")
        if not records:
            continue

        write_log(stage="CLEAN_UPLOAD_STATE", state=state,
                  records=len(records), message="Uploading clean records")

        supabase.table("agmarknet_prices").upsert(
            records,
            on_conflict="state,commodity,price_date"
        ).execute()

        total += len(records)

    write_log(stage="CLEAN_UPLOAD_DONE", records=total, message="Clean upload finished")
    print(f"\n✅ Uploaded rows: {total}\n")
