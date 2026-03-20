import os
import time

def clean_all_states():
    print("\n🧹 Starting DATA CLEANING PIPELINE...\n")

    import pandas as pd
    from datetime import datetime, date

    # =========================
    # PATHS
    # =========================
    RAW_BASE = r"D:\L.Y\Eigth Sem\Major\Data\RawData"
    PROCESSED_BASE = r"D:\L.Y\Eigth Sem\Major\Data\ProcessedData"

    os.makedirs(PROCESSED_BASE, exist_ok=True)

    # =========================
    # HELPERS
    # =========================

    def get_latest_csv(folder):
        """Return latest CSV file path from a folder"""
        csv_files = [f for f in os.listdir(folder) if f.endswith(".csv")]
        if not csv_files:
            return None

        csv_files.sort(key=lambda x: os.path.getmtime(os.path.join(folder, x)), reverse=True)
        return os.path.join(folder, csv_files[0])


    # =========================
    # MAIN CLEANING LOOP
    # =========================

    start_time = time.time()
    today_str = date.today().isoformat()   # ✅ ADDED

    states = os.listdir(RAW_BASE)

    print("\n" + "="*70)
    print("🧹  STARTING STATEWISE CLEANING PIPELINE")
    print("="*70)

    total_states = 0
    total_rows_written = 0

    for state in states:
        raw_state_folder = os.path.join(RAW_BASE, state)

        if not os.path.isdir(raw_state_folder):
            continue

        print(f"\n{'='*60}")
        print(f"⏳ Cleaning State: {state}")
        print(f"{'='*60}")

        try:
            # 1️⃣ Get latest raw CSV
            latest_csv = get_latest_csv(raw_state_folder)

            if latest_csv is None:
                print(f"⚠️ No CSV found for {state}, skipping...")
                continue

            print(f"📄 Using file: {os.path.basename(latest_csv)}")

            # 2️⃣ Read raw CSV (skip title rows)
            df = pd.read_csv(latest_csv, skiprows=2)

            # Normalize columns
            df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_")

            print("🔎 Columns detected:", df.columns.tolist())

            # 3️⃣ Detect price columns
            price_cols = [col for col in df.columns if col.startswith("price_on")]

            if not price_cols:
                print(f"⚠️ No price columns found for {state} (probably no data)")
                continue

            print("💰 Price columns:", price_cols)

            # 4️⃣ Source date
            SOURCE_DATE = datetime.today().date()

            cleaned_rows = []

            # 5️⃣ Transform rows
            for idx, row in df.iterrows():
                commodity = row.get("commodity")
                commodity_group = row.get("commodity_group", None)

                if pd.isna(commodity):
                    continue

                for col in price_cols:
                    try:
                        date_str = col.replace("price_on_", "").replace("_", " ").strip()
                        price_date = pd.to_datetime(date_str, format="%d %b, %Y", errors='coerce')

                        if pd.isna(price_date):
                            continue

                        price_date = price_date.date()

                        price_value = pd.to_numeric(row[col], errors="coerce")
                        if pd.isna(price_value):
                            continue

                        cleaned_rows.append({
                            "state": state,
                            "commodity_group": commodity_group,
                            "commodity": commodity,
                            "price_date": price_date,
                            "price": price_value,
                            "source_date": SOURCE_DATE,
                            "price_type": "Actual"   # ADD THIS
                        })

                    except Exception as e:
                        print(f"❌ Error in row {idx}, column {col}: {e}")
                        continue

            if not cleaned_rows:
                print(f"⚠️ No valid data rows for {state}, skipping save.")
                continue

            # 6️⃣ Create cleaned DataFrame
            clean_df = pd.DataFrame(cleaned_rows)

            # 7️⃣ Save DAILY snapshot (✅ CHANGED)
            processed_state_folder = os.path.join(PROCESSED_BASE, state)
            os.makedirs(processed_state_folder, exist_ok=True)

            output_file = os.path.join(
                processed_state_folder,
                f"{state}_processed_{today_str}.csv"
            )

            clean_df.to_csv(output_file, index=False)

            print(f"✅ Cleaned file saved: {output_file}")
            print(f"📊 Rows written: {len(clean_df)}")

            total_states += 1
            total_rows_written += len(clean_df)

        except Exception as e:
            print(f"❌ Failed processing {state}: {e}")
            continue


    # =========================
    # FINAL REPORT
    # =========================

    elapsed = int(time.time() - start_time)

    hours = elapsed // 3600
    minutes = (elapsed % 3600) // 60
    seconds = elapsed % 60

    print("\n" + "="*70)
    print("🎉 CLEANING PIPELINE COMPLETED")
    print("="*70)
    print(f"🗂️ States processed successfully : {total_states}")
    print(f"📄 Total cleaned rows written     : {total_rows_written}")
    print(f"⏱️ Total time taken               : {hours}h {minutes}m {seconds}s")
    print("="*70)
    print("\n✅ CLEANING COMPLETED SUCCESSFULLY\n")
