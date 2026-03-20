def run_price_prediction_for_all_states():

    import os
    import pandas as pd
    import numpy as np
    from datetime import timedelta, date

    # ✅ LOGGER ADDED
    import csv
    from datetime import datetime
    LOG_FILE = r"D:\L.Y\Eigth Sem\Major\prediction_pipeline_log.csv"

    def write_log(stage="", state="", commodity="", model="", records="", metrics="", message=""):
        file_exists = os.path.exists(LOG_FILE)
        with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if not file_exists:
                w.writerow(["timestamp","stage","state","commodity","model","records","metrics_status","message"])
            w.writerow([datetime.now(), stage, state, commodity, model, records, metrics, message])

    print("\n📈 Starting HYBRID PRICE PREDICTION (ALL STATES)...\n")
    write_log(stage="PREDICT_START", message="Prediction run started")

    BASE_CLEAN_PATH = r"D:\L.Y\Eigth Sem\Major\Data\ProcessedData"
    OUTPUT_PATH = r"D:\L.Y\Eigth Sem\Major\Data\PredictedData"
    METRIC_PATH = r"D:\L.Y\Eigth Sem\Major\Data\PredictionMetrics"

    os.makedirs(OUTPUT_PATH, exist_ok=True)
    os.makedirs(METRIC_PATH, exist_ok=True)

    today = date.today()

    def latest_csv(folder):
        files = [f for f in os.listdir(folder) if f.endswith(".csv")]
        if not files:
            return None
        files.sort(key=lambda x: os.path.getmtime(os.path.join(folder, x)), reverse=True)
        return os.path.join(folder, files[0])

    def rmse(a, p): return float(np.sqrt(np.mean((a - p) ** 2)))
    def mae(a, p): return float(np.mean(np.abs(a - p)))
    def mape(a, p):
        a = np.array(a); p = np.array(p)
        mask = a != 0
        if mask.sum() == 0: return None
        return float(np.mean(np.abs((a[mask]-p[mask])/a[mask]))*100)

    def confidence_from_rmse(r):
        if r is None: return "LOW"
        if r < 20: return "HIGH"
        elif r < 50: return "MEDIUM"
        else: return "LOW"

    for state in os.listdir(BASE_CLEAN_PATH):

        state_path = os.path.join(BASE_CLEAN_PATH, state)
        if not os.path.isdir(state_path):
            continue

        write_log(stage="STATE_START", state=state, message="State processing")

        clean_file = latest_csv(state_path)
        if not clean_file:
            continue

        df = pd.read_csv(clean_file)

        if "price_date" not in df or "price" not in df:
            continue

        df["price_date"] = pd.to_datetime(df["price_date"])
        df = df.dropna(subset=["price"])
        df = df.sort_values("price_date")

        state_predictions = []
        state_metrics = []

        for commodity, gdf in df.groupby("commodity"):

            gdf = gdf.sort_values("price_date")
            prices = gdf["price"].astype(float).values
            if len(prices) == 0:
                continue

            commodity_group = gdf["commodity_group"].iloc[0] if "commodity_group" in gdf.columns else None
            district = gdf["district"].iloc[0] if "district" in gdf.columns else None
            market = gdf["market"].iloc[0] if "market" in gdf.columns else None

            if len(prices) >= 4:
                train = prices[:-1]; test = prices[-1:]
            else:
                train = prices; test = []

            if len(train) == 0:
                continue

            if len(train) >= 3:
                base = train[-1]
                trend = train[-1] - train[-2]
                preds = [base+trend*0.6, base+trend*1.0, base+trend*1.4]
                model = "hybrid_trend"

            elif len(train) == 2:
                base = train[-1]
                trend = train[-1] - train[-2]
                avg = np.mean(train)
                preds = [avg*0.99, avg, avg*1.01]
                model = "hybrid_ma"

            else:
                base = train[-1]
                trend = 0
                preds = [base, base, base]
                model = "fallback_copy"

            write_log(stage="MODEL_SELECT", state=state, commodity=commodity,
                      model=model, records=len(prices), message="Model chosen")

            if len(test) > 0:
                predicted_test = [preds[0]]
                r = rmse(test, predicted_test)
                a = mae(test, predicted_test)
                mp = mape(test, predicted_test)
                conf = confidence_from_rmse(r)
                write_log(stage="METRICS", state=state, commodity=commodity,
                          metrics="COMPUTED", message=f"RMSE={r:.2f}")
            else:
                r = 0.0; a = 0.0; mp = 0.0; conf = "LOW"
                write_log(stage="METRICS", state=state, commodity=commodity,
                          metrics="SKIPPED", message="No test data")

            last_date = gdf["price_date"].max()
            future_dates = [last_date + timedelta(days=i) for i in range(1, 4)]

            for d, p in zip(future_dates, preds):
                state_predictions.append({
                    "state": state,
                    "commodity": commodity,
                    "commodity_group": commodity_group,
                    "district": district,
                    "market": market,
                    "predicted_for_date": d.date(),
                    "predicted_price": round(float(p), 2),
                    "model_used": model,
                    "rmse": float(r),
                    "mae": float(a),
                    "mape": float(mp),
                    "confidence_score": conf,
                    "trained_till_date": last_date.date(),
                    "prediction_run_date": today,
                    "base_last_price": float(base),
                    "trend_used": float(trend),
                    "price_date": d.date(),
                    "price": round(float(p), 2),
                    "price_type": "predicted"
                })

        if state_predictions:
            out_state_dir = os.path.join(OUTPUT_PATH, state)
            os.makedirs(out_state_dir, exist_ok=True)
            out_file = os.path.join(out_state_dir, f"{state}_predicted_{today}.csv")
            pd.DataFrame(state_predictions).to_csv(out_file, index=False)
            write_log(stage="STATE_DONE", state=state, records=len(state_predictions),
                      message="Prediction CSV saved")

    write_log(stage="PREDICT_END", message="Prediction run finished")
    print("\n🎯 ALL STATE PREDICTIONS + METRICS SAVED\n")
