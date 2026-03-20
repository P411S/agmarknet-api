import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

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


def extract_all_states():
    print("\n🚀 Starting DATA EXTRACTION PIPELINE...\n")
    write_log(stage="EXTRACT_START", message="Extraction started")

    BASE_DOWNLOAD_PATH = r"D:\L.Y\Eigth Sem\Major\Data\RawData"

    STATE_NAMES = [
        "Andaman and Nicobar Islands", "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar",
        "Chandigarh", "Chhattisgarh", "Dadra and Nagar Haveli and Daman and Diu", "Delhi", "Goa",
        "Gujarat", "Haryana", "Himachal Pradesh", "Jammu and Kashmir", "Jharkhand", "Karnataka",
        "Kerala", "Ladakh", "Lakshadweep", "Madhya Pradesh", "Maharashtra", "Manipur", "Meghalaya",
        "Mizoram", "Nagaland", "Odisha", "Puducherry", "Punjab", "Rajasthan", "Sikkim",
        "Tamil Nadu", "Telangana", "Tripura", "Uttar Pradesh", "Uttarakhand", "West Bengal"
    ]

    def wait_for_download_complete(download_path, timeout=120):
        start_time = time.time()
        before_files = set(os.listdir(download_path))

        while True:
            files = os.listdir(download_path)
            cr_files = [f for f in files if f.endswith(".crdownload")]
            csv_files = [f for f in files if f.endswith(".csv")]
            new_files = set(csv_files) - before_files

            if not cr_files and new_files:
                latest_file = max(
                    [os.path.join(download_path, f) for f in new_files],
                    key=os.path.getmtime
                )
                return latest_file

            if time.time() - start_time > timeout:
                raise Exception("❌ Download failed: No new CSV detected.")

            time.sleep(1)

    options = Options()

    prefs = {
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "profile.default_content_settings.popups": 0,
        "safebrowsing.enabled": True
    }
    options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 20)

    total_start_time = time.time()

    print("[DEBUG] Opening website...")
    driver.get("https://www.agmarknet.gov.in")
    driver.maximize_window()
    time.sleep(3)

    for STATE_NAME in STATE_NAMES:

        print(f"\n{'='*60}")
        print(f"⏳ Processing State: {STATE_NAME}")
        print(f"{'='*60}")

        write_log(stage="EXTRACT_STATE_START", state=STATE_NAME, message="State extraction start")

        try:
            driver.get("https://www.agmarknet.gov.in")
            time.sleep(3)

            STATE_FOLDER = os.path.join(BASE_DOWNLOAD_PATH, STATE_NAME)
            os.makedirs(STATE_FOLDER, exist_ok=True)

            driver.execute_cdp_cmd(
                "Page.setDownloadBehavior",
                {
                    "behavior": "allow",
                    "downloadPath": STATE_FOLDER
                }
            )
            time.sleep(1)

            state_dropdown = wait.until(
                EC.element_to_be_clickable((By.ID, "state"))
            )
            driver.execute_script("arguments[0].click();", state_dropdown)
            time.sleep(1)

            all_states = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//*[normalize-space()='All States']"))
            )
            driver.execute_script("arguments[0].click();", all_states)
            time.sleep(1)

            state_element = wait.until(
                EC.presence_of_element_located((
                    By.XPATH, f"//*[normalize-space()='{STATE_NAME}']"
                ))
            )

            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", state_element)
            time.sleep(0.5)
            driver.execute_script("arguments[0].click();", state_element)

            print(f"✅ {STATE_NAME} selected")
            time.sleep(1)

            go_button = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[.//span[contains(text(),'Go')]]"))
            )
            driver.execute_script("arguments[0].click();", go_button)
            print(f"🚀 GO clicked for {STATE_NAME}")
            time.sleep(5)

            tables = driver.find_elements(By.CSS_SELECTOR, "table.table-custom")

            if not tables:
                print(f"⚠️ No data available for {STATE_NAME} — skipping.")
                write_log(stage="EXTRACT_STATE_SKIP", state=STATE_NAME, message="No table data")
                continue

            download_btn = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[@title='Download Report']"))
            )
            driver.execute_script("arguments[0].click();", download_btn)
            time.sleep(1)

            csv_btn = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Download as CSV']]"))
            )
            driver.execute_script("arguments[0].click();", csv_btn)

            latest_file = wait_for_download_complete(STATE_FOLDER)

            write_log(stage="EXTRACT_STATE_DONE", state=STATE_NAME,
                      message=f"Downloaded {os.path.basename(latest_file)}")

            time.sleep(2)

        except Exception as e:
            print(f"❌ Error processing {STATE_NAME}: {str(e)}")
            write_log(stage="EXTRACT_STATE_FAIL", state=STATE_NAME, message=str(e))
            continue

    total_end_time = time.time()
    total_seconds = int(total_end_time - total_start_time)

    write_log(stage="EXTRACT_END", records=len(STATE_NAMES), message="Extraction finished")

    driver.quit()
    print("\n✅ EXTRACTION COMPLETED SUCCESSFULLY\n")
    print(f"⏱️ Total Extraction Time: {total_seconds} seconds\n")