
import os
import time
import pandas as pd
from glob import glob
from datetime import datetime
import nselib
from nselib import libutil

# ================= PATHS =================
# ================= PATHS =================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

SYMBOL_SOURCE = os.path.join(SCRIPT_DIR, "NSE_Bhavcopy_Scriptwsie_Data")
CA_OUTPUT_DIR = os.path.join(SCRIPT_DIR, "NSE_Corporate_Actions_Data")

os.makedirs(CA_OUTPUT_DIR, exist_ok=True)

# ================= FETCH CA =================
def fetch_corporate_actions(symbol):
    try:
        # Use nselib's internal fetcher which handles headers/session
        url = f"https://www.nseindia.com/api/corporates-corporateActions?index=equities&symbol={symbol}"
        
        # nse_urlfetch returns a requests.Response object or json data?
        # Based on typical usage, it returns a response object or raises error.
        # Let's assume it returns a response object or similar content.
        # Inspecting source usually shows it returns .json() output.
        
        data = libutil.nse_urlfetch(url)
        return data.json()
    except Exception as e:
        raise e

# ================= CONFIG =================
SYMBOL_LIST_PATH = os.path.join(SCRIPT_DIR, "0_Script_Master_List.csv")

# Load Target Symbols from CSV
if os.path.exists(SYMBOL_LIST_PATH):
    try:
        TARGET_SYMBOLS = sorted(pd.read_csv(SYMBOL_LIST_PATH)["Symbol"].dropna().unique().tolist())
    except Exception as e:
        print(f"[ERROR] Reading symbol list: {e}", flush=True)
        TARGET_SYMBOLS = []
else:
    print(f"[WARNING] Symbol file not found: {SYMBOL_LIST_PATH}", flush=True)
    TARGET_SYMBOLS = []

# ================= MAIN =================
def main():
    import sys # Import sys for exit codes

    # Remove directory cleanup for incremental updates
    # if os.path.exists(CA_OUTPUT_DIR):
    #     print(f"Cleaning output directory: {CA_OUTPUT_DIR}")
    #     shutil.rmtree(CA_OUTPUT_DIR)
    os.makedirs(CA_OUTPUT_DIR, exist_ok=True)
    
    print(f"[*] Symbols to download CA for: {len(TARGET_SYMBOLS)} (NIFTY 50 + NEXT 50)", flush=True)
    print(f"USING NSELIB Version: {nselib.__version__}", flush=True)

    updates_count = 0
    failed_count = 0
    skipped_count = 0

    today = datetime.now().date()

    for i, symbol in enumerate(TARGET_SYMBOLS, start=1):
        try:
            out_file = f"{CA_OUTPUT_DIR}/{symbol}.csv"
            
            # --- Incremental Logic (Time Check) ---
            # If we already updated/checked this file today, skip the network call entirely
            if os.path.exists(out_file):
                mtime = datetime.fromtimestamp(os.path.getmtime(out_file)).date()
                if mtime == today:
                    skipped_count += 1
                    continue

            # Add a small delay to avoid rate limiting
            time.sleep(0.5) 
            
            raw_data = fetch_corporate_actions(symbol)
            
            if not raw_data:
                # We save an empty file or touch the existing one to mark it as "checked today"
                if os.path.exists(out_file):
                    os.utime(out_file, None)
                skipped_count += 1
                continue
                
            df = pd.DataFrame(raw_data)

            if df.empty:
                 if os.path.exists(out_file):
                    os.utime(out_file, None)
                 skipped_count += 1
                 continue

            # --- Content Comparison Logic ---
            # Use lineterminator='\n' to match Python's universal newline reading
            new_csv_content = df.to_csv(index=False, lineterminator='\n')
            file_changed = True
            
            if os.path.exists(out_file):
                with open(out_file, 'r', encoding='utf-8') as f:
                    old_csv_content = f.read()
                
                if old_csv_content.strip() == new_csv_content.strip():
                    file_changed = False
                    # Still update the timestamp so we don't re-check it today
                    os.utime(out_file, None)
            
            if file_changed:
                with open(out_file, 'w', encoding='utf-8', newline='') as f:
                    f.write(new_csv_content)
                updates_count += 1
                print(f"[OK] [{i}] {symbol} CA Updated", flush=True)
            else:
                skipped_count += 1
                # print(f"  [{i}] {symbol} No Change")

        except Exception as e:
            print(f"[FAIL] [{i}] {symbol} failed -> {str(e)}", flush=True)
            failed_count += 1

    print("\n[FINISH] Step-3 completed", flush=True)
    print(f"Updated Files: {updates_count}", flush=True)
    print(f"Failed: {failed_count}", flush=True)
    print(f"Skipped (No Change): {skipped_count}", flush=True)
    
    # Exit Codes for Orchestrator
    # 10 = New Data Found (Updates happened)
    # 20 = No New Data (All skipped/failed)
    if updates_count > 0:
        print("[EXIT] Corporate Actions Updated.", flush=True)
        sys.exit(10)
    else:
        print("[EXIT] No New Corporate Actions.", flush=True)
        sys.exit(20)

if __name__ == "__main__":
    main()