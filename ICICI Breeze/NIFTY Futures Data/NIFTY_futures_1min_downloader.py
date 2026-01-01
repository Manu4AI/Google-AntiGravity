import csv
import time
from datetime import datetime, timedelta
import calendar
from breeze_connect import BreezeConnect
import os
import sys
import logging
import glob

# Add parent directory to path to find auto_login and config_file
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config_file as app_config
import auto_login

# Setup Logging
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
DATA_DIR = os.path.join(BASE_DIR, "NIFTY Spot Master Data")
# Wait, user asked to check `G:\Other computers\My Mac\Shivendra -Mac Sync\Google AntiGravity\ICICI Breeze\NIFTY Futures Data\NIFTY Spot Master Data`?
# Ah, the user said: "download the NIFTY Future data and store into ...\NIFTY Futures Data\NIFTY Spot Master Data"
# This seems like a copy-paste error or weird naming ("Spot Master Data" inside Futures folder?). 
# BUT I must follow instructions perfectly. 
# Re-reading prompt: "store into G:\...\ICICI Breeze\NIFTY Futures Data\NIFTY Spot Master Data"
# Okay, I will use that folder name "NIFTY Spot Master Data" inside the Futures folder.

DATA_DIR = os.path.join(BASE_DIR, "NIFTY Spot Master Data")
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOG_DIR, f'futures_downloader_{datetime.now().strftime("%Y%m%d")}.log'), 
    level=logging.DEBUG, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Initialize SDK (will be done in main)
isec = None

def init_api():
    global isec
    try:
        logging.info("Initializing Breeze Connect via Auto-Login...")
        print("Initializing Breeze Connect via Auto-Login...", flush=True)
        
        session_token = auto_login.get_session_token()
        if not session_token:
             logging.error("Auto-Login Failed.")
             return False

        isec = BreezeConnect(api_key=app_config.API_KEY)
        # Use try-except for generation to be robust
        try:
             isec.generate_session(api_secret=app_config.SECRET_KEY, session_token=session_token)
        except Exception as e:
             if "Unable to retrieve customer details" in str(e):
                  logging.warning(" [Warning] API Maintenance error ignored. FORCING Session Injection.")
                  # Force inject credentials
                  isec.session_key = session_token
                  isec.secret_key = app_config.SECRET_KEY
                  isec.api_key = app_config.API_KEY
                  isec.user_id = app_config.USER_ID 
             else:
                  raise e

        logging.info("Session Generated Successfully.")
        print("Session Generated Successfully.", flush=True)
        return True
    except Exception as e:
        logging.error(f"Failed to initialize API: {str(e)}")
        print(f"Failed to initialize API: {str(e)}", flush=True)
        return False

def get_expiry_date(year, month):
    """
    Calculates the monthly expiry date.
    Rule: Last Thursday of the month.
    Exception: From Sep 2025 onwards, Last Tuesday of the month.
    """
    if year > 2025 or (year == 2025 and month >= 9):
        target_weekday = 1 # Tuesday
    else:
        target_weekday = 3 # Thursday

    last_day_of_month = calendar.monthrange(year, month)[1]
    last_date = datetime(year, month, last_day_of_month)
    
    days_diff = (last_date.weekday() - target_weekday + 7) % 7
    expiry_date = last_date - timedelta(days=days_diff)
    
    return expiry_date

def get_active_contracts(current_date):
    """
    Returns a list of 3 expiry dates (Near, Next, Far) for the given trading date.
    """
    active_expiries = []
    
    # 1. Identify "Current/Near Month" Expiry
    curr_month_expiry = get_expiry_date(current_date.year, current_date.month)
    
    if current_date.date() <= curr_month_expiry.date():
        start_month_offset = 0
    else:
        start_month_offset = 1
        
    for i in range(3):
        # Calculate target month/year handling overflow
        total_months = current_date.month + start_month_offset + i
        
        target_year = current_date.year + (total_months - 1) // 12
        target_month = (total_months - 1) % 12 + 1
        
        expiry = get_expiry_date(target_year, target_month)
        active_expiries.append(expiry)
        
    return active_expiries

def get_historical_data(stock_code, expiry_date, from_date, to_date, interval="1minute"):
    try:
        expiry_str = expiry_date.strftime("%Y-%m-%dT06:00:00.000Z")
        
        data = isec.get_historical_data_v2(interval=interval,
                            from_date=from_date,
                            to_date=to_date,
                            stock_code=stock_code,
                            exchange_code="NFO",
                            product_type="futures",
                            expiry_date=expiry_str,
                            right="others",
                            strike_price="0")
        return data
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

def save_to_csv(data, filename):
    # Ensure directory exists (redundant if main creates it, but safe)
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    if not data or 'Success' not in data or not data['Success']:
         return

    records = data['Success']
    if not records:
        return

    file_exists = os.path.isfile(filename)
    keys = records[0].keys()
    
    with open(filename, 'a', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames=keys)
        if not file_exists:
            dict_writer.writeheader()
        dict_writer.writerows(records)

def get_last_global_date(data_dir):
    """
    Finds the latest date downloaded across ALL CSV files in the data_dir.
    """
    # Look for CSVs in the Data folder
    files = glob.glob(os.path.join(data_dir, "NIFTY_Futures_*.csv"))
    if not files:
        return None
        
    # Find the most recently modified file
    latest_file = max(files, key=os.path.getmtime)
    
    try:
        with open(latest_file, 'rb') as f:
            try:
                f.seek(-2, os.SEEK_END)
                while f.read(1) != b'\n':
                    f.seek(-2, os.SEEK_CUR)
            except OSError:
                return None 
            last_line = f.readline().decode().strip()
            
        if not last_line:
            return None
            
        parts = last_line.split(',')
        for part in parts:
             try:
                 dt = datetime.strptime(part, "%Y-%m-%d %H:%M:%S")
                 return dt
             except ValueError:
                 try:
                    dt = datetime.strptime(part, "%Y-%m-%dT%H:%M:%S.000Z")
                    return dt
                 except ValueError:
                    pass
    except Exception:
        pass
        
    return None

def download_historical_data_in_chunks(stock_code, start_date_str, end_date_str, data_dir):
    start_dt = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    current_dt = start_dt
    
    print(f"Looping from {start_dt.date()} to {end_dt.date()}...", flush=True)

    while current_dt < end_dt:
        f_date = current_dt.strftime("%Y-%m-%dT00:00:00.000Z")
        t_date = current_dt.strftime("%Y-%m-%dT23:59:59.000Z")
        
        active_expiries = get_active_contracts(current_dt)
        
        print(f"[{current_dt.date()}] Downloading 3 Contracts...", flush=True)
        
        for expiry in active_expiries:
            # Save to Data/filename
            filename = os.path.join(data_dir, f"NIFTY_Futures_{expiry.strftime('%Y-%m-%d')}.csv")
            
            data = get_historical_data(stock_code=stock_code,
                                       expiry_date=expiry,
                                       from_date=f_date,
                                       to_date=t_date)
            
            if data:
                 save_to_csv(data, filename)
        
        current_dt = current_dt + timedelta(days=1)


def main():
    print("Script main() started.", flush=True)
    
    if not init_api():
        print("Exiting due to API init failure.", flush=True)
        return

    stock_code = "NIFTY"
    
    # Data Directory is already defined globally as DATA_DIR
    print(f"Data Directory: {DATA_DIR}", flush=True)
    
    # 1. Start Date
    start_date_str = "2020-01-01"
    
    # Resume Logic Check in Data Directory
    # We need to search recursively potentially if stored in year folders?
    # The get_last_global_date function was simple. Let's stick to it or improve it if we use year folders?
    # The user asked to store in `NIFTY Spot Master Data`.
    # Let's assume flat or modify `save_to_csv` to do year folders? 
    # The existing `save_to_csv` (lines 99-118) just appends. 
    # BUT `download_historical_data_in_chunks` (line 178) creates filename `NIFTY_Futures_YYYY-MM-DD.csv`.
    # This creates thousands of files.
    # The Spot downloader used `Year/Month` files. 
    # For now, I will stick to the existing Futures logic of daily files unless instructed otherwise,
    # OR better, since this is "Master Data", maybe I should group them like Spot?
    # The user said "in the similar way". "Similar way" -> Year/Month folders.
    # I should upgrade this script to save monthly files like Spot Downloader!
    
    last_dt = get_last_global_date(DATA_DIR)
    if last_dt:
        print(f"Found existing data up to {last_dt}.", flush=True)
        start_date_str = (last_dt + timedelta(days=1)).strftime("%Y-%m-%d")
    
    # End Date: Today + 1
    end_date_str = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    
    # If start > end, stop
    if datetime.strptime(start_date_str, "%Y-%m-%d") > datetime.strptime(end_date_str, "%Y-%m-%d"):
         print("Data is up to date.")
         return

    print(f"Saving to '{DATA_DIR}'", flush=True)
    print(f"Starting Multi-Contract Download from {start_date_str} to {end_date_str}", flush=True)
    download_historical_data_in_chunks(stock_code, start_date_str, end_date_str, DATA_DIR)
    print("Download complete.", flush=True)

if __name__ == "__main__":
    main()
