import csv
import time
import os
import sys
import logging
from datetime import datetime, timedelta
# Add parent directory to path to find auto_login and config_file
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config_file as paper_config
import auto_login
from breeze_connect import BreezeConnect
import glob

# Setup Logging
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
DATA_DIR = os.path.join(BASE_DIR, "NIFTY Spot Master Data")
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

log_filename = f"nifty_downloader_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, log_filename)),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Suppress noisy third-party logs
logging.getLogger("breeze_connect").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)

# Global Breeze Instance
breeze = None

def init_api():
    """Initializes Breeze Connect with Auto-Login."""
    global breeze
    try:
        logger.info("Initializing Breeze Connect via Auto-Login...")
        
        session_token = auto_login.get_session_token()
        if not session_token:
             logger.error("Auto-Login Failed.")
             return False

        breeze = BreezeConnect(api_key=paper_config.API_KEY)
        # Use try-except for generation to be robust
        try:
             breeze.generate_session(api_secret=paper_config.SECRET_KEY, session_token=session_token)
        except Exception as e:
             if "Unable to retrieve customer details" in str(e):
                  logger.warning(" [Warning] API Maintenance error ignored. FORCING Session Injection.")
                  # Force inject credentials since generate_session failed to set them
                  breeze.session_key = session_token
                  breeze.secret_key = paper_config.SECRET_KEY
                  breeze.api_key = paper_config.API_KEY
                  breeze.user_id = paper_config.USER_ID # Important if SDK uses it
             else:
                  raise e

        logger.info("Session Generated Successfully.")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize API: {str(e)}")
        return False

def test_api_connection():
    """Test API with a known working date."""
    logger.info("--- TEST CONNECTION START ---")
    try:
        # Test with 2024-01-03 (Working in debug script)
        data = breeze.get_historical_data_v2(interval="1minute",
                            from_date="2024-01-03T00:00:00.000Z",
                            to_date="2024-01-03T23:59:59.000Z",
                            stock_code="NIFTY",
                            exchange_code="NSE",
                            product_type="cash",
                            expiry_date="",
                            right="",
                            strike_price="0")
        logger.info(f"Test Call Response Type: {type(data)}")
        if data and 'Success' in data:
             logger.info(f"Test Call Success. Records: {len(data['Success'])}")
        else:
             logger.error(f"Test Call FAILED. Response: {data}")
    except Exception as e:
        logger.error(f"Test Call Exception: {e}")
    logger.info("--- TEST CONNECTION END ---")

def get_historical_data(stock_code, from_date, to_date, interval="1minute"):
    try:
        # Retry logic
        for attempt in range(3):
            try:
                # logger.info(f"Fetching: {stock_code} {from_date} -> {to_date}")
                data = breeze.get_historical_data_v2(interval=interval,
                                    from_date=from_date,
                                    to_date=to_date,
                                    stock_code=stock_code,
                                    exchange_code="NSE",
                                    product_type="cash",
                                    expiry_date="",
                                    right="",
                                    strike_price="0")
                if data is None:
                    logger.warning(f"API Returned None for {from_date}")
                return data
            except Exception as e:
                logger.warning(f"  Retry {attempt+1}... Error: {e}")
                time.sleep(2)
        return None
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        return None

def save_to_monthly_file(data, base_filename="NIFTY_Spot_1min"):
    if not data or 'Success' not in data or not data['Success']:
         return

    records = data['Success']
    if not records:
        return

    # Data is fetched daily, so it belongs to one month.
    first_date_str = records[0]['datetime'] 
    try:
        dt_obj = datetime.strptime(first_date_str, "%Y-%m-%d %H:%M:%S")
        year = dt_obj.year
        month = dt_obj.strftime("%m")
        
        # Create Year Folder inside Master Data
        year_folder = os.path.join(DATA_DIR, str(year))
        if not os.path.exists(year_folder):
            os.makedirs(year_folder)
            
        # Monthly Filename: NIFTY_Spot_1min_2020_01.csv
        filename = os.path.join(year_folder, f"{base_filename}_{year}_{month}.csv")
        
        # Check if file exists to write header
        file_exists = os.path.isfile(filename)
        
        keys = records[0].keys()
        
        with open(filename, 'a', newline='') as output_file:
            dict_writer = csv.DictWriter(output_file, fieldnames=keys)
            if not file_exists:
                dict_writer.writeheader()
            dict_writer.writerows(records)
        logger.info(f"Data appended to {filename}")
        
    except Exception as e:
        logger.error(f"Error saving data: {e}")

def get_last_date():
    """Finds the latest date from the existing year-wise/monthly files."""
    try:
        # 1. Find all year folders in DATA_DIR
        if not os.path.exists(DATA_DIR):
             return None
             
        items = os.listdir(DATA_DIR)
        years = []
        for item in items:
            item_path = os.path.join(DATA_DIR, item)
            if os.path.isdir(item_path) and item.isdigit() and len(item) == 4:
                years.append(int(item))
        
        if not years:
            return None
            
        years.sort(reverse=True) # Latest year first
        latest_year = years[0]
        year_folder = os.path.join(DATA_DIR, str(latest_year))
        
        # 2. Find all CSV files in that year folder
        files = [f for f in os.listdir(year_folder) if f.endswith(".csv") and "NIFTY_Spot_1min" in f]
        
        if not files:
            return None

        # 3. Sort files to find the latest one
        # NIFTY_Spot_1min_2020_12.csv > NIFTY_Spot_1min_2020.csv (ASCII '_' > '.')
        # So reverse sort puts December (or latest month) first.
        files.sort(reverse=True)
        
        latest_file = os.path.join(year_folder, files[0])
        logger.info(f"Checking latest file: {latest_file}")
        
        return get_last_date_from_file(latest_file)
            
    except Exception as e:
        logger.error(f"Error finding last date: {e}")
    return None

def get_last_date_from_file(filename):
    if not os.path.exists(filename): return None
    try:
        with open(filename, 'rb') as f:
            try:
                f.seek(-2, os.SEEK_END)
                while f.read(1) != b'\n':
                    f.seek(-2, os.SEEK_CUR)
            except OSError:
                return None
            last_line = f.readline().decode().strip()
            
        if not last_line: return None
        
        parts = last_line.split(',')
        if len(parts) > 1:
            # Look for date format
            # API usually returns "datetime" key, but CSV order depends on dict writer.
            # Usually: close, datetime, exchange_code, ...
            # Let's try to parse the one that looks like a date.
            
            # Known Breeze format keys: ... datetime ...
            # We used DictWriter with keys from API response.
            # Typically 'datetime' is the key.
            # Since we don't know the exact column index without header, 
            # we rely on the fact that the date string has a specific format.
            
            for part in parts:
                try:
                    dt = datetime.strptime(part, "%Y-%m-%d %H:%M:%S")
                    return dt
                except:
                    continue
    except Exception as e:
        logger.error(f"Error reading file {filename}: {e}")
    return None

# NSE Trading Holidays (2020-2025)
NSE_HOLIDAYS = {
    # 2020
    "2020-02-21": "Mahashivratri",
    "2020-03-10": "Holi",
    "2020-04-02": "Ram Navami",
    "2020-04-06": "Mahavir Jayanti",
    "2020-04-10": "Good Friday",
    "2020-04-14": "Dr. Baba Saheb Ambedkar Jayanti",
    "2020-05-01": "Maharashtra Day",
    "2020-05-25": "Id-Ul-Fitr",
    "2020-10-02": "Mahatma Gandhi Jayanti",
    "2020-11-16": "Diwali Balipratipada",
    "2020-11-30": "Gurunanak Jayanti",
    "2020-12-25": "Christmas",
    # 2021
    "2021-01-26": "Republic Day",
    "2021-03-11": "Mahashivratri",
    "2021-03-29": "Holi",
    "2021-04-02": "Good Friday",
    "2021-04-14": "Dr. Baba Saheb Ambedkar Jayanti",
    "2021-04-21": "Ram Navami",
    "2021-05-13": "Id-Ul-Fitr",
    "2021-07-21": "Bakri Id",
    "2021-08-19": "Moharram",
    "2021-09-10": "Ganesh Chaturthi",
    "2021-10-15": "Dussehra",
    "2021-11-04": "Diwali Laxmi Pujan",
    "2021-11-05": "Diwali Balipratipada",
    "2021-11-19": "Gurunanak Jayanti",
    # 2022
    "2022-01-26": "Republic Day",
    "2022-03-01": "Mahashivratri",
    "2022-03-18": "Holi",
    "2022-04-14": "Ambedkar / Mahavir Jayanti",
    "2022-04-15": "Good Friday",
    "2022-05-03": "Id-Ul-Fitr",
    "2022-08-09": "Moharram",
    "2022-08-15": "Independence Day",
    "2022-08-31": "Ganesh Chaturthi",
    "2022-10-05": "Dussehra",
    "2022-10-24": "Diwali Laxmi Pujan",
    "2022-10-26": "Diwali Balipratipada",
    "2022-11-08": "Gurunanak Jayanti",
    # 2023
    "2023-01-26": "Republic Day",
    "2023-03-07": "Holi",
    "2023-03-30": "Ram Navami",
    "2023-04-04": "Mahavir Jayanti",
    "2023-04-07": "Good Friday",
    "2023-04-14": "Dr. Baba Saheb Ambedkar Jayanti",
    "2023-05-01": "Maharashtra Day",
    "2023-06-28": "Bakri Id",
    "2023-08-15": "Independence Day",
    "2023-09-19": "Ganesh Chaturthi",
    "2023-10-02": "Mahatma Gandhi Jayanti",
    "2023-10-24": "Dussehra",
    "2023-11-14": "Diwali Balipratipada",
    "2023-11-27": "Gurunanak Jayanti",
    "2023-12-25": "Christmas",
    # 2024
    "2024-01-26": "Republic Day",
    "2024-03-08": "Mahashivratri",
    "2024-03-25": "Holi",
    "2024-03-29": "Good Friday",
    "2024-04-11": "Id-Ul-Fitr",
    "2024-04-17": "Shri Ram Navami",
    "2024-05-01": "Maharashtra Day",
    "2024-06-17": "Bakri Id",
    "2024-07-17": "Moharram",
    "2024-08-15": "Independence Day",
    "2024-10-02": "Mahatma Gandhi Jayanti",
    "2024-11-01": "Diwali Laxmi Pujan",
    "2024-11-15": "Gurunanak Jayanti",
    "2024-12-25": "Christmas",
    # 2025 (Tentative/Announced)
    "2025-01-26": "Republic Day",
    "2025-02-26": "Mahashivratri",
    "2025-03-14": "Holi",
    "2025-03-31": "Id-Ul-Fitr",
    "2025-04-06": "Ram Navami",
    "2025-04-10": "Mahavir Jayanti",
    "2025-04-14": "Ambedkar Jayanti",
    "2025-04-18": "Good Friday",
    "2025-05-01": "Maharashtra Day",
    "2025-06-07": "Eid-ul-Adha",
    "2025-07-06": "Muharram",
    "2025-08-15": "Independence Day",
    "2025-08-27": "Ganesh Chaturthi",
    "2025-10-21": "Diwali Laxmi Pujan",
    "2025-11-02": "Diwali Balipratipada",
    "2025-12-25": "Christmas"
}

def main():
    logger.info("Script started.")
    
    if not init_api():
        return

    test_api_connection()

    stock_code = "NIFTY"
    
    # 1. Determine Start Date
    start_date = datetime(2020, 1, 1) # Default start set to Jan 2020 as requested
    
    last_dt = get_last_date()
    if last_dt:
        logger.info(f"Found existing year-wise data up to {last_dt}.")
        start_date = last_dt + timedelta(days=1)
    else:
        logger.info("No existing year-wise data found. Starting fresh from 2020-01-01.")
    
    end_date = datetime.now()
    
    if start_date.date() > end_date.date():
         logger.info("Data is up to date.")
         return
         
    logger.info(f"Starting download for {stock_code} from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    current_dt = start_date
    while current_dt.date() <= end_date.date():
        
        # Don't download for future
        if current_dt.date() > datetime.now().date():
            break
            
        f_date = current_dt.strftime("%Y-%m-%dT00:00:00.000Z")
        t_date = current_dt.strftime("%Y-%m-%dT23:59:59.000Z")
        date_str = current_dt.strftime("%Y-%m-%d")

        # 1. Skip Weekends
        if current_dt.weekday() >= 5:
            logger.info(f" [Skipped] {date_str} is a Weekend.")
            current_dt = current_dt + timedelta(days=1)
            continue

        # 2. Skip Known Holidays
        if date_str in NSE_HOLIDAYS:
            logger.info(f" [Skipped] {date_str} is a Trading Holiday: {NSE_HOLIDAYS[date_str]}")
            current_dt = current_dt + timedelta(days=1)
            continue
            
        logger.info(f"Downloading {stock_code} for {date_str}...")
        
        data = get_historical_data(stock_code=stock_code,
                                   from_date=f_date,
                                   to_date=t_date)
                                   
        if data and 'Success' in data and data['Success']:
            save_to_monthly_file(data)
            time.sleep(0.5) # Prevent Rate Limiting
        else:
            # If it's not a weekend and not in our holiday list, but still no data
            # If it's not a weekend and not in our holiday list, but still no data
            # Log the raw response to debug WHY it is missing (Rate Limit, Session, etc.)
            logger.info(f" [Info] No data for {date_str}. Response: {data}")
            time.sleep(1.0) # Increased backoff to prevent cascading rate limit failures
            
        current_dt = current_dt + timedelta(days=1)

    logger.info("Download complete.")

if __name__ == "__main__":
    main()
