import os
import datetime
import time
import logging
import schedule
import pandas as pd
from nselib import capital_market
try:
    from nselib import trading_holiday_calendar
except ImportError:
    trading_holiday_calendar = None
from gcs_handler import GCSHandler

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CloudBhavcopyManager:
    def __init__(self):
        self.gcs = GCSHandler()
        self.base_prefix = "bhavcopy" # Folder in bucket
        self.holidays = self.get_trading_holidays()
        
    def get_trading_holidays(self):
        """Fetches trading holidays from nselib."""
        if trading_holiday_calendar is None:
            logging.warning("trading_holiday_calendar not available in installed nselib version.")
            return []
        try:
            holiday_df = trading_holiday_calendar()
            if 'tradingDate' in holiday_df.columns:
                 holidays = pd.to_datetime(holiday_df['tradingDate'], format='%d-%b-%Y').dt.date.tolist()
            else:
                 holidays = []
            return holidays
        except Exception as e:
            logging.error(f"Failed to fetch trading holidays: {e}")
            return []

    def is_trading_day(self, date_obj):
        """Checks if the given date is a trading day."""
        if date_obj.weekday() >= 5: return False, "Weekend"
        if date_obj in self.holidays: return False, "Holiday"
        return True, "Trading Day"

    def download_bhavcopy(self, date_obj):
        """Downloads Bhavcopy and uploads to GCS."""
        date_str = date_obj.strftime('%d-%m-%Y')
        try:
            logging.info(f"Attempting download for {date_str}...")
            df = capital_market.bhav_copy_with_delivery(date_str)
            
            if df is None or df.empty:
                logging.warning(f"No data found for {date_str}")
                return False
            
            # GCS Path: bhavcopy/2024/bhavcopy_20240101.csv
            year_str = date_obj.strftime('%Y')
            filename = f"bhavcopy_{date_obj.strftime('%Y%m%d')}.csv"
            blob_name = f"{self.base_prefix}/{year_str}/{filename}"
            
            # Write directly to GCS
            success = self.gcs.write_csv(df, blob_name)
            
            if success:
                logging.info(f"Successfully uploaded: {blob_name}")
                return True
            return False
            
        except Exception as e:
            logging.error(f"Error downloading {date_str}: {e}")
            return False

    def download_range(self, start_date, end_date):
        """Downloads Bhavcopy for a date range."""
        current_date = start_date
        files_downloaded = False
        
        while current_date <= end_date:
            is_trading, reason = self.is_trading_day(current_date)
            if is_trading:
                # Check GCS if exists
                year_str = current_date.strftime('%Y')
                filename = f"bhavcopy_{current_date.strftime('%Y%m%d')}.csv"
                blob_name = f"{self.base_prefix}/{year_str}/{filename}"
                
                if self.gcs.file_exists(blob_name):
                    logging.info(f"File already exists in cloud: {blob_name}, skipping.")
                else:
                    success = self.download_bhavcopy(current_date)
                    if success: files_downloaded = True
            else:
                logging.info(f"Skipping {current_date}: {reason}")

            time.sleep(1.5)
            current_date += datetime.timedelta(days=1)
            
        return files_downloaded

    def get_last_downloaded_date(self):
        """Scans GCS bucket to find the latest available Bhavcopy date."""
        try:
            files = self.gcs.list_files(prefix=self.base_prefix)
            if not files: return None
            
            dates = []
            for f in files:
                # Expected format: bhavcopy/2021/bhavcopy_20210101.csv
                try:
                    basename = os.path.basename(f) # bhavcopy_20210101.csv
                    if basename.startswith('bhavcopy_') and basename.endswith('.csv'):
                        date_str = basename.split('_')[1].split('.')[0]
                        date_obj = datetime.datetime.strptime(date_str, "%Y%m%d").date()
                        dates.append(date_obj)
                except Exception:
                    continue
            
            if dates: return max(dates)
            return None
        except Exception as e:
            logging.error(f"Error finding last date: {e}")
            return None

    def daily_job(self):
        """Scheduled daily job."""
        today = datetime.date.today()
        logging.info(f"--- Running Cloud Daily Job for {today} ---")
        
        last_date = self.get_last_downloaded_date()
        downloaded = False
        
        if last_date:
            logging.info(f"Last cloud data found for: {last_date}")
            start_date = last_date + datetime.timedelta(days=1)
            
            if start_date <= today:
                downloaded = self.download_range(start_date, today)
            else:
                logging.info("Cloud data is already up to date.")
        else:
            logging.info("No cloud data found. Auto-Switching to History Backfill (From 2020-01-01).")
            # Default to backfilling from Jan 1st, 2020 if bucket is empty
            start_date = datetime.date(2020, 1, 1)
            downloaded = self.download_range(start_date, today)
            
        return downloaded

if __name__ == "__main__":
    import argparse
    import sys
    
    parser = argparse.ArgumentParser(description="Cloud Bhavcopy Downloader")
    parser.add_argument('--mode', choices=['history', 'check_today'], default='check_today')
    
    args = parser.parse_args()
    
    manager = CloudBhavcopyManager()
    
    files_downloaded = False
    
    if args.mode == 'history':
        logging.info("--> Mode: Full History Download to Cloud")
        start_date = datetime.date(2021, 1, 1)
        end_date = datetime.date.today()
        files_downloaded = manager.download_range(start_date, end_date)
        
    elif args.mode == 'check_today':
        logging.info("--> Mode: Check Today (Cloud)")
        files_downloaded = manager.daily_job()

    if files_downloaded:
        sys.exit(10)
    else:
        sys.exit(20)
