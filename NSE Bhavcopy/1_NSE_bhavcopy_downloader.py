
import os
import datetime
import time
import logging
import schedule
import pandas as pd
from nselib import capital_market

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bhavcopy_downloader.log"),
        logging.StreamHandler()
    ]
)

class BhavcopyManager:
    def __init__(self, download_dir='nse_bhavcopy_downloads'):
        self.download_dir = download_dir
        if not os.path.exists(self.download_dir):
            os.makedirs(self.download_dir)
            logging.info(f"Created download directory: {self.download_dir}")
        
    def is_trading_day(self, date_obj):
        """
        Always returns True to check every day, but identifies Weekends for logging.
        """
        if date_obj.weekday() >= 5:
            return True, "Weekend"
        return True, "Weekday"

    def download_bhavcopy(self, date_obj):
        """Downloads Bhavcopy for a specific date using nselib."""
        date_str = date_obj.strftime('%d-%m-%Y')
        try:
            logging.info(f"Attempting download for {date_str}...")
            df = capital_market.bhav_copy_with_delivery(date_str)
            
            if df is None or df.empty:
                logging.error(f"Error downloading {date_str}:  Data not found")
                return False

            # --- Strict Date Validation ---
            try:
                # Identify date column
                date_col = 'DATE1' if 'DATE1' in df.columns else 'TIMESTAMP'
                if date_col in df.columns:
                    first_date_val = df[date_col].iloc[0]
                    # Format found in file is often 'dd-Mmm-yyyy' (e.g. 09-Mar-2020)
                    file_date = pd.to_datetime(first_date_val).date()
                    
                    if file_date != date_obj:
                        logging.warning(f"Date Mismatch! Requested: {date_obj}, Received: {file_date}")
                        return False
            except Exception as e:
                logging.warning(f"Date validation failed for {date_str}: {e}. Proceeding with caution.")
            # ------------------------------
            
            # Organize into Year folders
            year_str = date_obj.strftime('%Y')
            year_dir = os.path.join(self.download_dir, year_str)
            if not os.path.exists(year_dir):
                os.makedirs(year_dir)
            
            filename = f"bhavcopy_{date_obj.strftime('%Y%m%d')}.csv"
            filepath = os.path.join(year_dir, filename)
            df.to_csv(filepath, index=False)
            logging.info(f"Successfully downloaded: {filename} -> {year_str}/")
            

            
            return True
        except Exception as e:
            logging.error(f"Error downloading {date_str}: {e}")
            return False



    def download_range(self, start_date, end_date):
        """Downloads Bhavcopy for a date range. Returns True if any file was downloaded."""
        current_date = start_date
        files_downloaded = False
        
        while current_date <= end_date:
            is_trading, reason = self.is_trading_day(current_date)
            # is_trading is always True now
            
            if reason == "Weekend":
                logging.info(f"{current_date} is a Weekend. Checking for data...")
            
            # Check if file already exists
            year_str = current_date.strftime('%Y')
            filename = f"bhavcopy_{current_date.strftime('%Y%m%d')}.csv"
            filepath = os.path.join(self.download_dir, year_str, filename)
            
            if os.path.exists(filepath):
                logging.info(f"File already exists for {current_date}, skipping.")
            else:
                success = self.download_bhavcopy(current_date)
                if success:
                    files_downloaded = True

            
            # Polite delay to avoid rate limiting
            time.sleep(1.5)
            current_date += datetime.timedelta(days=1)
            
        return files_downloaded


    def get_last_downloaded_date(self):
        """Scans the directory (recursively) to find the latest available Bhavcopy date."""
        try:
            # Recursive glob
            files = []
            # Scan root and subdirs
            for root, dirs, filenames in os.walk(self.download_dir):
                for f in filenames:
                    if f.startswith('bhavcopy_') and f.endswith('.csv'):
                        files.append(f)
            
            if not files:
                return None
            
            dates = []
            for f in files:
                try:
                    # bhavcopy_20210101.csv
                    date_str = f.split('_')[1].split('.')[0]
                    date_obj = datetime.datetime.strptime(date_str, "%Y%m%d").date()
                    dates.append(date_obj)
                except Exception:
                    continue
            
            if dates:
                return max(dates)
            return None
        except Exception as e:
            logging.error(f"Error finding last date: {e}")
            return None

    def daily_job(self):
        """Scheduled daily job with Catch-Up capability. Returns True if new data downloaded."""
        today = datetime.date.today()
        logging.info(f"--- Running Daily Job for {today} ---")
        
        last_date = self.get_last_downloaded_date()
        downloaded_any = False
        
        if last_date:
            logging.info(f"Last downloaded data found for: {last_date}")
            start_date = last_date + datetime.timedelta(days=1)
            
            if start_date <= today:
                logging.info(f"Catching up from {start_date} to {today}...")
                downloaded_any = self.download_range(start_date, today)
            else:
                logging.info("Data is already up to date.")
        else:
            logging.info("No existing data found. Downloading full history from 2020-01-01.")
            # Fallback to downloading history if no data exists
            start_date = datetime.date(2020, 1, 1)
            downloaded_any = self.download_range(start_date, today)
            
        return downloaded_any

    def run_scheduler(self, time_str="18:00"):
        """Runs the scheduler loop."""
        logging.info(f"Starting scheduler at {time_str} daily...")
        
        # Run once immediately on startup to ensure we are up to date
        logging.info("Performing initial check on startup...")
        self.daily_job()
        
        schedule.every().day.at(time_str).do(self.daily_job)
        
        while True:
            schedule.run_pending()
            time.sleep(60)

if __name__ == "__main__":
    import argparse
    import sys
    
    parser = argparse.ArgumentParser(description="NSE Bhavcopy Downloader")
    parser.add_argument('--mode', choices=['history', 'scheduler', 'check_today'], default='check_today', 
                        help="Mode: 'history' (Jan 2021-Now), 'scheduler' (Runs daily at 18:00), 'check_today' (One-time check for today)")
    
    args = parser.parse_args()
    
    # Truth Source Directory - Dynamic Path based on script location
    script_dir = os.path.dirname(os.path.abspath(__file__))
    download_dir = os.path.join(script_dir, "NSE_Bhavcopy_Master_Data")
    
    manager = BhavcopyManager(download_dir=download_dir)
    
    files_downloaded = False
    
    if args.mode == 'history':
        logging.info("--> Mode: Full History Download (Jan 2020 - Present)")
        start_date = datetime.date(2020, 1, 1)
        end_date = datetime.date.today()
        files_downloaded = manager.download_range(start_date, end_date)
        logging.info("History download complete. To run daily automation, run with --mode scheduler")
        
    elif args.mode == 'scheduler':
        logging.info("--> Mode: Continuous Scheduler (Runs daily at 18:00)")
        manager.run_scheduler()
        
    elif args.mode == 'check_today':
        logging.info("--> Mode: Catch-Up / Check Today (For Task Scheduler)")
        files_downloaded = manager.daily_job()

    # Exit Codes for Orchestrator
    # 10 = New Data Downloaded
    # 20 = No New Data (Skipping subsequent steps)
    if files_downloaded:
        print("[EXIT] New files were downloaded.")
        sys.exit(10)
    else:
        print("[EXIT] No new files downloaded.")
        sys.exit(20)


