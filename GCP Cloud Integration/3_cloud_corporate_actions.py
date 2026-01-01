import os
import time
import pandas as pd
import logging
from datetime import datetime
import nselib
from nselib import libutil
from gcs_handler import GCSHandler

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CloudCorporateActions:
    def __init__(self):
        self.gcs = GCSHandler()
        self.master_list_blob = "config/0_Script_Master_List.csv"
        self.ca_prefix = "corporate_actions"
        self.target_symbols = self.load_master_list()
        
    def load_master_list(self):
        """Loads symbols from GCS."""
        try:
            if self.gcs.file_exists(self.master_list_blob):
                df = self.gcs.read_csv(self.master_list_blob)
                symbols = df['Symbol'].unique().tolist()
                return sorted(symbols)
            return []
        except Exception as e:
            logging.error(f"Failed to load master list: {e}")
            return []

    def fetch_corporate_actions(self, symbol):
        try:
            url = f"https://www.nseindia.com/api/corporates-corporateActions?index=equities&symbol={symbol}"
            data = libutil.nse_urlfetch(url)
            return data.json()
        except Exception as e:
            # logging.error(f"Fetch failed for {symbol}: {e}")
            return None

    def run(self):
        if not self.target_symbols:
            logging.error("No symbols to process.")
            return

        updates_count = 0
        skipped_count = 0
        failed_count = 0
        
        logging.info(f"Checking Corporate Actions for {len(self.target_symbols)} symbols...")
        
        for i, symbol in enumerate(self.target_symbols, start=1):
            try:
                # API Call
                time.sleep(0.5)
                raw_data = self.fetch_corporate_actions(symbol)
                
                if not raw_data:
                    skipped_count += 1
                    continue
                
                df = pd.DataFrame(raw_data)
                if df.empty:
                    skipped_count += 1
                    continue

                # Content Check
                blob_name = f"{self.ca_prefix}/{symbol}.csv"
                write_needed = True
                
                if self.gcs.file_exists(blob_name):
                    existing_df = self.gcs.read_csv(blob_name)
                    
                    # Simple comparison of CSV string representation
                    # Normalizing to avoid float/int mismatches if possible, 
                    # but direct string compare of `to_csv` is robust enough for identical data
                    current_csv = df.to_csv(index=False)
                    existing_csv = existing_df.to_csv(index=False)
                    
                    if current_csv.strip() == existing_csv.strip():
                        write_needed = False
                
                if write_needed:
                    self.gcs.write_csv(df, blob_name)
                    updates_count += 1
                    logging.info(f"✔ [{i}] {symbol} Updated")
                else:
                    skipped_count += 1
            
            except Exception as e:
                logging.error(f"✖ [{i}] {symbol} Failed: {e}")
                failed_count += 1

        logging.info("Step Completed.")
        logging.info(f"Updated: {updates_count}, Skipped: {skipped_count}, Failed: {failed_count}")
        
        # Exit Codes for Orchestrator
        if updates_count > 0:
            import sys
            sys.exit(10)
        else:
            import sys
            sys.exit(20)

if __name__ == "__main__":
    app = CloudCorporateActions()
    app.run()
