import os
import datetime
import logging
import pandas as pd
from io import StringIO
from gcs_handler import GCSHandler

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CloudScriptUpdater:
    def __init__(self):
        self.gcs = GCSHandler()
        self.master_list_blob = "config/0_Script_Master_List.csv" # Expects this in cloud
        self.bhavcopy_prefix = "bhavcopy"
        self.script_data_prefix = "script_data"
        self.target_symbols = self.load_master_list()
        
    def load_master_list(self):
        """Loads symbols from GCS config or local fallback."""
        try:
            # Try reading from GCS first
            if self.gcs.file_exists(self.master_list_blob):
                df = self.gcs.read_csv(self.master_list_blob)
            else:
                # Fallback to local if running in mixed env
                local_path = "0_Script_Master_List.csv"
                if os.path.exists(local_path):
                    logging.info("Uploading local Master List to GCS...")
                    df = pd.read_csv(local_path)
                    self.gcs.write_csv(df, self.master_list_blob)
                else:
                    logging.error("Master List not found!")
                    return set()
            
            symbols = df['Symbol'].unique().tolist()
            logging.info(f"Loaded {len(symbols)} symbols.")
            return set(symbols)
        except Exception as e:
            logging.error(f"Failed to load master list: {e}")
            return set()

    def get_latest_bhavcopy_blob(self):
        """Finds the most recent Bhavcopy CSV in GCS."""
        files = self.gcs.list_files(prefix=self.bhavcopy_prefix)
        # Filter for csv
        csv_files = [f for f in files if f.endswith('.csv')]
        if not csv_files: return None
        # Sort by name (bhavcopy_YYYYMMDD) implies sort by date
        return sorted(csv_files)[-1]

    def process_latest_bhavcopy(self):
        """Reads latest bhavcopy and updates script files."""
        if not self.target_symbols: return
        
        latest_blob = self.get_latest_bhavcopy_blob()
        if not latest_blob:
            logging.warning("No Bhavcopy found in GCS.")
            return

        logging.info(f"Processing latest file: {latest_blob}")
        
        # Read Bhavcopy
        df = self.gcs.read_csv(latest_blob)
        if df is None: return

        # Standardize Columns
        df.columns = [c.strip().upper() for c in df.columns]
        
        # Identify Date
        date_col = 'DATE1' if 'DATE1' in df.columns else 'TIMESTAMP'
        if date_col not in df.columns:
            logging.error("Date column missing.")
            return
            
        # Get the Date object of this file
        file_date_str = df[date_col].iloc[0]
        try:
            file_date_obj = pd.to_datetime(file_date_str).date()
        except:
             logging.error(f"Could not parse date: {file_date_str}")
             return

        # Prepare column mapping
        rename_map = {
            'SYMBOL': 'symbol', 'SERIES': 'series', date_col: 'date',
            'OPEN_PRICE': 'open_price', 'OPEN': 'open_price',
            'HIGH_PRICE': 'high_price', 'HIGH': 'high_price',
            'LOW_PRICE': 'low_price', 'LOW': 'low_price',
            'CLOSE_PRICE': 'close_price', 'CLOSE': 'close_price',
            'PREV_CLOSE': 'prev_close', 'PREVCLOSE': 'prev_close',
            'LAST_PRICE': 'last_price', 'LAST': 'last_price',
            'AVG_PRICE': 'avg_price', 'AVG': 'avg_price',
            'TTL_TRD_QNTY': 'ttl_trd_qnty', 'TOTTRDQTY': 'ttl_trd_qnty', 
            'TURNOVER_LACS': 'turnover_lacs', 'TOTTRDVAL': 'turnover_lacs',
            'NO_OF_TRADES': 'no_of_trades', 'TOTALTRADES': 'no_of_trades',
            'DELIV_QTY': 'deliv_qty', 'DELIV_PER': 'deliv_per'
        }
        
        out_cols = [
            "date", "open_price", "high_price", "low_price", "close_price", 
            "last_price", "prev_close", "avg_price", 
            "ttl_trd_qnty", "turnover_lacs", "no_of_trades", "deliv_qty", "deliv_per"
        ]

        # Filter & Rename
        df = df[df['SYMBOL'].isin(self.target_symbols)]
        if 'SERIES' in df.columns: df = df[df['SERIES'] == 'EQ']
        df = df.rename(columns=rename_map)

        updates_count = 0
        
        for idx, row in df.iterrows():
            sym = row['symbol']
            
            # Script Blob Path
            script_blob = f"{self.script_data_prefix}/{sym}.csv"
            
            # Prepare new row dataframe
            row_data = {c: row.get(c, "") for c in out_cols}
            new_row_df = pd.DataFrame([row_data])

            # Read existing file
            if self.gcs.file_exists(script_blob):
                existing_df = self.gcs.read_csv(script_blob)
                
                # Check duplication
                # Convert date col to strings for comparison
                existing_dates = existing_df['date'].astype(str).values
                current_date_str = str(row_data['date'])
                
                if current_date_str in existing_dates:
                    # Already updated
                    continue
                
                # Append
                updated_df = pd.concat([existing_df, new_row_df], ignore_index=True)
            else:
                updated_df = new_row_df

            # Write back
            self.gcs.write_csv(updated_df, script_blob)
            updates_count += 1
            if updates_count % 10 == 0:
                logging.info(f"Updated {updates_count} scripts...")

        logging.info(f"Total Updates: {updates_count}")

if __name__ == "__main__":
    updater = CloudScriptUpdater()
    updater.process_latest_bhavcopy()
