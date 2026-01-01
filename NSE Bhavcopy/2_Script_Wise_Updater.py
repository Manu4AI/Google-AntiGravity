
import os
import datetime
import logging
import pandas as pd
import pathlib

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("script_updater.log"),
        logging.StreamHandler()
    ]
)

class ScriptWiseUpdater:
    def __init__(self, master_list_path='0_Script_Master_List.csv', 
                 source_dir='NSE_Bhavcopy_Master_Data', 
                 target_dir='NSE_Bhavcopy_Scriptwsie_Data'):
        self.master_list_path = master_list_path
        self.source_dir = source_dir
        self.target_dir = target_dir
        
        # Ensure target directory exists
        if not os.path.exists(self.target_dir):
            os.makedirs(self.target_dir)
            logging.info(f"Created target directory: {self.target_dir}")
            
    def load_master_list(self):
        """Loads the list of symbols to track."""
        try:
            df = pd.read_csv(self.master_list_path)
            symbols = df['Symbol'].unique().tolist()
            logging.info(f"Loaded {len(symbols)} symbols from master list.")
            return set(symbols) # Use set for faster lookup
        except Exception as e:
            logging.error(f"Failed to load master list: {e}")
            return set()

    def get_last_line(self, filepath):
        """Reads the last non-empty line of a file efficiently."""
        try:
            with open(filepath, 'rb') as f:
                # Optimized reverse reading
                f.seek(0, os.SEEK_END)
                size = f.tell()
                block_size = 1024
                block = b''
                
                for i in range(1, (size // block_size) + 2):
                    seek_pos = size - (i * block_size)
                    if seek_pos < 0:
                        seek_pos = 0
                    
                    f.seek(seek_pos)
                    read_size = block_size if seek_pos > 0 else size - ((i-1) * block_size)
                    chunk = f.read(read_size)
                    block = chunk + block
                    
                    if b'\n' in block:
                        lines = block.split(b'\n')
                        # Strip empty trailing lines
                        non_empty_lines = [l.strip() for l in lines if l.strip()]
                        
                        if non_empty_lines:
                            return non_empty_lines[-1].decode(errors='ignore')
                    
                    if seek_pos == 0:
                        break
            return None
        except Exception:
            return None

    def get_last_updated_dates(self, symbols):
        """
        Scans target directory to find the last update date for each symbol.
        Returns a dict: {symbol: last_date}
        """
        last_dates = {}
        for symbol in symbols:
            file_path = os.path.join(self.target_dir, f"{symbol}.csv")
            if os.path.exists(file_path):
                # Default if file exists but unreadable
                found_date = False
                
                try:
                    last_line = self.get_last_line(file_path)
                    
                    if last_line:
                        parts = last_line.split(',')
                        if parts:
                            date_str = parts[0].strip()
                            if date_str and date_str.lower() != 'date':
                                try:
                                    date_obj = pd.to_datetime(date_str).date()
                                    last_dates[symbol] = date_obj
                                    found_date = True
                                except:
                                    pass
                except Exception as e:
                     logging.warning(f"Error reading last date for {symbol}: {e}")
                
                if not found_date:
                    # If file exists but we couldn't parse date, assume it's corrupt/empty
                    # OR we can try to read the whole file? 
                    # If we return 2000, we risk duplication. 
                    # Safer to clear the file if it's corrupt? No, unsafe.
                    # Best fallback: Assume TODAY to prevent duplicates, or 2000 to Force Retry?
                    # FORCE RETRY causes duplicates.
                    # Assume 2000 ONLY if file is tiny (header only).
                    if os.path.getsize(file_path) < 100:
                         last_dates[symbol] = datetime.date(2000, 1, 1)
                    else:
                         # Big file but fail to read date? Danger. 
                         # Log critical warning and SKIP updating this file this run to avoid mess.
                         logging.error(f"CRITICAL: Could not read date from {symbol}.csv despite data present. Skipping update for safety.")
                         last_dates[symbol] = datetime.date(2099, 12, 31) # Future date prevents appending
            
            # If not found (new file), default to min date
            if symbol not in last_dates:
                last_dates[symbol] = datetime.date(2000, 1, 1)
        
        return last_dates

    def get_daily_files(self):
        """
        Returns a list of (date, filepath) tuples for all Bhavcopy files, sorted by date.
        """
        daily_files = []
        for root, dirs, filenames in os.walk(self.source_dir):
            for f in filenames:
                if f.startswith('bhavcopy_') and f.endswith('.csv'):
                    try:
                        # bhavcopy_20210101.csv
                        date_str = f.split('_')[1].split('.')[0]
                        date_obj = datetime.datetime.strptime(date_str, "%Y%m%d").date()
                        full_path = os.path.join(root, f)
                        daily_files.append((date_obj, full_path))
                    except Exception:
                        continue
        
        # Sort by date
        daily_files.sort(key=lambda x: x[0])
        return daily_files

    def process_updates(self):
        """Main execution logic."""
        symbols = self.load_master_list()
        if not symbols:
            logging.error("No symbols to process. Exiting.")
            return

        logging.info("Scanning existing script files for last dates...")
        last_dates = self.get_last_updated_dates(symbols)
        
        logging.info("Scanning daily Bhavcopy files...")
        daily_files = self.get_daily_files()
        logging.info(f"Found {len(daily_files)} daily files.")
        
        # Optimization: Find global min last_date to skip early files
        # If all scripts are updated till 2024-01-01, we can skip files before that.
        
        # Prevent NaT errors
        valid_dates = []
        default_start = datetime.date(2000, 1, 1)
        
        for sym, d in last_dates.items():
            if pd.isna(d): # NaT check
                last_dates[sym] = default_start
                valid_dates.append(default_start)
            else:
                valid_dates.append(d)
                
        global_min_date = min(valid_dates) if valid_dates else default_start
        
        # Filter files to process
        files_to_process = [f for f in daily_files if f[0] > global_min_date]
        if not files_to_process:
            logging.info("All scripts are up to date.")
            return

        logging.info(f"Processing {len(files_to_process)} files for updates...")
        
        # Define output columns
        out_cols = [
            "date", 
            "open_price", "high_price", "low_price", "close_price", 
            "last_price", "prev_close", "avg_price", 
            "ttl_trd_qnty", "turnover_lacs", "no_of_trades", "deliv_qty", "deliv_per"
        ]

        # Process logic
        processed_count = 0
        for date_obj, filepath in files_to_process:
            try:
                # Load Daily CSV
                df = pd.read_csv(filepath)
                
                # Standardize Columns
                df.columns = [c.strip().upper() for c in df.columns]
                
                # Identify Date Column & Symbol Column
                date_col = None
                if 'DATE1' in df.columns: date_col = 'DATE1' 
                elif 'TIMESTAMP' in df.columns: date_col = 'TIMESTAMP'
                
                if not date_col or 'SYMBOL' not in df.columns:
                    logging.warning(f"Skipping {filepath}: Missing required columns.")
                    continue

                # Filter for target symbols immediately
                df = df[df['SYMBOL'].isin(symbols)]
                
                if df.empty:
                    continue

                # Filter EQ Series
                if 'SERIES' in df.columns:
                    df = df[df['SERIES'] == 'EQ']

                # Rename Map
                rename_map = {
                    'SYMBOL': 'symbol',
                    'SERIES': 'series',
                    date_col: 'date',
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
                    'DELIV_QTY': 'deliv_qty', 
                    'DELIV_PER': 'deliv_per'
                }
                df = df.rename(columns=rename_map)
                
                # Process each symbol in this day's file
                # But we only want to write if date_obj > last_dates[symbol]
                
                # Iterate rows to check date condition per symbol
                updates_buffer = {} # symbol -> row_string/dataframe_row
                
                for idx, row in df.iterrows():
                    sym = row['symbol']
                    if date_obj > last_dates[sym]:
                        # Prepare row data
                        row_data = {}
                        for c in out_cols:
                            row_data[c] = row.get(c, "") # Fill missing with empty string
                        
                        # Convert to csv line
                        # We can append directly to file here or buffer. 
                        # Direct append is safer for crash recovery, though slower. 
                        # Given 200 stocks, it's 200 IO ops per day file. Acceptable.
                        
                        target_file = os.path.join(self.target_dir, f"{sym}.csv")
                        
                        # Check exist to write header
                        # Note: We checked start date, but file might not verify header existence if it was empty/new
                        # But logic: if last_date is min, file might not exist or be empty.
                        
                        file_exists = os.path.exists(target_file)
                        
                        mode = 'a' if file_exists else 'w'
                        header = not file_exists
                        
                        pd.DataFrame([row_data], columns=out_cols).to_csv(target_file, mode=mode, header=header, index=False)
                        
                        # Update memory state
                        last_dates[sym] = date_obj
                
                processed_count += 1
                if processed_count % 10 == 0:
                    logging.info(f"Processed {processed_count}/{len(files_to_process)} days...")

            except Exception as e:
                logging.error(f"Error processing {filepath}: {e}")

        logging.info("Update complete.")

if __name__ == "__main__":
    # Determine paths relative to script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Paths
    master_list = os.path.join(script_dir, "0_Script_Master_List.csv")
    source_dir = os.path.join(script_dir, "NSE_Bhavcopy_Master_Data")
    target_dir = os.path.join(script_dir, "NSE_Bhavcopy_Scriptwsie_Data")
    
    updater = ScriptWiseUpdater(master_list, source_dir, target_dir)
    updater.process_updates()
