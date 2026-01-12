
import os
import datetime
import logging
import pandas as pd
import pathlib
import json

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
                 target_dir='NSE_Bhavcopy_Scriptwsie_Data',
                 mapping_path='symbol_change_map.json'):
        self.master_list_path = master_list_path
        self.source_dir = source_dir
        self.target_dir = target_dir
        self.mapping_path = mapping_path
        self.mapping = self.load_mapping()
        
        # Ensure target directory exists
        if not os.path.exists(self.target_dir):
            os.makedirs(self.target_dir)
            logging.info(f"Created target directory: {self.target_dir}")

    def load_mapping(self):
        """Loads symbol change mapping from JSON."""
        if os.path.exists(self.mapping_path):
            try:
                with open(self.mapping_path, 'r') as f:
                    mapping = json.load(f)
                    logging.info(f"Loaded {len(mapping)} symbol mappings.")
                    return mapping
            except Exception as e:
                logging.error(f"Failed to load mapping: {e}")
        return {}

    def get_current_symbol(self, symbol):
        """Returns the mapped symbol if it exists, else the original."""
        return self.mapping.get(symbol, symbol)

    def load_master_list(self):
        """Loads the list of symbols to track."""
        try:
            df = pd.read_csv(self.master_list_path, comment='#')
            symbols = [str(s).strip() for s in df['Symbol'].unique().tolist()]
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
        
        # We need to check both original and mapped symbols
        # Actually, for the master list, we always want to know when the "current" file was last updated.
        for symbol in symbols:
            # current_sym = self.get_current_symbol(symbol) # DISABLE MAPPING
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
                    if os.path.getsize(file_path) < 100:
                         last_dates[symbol] = datetime.date(2000, 1, 1)
                    else:
                         logging.error(f"CRITICAL: Could not read date from {file_path} despite data present. Skipping update for safety.")
                         last_dates[symbol] = datetime.date(2099, 12, 31) 
            
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
                        date_str = f.split('_')[1].split('.')[0]
                        date_obj = datetime.datetime.strptime(date_str, "%Y%m%d").date()
                        full_path = os.path.join(root, f)
                        daily_files.append((date_obj, full_path))
                    except Exception:
                        continue
        
        daily_files.sort(key=lambda x: x[0])
        return daily_files

    def perform_symbol_migration(self):
        """
        Automatically migrates historical data for symbols that have changed names.
        """
        if not self.mapping:
            return

        logging.info("Checking for symbol migrations...")
        
        # Directories to check for migration
        script_dir = os.path.dirname(os.path.abspath(__file__))
        target_dirs = [
            self.target_dir, # NSE_Bhavcopy_Scriptwsie_Data
            os.path.join(script_dir, "NSE_Bhavcopy_Adjusted_Data"),
            os.path.join(script_dir, "NSE_Corporate_Actions_Data")
        ]

        for old_sym, new_sym in self.mapping.items():
            for directory in target_dirs:
                if not os.path.exists(directory):
                    continue

                old_file = os.path.join(directory, f"{old_sym}.csv")
                new_file = os.path.join(directory, f"{new_sym}.csv")

                if os.path.exists(old_file):
                    logging.info(f"Migrating data for {old_sym} -> {new_sym} in {os.path.basename(directory)}")
                    try:
                        # Load old data
                        df_old = pd.read_csv(old_file)
                        
                        # Check for existing new file
                        if os.path.exists(new_file):
                            df_new = pd.read_csv(new_file)
                            df_combined = pd.concat([df_old, df_new], ignore_index=True)
                        else:
                            df_combined = df_old

                        # Deduplicate and Sort
                        # Determine date column name (usually 'date' but might be different in CA data)
                        date_col = 'date'
                        if 'ex_date' in df_combined.columns:
                            date_col = 'ex_date'
                        elif 'ExDate' in df_combined.columns:
                            date_col = 'ExDate'
                        
                        if date_col in df_combined.columns:
                            df_combined[date_col] = pd.to_datetime(df_combined[date_col])
                            df_combined = df_combined.drop_duplicates(subset=[date_col], keep='last')
                            df_combined = df_combined.sort_values(by=date_col)
                        
                        # Save to new file
                        df_combined.to_csv(new_file, index=False)
                        logging.info(f"  Successfully merged into {new_sym}.csv")
                        
                        # Keep the old file for safety (as previously requested by user)
                        logging.info(f"  [KEEPING] {old_sym}.csv for safety.")

                    except Exception as e:
                        logging.error(f"  Failed migration for {old_sym}: {e}")

    def process_updates(self):
        """Main execution logic."""
        # 0. Perform automated symbol migration first
        # 0. Perform automated symbol migration first - MOVED TO END
        # self.perform_symbol_migration()

        symbols = self.load_master_list()
        if not symbols:
            logging.error("No symbols to process. Exiting.")
            return

        logging.info("Scanning existing script files for last dates...")
        last_dates = self.get_last_updated_dates(symbols)
        
        logging.info("Scanning daily Bhavcopy files...")
        daily_files = self.get_daily_files()
        logging.info(f"Found {len(daily_files)} daily files.")
        
        valid_dates = []
        default_start = datetime.date(2000, 1, 1)
        
        for sym, d in last_dates.items():
            if pd.isna(d): 
                last_dates[sym] = default_start
                valid_dates.append(default_start)
            else:
                valid_dates.append(d)
                
        global_min_date = min(valid_dates) if valid_dates else default_start
        
        files_to_process = [f for f in daily_files if f[0] > global_min_date]
        if not files_to_process:
            logging.info("All scripts are up to date.")
            return

        # Out columns for target files
        out_cols = [
            "date", 
            "open_price", "high_price", "low_price", "close_price", 
            "last_price", "prev_close", "avg_price", 
            "ttl_trd_qnty", "turnover_lacs", "no_of_trades", "deliv_qty", "deliv_per"
        ]

        # Prepare Buffer for batch writing
        symbol_data_buffer = {sym: [] for sym in symbols}
        
        logging.info(f"Processing {len(files_to_process)} files into memory...")
        
        processed_count = 0
        for date_obj, filepath in files_to_process:
            try:
                df = pd.read_csv(filepath)
                df.columns = [c.strip().upper() for c in df.columns]
                
                date_col = None
                if 'DATE1' in df.columns: date_col = 'DATE1' 
                elif 'TIMESTAMP' in df.columns: date_col = 'TIMESTAMP'
                
                if not date_col or 'SYMBOL' not in df.columns:
                    logging.warning(f"Skipping {filepath}: Missing required columns.")
                    continue

                df = df[df['SYMBOL'].isin(symbols)]
                if df.empty:
                    continue

                if 'SERIES' in df.columns:
                    df = df[df['SERIES'] == 'EQ']

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
                
                # Check for delivery column variant
                if 'DELIV_QTY' not in df.columns and 'DELIV_QTY' in rename_map.values():
                     # Sometimes delivery is a separate file, but here we expect it in bhavcopy
                     pass

                for idx, row in df.iterrows():
                    sym = row['symbol']
                    if date_obj > last_dates.get(sym, datetime.date(2000, 1, 1)):
                        row_data = {c: row.get(c, "") for c in out_cols}
                        symbol_data_buffer[sym].append(row_data)
                        last_dates[sym] = date_obj
                
                processed_count += 1
                if processed_count % 10 == 0:
                    logging.info(f"Buffered {processed_count}/{len(files_to_process)} days...")

            except Exception as e:
                logging.error(f"Error buffering {filepath}: {e}")

        # Final Step: Batch Write to files
        logging.info("Writing buffered data to disk (Batch Mode)...")
        write_count = 0
        for sym, rows in symbol_data_buffer.items():
            if not rows:
                continue
                
            # current_sym = self.get_current_symbol(sym) # DISABLE MAPPING
            target_file = os.path.join(self.target_dir, f"{sym}.csv")
            file_exists = os.path.exists(target_file)
            
            try:
                mode = 'a' if file_exists else 'w'
                header = not file_exists
                
                # Convert list of dicts to DataFrame for fast writing
                pd.DataFrame(rows, columns=out_cols).to_csv(target_file, mode=mode, header=header, index=False)
                write_count += 1
                if write_count % 50 == 0:
                     logging.info(f"Updated {write_count} symbol files...")
            except Exception as e:
                logging.error(f"Failed to write data for {sym}: {e}")

        logging.info(f"Update complete. {write_count} symbols updated.")
        
        # Perform Merge (Migration) AFTER generation
        self.perform_symbol_migration()

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    master_list = os.path.join(script_dir, "0_Script_Master_List.csv")
    source_dir = os.path.join(script_dir, "NSE_Bhavcopy_Master_Data")
    target_dir = os.path.join(script_dir, "NSE_Bhavcopy_Scriptwsie_Data")
    mapping_path = os.path.join(script_dir, "symbol_change_map.json")
    
    updater = ScriptWiseUpdater(master_list, source_dir, target_dir, mapping_path)
    updater.process_updates()
