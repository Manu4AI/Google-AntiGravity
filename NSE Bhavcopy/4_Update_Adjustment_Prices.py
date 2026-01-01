import pandas as pd
import os
import datetime
import warnings

# Suppress SettingWithCopyWarning and UserWarning for date parsing
warnings.simplefilter(action='ignore', category=Warning)

# ================= PATHS =================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Input Paths
RAW_DATA_DIR = os.path.join(SCRIPT_DIR, "NSE_Bhavcopy_Scriptwsie_Data")
ADJUSTMENT_FILE = os.path.join(SCRIPT_DIR, "Calculated_Adjustments.csv")

# Output Path
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "NSE_Bhavcopy_Adjusted_Data")

# Ensure Output Directory Exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

def update_adjustment_prices():
    print("="*50)
    print("STARTING PRICE ADJUSTMENT PROCESS")
    print("="*50)

    # 1. Load Adjustments
    if not os.path.exists(ADJUSTMENT_FILE):
        print(f"Error: Adjustment file not found at {ADJUSTMENT_FILE}")
        return

    # Check modification time of Adjustment File
    adj_file_mtime = os.path.getmtime(ADJUSTMENT_FILE)

    try:
        adj_df = pd.read_csv(ADJUSTMENT_FILE)
        # Ensure ex_date is datetime
        adj_df['ex_date'] = pd.to_datetime(adj_df['ex_date'], dayfirst=True, errors='coerce')
    except Exception as e:
        print(f"Error reading adjustment file: {e}")
        return

    # Get unique symbols that have adjustments
    adj_symbols = set(adj_df['symbol'].unique())
    print(f"Found adjustments for {len(adj_symbols)} symbols.")

    # 2. Iterate through all files in Raw Data Directory
    # We process ALL files. If a file has no adjustment, we just copy it to the adjusted folder?
    # Or do we only process those with adjustments?
    # Ideally, the "Adjusted Data" folder should contain ALL stocks, with adjustments applied where necessary.
    # So we iterate through the raw files.

    if not os.path.exists(RAW_DATA_DIR):
        print(f"Error: Raw data directory not found at {RAW_DATA_DIR}")
        return

    raw_files = [f for f in os.listdir(RAW_DATA_DIR) if f.endswith(".csv")]
    print(f"Found {len(raw_files)} raw data files.")

    processed_count = 0
    skipped_count = 0
    adjusted_count = 0

    for file_name in raw_files:
        symbol = file_name.replace(".csv", "")
        file_path = os.path.join(RAW_DATA_DIR, file_name)
        out_path = os.path.join(OUTPUT_DIR, file_name)

        # --- Incremental Logic ---
        # We process IF:
        # 1. Output file does not exist
        # 2. Raw Input file is NEWER than Output file
        # 3. Adjustment File is NEWER than Output file (global change)
        
        should_process = True
        if os.path.exists(out_path):
            input_mtime = os.path.getmtime(file_path)
            output_mtime = os.path.getmtime(out_path)
            
            # If Input is older than Output AND Adjustment File is older than Output
            if input_mtime < output_mtime and adj_file_mtime < output_mtime:
                should_process = False

        if not should_process:
            skipped_count += 1
            continue

        try:
            # Read Data
            df = pd.read_csv(file_path)
            if 'date' not in df.columns:
                print(f"Skipping {symbol}: 'date' column missing.")
                continue
            
            # Standardize Date (Suppress warnings by ensuring cleaner input or ignoring)
            # We already ignored warnings globally.
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df = df.sort_values('date') 
            
            # Check if this symbol has adjustments
            has_adjustment = symbol in adj_symbols
            
            if has_adjustment:
                symbol_adjs = adj_df[adj_df['symbol'] == symbol]
                
                # Apply Adjustments
                # We need to apply them cumulatively?
                # The logic:
                # If a split happened in 2024 (factor 0.2), all prices BEFORE 2024 must be * 0.2.
                # If another split happened in 2010 (factor 0.5), all prices BEFORE 2010 must be * 0.5.
                # Effectively, a 2009 price gets multiplied by BOTH (0.2 * 0.5 = 0.1).
                
                # We can iterate through each adjustment event.
                for _, adj_row in symbol_adjs.iterrows():
                    ex_date = adj_row['ex_date']
                    factor = float(adj_row['price_multiplier'])

                    if pd.isnull(ex_date): continue

                    # Filter rows before ex_date
                    mask = df['date'] < ex_date
                    
                    if mask.any():
                        # Update OHLC
                        cols_to_adjust = ['open_price', 'high_price', 'low_price', 'close_price']
                        # Check which columns exist (sometimes named differently? assuming standard naming)
                        # Based on typical bhavcopy: open, high, low, close or open_price etc.
                        # Let's check columns first.
                        
                        cols_found = [c for c in cols_to_adjust if c in df.columns]
                        if not cols_found and 'close' in df.columns:
                            # Try simple names
                            cols_to_adjust = ['open', 'high', 'low', 'close']
                            cols_found = [c for c in cols_to_adjust if c in df.columns]
                            
                        # Apply Multipier
                        for col in cols_found:
                            df.loc[mask, col] = df.loc[mask, col] * factor
                            
                        # Update Volume (Inverse)
                        # Volume increases when price splits
                        # Factor = Old/New Price. (e.g. 0.5 for 1:2 split).
                        # New Qty = Old Qty / Factor = Old * (1/0.5) = Old * 2. Correct.
                        vol_cols = ['ttl_trd_qnty', 'volume', 'qty']
                        vol_found = [c for c in vol_cols if c in df.columns]
                        
                        # Cast to float to avoid IncompatibleDtype warning
                        for col in vol_found:
                            df[col] = df[col].astype(float)
                            df.loc[mask, col] = df.loc[mask, col] / factor
                            
                adjusted_count += 1
            
            # Save to Output
            # Rounding to 2 decimals for prices, 0 for volume?
            # Keeping precision is safer for analysis, but standard bhavcopy is 2 decimals.
            # Let's round prices to 2 decimals.
            price_cols = [c for c in df.columns if 'price' in c or c in ['open','high','low','close']]
            if price_cols:
                df[price_cols] = df[price_cols].round(2)
                
            df.to_csv(out_path, index=False)
            processed_count += 1
            
            if processed_count % 10 == 0:
                print(f"Processed {processed_count} files...", end='\r')

        except Exception as e:
            print(f"Error processing {symbol}: {e}")

    print("\n" + "="*50)
    print("COMPLETED")
    print(f"Total Files Processed (Updated): {processed_count}")
    print(f"Total Files Skipped (Up-to-date): {skipped_count}")
    print(f"Files Modified with Adjustments: {adjusted_count}")
    print(f"Adjusted data saved to: {OUTPUT_DIR}")
    print("="*50)

if __name__ == "__main__":
    update_adjustment_prices()
