import pandas as pd
import os
import glob
from datetime import datetime

# --- Configuration ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MASTER_DATA_DIR = os.path.join(BASE_DIR, "NIFTY Spot Master Data")
OUTPUT_DIR = os.path.join(BASE_DIR, "Reports")

# Timeframe Config
TIMEFRAMES = {
    '3m': '3min',
    '5m': '5min',
    '15m': '15min',
    '1h': '1h',
    '1d': '1D'
}

def convert_multiframe():
    print(f"--- Starting Multi-Timeframe Conversion ---")
    print(f"Source: {MASTER_DATA_DIR}")
    
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"Created directory: {OUTPUT_DIR}")

    # 1. Load Master Data
    search_pattern = os.path.join(MASTER_DATA_DIR, "**", "*.csv")
    files = glob.glob(search_pattern, recursive=True)
    
    if not files:
        print("Error: No CSV files found in Master Data!")
        return

    print(f"Found {len(files)} CSV files. Loading data...")

    df_list = []
    for f in files:
        try:
            df_temp = pd.read_csv(f, usecols=lambda x: x.lower() in ['datetime', 'open', 'high', 'low', 'close', 'volume', 'stock_code'])
            df_list.append(df_temp)
        except Exception as e:
            print(f"Warning: Failed to read {os.path.basename(f)}: {e}")

    if not df_list:
        print("Error: Could not load any data.")
        return

    print("Combining data...")
    full_df = pd.concat(df_list, ignore_index=True)
    full_df.columns = [c.lower() for c in full_df.columns]
    print("Parsing datetimes...")
    full_df['datetime'] = pd.to_datetime(full_df['datetime'])
    full_df = full_df.sort_values('datetime').set_index('datetime')
    
    print(f"Total 1-min rows: {len(full_df)}")

    # 2. Extract Pre-Market Data (Common for all)
    print("Extracting Pre-Market (09:00-09:14) prices...")
    pre_market_data = full_df.between_time('09:00', '09:14:59')
    daily_pre_open = pre_market_data['close'].resample('1D').last().dropna()
    daily_pre_high = pre_market_data['high'].resample('1D').max().dropna()
    daily_pre_low = pre_market_data['low'].resample('1D').min().dropna()

    # 3. Process Each Timeframe and Collect for Consolidation
    consolidated_data = {}
    
    for tf_name, tf_rule in TIMEFRAMES.items():
        df_result = process_timeframe(full_df, tf_name, tf_rule, daily_pre_open, daily_pre_high, daily_pre_low)
        if df_result is not None:
             consolidated_data[tf_name] = df_result

    # 4. Save Consolidated Excel Output (NIFTY_Spot_3m_5m_15m_1h_1d.xlsx)
    if consolidated_data:
        merged_path = os.path.join(OUTPUT_DIR, "NIFTY_Spot_3m_5m_15m_1h_1d.xlsx")
        print(f"Saving Merged Report: {merged_path}...")
        try:
             save_consolidated_excel(consolidated_data, merged_path)
             print("Merged Consolidated Report Saved Successfully.")
        except Exception as e:
             print(f"Error saving consolidated Excel: {e}")

def process_timeframe(full_df, tf_name, tf_rule, daily_pre_open, daily_pre_high, daily_pre_low):
    print(f"\nProcessing {tf_name} ({tf_rule})...")
    
    agg_dict = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }
    if 'stock_code' in full_df.columns:
        agg_dict['stock_code'] = 'first'

    # Resample
    if tf_rule == '1D':
        # Align 1D to midnight to capture full calendar day (including 09:00 Pre-Market)
        resampled = full_df.resample(tf_rule, origin='start_day').agg(agg_dict)
    else:
        # Align Intraday to 09:15 start
        resampled = full_df.resample(tf_rule, origin=pd.Timestamp('2020-01-01 09:15:00')).agg(agg_dict)
        
    resampled = resampled.dropna(subset=['open'])
    
    # Apply Pre-Market Logic
    resampled['date_only'] = resampled.index.date
    
    for date_val, pre_price in daily_pre_open.items():
        if tf_rule == '1D':
            # For 1D, the record for 'date_val' should simply be the one with that index
            # (since we are now aligned to midnight, the index is 2025-12-01 00:00:00)
             try:
                target_ts = pd.Timestamp(date_val)
                if target_ts in resampled.index:
                    resampled.at[target_ts, 'open'] = pre_price
                    if pre_price > resampled.at[target_ts, 'high']: resampled.at[target_ts, 'high'] = pre_price
                    if pre_price < resampled.at[target_ts, 'low']: resampled.at[target_ts, 'low'] = pre_price
                    
                    if date_val in daily_pre_high.index:
                         p_high = daily_pre_high[date_val]
                         if p_high > resampled.at[target_ts, 'high']: resampled.at[target_ts, 'high'] = p_high
                    
                    if date_val in daily_pre_low.index:
                         p_low = daily_pre_low[date_val]
                         if p_low < resampled.at[target_ts, 'low']: resampled.at[target_ts, 'low'] = p_low
             except Exception as e:
                print(f"Error processing pre-market for {date_val}: {e}")
                continue
                
        else:
            # Intraday Logic (unchanged)
            target_ts = pd.Timestamp(date_val).replace(hour=9, minute=15, second=0)
        
            if target_ts in resampled.index:
                resampled.at[target_ts, 'open'] = pre_price
                if pre_price > resampled.at[target_ts, 'high']: resampled.at[target_ts, 'high'] = pre_price
                if pre_price < resampled.at[target_ts, 'low']: resampled.at[target_ts, 'low'] = pre_price
                
                if date_val in daily_pre_high.index:
                     p_high = daily_pre_high[date_val]
                     if p_high > resampled.at[target_ts, 'high']: resampled.at[target_ts, 'high'] = p_high
                
                if date_val in daily_pre_low.index:
                     p_low = daily_pre_low[date_val]
                     if p_low < resampled.at[target_ts, 'low']: resampled.at[target_ts, 'low'] = p_low
 
    # Filter Time (Intraday only between market hours)
    if tf_rule != '1D':
        resampled = resampled[resampled.index.time >= pd.to_datetime('09:15').time()]

    # --- Limit to Last 2 Years (Rolling) for 3m, 5m, 15m ---
    if tf_name in ['3m', '5m', '15m']:
        cutoff_date = pd.Timestamp.now() - pd.DateOffset(years=2)
        print(f"Limiting {tf_name} to last 2 years (>= {cutoff_date.date()})...")
        resampled = resampled[resampled.index >= cutoff_date]

    # Cleanup & Filter Weekends
    if 'date_only' in resampled.columns:
        resampled = resampled.drop(columns=['date_only'])
        
    # Explicit Weekend Filter (Safety Net)
    # 5=Sat, 6=Sun
    resampled = resampled[resampled.index.dayofweek < 5]

    resampled = resampled.reset_index()
    
    # Format Date for 1D Output
    if tf_rule == '1D':
         resampled['datetime'] = resampled['datetime'].dt.date
    
    # Save Individual CSV
    out_file_csv = os.path.join(OUTPUT_DIR, f"NIFTY_Spot_{tf_name}.csv")
    print(f"Saving CSV: {out_file_csv}...")
    resampled.to_csv(out_file_csv, index=False)
    
    # Return df for consolidation
    return resampled

def save_consolidated_excel(data_dict, path):
    # Using xlsxwriter for formatting
    with pd.ExcelWriter(path, engine='xlsxwriter') as writer:
        for sheet_name, df in data_dict.items():
            df.to_excel(writer, index=False, sheet_name=sheet_name)
            worksheet = writer.sheets[sheet_name]
            
            # 1. Freeze Top Row
            worksheet.freeze_panes(1, 0)
            
            # 2. Add Filter
            (max_row, max_col) = df.shape
            worksheet.autofilter(0, 0, max_row, max_col - 1)
            
            # 3. Autofit Columns
            for i, col in enumerate(df.columns):
                header_len = len(str(col))
                max_val_len = 0
                if len(df) > 0:
                    if pd.api.types.is_numeric_dtype(df[col]):
                        max_val_len = 12
                    else:
                        sample = df[col].astype(str).head(1000)
                        max_val_len = sample.map(len).max()
                column_len = max(header_len, max_val_len) + 2
                worksheet.set_column(i, i, column_len)

if __name__ == "__main__":
    convert_multiframe()
