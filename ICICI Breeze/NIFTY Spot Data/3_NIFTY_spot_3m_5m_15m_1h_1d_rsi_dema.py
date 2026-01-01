import pandas as pd
import os

# --- Configuration ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPORTS_DIR = os.path.join(BASE_DIR, "Reports")

# Timeframes to process
TIMEFRAMES = ['3m', '5m', '15m', '1h', '1d']

def calculate_rsi(data, period=14):
    delta = data['close'].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.ewm(com=period-1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period-1, min_periods=period).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def calculate_dema(series, period):
    ema1 = series.ewm(span=period, adjust=False).mean()
    ema2 = ema1.ewm(span=period, adjust=False).mean()
    dema = 2 * ema1 - ema2
    return dema

def process_all_timeframes():
    print("--- Calculating RSI & DEMA and Merging Reports ---")
    
    consolidated_data = {}

    for tf in TIMEFRAMES:
        input_filename = f"NIFTY_Spot_{tf}.csv"
        input_path = os.path.join(REPORTS_DIR, input_filename)
        
        output_filename = f"NIFTY_Spot_{tf}_RSI_DEMA.csv"
        output_path = os.path.join(REPORTS_DIR, output_filename)
        
        if not os.path.exists(input_path):
            print(f"Skipping {tf}: Input file not found ({input_filename})")
            continue
            
        print(f"Processing {tf}...")
        
        try:
            df = pd.read_csv(input_path)
            
            if 'close' not in df.columns:
                print(f"Error: 'close' column missing in {tf}")
                continue
                
            # Calculate Indicators
            df['rsi'] = calculate_rsi(df, 14).round(2)
            df['dema_100'] = calculate_dema(df['close'], 100).round(2)
            
            # Save CSV
            df.to_csv(output_path, index=False)
            print(f"Saved CSV: {output_filename}")
            
            # Add to Consolidated Map
            consolidated_data[tf] = df
            
        except Exception as e:
            print(f"Error processing {tf}: {e}")

    # Save Consolidated Excel
    if consolidated_data:
        merged_path = os.path.join(REPORTS_DIR, "NIFTY_Spot_3m_5m_15m_1h_1d_signals.xlsx")
        print(f"Saving Merged RSI/DEMA Report: {merged_path}...")
        try:
            save_consolidated_excel(consolidated_data, merged_path)
            print("Merged Report Saved Successfully.")
        except Exception as e:
            print(f"Error saving merged report: {e}")

    print("--- Done ---")

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
    process_all_timeframes()
