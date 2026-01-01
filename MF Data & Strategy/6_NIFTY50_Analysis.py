
import pandas as pd
import os

# Configuration
DATA_DIR = 'MF_Data'
INPUT_FILE = 'NIFTY50_Data.csv'
OUTPUT_FILE = 'NIFTY50_Analysis.csv'
INPUT_PATH = os.path.join(DATA_DIR, INPUT_FILE)
OUTPUT_PATH = os.path.join(DATA_DIR, OUTPUT_FILE)

def analyze_nifty_data():
    if not os.path.exists(INPUT_PATH):
        print(f"Input file not found: {INPUT_PATH}")
        return

    print("Reading NIFTY data...")
    df = pd.read_csv(INPUT_PATH)
    df['Date'] = pd.to_datetime(df['Date'])
    df.sort_values('Date', ascending=True, inplace=True)
    
    # Set index for rolling
    df_indexed = df.set_index('Date')
    
    # Rolling 1-Year (365 Days) Window as requested
    window_size = '365D'
    print(f"Applying Rolling Window: {window_size}")
    
    df_indexed['Rolling_Max_1Y'] = df_indexed['nav'].rolling(window=window_size, min_periods=1).max()
    
    # Reset index
    df_result = df_indexed.reset_index()
    
    # Drawdown Calculation
    df_result['Drawdown_Pct'] = (df_result['nav'] - df_result['Rolling_Max_1Y']) / df_result['Rolling_Max_1Y'] * 100
    
    # Flag Drops >= 2% (User requested update)
    df_result['Is_Dip'] = df_result['Drawdown_Pct'] <= -2.0
    
    # Save
    df_result.to_csv(OUTPUT_PATH, index=False)
    print(f"Analysis saved to {OUTPUT_PATH}")
    
    # Stats
    drops = df_result[df_result['Is_Dip']]
    print(f"Total Records: {len(df_result)}")
    print(f"Days with >= 2% Drop: {len(drops)}")

if __name__ == "__main__":
    analyze_nifty_data()
