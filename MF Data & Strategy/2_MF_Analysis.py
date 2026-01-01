
import pandas as pd
import os

# Configuration
DATA_DIR = 'MF_Data'
INPUT_FILE = 'ICICI_Pru_Gilt_Direct_Growth.csv'
OUTPUT_FILE = 'ICICI_Pru_Gilt_Analysis.csv'
INPUT_PATH = os.path.join(DATA_DIR, INPUT_FILE)
OUTPUT_PATH = os.path.join(DATA_DIR, OUTPUT_FILE)

def analyze_mf_data():
    if not os.path.exists(INPUT_PATH):
        print(f"Input file not found: {INPUT_PATH}")
        return

    print("Reading data...")
    df = pd.read_csv(INPUT_PATH)
    df['Date'] = pd.to_datetime(df['Date'])
    df.sort_values('Date', ascending=True, inplace=True)
    df.reset_index(drop=True, inplace=True)

    # Rolling 1-Year Max Analysis
    # We want to check: Is Today's NAV <= (Max NAV of LAST 365 DAYS including today) * 0.99?
    # Actually user said "falls 1% or more in last one year".
    # Interpretation: Drawdown from 52-Week High >= 1%.
    # 52-Week High = Max(NAV) in window [t-365, t]
    
    # We can use pandas rolling.
    # We need a time-aware rolling since data might have gaps (weekends/holidays).
    # set index to date
    df_indexed = df.set_index('Date')
    
    # Rolling max over last 3 years (approx 1095 days)
    df_indexed['Rolling_Max_1Y'] = df_indexed['nav'].rolling(window='1095D', min_periods=1).max()
    
    # Reset index to get Date back as col
    df_result = df_indexed.reset_index()
    
    # Calculate Drawdown
    # Drawdown = (Current NAV - Peak) / Peak * 100
    df_result['Drawdown_Pct'] = (df_result['nav'] - df_result['Rolling_Max_1Y']) / df_result['Rolling_Max_1Y'] * 100
    
    # Flag: Drop >= 1% (meaning Drawdown <= -1.0)
    df_result['Falls_1Pct_Or_More'] = df_result['Drawdown_Pct'] <= -1.0
    
    # Save Results
    df_result.to_csv(OUTPUT_PATH, index=False)
    print(f"Analysis completed. Saved to {OUTPUT_PATH}")
    
    # Quick stats
    drops = df_result[df_result['Falls_1Pct_Or_More']]
    print(f"Total trading days: {len(df_result)}")
    print(f"Days with >= 1% Drop from 52-Week High: {len(drops)}")

if __name__ == "__main__":
    analyze_mf_data()
