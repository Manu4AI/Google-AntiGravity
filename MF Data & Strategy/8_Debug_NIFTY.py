
import pandas as pd
import os

DATA_DIR = 'MF_Data'
ANALYSIS_FILE = 'NIFTY50_Analysis.csv'
FILE_PATH = os.path.join(DATA_DIR, ANALYSIS_FILE)

def debug_nifty():
    if not os.path.exists(FILE_PATH):
        print("Analysis file not found.")
        return

    df = pd.read_csv(FILE_PATH)
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Filter 2023
    df_yr = df[(df['Date'] >= '2023-01-01') & (df['Date'] <= '2023-12-31')].copy()
    df_yr.sort_values('Date', inplace=True)
    
    if df_yr.empty:
        print("No 2023 data.")
        return
        
    start_row = df_yr.iloc[0]
    end_row = df_yr.iloc[-1]
    
    total_days = len(df_yr)
    dip_days = len(df_yr[df_yr['Falls_1Pct_Or_More']])
    
    print("-" * 30)
    print(f"NIFTY 2023 Analysis")
    print("-" * 30)
    print(f"Start: {start_row['Date'].date()} | {start_row['nav']:.2f}")
    print(f"End  : {end_row['Date'].date()} | {end_row['nav']:.2f}")
    print(f"Total Days: {total_days} | Trigger Days: {dip_days} ({dip_days/total_days*100:.1f}%)")
    
    # Calculate Avg Buy Price
    sip_amt = 1000
    topup = 1000
    
    s1_inv = 0; s1_units = 0
    s2_inv = 0; s2_units = 0
    
    for _, row in df_yr.iterrows():
        nav = row['nav']
        is_dip = row['Falls_1Pct_Or_More']
        
        # S1
        s1_units += sip_amt / nav
        s1_inv += sip_amt
        
        # S2
        inv = sip_amt + (topup if is_dip else 0)
        s2_units += inv / nav
        s2_inv += inv
        
    avg_price_s1 = s1_inv / s1_units
    avg_price_s2 = s2_inv / s2_units
    
    print(f"S1 (Normal) Avg Buy Price: {avg_price_s1:.2f}")
    print(f"S2 (Startgy) Avg Buy Price: {avg_price_s2:.2f}")
    print(f"Price Improvement: {avg_price_s1 - avg_price_s2:.2f} ({(avg_price_s1 - avg_price_s2)/avg_price_s1*100:.2f}%)")
    print("-" * 30)

if __name__ == "__main__":
    debug_nifty()
