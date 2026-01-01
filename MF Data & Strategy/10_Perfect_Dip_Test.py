
import pandas as pd
import numpy as np
import os

DATA_DIR = 'MF_Data'
INPUT_FILE = 'NIFTY50_Data.csv'
FILE_PATH = os.path.join(DATA_DIR, INPUT_FILE)

SIP_AMOUNT = 1000.0

def xirr(transactions):
    if not transactions: return 0.0
    dates = [t[0] for t in transactions]
    amounts = [t[1] for t in transactions]
    min_date = min(dates)
    
    def xnpv(rate):
        if rate <= -1.0: return float('inf')
        return sum([a / pow(1 + rate, (d - min_date).days / 365.0) for d, a in zip(dates, amounts)])

    try:
        r = 0.1
        for _ in range(100):
            val = xnpv(r)
            if abs(val) < 1e-6: return r * 100
            deriv = sum([a * (-(d-min_date).days/365.0) / pow(1+r, ((d-min_date).days/365.0)+1) for d, a in zip(dates, amounts)])
            if deriv == 0: break
            r = r - val / deriv
        return r * 100
    except: return 0.0

def run_perfect_test():
    if not os.path.exists(FILE_PATH):
        print("Data file not found.")
        return

    df = pd.read_csv(FILE_PATH)
    df['Date'] = pd.to_datetime(df['Date'])
    df['YearMonth'] = df['Date'].dt.to_period('M')
    
    # Filter 2021-2025
    df = df[(df['Date'] >= '2021-01-01') & (df['Date'] <= '2025-12-31')].copy()
    
    print(f"Testing 'Perfect Foresight' on {len(df)} days...")

    # Group by Month
    months = df['YearMonth'].unique()
    
    s1_tx = [] # 1st of Month
    s2_tx = [] # Lowest of Month
    
    s1_units = 0; s1_inv = 0
    s2_units = 0; s2_inv = 0
    
    for m in months:
        monthly_data = df[df['YearMonth'] == m]
        
        # S1: First Day
        day1 = monthly_data.iloc[0]
        s1_units += SIP_AMOUNT / day1['nav']
        s1_inv += SIP_AMOUNT
        s1_tx.append((day1['Date'], -SIP_AMOUNT))
        
        # S2: Lowest Price Day
        min_day = monthly_data.loc[monthly_data['nav'].idxmin()]
        s2_units += SIP_AMOUNT / min_day['nav']
        s2_inv += SIP_AMOUNT
        s2_tx.append((min_day['Date'], -SIP_AMOUNT))
        
    latest_nav = df.iloc[-1]['nav']
    latest_date = df.iloc[-1]['Date']
    
    v1 = s1_units * latest_nav
    x1 = xirr(s1_tx + [(latest_date, v1)])
    
    v2 = s2_units * latest_nav
    x2 = xirr(s2_tx + [(latest_date, v2)])
    
    print("-" * 60)
    print("THEORETICAL MAXIMUM TEST (2021-2025)")
    print("Scenario 1: Dumb SIP (1st of Month)")
    print("Scenario 2: God Mode (Lowest Price of Month)")
    print("-" * 60)
    print(f"Normal SIP XIRR       : {x1:.2f}%")
    print(f"Perfect Timing XIRR   : {x2:.2f}%")
    print(f"Maximum Possible Alpha: {x2-x1:+.2f}%")
    print("-" * 60)
    print("If 'Perfect God-Mode Timing' only adds this much alpha,")
    print("then realistic strategies (RSI, MA) will naturally adds less.")
    print("-" * 60)

if __name__ == "__main__":
    run_perfect_test()
