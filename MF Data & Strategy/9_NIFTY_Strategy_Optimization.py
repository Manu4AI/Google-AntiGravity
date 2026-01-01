
import pandas as pd
import numpy as np
import os

# Configuration
DATA_DIR = 'MF_Data'
INPUT_FILE = 'NIFTY50_Data.csv'
OUTPUT_FILE = 'Strategy_Comparison_Report.txt'
INPUT_PATH = os.path.join(DATA_DIR, INPUT_FILE)
OUTPUT_PATH = os.path.join(DATA_DIR, OUTPUT_FILE)

SIP_AMOUNT = 1000.0
TOPUP_AMOUNT = 1000.0

def calculate_rsi(data, window=14):
    delta = data.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

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
            f_val = xnpv(r)
            if abs(f_val) < 1e-6: return r * 100
            f_prime = sum([a * (-(d-min_date).days/365.0) / pow(1+r, ((d-min_date).days/365.0)+1) for d, a in zip(dates, amounts) if (d-min_date).days > 0])
            if f_prime == 0: break
            new_r = r - f_val / f_prime
            if abs(new_r - r) < 1e-6: return new_r * 100
            r = new_r
        return r * 100
    except: return 0.0

def run_optimization():
    if not os.path.exists(INPUT_PATH):
        print(f"File not found: {INPUT_PATH}")
        return

    print("Loading Data & Calculating Indicators...")
    df = pd.read_csv(INPUT_PATH)
    df['Date'] = pd.to_datetime(df['Date'])
    df.sort_values('Date', inplace=True)
    
    # Calculate Indicators
    df['SMA_50'] = df['nav'].rolling(window=50).mean()
    df['SMA_200'] = df['nav'].rolling(window=200).mean()
    
    # RSI (Wilder's Smoothing is standard, but simple rolling is close enough for approximation or we can implement EMA based)
    # Let's use simple rolling for stability as implemented in helper
    # For better accuracy let's use EMA method for RSI
    delta = df['nav'].diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    ema_up = up.ewm(com=13, adjust=False).mean()
    ema_down = down.ewm(com=13, adjust=False).mean()
    rs = ema_up / ema_down
    df['RSI'] = 100 - (100 / (1 + rs))

    # Filter 2021-2025
    df_test = df[(df['Date'] >= '2021-01-01') & (df['Date'] <= '2025-12-31')].copy()
    
    if df_test.empty:
        print("No data for backtest period.")
        return

    print(f"Backtesting on {len(df_test)} trading days...")

    # Portfolios
    # 0: Normal, 1: RSI<40, 2: Price<SMA50, 3: Price<SMA200
    names = ["Normal SIP", "RSI < 40", "Price < 50-DMA", "Price < 200-DMA"]
    invested = [0.0] * 4
    units = [0.0] * 4
    txs = [[] for _ in range(4)]
    triggers = [0] * 4

    for idx, row in df_test.iterrows():
        date = row['Date']
        nav = row['nav']
        rsi = row['RSI']
        sma50 = row['SMA_50']
        sma200 = row['SMA_200']
        
        # Conditions
        conds = [False] * 4
        conds[0] = False # Normal is base
        conds[1] = rsi < 40
        conds[2] = nav < sma50
        conds[3] = nav < sma200
        
        # Execute
        for i in range(4):
            # Base SIP
            amt = SIP_AMOUNT
            # Topup
            if i > 0 and conds[i]: # Strategy Portfolios
                amt += TOPUP_AMOUNT
                triggers[i] += 1
            elif i == 0: # Normal SIP
                pass
                
            units[i] += amt / nav
            invested[i] += amt
            txs[i].append((date, -amt))

    # Valuation
    latest_nav = df_test.iloc[-1]['nav']
    latest_date = df_test.iloc[-1]['Date']
    
    results = []
    
    print("-" * 80)
    print(f"{'Strategy':<20} | {'Triggers':<8} | {'Invested':<12} | {'Value':<12} | {'XIRR':<8} | {'Diff':<8}")
    print("-" * 80)
    
    base_xirr = 0.0
    
    out_lines = []
    out_lines.append(f"STRATEGY OPTIMIZATION REPORT (2021-2025)")
    out_lines.append(f"Base SIP: {SIP_AMOUNT}, Top-up: {TOPUP_AMOUNT}")
    out_lines.append("-" * 60)
    
    for i in range(4):
        val = units[i] * latest_nav
        my_xirr = xirr(txs[i] + [(latest_date, val)])
        
        if i == 0: base_xirr = my_xirr
        
        diff = my_xirr - base_xirr
        
        row_str = f"{names[i]:<20} | {triggers[i]:<8} | {invested[i]:<12,.0f} | {val:<12,.0f} | {my_xirr:>6.2f}% | {diff:>+6.2f}%"
        print(row_str)
        out_lines.append(row_str)
        
    print("-" * 80)
    out_lines.append("-" * 60)
    
    # Save
    with open(OUTPUT_PATH, 'w') as f:
        f.write("\n".join(out_lines))
    print(f"Report saved to {OUTPUT_PATH}")

if __name__ == "__main__":
    run_optimization()
