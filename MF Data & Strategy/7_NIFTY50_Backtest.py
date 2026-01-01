
import pandas as pd
import os
from datetime import datetime

# Configuration
DATA_DIR = 'MF_Data'
ANALYSIS_FILE = 'NIFTY50_Analysis.csv'
OUTPUT_FILE = 'NIFTY50_Backtest_Report.txt'
ANALYSIS_PATH = os.path.join(DATA_DIR, ANALYSIS_FILE)
OUTPUT_PATH = os.path.join(DATA_DIR, OUTPUT_FILE)

SIP_AMOUNT = 1000.0
TOPUP_AMOUNT = 1000.0

def xirr(transactions):
    if not transactions:
        return 0.0
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
            # Derivative approx
            f_prime = sum([a * (-(d-min_date).days/365.0) / pow(1+r, ((d-min_date).days/365.0)+1) for d, a in zip(dates, amounts) if (d-min_date).days > 0])
            if f_prime == 0: break
            new_r = r - f_val / f_prime
            if abs(new_r - r) < 1e-6: return new_r * 100
            r = new_r
        return r * 100
    except:
        return 0.0

def run_backtest():
    if not os.path.exists(ANALYSIS_PATH):
        print(f"File not found: {ANALYSIS_PATH}")
        return

    print("Loading NIFTY analysis data...")
    df = pd.read_csv(ANALYSIS_PATH)
    df['Date'] = pd.to_datetime(df['Date'])
    
    years = [2021, 2022, 2023, 2024, 2025]
    summary_data = []
    all_reports = []
    
    for year in years:
        start_date = f'{year}-01-01'
        end_date = f'{year}-12-31'
        df_year = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)].copy()
        df_year.sort_values('Date', inplace=True)
        
        if df_year.empty:
            print(f"No data for {year}")
            continue
            
        # Init Scenarios
        s1_inv = 0.0; s1_units = 0.0; s1_tx = []
        s2_inv = 0.0; s2_units = 0.0; s2_tx = []
        dip_days = 0
        
        for idx, row in df_year.iterrows():
            date = row['Date']
            nav = row['nav']
            is_dip = row['Is_Dip']
            
            # S1
            s1_units += SIP_AMOUNT / nav
            s1_inv += SIP_AMOUNT
            s1_tx.append((date, -SIP_AMOUNT))
            
            # S2
            amt = SIP_AMOUNT
            if is_dip:
                amt += TOPUP_AMOUNT
                dip_days += 1
            s2_units += amt / nav
            s2_inv += amt
            s2_tx.append((date, -amt))
            
        # Valuation
        latest_nav = df_year.iloc[-1]['nav']
        latest_date = df_year.iloc[-1]['Date']
        
        s1_val = s1_units * latest_nav
        s1_xirr = xirr(s1_tx + [(latest_date, s1_val)])
        
        s2_val = s2_units * latest_nav
        s2_xirr = xirr(s2_tx + [(latest_date, s2_val)])
        
        delta = s2_xirr - s1_xirr
        
        summary_data.append({
            'Year': year, 
            'Dips': dip_days, 
            'S1_XIRR': s1_xirr, 
            'S2_XIRR': s2_xirr, 
            'Delta': delta
        })
        
        chunk = f"YEAR: {year} | Dips: {dip_days}\nS1 (Normal): Invest {s1_inv:,.0f} -> Value {s1_val:,.0f} (XIRR {s1_xirr:.2f}%)\nS2 (Strategy): Invest {s2_inv:,.0f} -> Value {s2_val:,.0f} (XIRR {s2_xirr:.2f}%)\nDelta XIRR: {delta:+.2f}%\n" + "-"*40
        all_reports.append(chunk)

    # Consolidated Report
    final_output = ["NIFTY 50 BACKTEST (2021-2025)", "="*30]
    final_output.append(f"{'Year':<6} | {'Dips':<5} | {'S1 XIRR':<10} | {'S2 XIRR':<10} | {'Diff':<8}")
    final_output.append("-" * 50)
    for r in summary_data:
        final_output.append(f"{r['Year']:<6} | {r['Dips']:<5} | {r['S1_XIRR']:>9.2f}% | {r['S2_XIRR']:>9.2f}% | {r['Delta']:>+7.2f}%")
        
    final_output.append("\n" + "\n".join(all_reports))
    full_text = "\n".join(final_output)
    print(full_text)
    
    with open(OUTPUT_PATH, 'w') as f:
        f.write(full_text)
    print(f"\nReport saved to: {OUTPUT_PATH}")

if __name__ == "__main__":
    run_backtest()
