
import pandas as pd
import os
from datetime import datetime
import math

# Configuration
DATA_DIR = 'MF_Data'
ANALYSIS_FILE = 'ICICI_Pru_Gilt_Analysis.csv'
OUTPUT_FILE = '2025_Backtest_Report.txt'
ANALYSIS_PATH = os.path.join(DATA_DIR, ANALYSIS_FILE)
OUTPUT_PATH = os.path.join(DATA_DIR, OUTPUT_FILE)

SIP_AMOUNT = 1000.0
TOPUP_AMOUNT = 1000.0

def xirr(transactions):
    """
    Calculate XIRR for a series of transactions.
    transactions: list of (date, amount) tuples.
    Returns: float (XIRR percentage)
    """
    if not transactions:
        return 0.0

    dates = [t[0] for t in transactions]
    amounts = [t[1] for t in transactions]

    min_date = min(dates)
    
    # Function to solve: sum(amount / (1 + rate)^((date - min_date)/365)) = 0
    def xnpv(rate):
        if rate <= -1.0:
            return float('inf')
        val = 0.0
        for d, a in zip(dates, amounts):
            days = (d - min_date).days
            val += a /  pow(1 + rate, days / 365.0)
        return val

    # Newton-Raphson method
    try:
        r = 0.1 # Initial guess 10%
        for _ in range(100):
            f_val = xnpv(r)
            if abs(f_val) < 1e-6:
                return r * 100
            
            # Derivative
            f_prime = 0.0
            for d, a in zip(dates, amounts):
                days = (d - min_date).days
                if days == 0: continue
                term = a * (-days / 365.0) * pow(1 + r, (days / 365.0) - 1)
                if abs(term) > 1e-9: # Avoid division by zero issues
                     # Actually derivative of (1+r)^-k is -k * (1+r)^(-k-1)
                     f_prime += a * (-days/365.0) / pow(1+r, (days/365.0) + 1)
            
            if f_prime == 0:
                break
                
            new_r = r - f_val / f_prime
            if abs(new_r - r) < 1e-6:
                return new_r * 100
            r = new_r
        return r * 100
    except:
        return 0.0

def run_backtest():
    if not os.path.exists(ANALYSIS_PATH):
        print(f"File not found: {ANALYSIS_PATH}")
        return

    print("Loading data...")
    df = pd.read_csv(ANALYSIS_PATH)
    df['Date'] = pd.to_datetime(df['Date'])
    
    years = [2021, 2022, 2023, 2024, 2025]
    all_reports = []
    
    summary_data = []

    for year in years:
        # Filter for specific year
        start_date = f'{year}-01-01'
        end_date = f'{year}-12-31'
        
        # Filter logic: Year match
        df_year = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)].copy()
        df_year.sort_values('Date', ascending=True, inplace=True)
        
        if df_year.empty:
            print(f"No data found for {year}.")
            continue
            
        # --- Scenario 1: Normal Daily SIP ---
        s1_units = 0.0
        s1_invested = 0.0
        s1_transactions = [] # (date, amount)

        # --- Scenario 2: Strategy SIP (SIP + Top-up) ---
        s2_units = 0.0
        s2_invested = 0.0
        s2_transactions = []
        
        dip_days_count = 0

        # Iterate
        for idx, row in df_year.iterrows():
            date = row['Date']
            nav = row['nav']
            is_dip = row['Falls_1Pct_Or_More']
            
            # S1: Daily SIP
            units_bought = SIP_AMOUNT / nav
            s1_units += units_bought
            s1_invested += SIP_AMOUNT
            s1_transactions.append((date, -SIP_AMOUNT))
            
            # S2: Daily SIP + Topup
            invest_amt = SIP_AMOUNT
            if is_dip:
                invest_amt += TOPUP_AMOUNT
                dip_days_count += 1
            
            units_bought_s2 = invest_amt / nav
            s2_units += units_bought_s2
            s2_invested += invest_amt
            s2_transactions.append((date, -invest_amt))

        # Valuation at End of Year (or last available date)
        latest_nav = df_year.iloc[-1]['nav']
        latest_date = df_year.iloc[-1]['Date']
        
        # S1 Results
        s1_value = s1_units * latest_nav
        s1_abs_ret = (s1_value - s1_invested) / s1_invested * 100 if s1_invested > 0 else 0
        s1_transactions_final = s1_transactions + [(latest_date, s1_value)]
        s1_xirr = xirr(s1_transactions_final)
        
        # S2 Results
        s2_value = s2_units * latest_nav
        s2_abs_ret = (s2_value - s2_invested) / s2_invested * 100 if s2_invested > 0 else 0
        s2_transactions_final = s2_transactions + [(latest_date, s2_value)]
        s2_xirr = xirr(s2_transactions_final)

        # Delta
        imp_xirr = s2_xirr - s1_xirr
        extra_invest = s2_invested - s1_invested

        # Append to detailed report
        report_chunk = []
        report_chunk.append("==================================================")
        report_chunk.append(f" YEAR: {year} (Dip Days: {dip_days_count})")
        report_chunk.append("==================================================")
        report_chunk.append(f"Scenario 1 (Normal SIP):")
        report_chunk.append(f"  Invested: {s1_invested:,.0f} | Value: {s1_value:,.0f} | Abs: {s1_abs_ret:.2f}% | XIRR: {s1_xirr:.2f}%")
        report_chunk.append("-" * 50)
        report_chunk.append(f"Scenario 2 (Strategy):")
        report_chunk.append(f"  Invested: {s2_invested:,.0f} | Value: {s2_value:,.0f} | Abs: {s2_abs_ret:.2f}% | XIRR: {s2_xirr:.2f}%")
        report_chunk.append("-" * 50)
        report_chunk.append(f"Observation:")
        report_chunk.append(f"  Strategy deployed {extra_invest:,.0f} extra capital.")
        report_chunk.append(f"  XIRR Improvement: {imp_xirr:+.2f}%")
        
        all_reports.append("\n".join(report_chunk))
        
        summary_data.append({
            'Year': year,
            'Dip_Days': dip_days_count,
            'S1_Inv': s1_invested,
            'S1_XIRR': s1_xirr,
            'S2_Inv': s2_invested,
            'S2_XIRR': s2_xirr,
            'Delta_XIRR': imp_xirr
        })

    # Generate Final Output Text
    final_output = []
    final_output.append("##################################################")
    final_output.append(" MULTI-YEAR BACKTEST SUMMARY (2021-2025)")
    final_output.append("##################################################")
    final_output.append(f"{'Year':<6} | {'Dip Days':<8} | {'S1 XIRR':<10} | {'S2 XIRR':<10} | {'Diff':<8}")
    final_output.append("-" * 56)
    
    for row in summary_data:
        final_output.append(f"{row['Year']:<6} | {row['Dip_Days']:<8} | {row['S1_XIRR']:>9.2f}% | {row['S2_XIRR']:>9.2f}% | {row['Delta_XIRR']:>+7.2f}%")
    
    final_output.append("\n\n")
    final_output.append("\n".join(all_reports))
    
    full_text = "\n".join(final_output)
    print(full_text)
    
    with open(OUTPUT_PATH, 'w') as f:
        f.write(full_text)
    print(f"\nReport saved to: {OUTPUT_PATH}")

if __name__ == "__main__":
    run_backtest()
