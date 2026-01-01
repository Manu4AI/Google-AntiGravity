import os
import pandas as pd
import numpy as np
import datetime

# --- Configuration ---
CAPITAL_PER_TRADE = 10000

# Paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR) # Go up one level from 'RSI Strategies'
DATA_DIR = os.path.join(PROJECT_ROOT, "NSE Bhavcopy", "NSE_Bhavcopy_Adjusted_Data")
MASTER_LIST_PATH = os.path.join(PROJECT_ROOT, "NSE Bhavcopy", "0_Script_Master_List.csv")

def calculate_rsi(series, window=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).fillna(0)
    loss = (-delta.where(delta < 0, 0)).fillna(0)
    avg_gain = gain.rolling(window=window, min_periods=window).mean()
    avg_loss = loss.rolling(window=window, min_periods=window).mean()
    for i in range(window, len(series)):
        avg_gain.iloc[i] = (avg_gain.iloc[i-1] * 13 + gain.iloc[i]) / 14
        avg_loss.iloc[i] = (avg_loss.iloc[i-1] * 13 + loss.iloc[i]) / 14
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def load_all_data(symbols):
    """Loads and processes all symbol data into memory."""
    cache = {}
    print(f"Pre-loading data for {len(symbols)} symbols...")
    
    for i, s in enumerate(symbols):
        if i % 20 == 0: print(f"Loading {i+1}/{len(symbols)}...", end='\r')
        
        file_path = os.path.join(DATA_DIR, f"{s}.csv")
        if not os.path.exists(file_path):
            continue
            
        try:
            df = pd.read_csv(file_path)
            # Rename columns
            rename_map = {'date': 'TIMESTAMP', 'open_price': 'OPEN', 'high_price': 'HIGH', 'low_price': 'LOW', 'close_price': 'CLOSE'}
            df.rename(columns=rename_map, inplace=True)
            df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
            df.sort_values('TIMESTAMP', inplace=True)
            df.set_index('TIMESTAMP', inplace=True)
            
            # RSI Calc
            df['Daily_RSI'] = calculate_rsi(df['CLOSE'])
            weekly_rsi = calculate_rsi(df['CLOSE'].resample('W-FRI').last())
            monthly_rsi = calculate_rsi(df['CLOSE'].resample('ME').last())
            
            df['Weekly_RSI'] = df.index.map(lambda d: weekly_rsi.asof(d))
            df['Monthly_RSI'] = df.index.map(lambda d: monthly_rsi.asof(d))
            
            cache[s] = df
        except Exception as e:
            print(f"Error loading {s}: {e}")
            
    print("\nData loading complete.")
    return cache

def analyze_buy_hold(symbols, data_cache, vintage_years):
    # Dictionary to hold results per year
    # Structure: {'2021': [trades], '2022': [trades]}
    results = {year: [] for year in vintage_years}
    
    valid_symbols = [s for s in symbols if s in data_cache]
    
    for symbol in valid_symbols:
        df = data_cache[symbol]
        if df.empty: continue
        
        latest_price = df.iloc[-1]['CLOSE']
        latest_date = df.index[-1].date()
        
        for year in vintage_years:
            start_date = f"{year}-01-01"
            end_date = f"{year}-12-31"
            
            # Get data for that year
            try:
                year_data = df.loc[start_date:end_date]
            except KeyError:
                continue
                
            if year_data.empty: continue
            
            # Find FIRST entry signal in this year
            # We assume user takes ONE long-term position per year per stock (or keeps adding?)
            # Let's assume one main entry for "Buy & Hold" logic per vintage year.
            
            for date, row in year_data.iterrows():
                d = row['Daily_RSI']
                w = row['Weekly_RSI']
                m = row['Monthly_RSI']
                
                if pd.isna(d) or pd.isna(w) or pd.isna(m): continue
                
                strategy = None
                if (55 <= m <= 65) and (55 <= w <= 65) and (35 <= d <= 45): strategy = "GFS"
                elif (55 <= m <= 65) and (55 <= w <= 65) and (55 <= d <= 65): strategy = "AGFS"
                elif (35 <= m <= 45) and (35 <= w <= 45) and (35 <= d <= 45): strategy = "Value Buy"
                
                if strategy:
                    # Found Entry!
                    entry_price = row['CLOSE']
                    qty = int(CAPITAL_PER_TRADE / entry_price)
                    if qty < 1: qty = 1 # Force at least 1 for analysis
                    
                    inv_amt = qty * entry_price
                    curr_val = qty * latest_price
                    pnl = curr_val - inv_amt
                    ret_pct = (pnl / inv_amt) * 100
                    
                    days_held = (df.index[-1] - date).days
                    
                    trade = {
                        'Symbol': symbol,
                        'Entry Date': date.date(),
                        'Entry Price': entry_price,
                        'Strategy': strategy,
                        'Qty': qty,
                        'Current Date': latest_date,
                        'Current Price': latest_price,
                        'Invested': inv_amt,
                        'Current Value': curr_val,
                        'P&L': round(pnl, 2),
                        'Return %': round(ret_pct, 2),
                        'Days Held': days_held
                    }
                    results[year].append(trade)
                    break # Take only FIRST signal of the year for this analysis (Buy & Hold Vintage Report)

    return results

def main():
    if not os.path.exists(MASTER_LIST_PATH):
        print(f"Master list not found: {MASTER_LIST_PATH}")
        return
        
    master_df = pd.read_csv(MASTER_LIST_PATH)
    symbols = master_df['Symbol'].unique().tolist()
    
    data_cache = load_all_data(symbols)
    
    years = [2021, 2022, 2023, 2024, 2025]
    results = analyze_buy_hold(symbols, data_cache, years)
    
    output_path = os.path.join(SCRIPT_DIR, "RSI_Buy_Hold_Analysis.xlsx")
    
    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        summary_data = []
        
        for year in years:
            trades = results[year]
            if trades:
                df_res = pd.DataFrame(trades)
                df_res.to_excel(writer, sheet_name=f"Vintage_{year}", index=False)
                
                avg_ret = df_res['Return %'].mean()
                total_pnl = df_res['P&L'].sum()
                win_rate = (len(df_res[df_res['P&L'] > 0]) / len(df_res)) * 100
                
                summary_data.append({
                    'Vintage Year': year,
                    'Total Trades': len(df_res),
                    'Win Rate %': round(win_rate, 1),
                    'Avg Return %': round(avg_ret, 2),
                    'Total P&L': round(total_pnl, 2)
                })
        
        if summary_data:
            df_sum = pd.DataFrame(summary_data)
            df_sum.to_excel(writer, sheet_name="Summary", index=False)
            
    print(f"\nReport generated: {output_path}")

if __name__ == "__main__":
    main()
