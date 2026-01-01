
import os
import glob
import pandas as pd
import datetime
import numpy as np
import xlsxwriter

# --- Configuration ---
CAPITAL_PER_TRADE = 10000
STOP_LOSS_PCT = 0.05
# Targets iterated in main

# Logic Definitions (Standard)
# GFS: M[55-65], W[55-65], D[35-45]
# AGFS: M[55-65], W[55-65], D[55-65]
# Value Buy: M[35-45], W[35-45], D[35-45]

def load_adjustments(adjustments_file):
    if not os.path.exists(adjustments_file):
        return {}
    adj_df = pd.read_csv(adjustments_file)
    adj_df['ExDate'] = pd.to_datetime(adj_df['ExDate'])
    adjustments = {}
    for _, row in adj_df.iterrows():
        symbol = row['Symbol']
        if symbol not in adjustments:
            adjustments[symbol] = []
        adjustments[symbol].append({
            'date': row['ExDate'],
            'factor': float(row['AdjustmentFactor'])
        })
    return adjustments

def load_data(data_dir, adjustments_file=None):
    print("Loading data (2021-2025)...")
    all_files = []
    for year in range(2021, 2026):
        # Look for files in year subdirectory
        all_files.extend(glob.glob(os.path.join(data_dir, str(year), f"bhavcopy_{year}*.csv")))
    
    dfs = []
    # Using OHLC but NO Volume
    use_cols = ['SYMBOL', 'SERIES', 'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 'CLOSE_PRICE', 'DATE1']
    
    for f in all_files:
        try:
            df = pd.read_csv(f, usecols=use_cols)
            df = df[df['SERIES'] == 'EQ']
            dfs.append(df)
        except:
            pass
            
    if not dfs:
        return pd.DataFrame()
        
    full_df = pd.concat(dfs, ignore_index=True)
    full_df.rename(columns={
        'DATE1': 'TIMESTAMP', 
        'OPEN_PRICE': 'OPEN',
        'HIGH_PRICE': 'HIGH',
        'LOW_PRICE': 'LOW',
        'CLOSE_PRICE': 'CLOSE'
    }, inplace=True)
    full_df['TIMESTAMP'] = pd.to_datetime(full_df['TIMESTAMP'])
    full_df = full_df.sort_values('TIMESTAMP')
    
    if adjustments_file:
        adjustments = load_adjustments(adjustments_file)
        if adjustments:
            for symbol, adj_list in adjustments.items():
                for adj in adj_list:
                    mask = (full_df['SYMBOL'] == symbol) & (full_df['TIMESTAMP'] < adj['date'])
                    for col in ['OPEN', 'HIGH', 'LOW', 'CLOSE']:
                        full_df.loc[mask, col] = full_df.loc[mask, col] * adj['factor']
                    
    return full_df

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

def backtest_symbol(symbol, symbol_df, target_year, target_pct=0.10, stop_loss_pct=0.05):
    # expect symbol_df to have RSI columns and DatetimeIndex
    if symbol_df.empty:
        return []
    
    # Filter for the specific year
    start_date = f"{target_year}-01-01"
    end_date = f"{target_year}-12-31"
    
    try:
        test_data = symbol_df[start_date:end_date]
    except KeyError:
        return []
    
    if test_data.empty:
        return []
    
    trades = []
    active_trade = None
    
    for date, row in test_data.iterrows():
        open_price = row['OPEN']
        high = row['HIGH']
        low = row['LOW']
        close = row['CLOSE']
        
        d_rsi = row['Daily_RSI']
        w_rsi = row['Weekly_RSI']
        m_rsi = row['Monthly_RSI']
        
        if pd.isna(d_rsi) or pd.isna(w_rsi) or pd.isna(m_rsi):
            continue
            
        # --- Entry Logic (STANDARD) ---
        if active_trade is None:
            strategy = None
            
            # GFS
            if (55 <= m_rsi <= 65) and (55 <= w_rsi <= 65) and (35 <= d_rsi <= 45):
                strategy = "GFS"
            # AGFS
            elif (55 <= m_rsi <= 65) and (55 <= w_rsi <= 65) and (55 <= d_rsi <= 65):
                strategy = "AGFS"
            # Value Buy
            elif (35 <= m_rsi <= 45) and (35 <= w_rsi <= 45) and (35 <= d_rsi <= 45):
                strategy = "Value Buy"
                
            if strategy:
                qty = int(CAPITAL_PER_TRADE / close)
                active_trade = {
                    'Symbol': symbol,
                    'Entry Date': date.date(),
                    'Strategy': strategy,
                    'Buy Price': close,
                    'Qty': qty,
                    'Target': close * (1 + target_pct),
                    'Stop Loss': close * (1 - stop_loss_pct)
                }
        
        # --- Exit Logic ---
        else:
            target_price = active_trade['Target']
            stop_loss_price = active_trade['Stop Loss']
            exit_price = None
            status = None
            
            if open_price <= stop_loss_price:
                exit_price = open_price
                status = 'Stop Loss Hit (Gap)'
            elif open_price >= target_price:
                exit_price = open_price
                status = 'Target Hit (Gap)'
            elif low <= stop_loss_price:
                exit_price = stop_loss_price
                status = 'Stop Loss Hit'
            elif high >= target_price:
                exit_price = target_price
                status = 'Target Hit'
                
            if exit_price is not None:
                active_trade['Exit Date'] = date.date()
                active_trade['Sell Price'] = exit_price
                active_trade['Status'] = status
                active_trade['Return %'] = ((exit_price - active_trade['Buy Price']) / active_trade['Buy Price']) * 100
                trades.append(active_trade)
                active_trade = None
                
    if active_trade:
        active_trade['Exit Date'] = 'Open'
        # Mark to market at year end close
        current_close = test_data.iloc[-1]['CLOSE']
        active_trade['Sell Price'] = current_close
        active_trade['Status'] = 'Open'
        active_trade['Return %'] = ((active_trade['Sell Price'] - active_trade['Buy Price']) / active_trade['Buy Price']) * 100
        trades.append(active_trade)
        
    return trades

def load_symbols_from_csv(file_path):
    if not os.path.exists(file_path):
        print(f"Error: Symbols file not found at {file_path}")
        return []
    try:
        df = pd.read_csv(file_path)
        # Assumes the first column is the symbol if 'Symbol' header not found, but file has Symbol header
        if 'Symbol' in df.columns:
            return df['Symbol'].dropna().unique().tolist()
        else:
            return df.iloc[:, 0].dropna().unique().tolist()
    except Exception as e:
        print(f"Error reading symbols file: {e}")
        return []

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    data_dir = os.path.join(project_root, "NSE Bhavcopy", "NSE_Bhavcopy_Master_Data")
    # Updated path to match new folder name
    adjustments_file = os.path.join(project_root, "Script RSI Calculation", "script_adjustments.csv")
    
    # Path to the Stock List CSV
    symbols_file = os.path.join(project_root, "NSE Bhavcopy", "0_Script_Master_List.csv")
    
    if not os.path.exists(data_dir):
        print(f"Data directory not found: {data_dir}")
        return

    # Load Symbols
    print(f"Loading symbols from {symbols_file}...")
    symbols = load_symbols_from_csv(symbols_file)
    if not symbols:
        print("No symbols found or error reading CSV. Exiting.")
        return
    print(f"Loaded {len(symbols)} symbols.")

    # Load all data once
    full_df = load_data(data_dir, adjustments_file)
    if full_df.empty:
        print("No data loaded. Exiting.")
        return

    # --- OPTIMIZATION STEP ---
    print("Splitting data by symbol and pre-calculating RSI...")
    symbol_data_cache = {}
    
    # Group by symbol to create individual dataframes
    grouped = full_df.groupby('SYMBOL')
    count = 0
    total = len(grouped)
    
    # We only care about symbols in our list
    # Create a set for faster lookup
    symbols_set = set(symbols)
    
    for symbol, df_group in grouped:
        count += 1
        if count % 10 == 0:
             print(f"Pre-processing {count}/{total}: {symbol}...", end='\r')
             
        # Only process if in our symbols list
        if symbol not in symbols_set:
            continue
            
        # Process this symbol's dataframe
        df_sym = df_group.copy()
        df_sym.set_index('TIMESTAMP', inplace=True)
        # Sort just in case
        df_sym.sort_index(inplace=True)
        
        # Calculate RSI
        # Daily
        df_sym['Daily_RSI'] = calculate_rsi(df_sym['CLOSE'])

        # Weekly
        weekly_df = df_sym['CLOSE'].resample('W-FRI').last()
        weekly_rsi_series = calculate_rsi(weekly_df)

        # Monthly
        monthly_df = df_sym['CLOSE'].resample('ME').last()
        monthly_rsi_series = calculate_rsi(monthly_df)

        # Map back
        df_sym['Weekly_RSI'] = df_sym.index.map(lambda d: weekly_rsi_series.asof(d))
        df_sym['Monthly_RSI'] = df_sym.index.map(lambda d: monthly_rsi_series.asof(d))
        
        symbol_data_cache[symbol] = df_sym

    print("\nData pre-processing complete.")
    
    targets = [0.05, 0.10, 0.15, 0.20, 0.30]
    years = [2021, 2022, 2023, 2024, 2025]
    
    for year in years:
        output_xlsx = os.path.join(script_dir, f"RSI_Script_Report_MultiTarget_{year}.xlsx")
        print(f"\n{'='*60}")
        print(f"Running Backtest for YEAR: {year}")
        print(f"{'='*60}")
        
        with pd.ExcelWriter(output_xlsx, engine='xlsxwriter') as writer:
            for tgt in targets:
                print(f"Processing Target: {tgt*100}%...", end='\r')
                all_trades = []
                
                for i, symbol in enumerate(symbols):
                    # Progress indicator every 10 symbols
                    if i % 10 == 0:
                        print(f"Processing Target: {int(tgt*100)}% | Scanning {symbol}...", end='\r')
                    
                    if symbol in symbol_data_cache:
                        df = symbol_data_cache[symbol]
                        trades = backtest_symbol(symbol, df, target_year=year, target_pct=tgt)
                        all_trades.extend(trades)
                
                sheet_name = f"Target_{int(tgt*100)}%"
                if all_trades:
                    results_df = pd.DataFrame(all_trades)
                    column_order = [
                        'Symbol', 'Entry Date', 'Strategy', 'Buy Price', 'Qty', 
                        'Target', 'Stop Loss', 'Exit Date', 'Sell Price', 'Status', 'Return %'
                    ]
                    # Ensure columns exist even if empty
                    for col in column_order:
                        if col not in results_df.columns:
                            results_df[col] = None
                    
                    results_df = results_df[column_order]
                    results_df.to_excel(writer, sheet_name=sheet_name, index=False)
                    win_rate = len(results_df[results_df['Return %'] > 0]) / len(results_df) * 100
                    avg_ret = results_df['Return %'].mean()
                    print(f"Target {int(tgt*100)}%: {len(results_df)} Trades, Win {win_rate:.1f}%, Avg {avg_ret:.2f}%")
                else:
                    print(f"Target {int(tgt*100)}%: No trades found.                      ")
        
        print(f"\n[SUCCESS] Report saved to: {output_xlsx}")

if __name__ == "__main__":
    main()
