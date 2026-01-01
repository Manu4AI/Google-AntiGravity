
import os
import pandas as pd
import datetime
import numpy as np
import xlsxwriter

# --- Configuration ---
CAPITAL_PER_TRADE = 10000
STOP_LOSS_PCT = 0.05
# Targets: 5%, 10%, 15%, 20%, 25%, 30%
TARGETS = [0.05, 0.10, 0.15, 0.20, 0.25, 0.30]

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

def load_symbol_data(file_path):
    if not os.path.exists(file_path):
        return pd.DataFrame()
    
    try:
        # Schema: date,open_price,high_price,low_price,close_price,...
        df = pd.read_csv(file_path)
        
        # Rename columns to match strategy logic
        rename_map = {
            'date': 'TIMESTAMP',
            'open_price': 'OPEN',
            'high_price': 'HIGH',
            'low_price': 'LOW',
            'close_price': 'CLOSE'
        }
        df.rename(columns=rename_map, inplace=True)
        df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
        df = df.sort_values('TIMESTAMP')
        
        # --- Pre-calculate RSI here only ONCE ---
        df.set_index('TIMESTAMP', inplace=True)

        # Daily
        df['Daily_RSI'] = calculate_rsi(df['CLOSE'])

        # Weekly
        weekly_df = df['CLOSE'].resample('W-FRI').last()
        weekly_rsi_series = calculate_rsi(weekly_df)

        # Monthly
        monthly_df = df['CLOSE'].resample('ME').last()
        monthly_rsi_series = calculate_rsi(monthly_df)

        # Map back to daily timeframe
        df['Weekly_RSI'] = df.index.map(lambda d: weekly_rsi_series.asof(d))
        df['Monthly_RSI'] = df.index.map(lambda d: monthly_rsi_series.asof(d))

        return df
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        return pd.DataFrame()

def backtest_symbol(symbol, symbol_df, target_year, target_pct=0.10, stop_loss_pct=0.05):
    if symbol_df.empty:
        return []
    
    # Work on a view/copy of the pre-processed df
    start_date = f"{target_year}-01-01"
    end_date = f"{target_year}-12-31"
    
    # Ensure index is DatetimeIndex (it should be from load_symbol_data)
    if not isinstance(symbol_df.index, pd.DatetimeIndex):
         symbol_df.set_index('TIMESTAMP', inplace=True)
         
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
            
            # GFS: M[55-65], W[55-65], D[35-45]
            if (55 <= m_rsi <= 65) and (55 <= w_rsi <= 65) and (35 <= d_rsi <= 45):
                strategy = "GFS"
            # AGFS: M[55-65], W[55-65], D[55-65]
            elif (55 <= m_rsi <= 65) and (55 <= w_rsi <= 65) and (55 <= d_rsi <= 65):
                strategy = "AGFS"
            # Value Buy: M[35-45], W[35-45], D[35-45]
            elif (35 <= m_rsi <= 45) and (35 <= w_rsi <= 45) and (35 <= d_rsi <= 45):
                strategy = "Value Buy"
                
            if strategy:
                qty = int(CAPITAL_PER_TRADE / close) if close > 0 else 0
                if qty > 0:
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
            
            # Check Open High/Low for gaps
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
                buy_val = active_trade['Buy Price']
                if buy_val != 0:
                    active_trade['Return %'] = ((exit_price - buy_val) / buy_val) * 100
                else:
                    active_trade['Return %'] = 0
                trades.append(active_trade)
                active_trade = None
                
    if active_trade:
        active_trade['Exit Date'] = 'Open'
        # Mark to market at year end close
        current_close = test_data.iloc[-1]['CLOSE']
        active_trade['Sell Price'] = current_close
        active_trade['Status'] = 'Open'
        buy_val = active_trade['Buy Price']
        if buy_val != 0:
            active_trade['Return %'] = ((current_close - buy_val) / buy_val) * 100
        else:
            active_trade['Return %'] = 0
        trades.append(active_trade)
        
    return trades

def load_symbols_from_csv(file_path):
    if not os.path.exists(file_path):
        print(f"Error: Symbols file not found at {file_path}")
        return []
    try:
        df = pd.read_csv(file_path)
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
    
    # Adjusted Data Directory
    adjusted_data_dir = os.path.join(project_root, "NSE Bhavcopy", "NSE_Bhavcopy_Adjusted_Data")
    
    # Symbol List CSV
    symbols_file = os.path.join(project_root, "NSE Bhavcopy", "0_Script_Master_List.csv")
    
    if not os.path.exists(adjusted_data_dir):
        print(f"Error: Adjusted Data directory not found: {adjusted_data_dir}")
        return

    # Load Symbols
    print(f"Loading symbols from {symbols_file}...")
    symbols = load_symbols_from_csv(symbols_file)
    if not symbols:
        print("No symbols found. Exiting.")
        return
    print(f"Loaded {len(symbols)} symbols.")

    years = [2021, 2022, 2023, 2024, 2025]
    
    # Pre-load all symbol data to avoid re-reading files for each year/target
    # Dictionary: Symbol -> DataFrame
    print("Pre-loading symbol data and calculating RSI...")
    symbol_data_cache = {}
    for i, s in enumerate(symbols):
        if i % 10 == 0:
            print(f"Pre-loading {i}/{len(symbols)}: {s}...", end='\r')
        csv_path = os.path.join(adjusted_data_dir, f"{s}.csv")
        symbol_data_cache[s] = load_symbol_data(csv_path)
    print("\nData loading complete.")

    for year in years:
        output_xlsx = os.path.join(script_dir, f"RSI_MultiTarget_Adjusted_{year}.xlsx")
        print(f"\n{'='*60}")
        print(f"Running Backtest for YEAR: {year}")
        print(f"{'='*60}")
        
        with pd.ExcelWriter(output_xlsx, engine='xlsxwriter') as writer:
            for tgt in TARGETS:
                tgt_label = int(tgt*100)
                print(f"Processing Target: {tgt_label}%...", end='\r')
                all_trades = []
                
                for i, symbol in enumerate(symbols):
                    # Progress update
                    if i % 20 == 0:
                        print(f"Processing Target: {tgt_label}% | Scanning {symbol}...", end='\r')
                    
                    df = symbol_data_cache.get(symbol)
                    if df is not None and not df.empty:
                        trades = backtest_symbol(symbol, df, target_year=year, target_pct=tgt)
                        all_trades.extend(trades)
                
                sheet_name = f"Target_{tgt_label}%"
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
                    print(f"Target {tgt_label}%: {len(results_df)} Trades, Win {win_rate:.1f}%, Avg {avg_ret:.2f}%")
                else:
                    print(f"Target {tgt_label}%: No trades found.                      ")
        
        print(f"\n[SUCCESS] Report saved to: {output_xlsx}")

if __name__ == "__main__":
    main()
