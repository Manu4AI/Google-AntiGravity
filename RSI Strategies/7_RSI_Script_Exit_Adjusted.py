
import os
import pandas as pd
import datetime
import numpy as np
import xlsxwriter

# --- Configuration ---
CAPITAL_PER_TRADE = 10000
INITIAL_STOP_LOSS_PCT = 0.05
# Targets list is NOT used for exit logic, but we still loop over them 
# to maintain structure, although the strategy logic is now fixed.
# Actually, the user asked for SPECIFIC exit criteria.
# "First Hard SL will be 5%. When target reaches 8% keep SL to Cost..."
# This implies the logic is FIXED and not dependent on a variable "Target" parameter like 0.10, 0.20 etc.
# However, the previous script structure looped over multiple targets.
# To avoid confusion, we will run this logic ONCE per symbol per year.
# But to keep compatibility with the loop structure if desired, 
# we can just make a dummy list or refactor the loop.
# Decision: The logic is specific. We don't need to loop over targets [5%, 10%, 15%...].
# We will just run it once.
TARGETS = ["Fixed_Logic"] 

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

def backtest_symbol(symbol, symbol_df, target_year):
    if symbol_df.empty:
        return []
    
    # Work on a view/copy of the pre-processed df
    start_date = f"{target_year}-01-01"
    end_date = f"{target_year}-12-31"
    
    # Ensure index is DatetimeIndex
    if not isinstance(symbol_df.index, pd.DatetimeIndex):
         symbol_df.set_index('TIMESTAMP', inplace=True)
         
    try:
        test_data = symbol_df[start_date:end_date]
    except KeyError:
        return []
    
    if test_data.empty:
        return []
    
    trades = []
    
    # State variables for active trade
    active_trade = None
    
    # We need to access previous close prices for trailing SL logic
    # Convert to list/dict for faster index access or use iloc within loop
    # Using itertuples is faster, but we need lookback.
    # Let's straightforwardly iterate.
    
    # To get trailing low of last 3 closes efficiently:
    # We can pre-calculate a rolling min of close, shifted by 1
    # But the condition "lowest close of last 3 daily prices" usually means:
    # Min(Close[t-1], Close[t-2], Close[t-3])
    # Let's pre-calculate this column for the whole dataframe to avoid lookups in loop
    
    # However, passing whole dataframe to function. We can calculate it on the slice.
    # Shift 1 to exclude current day (since we are acting on current day Open/High/Low)
    # Actually, if we are making decisions at Open/During day, we know previous closes.
    
    # Lowest close of last 3 days (t-1, t-2, t-3)
    test_data = test_data.copy()
    test_data['Trailing_Base'] = test_data['CLOSE'].shift(1).rolling(window=3).min()
    
    for date, row in test_data.iterrows():
        open_price = row['OPEN']
        high = row['HIGH']
        low = row['LOW']
        close = row['CLOSE']
        
        d_rsi = row['Daily_RSI']
        w_rsi = row['Weekly_RSI']
        m_rsi = row['Monthly_RSI']
        trailing_base = row['Trailing_Base']
        
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
                        'Initial Qty': qty,
                        'Current Qty': qty,
                        'Stop Loss': close * (1 - INITIAL_STOP_LOSS_PCT),
                        # Flags for stages
                        'Reached_8': False,
                        'Reached_10': False,
                        'Reached_15': False
                    }
        
        # --- Exit Logic ---
        else:
            buy_price = active_trade['Buy Price']
            
            # 1. Update Stop Loss based on Targets (Logic checks High of the day)
            
            # Condition 2: Target reaches 8% -> SL to Cost
            if not active_trade['Reached_8']:
                if high >= buy_price * 1.08:
                    active_trade['Stop Loss'] = max(active_trade['Stop Loss'], buy_price)
                    active_trade['Reached_8'] = True
            
            # Condition 3: Target Hit 10% -> Exit 50% Qty, SL to 5%
            # We must process exits during the day.
            # Assuming High reached -> we could have exited at 10%
            
            # Check 10% trigger
            if not active_trade['Reached_10']:
                target_10_price = buy_price * 1.10
                if high >= target_10_price:
                    # Execute Partial Exit
                    exit_qty = int(active_trade['Initial Qty'] * 0.50)
                    if exit_qty > 0 and active_trade['Current Qty'] >= exit_qty:
                        trades.append({
                            'Symbol': symbol,
                            'Entry Date': active_trade['Entry Date'],
                            'Strategy': active_trade['Strategy'],
                            'Buy Price': buy_price,
                            'Qty': exit_qty,
                            'Exit Date': date.date(),
                            'Sell Price': target_10_price,
                            'Status': 'Target 10% Hit',
                            'Return %': 10.0
                        })
                        active_trade['Current Qty'] -= exit_qty
                    
                    # Move SL to 5%
                    new_sl = buy_price * 1.05
                    active_trade['Stop Loss'] = max(active_trade['Stop Loss'], new_sl)
                    active_trade['Reached_10'] = True
                    # Set Reached_8 to True implies we passed it
                    active_trade['Reached_8'] = True 

            # Condition 4: Target Hit 15% -> Exit next 25% Qty, SL to 10%
            if not active_trade['Reached_15']:
                target_15_price = buy_price * 1.15
                if high >= target_15_price:
                     # Execute Partial Exit
                    exit_qty = int(active_trade['Initial Qty'] * 0.25)
                    # Ensure we don't sell more than we have
                    if exit_qty > active_trade['Current Qty']:
                        exit_qty = active_trade['Current Qty']
                        
                    if exit_qty > 0:
                        trades.append({
                            'Symbol': symbol,
                            'Entry Date': active_trade['Entry Date'],
                            'Strategy': active_trade['Strategy'],
                            'Buy Price': buy_price,
                            'Qty': exit_qty,
                            'Exit Date': date.date(),
                            'Sell Price': target_15_price,
                            'Status': 'Target 15% Hit',
                            'Return %': 15.0
                        })
                        active_trade['Current Qty'] -= exit_qty
                    
                    # Move SL to 10%
                    new_sl = buy_price * 1.10
                    active_trade['Stop Loss'] = max(active_trade['Stop Loss'], new_sl)
                    active_trade['Reached_15'] = True
                    active_trade['Reached_10'] = True
                    active_trade['Reached_8'] = True

            # Condition 5: Target > 15% -> Trailing SL Logic
            if active_trade['Reached_15']:
                # "keep SL as lowest close of last 3 daily prices"
                if not pd.isna(trailing_base):
                    active_trade['Stop Loss'] = max(active_trade['Stop Loss'], trailing_base)
            
            # --- Check for Stop Loss Hit ---
            # We check SL against LOW of the day.
            # IMPORTANT: Did we get stopped out TODAY?
            # If Low <= SL, we exit.
            # But what if we hit Target AND SL in same day?
            # Aggressive backtesting assumption: If Open < SL, Gap Down Stop Loss.
            # Else if Low <= SL, we hit SL.
            
            # However, we just potentially moved the SL up based on High.
            # If the candle is huge (Low < New_SL < High), we might claim target hit then SL hit.
            # For 8, 10, 15% targets, they are "Limit" orders effectively.
            # So if High reached them, they filled.
            # But the remaining quantity needs to survive the SL.
            
            stop_loss_price = active_trade['Stop Loss']
            
            if active_trade['Current Qty'] > 0:
                exit_price = None
                status = None
                
                if open_price <= stop_loss_price:
                    exit_price = open_price
                    status = 'Stop Loss Hit (Gap)'
                elif low <= stop_loss_price:
                    exit_price = stop_loss_price
                    status = 'Stop Loss Hit'
                    
                if exit_price is not None:
                     trades.append({
                        'Symbol': symbol,
                        'Entry Date': active_trade['Entry Date'],
                        'Strategy': active_trade['Strategy'],
                        'Buy Price': buy_price,
                        'Qty': active_trade['Current Qty'],
                        'Exit Date': date.date(),
                        'Sell Price': exit_price,
                        'Status': status,
                        'Return %': ((exit_price - buy_price) / buy_price) * 100
                    })
                     active_trade = None # Trade completely closed

    # End of data loop
    if active_trade and active_trade['Current Qty'] > 0:
        # Close open position at end of year/period
        current_close = test_data.iloc[-1]['CLOSE']
        trades.append({
            'Symbol': symbol,
            'Entry Date': active_trade['Entry Date'],
            'Strategy': active_trade['Strategy'],
            'Buy Price': active_trade['Buy Price'],
            'Qty': active_trade['Current Qty'],
            'Exit Date': 'Open',
            'Sell Price': current_close,
            'Status': 'Open',
            'Return %': ((current_close - active_trade['Buy Price']) / active_trade['Buy Price']) * 100
        })
        
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
    
    # Pre-load all symbol data
    print("Pre-loading symbol data and calculating RSI...")
    symbol_data_cache = {}
    for i, s in enumerate(symbols):
        if i % 10 == 0:
            print(f"Pre-loading {i}/{len(symbols)}: {s}...", end='\r')
        csv_path = os.path.join(adjusted_data_dir, f"{s}.csv")
        symbol_data_cache[s] = load_symbol_data(csv_path)
    print("\nData loading complete.")

    for year in years:
        output_xlsx = os.path.join(script_dir, f"RSI_Exit_Adjusted_Report_{year}.xlsx")
        print(f"\n{'='*60}")
        print(f"Running Backtest for YEAR: {year}")
        print(f"{'='*60}")
        
        with pd.ExcelWriter(output_xlsx, engine='xlsxwriter') as writer:
            # We only have ONE fixed logic, but to keep excel format similar, 
            # we can put it in one sheet "Fixed_Exit_Logic"
            sheet_name = "Fixed_Exit_Logic"
            print(f"Processing...", end='\r')
            all_trades = []
            
            for i, symbol in enumerate(symbols):
                if i % 20 == 0:
                    print(f"Scanning {symbol}...", end='\r')
                
                df = symbol_data_cache.get(symbol)
                if df is not None and not df.empty:
                    trades = backtest_symbol(symbol, df, target_year=year)
                    all_trades.extend(trades)
            
            if all_trades:
                results_df = pd.DataFrame(all_trades)
                column_order = [
                    'Symbol', 'Entry Date', 'Strategy', 'Buy Price', 'Qty', 
                    'Exit Date', 'Sell Price', 'Status', 'Return %'
                ]
                # Ensure columns exist even if empty
                for col in column_order:
                    if col not in results_df.columns:
                        results_df[col] = None
                        
                results_df = results_df[column_order]
                results_df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                win_rate = len(results_df[results_df['Return %'] > 0]) / len(results_df) * 100
                avg_ret = results_df['Return %'].mean()
                total_trades = len(results_df)
                print(f"Results: {total_trades} Partial Trades, Win {win_rate:.1f}%, Avg {avg_ret:.2f}%")
            else:
                print(f"No trades found.                      ")
        
        print(f"\n[SUCCESS] Report saved to: {output_xlsx}")

if __name__ == "__main__":
    main()
