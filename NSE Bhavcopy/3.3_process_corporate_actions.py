
import os
import pandas as pd
import datetime
import importlib.util
import sys

try:
    # Load 3.2 Engine by path
    spec = importlib.util.spec_from_file_location("mod_3_2", "3.2_corporate_action_engine.py")
    ca_engine = importlib.util.module_from_spec(spec)
    sys.modules["mod_3_2"] = ca_engine
    spec.loader.exec_module(ca_engine)
    CorporateActionEngine = ca_engine.CorporateActionEngine
except Exception:
    CorporateActionEngine = None

MASTER_FILE = "Corporate_Actions_Master.csv"
DATA_DIR = "NSE_Bhavcopy_Scriptwsie_Data"
OUTPUT_FILE = "Calculated_Adjustments.csv"

def get_cum_rights_price(symbol, ex_date_str):
    """
    Finds the close price of the symbol on the last trading day BEFORE ex_date.
    """
    try:
        if not os.path.exists(DATA_DIR):
            print(f"Dataset directory not found: {DATA_DIR}")
            return None
            
        file_path = os.path.join(DATA_DIR, f"{symbol}.csv")
        if not os.path.exists(file_path):
            print(f"Data file not found for {symbol}")
            return None
        
        # Read file
        df = pd.read_csv(file_path)
        
        # Convert date column to datetime
        try:
            df['date'] = pd.to_datetime(df['date'])
        except Exception:
            # Fallback for mixed formats if needed, or ignoring errors
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df = df.dropna(subset=['date'])
        ex_date = pd.to_datetime(ex_date_str)
        
        # Filter dates strictly less than ex_date
        # Sort by date
        df = df.sort_values('date')
        
        pre_ex_df = df[df['date'] < ex_date]
        
        if pre_ex_df.empty:
            print(f"No data found before {ex_date_str} for {symbol}")
            return None
            
        # Get last row
        last_row = pre_ex_df.iloc[-1]
        close_price = last_row['close_price']
        trade_date = last_row['date'].date()
        
        print(f"Found Cum-Rights Price for {symbol}: {close_price} on {trade_date}")
        return close_price
        
    except Exception as e:
        print(f"Error fetching price for {symbol}: {e}")
        return None

def process():
    if not os.path.exists(MASTER_FILE):
        print(f"{MASTER_FILE} not found.")
        return

    # Read Master
    # Use comment filtering manually or use 'comment' param if consistently #
    # Pandas read_csv with comment='#' works
    try:
        master_df = pd.read_csv(MASTER_FILE, comment='#', skipinitialspace=True)
    except Exception as e:
        print(f"Error reading master file: {e}")
        return

    results = []
    
    print("Processing Corporate Actions...")
    
    for idx, row in master_df.iterrows():
        symbol = row['symbol']
        ex_date = row['ex_date']
        action = row['action_type'].strip().upper()
        ratio = row['ratio']
        issue_price = row.get('issue_price', pd.NA)
        
        # Skip empty lines if any
        if pd.isna(symbol): continue
        
        print(f"[{idx+1}] Processing {symbol} - {action} - {ex_date}")
        
        calc_res = {}
        
        if action == 'SPLIT':
            calc_res = CorporateActionEngine.calculate_split(ratio)
            
        elif action == 'BONUS':
            calc_res = CorporateActionEngine.calculate_bonus(ratio)
            
        elif action == 'RIGHTS':
            # Need Price
            if pd.isna(issue_price):
                print(f"  Error: Missing Issue Price for Rights {symbol}")
                continue
                
            market_price = get_cum_rights_price(symbol, ex_date)
            if market_price is None:
                # Fallback if testing without full data? 
                # User provided example Adani P=735 if specific case?
                # But let's trust the data lookup.
                pass 
                
            if market_price:
                calc_res = CorporateActionEngine.calculate_rights(ratio, issue_price, market_price)
            else:
                print(f"  Skipping RIGHTS calc for {symbol} due to missing price.")
                
        elif action == 'DEMERGER':
             calc_res = CorporateActionEngine.calculate_demerger(ratio)
        
        else:
            print(f"  Unknown Action: {action}")
            continue
            
        if calc_res:
             # Prepare output row matching Adjustment.csv roughly but with calculated fields
             # Adjustment.csv cols: symbol,action_type,ex_date,ratio,price_multiplier
             
             price_mult = calc_res.get('price_multiplier', '')
             if isinstance(price_mult, float):
                 price_mult = round(price_mult, 6)
             
             results.append({
                 'symbol': symbol,
                 'action_type': action,
                 'ex_date': ex_date,
                 'ratio': ratio,
                 'price_multiplier': price_mult,
                 # Extra info for debug/verification
                 # 'qty_multiplier': calc_res.get('qty_multiplier', ''),
                 # 'remarks': row.get('remarks', '')
             })
             print(f"  -> Adjustment Factor: {price_mult}")

    # Write Output
    if results:
        out_df = pd.DataFrame(results)
        # Reorder columns to match existing format preference
        cols = ['symbol', 'action_type', 'ex_date', 'ratio', 'price_multiplier']
        out_df = out_df[cols]
        
        out_df.to_csv(OUTPUT_FILE, index=False)
        print(f"\nSuccessfully wrote {len(results)} records to {OUTPUT_FILE}")
    else:
        print("No results generated.")

if __name__ == "__main__":
    process()
