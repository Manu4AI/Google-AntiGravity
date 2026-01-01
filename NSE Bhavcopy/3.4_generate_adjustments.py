
import pandas as pd
import os
import re
import importlib.util
import sys

def load_module_from_path(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module

try:
    ca_engine = load_module_from_path("mod_3_2", "3.2_corporate_action_engine.py")
    CorporateActionEngine = ca_engine.CorporateActionEngine
    
    proc_ca = load_module_from_path("mod_3_3", "3.3_process_corporate_actions.py")
    get_cum_rights_price = proc_ca.get_cum_rights_price
except Exception as e:
    print(f"Import Error: {e}")
    # Fallback/Definitions if needed to avoid crash before logging
    CorporateActionEngine = None
    get_cum_rights_price = None

MASTER_LIST = "0_Script_Master_List.csv"
DATA_DIR = "NSE_Corporate_Actions_Data"
OUTPUT_CALC = "Calculated_Adjustments.csv"
OUTPUT_MASTER = "Corporate_Actions_Master.csv" # To store the parsed events as well

# Regex Patterns
REGEX_BONUS = r"Bonus\s+(\d+):(\d+)"
REGEX_SPLIT_FROM_TO = r"Split.*From.*?(\d+(?:\.\d+)?).*?To.*?(\d+(?:\.\d+)?)"
REGEX_SPLIT_RATIO = r"Split.*?(\d+):(\d+)" # Fallback
REGEX_RIGHTS = r"Rights.*?(\d+):(\d+).*?Premium.*?(\d+(?:\.\d+)?)"
REGEX_RIGHTS_PRICE = r"Rights.*?Price.*?(\d+(?:\.\d+)?)" # If explicit price mentioned

def parse_corporate_actions_file(symbol, file_path):
    try:
        df = pd.read_csv(file_path)
    except:
        return []

    events = []
    
    # Sort by date descending usually in these files? check Reliance. Descending (2025 top).
    # We iterate and find events.
    
    for idx, row in df.iterrows():
        subject = str(row['subject'])
        ex_date = row['exDate']
        face_val = float(row['faceVal']) if row['faceVal'] != '-' else 0

        # --- BONUS ---
        bonus_match = re.search(REGEX_BONUS, subject, re.IGNORECASE)
        if bonus_match:
            a, b = bonus_match.groups()
            # "Bonus 1:1" -> A=1, B=1. 
            # Engine takes "Bonus:Held" ratio string.
            # Assuming pattern is "Bonus A:B" meaning A bonus for B held?
            # Reliance "Bonus 1:1". 
            ratio = f"{a}:{b}"
            events.append({
                'symbol': symbol,
                'ex_date': ex_date,
                'action_type': 'BONUS',
                'ratio': ratio,
                'issue_price': '',
                'remarks': subject
            })
            continue

        # --- SPLIT ---
        # "Face Value Split... From Rs 10/- ... To Re 1/-"
        split_match_ft = re.search(REGEX_SPLIT_FROM_TO, subject, re.IGNORECASE)
        if split_match_ft:
            old_fv, new_fv = float(split_match_ft.group(1)), float(split_match_ft.group(2))
            if old_fv > 0 and new_fv > 0:
                # Share Ratio = OldFV : NewFV
                # 1 share of 10 becomes 10 shares of 1.
                # Engine needs OldQty:NewQty.
                # NewQty = OldQty * (OldFV/NewFV) = 1 * (10/1) = 10.
                # Ratio 1:10.
                qty_mult = old_fv / new_fv
                ratio = f"1:{qty_mult:g}"
                events.append({
                    'symbol': symbol,
                    'ex_date': ex_date,
                    'action_type': 'SPLIT',
                    'ratio': ratio,
                    'issue_price': '',
                    'remarks': subject
                })
                continue
        
        # --- RIGHTS ---
        # "Rights 1:15 @ Premium Rs 1247"
        rights_match = re.search(REGEX_RIGHTS, subject, re.IGNORECASE)
        if rights_match:
            r_new, r_held = rights_match.group(1), rights_match.group(2)
            premium = float(rights_match.group(3))
            
            # Issue Price = Premium + FaceValue
            issue_price = premium + face_val
            
            ratio = f"{r_new}:{r_held}"
            events.append({
                'symbol': symbol,
                'ex_date': ex_date,
                'action_type': 'RIGHTS',
                'ratio': ratio,
                'issue_price': issue_price,
                'premium': premium, # Store Premium
                'remarks': subject
            })
            continue
            
        # --- DEMERGER ---
        if "Demerger" in subject:
             # Demerger usually doesn't have ratio in subject easily parsable logic without standard text
             # Reliance: "Demerger". No details.
             # We will add it as Demerger 1:1 default? Or mark as manual check.
             # Instructions: "Demerger... Cost split... No price adj".
             # We can add a placeholder.
             events.append({
                'symbol': symbol,
                'ex_date': ex_date,
                'action_type': 'DEMERGER',
                'ratio': '1:1',
                'issue_price': '',
                'remarks': subject
             })

    return events

def run_pipeline():
    # 1. Master List
    if not os.path.exists(MASTER_LIST): return
    
    # --- OPTIMIZATION START ---
    should_run = True  # Default to run
    
    # helper to get mtime or 0
    def get_mtime(path): 
        return os.path.getmtime(path) if os.path.exists(path) else 0

    if os.path.exists(OUTPUT_MASTER) and os.path.exists(DATA_DIR):
        master_mtime = get_mtime(OUTPUT_MASTER)
        
        # 1. Check Input Data vs Master
        # Get latest mtime of all data files
        data_files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith(".csv")]
        latest_data_mtime = max([get_mtime(f) for f in data_files]) if data_files else 0
        
        # 2. Check Script Logic vs Master (3.4 itself)
        script_mtime = get_mtime(__file__)
        
        # 3. Check Dependencies vs Output (3.3 contains calc logic)
        # We need to find path of 3.3 relative to here
        dep_33_path = "3.3_process_corporate_actions.py"
        dep_33_mtime = get_mtime(dep_33_path)
        
        # 4. Check Final Output existence and freshness
        calc_mtime = get_mtime(OUTPUT_CALC)
        
        # CHECK CONDITIONS
        
        # A. Do we need to re-parse inputs?
        inputs_changed = (latest_data_mtime > master_mtime) or (script_mtime > master_mtime)
        
        # B. Do we need to re-calculate?
        # Re-calc if:
        # - Inputs changed (Master will be updated)
        # - Final output missing
        # - Master is strictly newer than Final Output (meaning we parsed but didn't calc)
        # - Logic in 3.3 changed (it handles the heavy lifting)
        # - Logic in 3.4 changed (script_mtime)
        
        calc_needed = (
            inputs_changed or 
            (calc_mtime == 0) or 
            (master_mtime > calc_mtime) or 
            (dep_33_mtime > calc_mtime) or
            (script_mtime > calc_mtime)
        )
        
        if not calc_needed:
             print("="*60)
             print(">>> SKIPPING STEP 4: Adjustment Data and Logic are up to date.")
             print("="*60 + "\n")
             should_run = False
    
    if not should_run:
        return
    # --- OPTIMIZATION END ---

    master_df = pd.read_csv(MASTER_LIST)
    symbols = master_df['Symbol'].unique()
    
    all_events = []
    
    print("Parsing Corporate Action Files...")
    # ... (Rest of original function)
    for sym in symbols:
        fpath = os.path.join(DATA_DIR, f"{sym}.csv")
        if os.path.exists(fpath):
            evs = parse_corporate_actions_file(sym, fpath)
            all_events.extend(evs)
    
    # Save Parsed Events to Master
    out_master = pd.DataFrame(all_events)
    if not out_master.empty:
        out_master.sort_values(['symbol', 'ex_date'], inplace=True)
        # Select cols
        out_master.to_csv(OUTPUT_MASTER, index=False)
        print(f"Parsed {len(out_master)} events into {OUTPUT_MASTER}")
    
    # 2. Run Engine
    print("Running Calculation Engine...")
    results = []
    
    for _, row in out_master.iterrows():
        sym = row['symbol']
        action = row['action_type']
        ratio = row['ratio']
        issue_price = row['issue_price']
        ex_date = row['ex_date']
        
        calc_res = {}
        
        try:
            if action == 'SPLIT':
                calc_res = CorporateActionEngine.calculate_split(ratio)
            elif action == 'BONUS':
                calc_res = CorporateActionEngine.calculate_bonus(ratio)
            elif action == 'RIGHTS':
                # Parse Issue Price
                if not issue_price:
                    continue
                ip = float(issue_price)
                
                 # Lookup Market Price
                mkt_price = get_cum_rights_price(sym, ex_date)
                if mkt_price:
                    calc_res = CorporateActionEngine.calculate_rights(ratio, ip, mkt_price)
                else:
                    # Try to use a default or verify if data exists?
                    # If data missing (old date), maybe skip?
                    # Reliance Rights in 2020. Data might be there.
                    pass
                    
            elif action == 'DEMERGER':
                calc_res = CorporateActionEngine.calculate_demerger(ratio)
                
                
            if calc_res:
                price_mult = calc_res.get('price_multiplier', 1.0)
                
                results.append({
                    'symbol': sym,
                    'action_type': action,
                    'ex_date': ex_date,
                    'ratio': ratio,
                    'premium': row.get('premium', ''), 
                    'price_multiplier': round(price_mult, 6)
                })
        except Exception as e:
            print(f"Error calculating {sym} {action}: {e}")

    # Save Results
    final_df = pd.DataFrame(results)
    final_df.to_csv(OUTPUT_CALC, index=False)
    print(f"Generated {len(final_df)} adjustments in {OUTPUT_CALC}")

if __name__ == "__main__":
    run_pipeline()
