import re
import pandas as pd
from glob import glob

# ================= PATHS =================
CA_ROOT = r"F:\Shivendra -PC Sync\Google AntiGravity\NSE Bhavcopy\NSE_Corporate_Actions_Data"

MANUAL_FILE = r"F:\Shivendra -PC Sync\Google AntiGravity\NSE Bhavcopy\Manual_Adjustment.csv"

OUTPUT_FILE = r"F:\Shivendra -PC Sync\Google AntiGravity\NSE Bhavcopy\Adjustment.csv"

START_DATE = pd.Timestamp("2021-01-01")

# ================= PARSERS =================
def parse_ratio(text):
    """
    Parses '10:1', '1:1', or direct number strings.
    Returns (A, B) or (None, None)
    """
    text = str(text).strip()
    # Check for A:B format
    m = re.match(r'^(\d+(\.\d+)?)\s*:\s*(\d+(\.\d+)?)$', text)
    if m:
        return float(m.group(1)), float(m.group(3))
    
    # Check for direct number
    try:
        val = float(text)
        return val, 1.0 # Treat '5' as '5:1' for generic parsing? 
        # Actually context matters. 
        # For Split 10:1 (10 shares for 1), Ratio=10.
        # For Bonus 1:1, Ratio=1.
        # Retain as number for now.
    except:
        pass
        
    return None, None

def normalize_action(text):
    text = str(text).lower()
    if 'split' in text: return 'SPLIT'
    if 'bonus' in text: return 'BONUS'
    if 'demerger' in text: return 'DEMERGER'
    if 'rights' in text: return 'RIGHTS'
    if 'dividend' in text: return 'DIVIDEND'
    return 'OTHER'

def extract_details(text):
    """
    Extracts Ratio logic from specific Nse narrative text if possible.
    Returns (Type, RatioString)
    """
    text_lower = str(text).lower()
    
    # SPLIT
    if 'split' in text_lower:
        # "Split from Rs 10 to Rs 1" -> 10:1
        m = re.search(r'rs\.?\s*(\d+).+to.+rs\.?\s*(\d+)', text_lower)
        if m:
            older = m.group(1)
            newer = m.group(2)
            # Ratio is Older:Newer (e.g. 10:1)
            # Standard notation usually New:Old for shares? 
            # Split 1 share (FV 10) -> 10 shares (FV 1). Ratio 10:1.
            return 'SPLIT', f"{older}:{newer}"
            
    # BONUS
    if 'bonus' in text_lower:
        # "Bonus 1:1"
        m = re.search(r'(\d+)\s*:\s*(\d+)', text_lower)
        if m:
            return 'BONUS', f"{m.group(1)}:{m.group(2)}"
            
    return 'OTHER', ''

# ================= CALCULATIONS =================
def parse_ratio_values(ratio_str):
    if ':' in str(ratio_str):
        parts = str(ratio_str).split(':')
        return float(parts[0]), float(parts[1])
    try:
        return float(ratio_str), 1.0
    except:
        return 1.0, 1.0

def calculate_factor(action_type, ratio_str):
    """
    Returns the PRICE MULTIPLIER (Backward).
    """
    t = str(action_type).upper().strip()
    A, B = parse_ratio_values(ratio_str)
    
    if t == 'SPLIT':
        # Split A:B usually means Old FV : New FV (e.g. 10:1)
        # Price becomes 1/10th. Factor 0.1.
        if A == 0: return 1.0
        return B / A
        
    elif t == 'BONUS':
        # Bonus A:B usually means Received : Held (e.g. 1:1)
        # Factor = 1/(1 + A/B)
        if B == 0: return 1.0
        return 1.0 / (1.0 + (A / B))
        
    elif t == 'DEMERGER':
        # Indeterminate without valuation. 
        # If Ratio is scalar X (e.g. 28?), assume user provided Divisor/Factor directly?
        # If "3:25", return 1.0.
        if ':' not in str(ratio_str):
            try:
                val = float(ratio_str)
                # If val > 1, assume Divisor (Backwards compatible with old Adjustment.csv 28)
                if val > 1: return 1.0 / val 
                return val
            except:
                pass
        return 1.0
        
    return 1.0

# ================= MAIN =================
def main():
    all_actions = []

    # 1. PROCESS AUTOMATED FILES
    files = glob(f"{CA_ROOT}/*.csv")
    print(f"📂 Scanning {len(files)} automated files...")
    
    for file in files:
        symbol = file.split("/")[-1].replace(".csv", "")
        try:
            df = pd.read_csv(file)
            col_map = {c.lower(): c for c in df.columns}
            ex_col = col_map.get('exdate')
            subj_col = col_map.get('subject')
            
            if not ex_col or not subj_col:
                continue
                
            df[ex_col] = pd.to_datetime(df[ex_col], errors='coerce')
            df = df[df[ex_col] >= START_DATE]
            
            for _, row in df.iterrows():
                narrative = row[subj_col]
                act_type, ratio_str = extract_details(narrative)
                
                if act_type in ['SPLIT', 'BONUS'] and ratio_str:
                    all_actions.append({
                        'Symbol': symbol,
                        'ExDate': row[ex_col].date(),
                        'ActionType': act_type,
                        'Ratio': ratio_str
                    })
        except:
            pass

    # 2. PROCESS MANUAL FILE
    try:
        man_df = pd.read_csv(MANUAL_FILE, comment='#')
        print(f"📂 Processing Manual Adjustments ({len(man_df)} rows)...")
        
        for _, row in man_df.iterrows():
            try:
                ex_date = pd.to_datetime(row['ExDate']).date()
                act_type = str(row['ActionType']).upper().strip()
                ratio = str(row['Ratio']).strip()
                sym = str(row['Symbol']).strip()
                
                all_actions.append({
                    'Symbol': sym,
                    'ExDate': ex_date,
                    'ActionType': act_type,
                    'Ratio': ratio
                })
            except:
                continue
    except:
        print("ℹ️ Manual_Adjustment.csv not found or error.")

    # 3. CALCULATE FACTORS & CONSOLIDATE
    if not all_actions:
        print("No actions found.")
        return

    full_df = pd.DataFrame(all_actions)
    
    # Deduplicate before calc to save time? Or after?
    # Last entry wins.
    full_df.drop_duplicates(subset=['Symbol', 'ExDate', 'ActionType'], keep='last', inplace=True)
    
    # Calculate Factor
    full_df['Factor'] = full_df.apply(lambda x: calculate_factor(x['ActionType'], x['Ratio']), axis=1)
    
    # Round Factor
    full_df['Factor'] = full_df['Factor'].round(6)

    # Sort
    full_df.sort_values(by=['Symbol', 'ExDate'], inplace=True)
    
    # Save
    full_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Generated {OUTPUT_FILE}")
    print(f"📊 Total Actions: {len(full_df)}")
    
    # Show ADANIENT specifically if present
    adani = full_df[full_df['Symbol'] == 'ADANIENT']
    if not adani.empty:
        print("\n🔍 ADANIENT Entry:")
        print(adani.to_string(index=False))
    else:
        print("\nℹ️ ADANIENT not found in adjustments.")
        
if __name__ == "__main__":
    main()