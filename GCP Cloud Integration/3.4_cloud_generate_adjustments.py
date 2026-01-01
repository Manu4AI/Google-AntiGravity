import pandas as pd
import re
import logging
from gcs_handler import GCSHandler
from importlib import import_module

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Dynamic import of the copied engine
# Assuming 3.2_corporate_action_engine.py is in the same folder
try:
    engine_module = import_module("3.2_corporate_action_engine")
    CorporateActionEngine = engine_module.CorporateActionEngine
except ImportError:
    # Try importing with underscore replacement if needed or just filename
    # Python doesn't like dots in filenames for import. 
    # Valid strategy: Rename 3.2... to mod_engine.py in cloud or use importlib machinery.
    import importlib.util
    import sys
    import os
    spec = importlib.util.spec_from_file_location("mod_engine", os.path.join(os.path.dirname(__file__), "3.2_corporate_action_engine.py"))
    mod = importlib.util.module_from_spec(spec)
    sys.modules["mod_engine"] = mod
    spec.loader.exec_module(mod)
    CorporateActionEngine = mod.CorporateActionEngine

class CloudAdjustmentGenerator:
    def __init__(self):
        self.gcs = GCSHandler()
        self.master_list_blob = "config/0_Script_Master_List.csv"
        self.ca_prefix = "corporate_actions"
        self.script_prefix = "script_data"
        self.output_master = "config/Corporate_Actions_Master.csv"
        self.output_calc = "config/Calculated_Adjustments.csv"
        
        # Regex (Same as local)
        self.REGEX_BONUS = r"Bonus\s+(\d+):(\d+)"
        self.REGEX_SPLIT_FROM_TO = r"Split.*From.*?(\d+(?:\.\d+)?).*?To.*?(\d+(?:\.\d+)?)"
        self.REGEX_RIGHTS = r"Rights.*?(\d+):(\d+).*?Premium.*?(\d+(?:\.\d+)?)"
    
    def get_cum_rights_price(self, symbol, ex_date):
        """Fetches cum-rights price from GCS script data."""
        blob_name = f"{self.script_prefix}/{symbol}.csv"
        if not self.gcs.file_exists(blob_name): return None
        
        df = self.gcs.read_csv(blob_name)
        if df is None or 'date' not in df.columns: return None
        
        # Parse date and filter
        df['date'] = pd.to_datetime(df['date']) # Usually fast enough for single file
        ex_dt = pd.to_datetime(ex_date)
        
        pre_df = df[df['date'] < ex_dt]
        if pre_df.empty: return None
        
        # Last close
        return pre_df.iloc[-1]['close_price']

    def parse_ca_file(self, symbol, df):
        events = []
        for idx, row in df.iterrows():
            subject = str(row['subject'])
            ex_date = row['exDate']
            face_val = float(row['faceVal']) if row['faceVal'] != '-' else 0

            # BONUS
            bonus_match = re.search(self.REGEX_BONUS, subject, re.IGNORECASE)
            if bonus_match:
                a, b = bonus_match.groups()
                events.append({
                    'symbol': symbol, 'ex_date': ex_date, 'action_type': 'BONUS',
                    'ratio': f"{a}:{b}", 'issue_price': '', 'remarks': subject
                })
                continue
            
            # SPLIT
            split_match = re.search(self.REGEX_SPLIT_FROM_TO, subject, re.IGNORECASE)
            if split_match:
                old_fv, new_fv = float(split_match.group(1)), float(split_match.group(2))
                if new_fv > 0:
                     # Ratio logic from local script: 1 : (Old/New)
                     qty_mult = old_fv / new_fv
                     events.append({
                        'symbol': symbol, 'ex_date': ex_date, 'action_type': 'SPLIT',
                        'ratio': f"1:{qty_mult:g}", 'issue_price': '', 'remarks': subject
                    })
                continue

            # RIGHTS
            rights_match = re.search(self.REGEX_RIGHTS, subject, re.IGNORECASE)
            if rights_match:
                r_new, r_held = rights_match.group(1), rights_match.group(2)
                premium = float(rights_match.group(3))
                issue_price = premium + face_val
                events.append({
                    'symbol': symbol, 'ex_date': ex_date, 'action_type': 'RIGHTS',
                    'ratio': f"{r_new}:{r_held}", 'issue_price': issue_price, 'remarks': subject
                })
                continue
                
        return events

    def run(self):
        # 1. Load Master List to know symbols
        if not self.gcs.file_exists(self.master_list_blob):
            logging.error("Master list missing.")
            return

        master_df = self.gcs.read_csv(self.master_list_blob)
        symbols = master_df['Symbol'].unique()
        
        all_events = []
        logging.info("Parsing Corporate Actions from GCS...")
        
        # Optimized: List all CA files first?
        # ca_blobs = self.gcs.list_files(prefix=self.ca_prefix) # corporate_actions/RELIANCE.csv
        # Doing symbol by symbol is safer for matching
        
        for sym in symbols:
            blob = f"{self.ca_prefix}/{sym}.csv"
            if self.gcs.file_exists(blob):
                df = self.gcs.read_csv(blob)
                if df is not None and not df.empty:
                    evs = self.parse_ca_file(sym, df)
                    all_events.extend(evs)
        
        # Save Parsed Master
        if not all_events:
            logging.info("No corporate actions found.")
            return

        out_master = pd.DataFrame(all_events)
        out_master.sort_values(['symbol', 'ex_date'], inplace=True)
        self.gcs.write_csv(out_master, self.output_master)
        logging.info(f"Saved Master Events to {self.output_master}")

        # 2. Calculate Adjustments
        logging.info("Calculating Price Factors...")
        results = []
        
        for _, row in out_master.iterrows():
            sym = row['symbol']
            action = row['action_type']
            ratio = row['ratio']
            ex_date = row['ex_date']
            
            calc_res = {}
            try:
                if action == 'SPLIT':
                    calc_res = CorporateActionEngine.calculate_split(ratio)
                elif action == 'BONUS':
                    calc_res = CorporateActionEngine.calculate_bonus(ratio)
                elif action == 'RIGHTS':
                    ip = float(row['issue_price'])
                    mp = self.get_cum_rights_price(sym, ex_date)
                    if mp:
                        calc_res = CorporateActionEngine.calculate_rights(ratio, ip, mp)
                
                if calc_res:
                    pm = calc_res.get('price_multiplier', 1.0)
                    results.append({
                        'symbol': sym, 'action_type': action,
                        'ex_date': ex_date, 'ratio': ratio,
                        'price_multiplier': round(pm, 6)
                    })
            except Exception as e:
                logging.error(f"Calc Error {sym} {action}: {e}")
        
        if results:
            final_df = pd.DataFrame(results)
            self.gcs.write_csv(final_df, self.output_calc)
            logging.info(f"Saved {len(final_df)} adjustments to {self.output_calc}")
        else:
             logging.info("No adjustments calculated.")

if __name__ == "__main__":
    gen = CloudAdjustmentGenerator()
    gen.run()
