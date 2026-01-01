import os
import pandas as pd
import datetime

# Paths
base_dir = r"F:\Shivendra -PC Sync\Google AntiGravity\NSE Bhavcopy"
master_dir = os.path.join(base_dir, "NSE_Bhavcopy_Master_Data", "2025")
target_file = os.path.join(master_dir, "bhavcopy_20251231.csv")

print(f"Checking file: {target_file}")

if not os.path.exists(target_file):
    print("ERROR: File does not exist!")
else:
    print("File exists.")
    try:
        # Replicate reading logic
        df = pd.read_csv(target_file)
        
        # Standardize Columns
        df.columns = [c.strip().upper() for c in df.columns]
        print("Columns found:", df.columns.tolist())
        
        # Check Symbol
        symbol = "3MINDIA"
        if 'SYMBOL' in df.columns:
            matches = df[df['SYMBOL'] == symbol]
            print(f"Rows for {symbol}: {len(matches)}")
            if not matches.empty:
                print(matches.head())
                
                # Check Series
                if 'SERIES' in df.columns:
                    eq_matches = matches[matches['SERIES'] == 'EQ']
                    print(f"EQ Series Rows: {len(eq_matches)}")
                    
                    # Check Date parsing
                    if 'DATE1' in df.columns:
                        date_val = eq_matches.iloc[0]['DATE1']
                        print(f"Raw Date Value: '{date_val}'")
                        try:
                            # Note: ScriptWiseUpdater doesn't parse date from column value for output? 
                            # It uses filenames for date_obj.
                            # But let's check content.
                            pass
                        except Exception as e:
                            print(f"Date check error: {e}")
            else:
                print("Symbol not found in dataframe.")
        else:
            print("SYMBOL column missing.")
            
    except Exception as e:
        print(f"Error reading file: {e}")
