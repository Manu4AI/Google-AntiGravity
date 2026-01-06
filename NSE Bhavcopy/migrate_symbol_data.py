
import os
import json
import pandas as pd
import logging

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def merge_symbol_data(mapping_file, directories):
    """
    Merges data from old symbol files to new symbol files based on a mapping.
    """
    if not os.path.exists(mapping_file):
        logging.error(f"Mapping file not found: {mapping_file}")
        return

    with open(mapping_file, 'r') as f:
        mapping = json.load(f)

    for old_sym, new_sym in mapping.items():
        logging.info(f"Processing migration: {old_sym} -> {new_sym}")
        
        for directory in directories:
            if not os.path.exists(directory):
                logging.warning(f"Directory not found: {directory}")
                continue

            old_file = os.path.join(directory, f"{old_sym}.csv")
            new_file = os.path.join(directory, f"{new_sym}.csv")

            if not os.path.exists(old_file):
                logging.info(f"  No old file found in {os.path.basename(directory)} for {old_sym}")
                continue

            logging.info(f"  Merging data in {os.path.basename(directory)}...")
            
            try:
                # Load old data
                df_old = pd.read_csv(old_file)
                
                # Check for new file
                if os.path.exists(new_file):
                    df_new = pd.read_csv(new_file)
                    # Merge and deduplicate
                    df_combined = pd.concat([df_old, df_new], ignore_index=True)
                else:
                    df_combined = df_old

                # Ensure date is datetime for sorting
                if 'date' in df_combined.columns:
                    df_combined['date'] = pd.to_datetime(df_combined['date'])
                    # Drop duplicates based on date
                    df_combined = df_combined.drop_duplicates(subset=['date'], keep='last')
                    # Sort by date
                    df_combined = df_combined.sort_values(by='date')
                
                # Save to new file
                df_combined.to_csv(new_file, index=False)
                logging.info(f"  Successfully merged data into {new_file}")
                
                # Note: Not deleting old_file yet as per user request.
                logging.info(f"  [KEEPING] {old_file} for now.")

            except Exception as e:
                logging.error(f"  Failed to merge data for {old_sym}: {e}")

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    map_file = os.path.join(script_dir, "symbol_change_map.json")
    
    # Directories to process
    target_dirs = [
        os.path.join(script_dir, "NSE_Bhavcopy_Scriptwsie_Data"),
        os.path.join(script_dir, "NSE_Bhavcopy_Adjusted_Data"),
        # Note: Moving Average Strategy Folder in Github might be nested differently?
        # Checking if 'Moving Average Strategy' exists at same level as 'NSE Bhavcopy'
    ]
    
    # Check parent dir for common project folders
    parent_dir = os.path.dirname(script_dir)
    ma_dir = os.path.join(parent_dir, "Moving Average Strategy", "Scriptwise MA Calculation")
    if os.path.exists(ma_dir):
        target_dirs.append(ma_dir)
    
    merge_symbol_data(map_file, target_dirs)
