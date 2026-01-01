
import os
import pandas as pd
from mftool import Mftool
from datetime import datetime, timedelta

# Configuration
SCHEME_CODE = '120590' # ICICI Prudential Gilt Fund - Direct Plan - Growth
Start_Date = '01-Jan-2020'
DATA_DIR = 'MF_Data'
FILE_NAME = 'ICICI_Pru_Gilt_Direct_Growth.csv'
FILE_PATH = os.path.join(DATA_DIR, FILE_NAME)

def get_mf_data():
    obj = Mftool()
    
    # Create Data Directory if it doesn't exist
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        print(f"Created directory: {DATA_DIR}")

    # Check if file exists to determine start date
    if os.path.exists(FILE_PATH):
        print(f"File found: {FILE_PATH}")
        try:
            # Read existing data to find the last date
            df_existing = pd.read_csv(FILE_PATH)
            df_existing['Date'] = pd.to_datetime(df_existing['Date'], dayfirst=True) # Ensure correct format
            last_date_dt = df_existing['Date'].max()
            
            # Start from next day
            s_date = (last_date_dt + timedelta(days=1)).strftime('%d-%b-%Y')
            print(f"Last date in file: {last_date_dt.strftime('%d-%b-%Y')}. Fetching from: {s_date}")
            
            # Fetch new data
            # mftool get_scheme_historical_nav returns data in dict format
            # we need to be careful if start date > end date (today)
            if last_date_dt.date() >= datetime.today().date():
                 print("Data is already up to date.")
                 return

            data = obj.get_scheme_historical_nav(SCHEME_CODE, s_date, datetime.today().strftime('%d-%b-%Y'))
        except Exception as e:
            print(f"Error reading existing file: {e}. Starting fresh.")
            s_date = Start_Date
            data = obj.get_scheme_historical_nav(SCHEME_CODE, s_date, datetime.today().strftime('%d-%b-%Y'))
            df_existing = pd.DataFrame() # Reset
    else:
        print(f"File not found. Starting fresh from {Start_Date}")
        s_date = Start_Date
        data = obj.get_scheme_historical_nav(SCHEME_CODE, s_date, datetime.today().strftime('%d-%b-%Y'))
        df_existing = None

    if data and 'data' in data:
        # Convert to DataFrame
        # mftool returns 'data': [{'date': 'DD-MM-YYYY', 'nav': '12.34'}, ...]
        new_data = data['data']
        if not new_data:
             print("No new data received.")
             return

        df_new = pd.DataFrame(new_data)
        # Rename columns to standard
        df_new.rename(columns={'date': 'Date', 'nav': 'nav'}, inplace=True)
        # Ensure date format matches existing (mftool returns DD-MM-YYYY)
        # We want to be consistent. Let's keep it as standard YYYY-MM-DD or whatever panda prefers, 
        # but to match mftool's input requirement and common CSV usage, let's stick to a standard format in CSV.
        # Actually proper standard is datetime objects.
        
        # Convert to datetime to sort
        df_new['Date'] = pd.to_datetime(df_new['Date'], dayfirst=True)
        df_new['nav'] = pd.to_numeric(df_new['nav'])
        
        # Merge if existing
        if df_existing is not None and not df_existing.empty:
            df_final = pd.concat([df_existing, df_new])
            df_final.drop_duplicates(subset=['Date'], keep='last', inplace=True)
            df_final.sort_values(by='Date', ascending=True, inplace=True)
        else:
            df_final = df_new
            df_final.sort_values(by='Date', ascending=True, inplace=True)
            
        # Format Date back to string for CSV if desired, or keep as standard ISO (YYYY-MM-DD) which is best for pandas
        # Let's use YYYY-MM-DD
        df_final.to_csv(FILE_PATH, index=False, date_format='%Y-%m-%d')
        print(f"Data updated. Total records: {len(df_final)}")
        print(f"Saved to: {FILE_PATH}")
    else:
        print("Failed to fetch data or invalid response from AMFI.")

if __name__ == "__main__":
    get_mf_data()
