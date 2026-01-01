
import yfinance as yf
import pandas as pd
import os

# Configuration
TICKER = '^NSEI'
DATA_DIR = 'MF_Data'
FILE_NAME = 'NIFTY50_Data.csv'
FILE_PATH = os.path.join(DATA_DIR, FILE_NAME)

def download_nifty_data():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    
    print(f"Downloading data for {TICKER}...")
    # Download max history
    ticker = yf.Ticker(TICKER)
    df = ticker.history(period="max")
    
    if df.empty:
        print("Failed to download data.")
        return

    # Reset index to make Date a column
    df.reset_index(inplace=True)
    
    # Keep only Date and Close
    df_final = df[['Date', 'Close']].copy()
    
    # Rename Close to nav for consistency with other scripts
    df_final.rename(columns={'Close': 'nav'}, inplace=True)
    
    # Ensure Date is simple YYYY-MM-DD (remove timezone if any)
    df_final['Date'] = df_final['Date'].dt.date
    
    # Save
    df_final.to_csv(FILE_PATH, index=False)
    print(f"Data saved to {FILE_PATH}")
    print(f"Total Records: {len(df_final)}")
    print(df_final.tail())

if __name__ == "__main__":
    download_nifty_data()
