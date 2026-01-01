import pandas as pd
import logging
import datetime
from io import StringIO
import gspread
from google.oauth2.service_account import Credentials
from gcs_handler import GCSHandler

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CloudRSICalculator:
    def __init__(self):
        self.gcs = GCSHandler()
        self.adjusted_prefix = "adjusted_data"
        self.master_list_blob = "config/0_Script_Master_List.csv"
        self.output_signals = "output/Script_RSI_Strategy_Signals.csv"
        self.output_report = "output/Script_RSI_Report_Adjusted.csv"
        
        # Google Sheets Config
        self.service_account_file = "service_account.json" # Expected in same dir
        self.sheet_name = "Script RSI Tracker"
        self.enable_sheets = True

    def load_master_symbols(self):
        if self.gcs.file_exists(self.master_list_blob):
            df = self.gcs.read_csv(self.master_list_blob)
            return set(df['Symbol'].dropna().unique())
        return set()

    def calculate_rsi_fast(self, prices, window=14):
        delta = prices.diff()
        gain = delta.where(delta > 0, 0.0)
        loss = -delta.where(delta < 0, 0.0)
        
        avg_gain = gain.rolling(window=window, min_periods=window).mean()
        avg_loss = loss.rolling(window=window, min_periods=window).mean()
        
        # Wilder's Smoothing
        # Recursion is slow in pandas apply, using loop for now is acceptable for small window
        # Or effectively:
        for i in range(window, len(prices)):
            avg_gain.iloc[i] = (avg_gain.iloc[i-1] * 13 + gain.iloc[i]) / 14
            avg_loss.iloc[i] = (avg_loss.iloc[i-1] * 13 + loss.iloc[i]) / 14
            
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    def process_symbol(self, symbol, df):
        try:
             if 'date' not in df.columns or 'close_price' not in df.columns: return None, []
             
             df['date'] = pd.to_datetime(df['date'])
             df = df.sort_values('date').set_index('date')
             
             if len(df) < 20: return None, []

             # Calculations
             daily_series = df['close_price']
             daily_rsi = self.calculate_rsi_fast(daily_series)
             
             weekly_series = df['close_price'].resample('W-FRI').last().dropna()
             weekly_rsi = self.calculate_rsi_fast(weekly_series)
             
             # Month End 'ME' is pandas 2.1+, use 'M' if on older but 'ME' is future proof
             # Let's try ME, fallback to M
             try:
                 monthly_series = df['close_price'].resample('ME').last().dropna()
             except:
                 monthly_series = df['close_price'].resample('M').last().dropna()
                 
             monthly_rsi = self.calculate_rsi_fast(monthly_series)

             # Latest
             cur_d = daily_rsi.iloc[-1] if not daily_rsi.isna().iloc[-1] else 0
             cur_w = weekly_rsi.iloc[-1] if not weekly_rsi.isna().iloc[-1] else 0
             cur_m = monthly_rsi.iloc[-1] if not monthly_rsi.isna().iloc[-1] else 0
             close = daily_series.iloc[-1]
             last_date = df.index[-1]

             # Strategy
             matched = []
             # GFS: D[35-45], W[55-65], M[55-65]
             if (35 <= cur_d <= 45) and (55 <= cur_w <= 65) and (55 <= cur_m <= 65): matched.append("GFS")
             # AGFS: D[55-65]...
             if (55 <= cur_d <= 65) and (55 <= cur_w <= 65) and (55 <= cur_m <= 65): matched.append("AGFS")
             # Value Buy: D[35-45]...
             if (35 <= cur_d <= 45) and (35 <= cur_w <= 45) and (35 <= cur_m <= 45): matched.append("Value Buy")

             result = {
                 'Symbol': symbol, 'Close': close,
                 'Daily_RSI': cur_d, 'Weekly_RSI': cur_w, 'Monthly_RSI': cur_m,
                 'Last_Date': last_date
             }
             
             signals = []
             for s in matched:
                 signals.append({
                     'Strategy': s, 'Symbol': symbol,
                     'Close': close, 'Daily_RSI': cur_d, 'Weekly_RSI': cur_w, 'Monthly_RSI': cur_m,
                     'Signal_Date': last_date
                 })
                 
             return result, signals
             
        except Exception as e:
            logging.error(f"Error {symbol}: {e}")
            return None, []

    def update_google_sheets(self, df_main, df_signals, last_date):
        logging.info("Updating Google Sheets...")
        try:
            scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
            creds = Credentials.from_service_account_file(self.service_account_file, scopes=scopes)
            client = gspread.authorize(creds)
            
            try:
                sheet = client.open(self.sheet_name)
            except:
                logging.warning("Sheet not found or permission error.")
                return

            # Helper
            def update_tab(tab_name, df, is_main=False):
                try:
                    try: ws = sheet.worksheet(tab_name)
                    except: ws = sheet.add_worksheet(tab_name, 100, 20)
                    ws.clear()
                    
                    # Convert dates/timestamps to string for JSON serialization
                    df_str = df.astype(str)
                    data = [df_str.columns.tolist()] + df_str.values.tolist()
                    
                    if is_main:
                         # Add metadata
                         meta = [["Script RSI Report"], [f"Last Date: {last_date}"], [""]]
                         ws.update('A1', meta + data)
                    else:
                         ws.update('A1', data)
                    logging.info(f"Updated {tab_name}")
                except Exception as ex:
                    logging.error(f"Tab {tab_name} error: {ex}")

            # Update Main
            update_tab("RSI Data", df_main, is_main=True)
            
            # Update Today's Signals
            today_str = datetime.datetime.now().strftime('%Y-%m-%d')
            if not df_signals.empty:
                df_signals = df_signals.sort_values(['Strategy', 'Symbol'])
                update_tab(today_str, df_signals)
            else:
                try:
                    ws = sheet.worksheet(today_str)
                    ws.clear()
                    ws.update('A1', [["No signals today."]])
                except: pass

        except Exception as e:
            logging.error(f"Google Sheets Error: {e}")

    def run(self):
        symbols = self.load_master_symbols()
        blobs = self.gcs.list_files(prefix=self.adjusted_prefix)
        csv_blobs = [b for b in blobs if b.endswith('.csv')]
        
        logging.info(f"Analyzing {len(csv_blobs)} files...")
        
        results = []
        signals = []
        last_date = "Unknown"
        
        for blob in csv_blobs:
            symbol = blob.split('/')[-1].replace('.csv', '')
            if symbols and symbol not in symbols: continue
            
            df = self.gcs.read_csv(blob)
            if df is not None:
                res, sigs = self.process_symbol(symbol, df)
                if res:
                    results.append(res)
                    last_date = str(res['Last_Date']).split()[0]
                    if sigs: signals.extend(sigs)

        # Save Output to Cloud
        if results:
            df_res = pd.DataFrame(results)
            self.gcs.write_csv(df_res, self.output_report)
            logging.info(f"Saved Report to {self.output_report}")
            
            df_sig = pd.DataFrame(signals) if signals else pd.DataFrame(columns=['Strategy','Symbol','Close'])
            self.gcs.write_csv(df_sig, self.output_signals)
            logging.info(f"Saved Signals to {self.output_signals}")
            
            if self.enable_sheets and GCSHandler().file_exists("service_account.json"): # Check local file existence for gspread
                 # Note: "service_account.json" must be in the container working dir
                 self.update_google_sheets(df_res, df_sig, last_date)
        else:
            logging.warning("No results generated.")

if __name__ == "__main__":
    calc = CloudRSICalculator()
    calc.run()
