import pandas as pd
import os
import glob
from datetime import datetime

# Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(BASE_DIR)
# Path to the Adjusted Data Folder
DATA_DIR = os.path.join(PARENT_DIR, "NSE Bhavcopy", "NSE_Bhavcopy_Adjusted_Data")
OUTPUT_SIGNALS_CSV = os.path.join(BASE_DIR, "Script_RSI_Strategy_Signals.csv")
OUTPUT_REPORT_CSV = os.path.join(BASE_DIR, "Script_RSI_Report_Adjusted.csv")
PAPER_BOOK_PATH = os.path.join(PARENT_DIR, "Paper Trading Simulator", "paper_trade_book.csv")

# Google Sheets Config
CONFIG = {
    'service_account_file': os.path.join(BASE_DIR, "service_account.json"),
    'google_sheet_name': "Script RSI Tracker",
    'enable_google_sheets': True
}

import gspread
from google.oauth2.service_account import Credentials
import sys

def load_master_symbols():
    """Load symbols from the master CSV file."""
    master_path = os.path.join(PARENT_DIR, "NSE Bhavcopy", "0_Script_Master_List.csv")
    if os.path.exists(master_path):
        try:
            df = pd.read_csv(master_path)
            symbols = [str(s).strip() for s in df['Symbol'].dropna() if str(s).strip()]
            print(f"Loaded {len(symbols)} symbols from master list.")
            return set(symbols)
        except Exception as e:
            print(f"Error reading master list: {e}")
            return set()
    else:
        print(f"Warning: Master list not found at {master_path}")
        return set()

def get_open_paper_trades():
    """Returns a set of symbols that are currently OPEN in the paper trade book."""
    if os.path.exists(PAPER_BOOK_PATH):
        try:
            df = pd.read_csv(PAPER_BOOK_PATH)
            # Check if columns exist (file might be empty or just headers)
            if 'Status' in df.columns and 'Symbol' in df.columns:
                open_trades = df[df['Status'] == 'OPEN']['Symbol'].unique().tolist()
                print(f"Loaded {len(open_trades)} open positions/trades from Paper Book.")
                return set(open_trades)
        except Exception as e:
            print(f"Warning: Could not read paper trade book for filtering: {e}")
    return set()

def calculate_rsi_fast(prices, window=14):
    """Fast RSI calculation using Wilder's smoothing."""
    delta = prices.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    
    # First simple average for initial window
    avg_gain = gain.rolling(window=window, min_periods=window).mean()
    avg_loss = loss.rolling(window=window, min_periods=window).mean()
    
    for i in range(window, len(prices)):
        avg_gain.iloc[i] = (avg_gain.iloc[i-1] * 13 + gain.iloc[i]) / 14
        avg_loss.iloc[i] = (avg_loss.iloc[i-1] * 13 + loss.iloc[i]) / 14
    
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    
    return rsi

def process_file(filepath):
    try:
        symbol = os.path.basename(filepath).replace('.csv', '')
        df = pd.read_csv(filepath)
        
        # Ensure correct columns
        if 'date' not in df.columns or 'close_price' not in df.columns:
            return None, []
            
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').set_index('date')
        
        if len(df) < 20: # Need enough data for RSI
            return None, []

        # --- RSI Calculations ---
        # Daily
        daily_series = df['close_price']
        daily_rsi = calculate_rsi_fast(daily_series)
        
        # Weekly
        weekly_series = df['close_price'].resample('W-FRI').last().dropna()
        weekly_rsi = calculate_rsi_fast(weekly_series)
        
        # Monthly
        monthly_series = df['close_price'].resample('ME').last().dropna()
        monthly_rsi = calculate_rsi_fast(monthly_series)
        
        # Get latest values
        current_close = daily_series.iloc[-1]
        cur_d_rsi = daily_rsi.iloc[-1] if not daily_rsi.isna().iloc[-1] else 0
        cur_w_rsi = weekly_rsi.iloc[-1] if not weekly_rsi.isna().iloc[-1] else 0
        cur_m_rsi = monthly_rsi.iloc[-1] if not monthly_rsi.isna().iloc[-1] else 0
        
        last_date = df.index[-1]

        # --- Strategy Logic ---
        matched_strategies = []
        
        # GFS Strategy: D[35-45], W[55-65], M[55-65]
        if (35 <= cur_d_rsi <= 45) and (55 <= cur_w_rsi <= 65) and (55 <= cur_m_rsi <= 65):
            matched_strategies.append("GFS")
            
        # AGFS Strategy: D[55-65], W[55-65], M[55-65]
        if (55 <= cur_d_rsi <= 65) and (55 <= cur_w_rsi <= 65) and (55 <= cur_m_rsi <= 65):
            matched_strategies.append("AGFS")
            
        # Value Buy Strategy: D[35-45], W[35-45], M[35-45]
        if (35 <= cur_d_rsi <= 45) and (35 <= cur_w_rsi <= 45) and (35 <= cur_m_rsi <= 45):
            matched_strategies.append("Value Buy")

        result = {
            'Symbol': symbol,
            'Close': current_close,
            'Daily_RSI': cur_d_rsi,
            'Weekly_RSI': cur_w_rsi,
            'Monthly_RSI': cur_m_rsi,
            'Last_Date': last_date
        }
        
        signals = []
        for stra in matched_strategies:
            signals.append({
                'Strategy': stra,
                'Symbol': symbol,
                'Close': current_close,
                'Daily_RSI': cur_d_rsi,
                'Weekly_RSI': cur_w_rsi,
                'Monthly_RSI': cur_m_rsi,
                'Signal_Date': last_date
            })
            
        return result, signals

    except Exception as e:
        print(f"Error processing {filepath}: {e}")
        return None, []

def update_all_google_sheets(df_main, strategy_matches, service_account_file, sheet_name, last_bhavcopy_date):
    """Updates ALL Google Sheets tabs in a single session for performance."""
    print("\nConnecting to Google Sheets...")
    try:
        # Authenticate ONCE
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_file(service_account_file, scopes=scopes)
        client = gspread.authorize(creds)
        
        # Open Spreadsheet ONCE
        try:
            spreadsheet = client.open(sheet_name)
            print(f"Opened spreadsheet: {sheet_name}")
        except gspread.SpreadsheetNotFound:
            spreadsheet = client.create(sheet_name)
            print(f"Created new spreadsheet: {sheet_name}")
            print(f"URL: {spreadsheet.url}")
            print("IMPORTANT: Share this with your email!")

        # 1. Update Main Report
        # Ensure datatypes for JSON serialization (convert Timestamps to str)
        df_main_serializable = df_main.copy()
        if 'Last_Date' in df_main_serializable.columns:
             df_main_serializable['Last_Date'] = df_main_serializable['Last_Date'].astype(str)
             
        if 'Last_Date' in df_main_serializable.columns:
             df_main_serializable['Last_Date'] = df_main_serializable['Last_Date'].astype(str)
             
        # Main Report -> "RSI Data" (Summary)
        update_worksheet(spreadsheet, "RSI Data", df_main_serializable, is_main_report=True, last_date=last_bhavcopy_date)
        
        # 2. Update Strategy Sheet (History -> Date)
        execution_date = datetime.now().strftime('%Y-%m-%d')
        
        if strategy_matches:
            df_matches = pd.DataFrame(strategy_matches)
            # Serialize dates
            if 'Signal_Date' in df_matches.columns:
                df_matches['Signal_Date'] = df_matches['Signal_Date'].astype(str)
            
            # Sort by Strategy for better readability
            df_matches.sort_values(by=['Strategy', 'Symbol'], inplace=True)
            
            # Pass sheet_index=2 to place it after "Paper Trade Book" (Index 1) which is after Main Report (Index 0)
            update_worksheet(spreadsheet, execution_date, df_matches, is_main_report=False, strategy_name="Consolidated", sheet_index=2)
        else:
            try:
                 # Create/Update Date Sheet with "No Signals"
                ws = spreadsheet.worksheet(execution_date)
            except gspread.WorksheetNotFound:
                ws = spreadsheet.add_worksheet(title=execution_date, rows=20, cols=10)
            
            ws.clear()
            ws.update(range_name='A1', values=[["No signals for any strategy today."]])


    except Exception as e:
        print(f"[ERROR] Google Sheets Update Failed: {e}")

def update_worksheet(spreadsheet, title, df, is_main_report=False, last_date=None, strategy_name=None, sheet_index=None):
    """Helper to update a single worksheet."""
    try:
        try:
            worksheet = spreadsheet.worksheet(title)
            # If sheet exists and index is specified, ensure it's at the correct position
            if sheet_index is not None:
                try:
                    worksheet.update_index(sheet_index)
                except Exception as e:
                    print(f"[WARN] Could not update index for {title}: {e}")
        except gspread.WorksheetNotFound:
            # Create new worksheet at specific index if provided
            if sheet_index is not None:
                worksheet = spreadsheet.add_worksheet(title=title, rows=100, cols=10, index=sheet_index)
            else:
                worksheet = spreadsheet.add_worksheet(title=title, rows=100, cols=10)
        
        worksheet.clear()
        
        # Prepare Data
        headers = list(df.columns)
        data_rows = df.values.tolist()
        
        if is_main_report:
            metadata = [
                ["Script RSI Report"],
                [f"Last Data Date: {last_date}"],
                [f"Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"],
                [""]
            ]
            all_data = metadata + [headers] + data_rows
            start_row = 5
        else:
            all_data = [headers] + data_rows
            start_row = 2

        # Upload Data
        worksheet.update(range_name='A1', values=all_data)
        
        # Helper to get column letter
        def get_col_letter(col_idx):
            """Convert 0-based index to A1 notation"""
            # Handles up to Z (26 columns). 
            # For >26 cols, it needs more logic but we have < 10.
            return chr(65 + col_idx)

        last_col_letter = get_col_letter(len(headers) - 1)

        # Formatting
        if is_main_report:
            # Title
            worksheet.format(f'A1:{last_col_letter}1', {
                "backgroundColor": {"red": 0.1, "green": 0.3, "blue": 0.6},
                "textFormat": {"bold": True, "fontSize": 14, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
                "horizontalAlignment": "CENTER"
            })
            worksheet.merge_cells(f'A1:{last_col_letter}1')
            
            # Metadata
            worksheet.format(f'A2:{last_col_letter}3', {"textFormat": {"italic": True}, "backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.95}})
            
            # Header
            worksheet.format(f'A{start_row}:{last_col_letter}{start_row}', {
                "backgroundColor": {"red": 0.2, "green": 0.4, "blue": 0.8},
                "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
                "horizontalAlignment": "CENTER"
            })
            
            # Numbers
            # Assuming columns: Symbol, Close, Daily_RSI, Weekly, Monthly, Last_Date
            # Close is col B (index 1) if symbol is A
            worksheet.format(f'B{start_row+1}:B{start_row + len(data_rows)}', {"numberFormat": {"type": "CURRENCY", "pattern": "₹#,##,##0.00"}})
            worksheet.format(f'C{start_row+1}:E{start_row + len(data_rows)}', {"numberFormat": {"type": "NUMBER", "pattern": "0.00"}})

        else:
            # consolidated strategy sheet formatting
            # Header (Generic Purple)
            worksheet.format(f'A1:{last_col_letter}1', {
                "backgroundColor": {"red": 0.5, "green": 0.0, "blue": 0.5},
                "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
                "horizontalAlignment": "CENTER"
            })
            
            worksheet.format(f'C2:C{1 + len(data_rows)}', {"numberFormat": {"type": "CURRENCY", "pattern": "₹#,##,##0.00"}})
            worksheet.format(f'D2:F{1 + len(data_rows)}', {"numberFormat": {"type": "NUMBER", "pattern": "0.00"}})

        # Auto Resize & Explicit Expansion
        # Auto-resize first to fit content
        worksheet.columns_auto_resize(0, len(headers))
        
        # Then, if you want them "expanded" (wider than just content), we can set a min width
        # The gspread library's columns_auto_resize is pretty good, but sometimes too tight.
        # Let's ensure a minimum width using batch updates if needed, but auto-resize is usually what people mean.
        # Given "keep the length of the column expanded", I will try to be safe and just stick to auto-resize 
        # BUT ensure it covers all columns (which it does with len(headers)).
        # However, to be extra "expanded", maybe we can set a specific width range.
        # Let's trust auto_resize but ensure it definitely ran for ALL headers.
        
        print(f"[SUCCESS] Updated sheet: {title}")
        
    except Exception as e:
        print(f"[ERROR] Failed to update sheet {title}: {e}")

def main():
    print("="*60)
    print("RSI Calculator (Adjusted Data Source)")
    print(f"Data Source: {DATA_DIR}")
    print("="*60)
    
    if not os.path.exists(DATA_DIR):
        print(f"[ERROR] Data directory not found: {DATA_DIR}")
        return

    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    
    # Filter by Master List
    valid_symbols = load_master_symbols()
    if valid_symbols:
        original_count = len(csv_files)
        csv_files = [f for f in csv_files if os.path.basename(f).replace('.csv', '') in valid_symbols]
        print(f"Filtered to {len(csv_files)} files (from {original_count}) based on Master List.")
    else:
        print(f"Found {len(csv_files)} files. (Master list filter not applied or empty)")

    print(f"Processing {len(csv_files)} files...")
    
    all_results = []
    all_signals = []
    last_date_str = "Unknown"
    
    for i, f in enumerate(csv_files, 1):
        res, sigs = process_file(f)
        if res:
            all_results.append(res)
            # Keep track of latest date for report header
            last_date_str = str(res['Last_Date']).split()[0]
            
            if sigs:
                all_signals.extend(sigs)
                ticker = res['Symbol']
                print(f"[{i}/{len(csv_files)}] {ticker:<15} MATCH: {[s['Strategy'] for s in sigs]}")
        
        if i % 10 == 0:
            print(f"Processed {i}...", end='\r')

    print(f"\nProcessing complete.")
    
    # Save Report
    if all_results:
        df_res = pd.DataFrame(all_results)
        df_res.to_csv(OUTPUT_REPORT_CSV, index=False)
        print(f"[SUCCESS] Report saved: {OUTPUT_REPORT_CSV} ({len(df_res)} rows)")
        
        # Upload to Google Sheets
        if CONFIG['enable_google_sheets']:
            update_all_google_sheets(
                df_res,
                all_signals,
                CONFIG['service_account_file'],
                CONFIG['google_sheet_name'],
                last_date_str
            )
        
    # Save Signals (Filtered for Telegram/CSV)
    # We want GSheets to have ALL signals (history), but CSV (Telegram) to have ONLY NEW liquid signals.
    
    final_signals_for_csv = []
    if all_signals:
        open_positions = get_open_paper_trades()
        for s in all_signals:
            if s['Symbol'] in open_positions:
                # Log but exclude from CSV
                pass 
                # print(f"   [FILTER] Excluding {s['Symbol']} from Alert CSV (Already OPEN)")
            else:
                final_signals_for_csv.append(s)
    
    # Always create the file, even if empty, so Paper Trader knows there are no signals
    if final_signals_for_csv:
        df_sig = pd.DataFrame(final_signals_for_csv)
        df_sig.to_csv(OUTPUT_SIGNALS_CSV, index=False)
        print(f"[SUCCESS] Signals saved (Filtered): {OUTPUT_SIGNALS_CSV} ({len(df_sig)} signals)")
        if len(all_signals) > len(final_signals_for_csv):
             print(f"          (Filtered out {len(all_signals) - len(final_signals_for_csv)} duplicate open trades)")
    else:
        # Empty DataFrame with headers
        pd.DataFrame(columns=['Strategy','Symbol','Close','Daily_RSI','Weekly_RSI','Monthly_RSI','Signal_Date']).to_csv(OUTPUT_SIGNALS_CSV, index=False)
        print(f"[INFO] No new unique signals found. Created empty signals file.")

if __name__ == "__main__":
    main()
