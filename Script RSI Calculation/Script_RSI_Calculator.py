import pandas as pd
import glob
import os
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

def load_master_symbols():
    """Load symbols from the master CSV file."""
    master_path = os.path.join(PARENT_DIR, "NSE Bhavcopy", "0_Script_Master_List.csv")
    if os.path.exists(master_path):
        try:
            df = pd.read_csv(master_path)
            # Filter out any empty strings or non-string values just in case
            symbols = [str(s).strip() for s in df['Symbol'].dropna() if str(s).strip()]
            print(f"Loaded {len(symbols)} symbols from master list.")
            return sorted(list(set(symbols)))
        except Exception as e:
            print(f"Error reading master list: {e}")
            return []
    else:
        print(f"Warning: Master list not found at {master_path}")
        return []

# Load Symbols from Master List
ALL_SYMBOLS = load_master_symbols()

if not ALL_SYMBOLS:
    print("CRITICAL: No symbols loaded. Please check 0_Script_Master_List.csv")
    exit()

# Configuration
# Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(BASE_DIR)

# Auto-detect data directory (it might be in parent or sibling)
# Current structure: Parent/nse_bhavcopy_downloader
DATA_DIR_PATH = os.path.join(PARENT_DIR, "NSE Bhavcopy", "NSE_Bhavcopy_Master_Data")

# If valid, use it, else fallback or use local
if not os.path.exists(DATA_DIR_PATH):
    # Try local 'data' folder
    DATA_DIR_PATH = os.path.join(BASE_DIR, "data")

CONFIG = {
    'data_dir': DATA_DIR_PATH,
    'output_csv': os.path.join(BASE_DIR, "Script RSI Report.csv"),
    'service_account_file': os.path.join(BASE_DIR, "service_account.json"),
    'google_sheet_name': "Script RSI Tracker",
    'enable_google_sheets': True,
    'adjustments_file': os.path.join(BASE_DIR, "script_adjustments.csv")
}

def load_adjustments(adjustments_file):
    """Load corporate action adjustments."""
    if not os.path.exists(adjustments_file):
        return {}
    
    adj_df = pd.read_csv(adjustments_file)
    adj_df['ExDate'] = pd.to_datetime(adj_df['ExDate'])
    
    # Organize by Symbol
    adjustments = {}
    for _, row in adj_df.iterrows():
        symbol = row['Symbol']
        if symbol not in adjustments:
            adjustments[symbol] = []
        adjustments[symbol].append({
            'date': row['ExDate'],
            'factor': float(row['AdjustmentFactor'])
        })
    return adjustments

def load_all_data(data_dir, adjustments_file=None):
    """Load ALL Bhavcopy data and apply normalization."""
    print("Loading Bhavcopy data (2021-2025)...")
    
    files = []
    for year in range(2021, 2026):
        year_path = os.path.join(data_dir, str(year))
        if os.path.exists(year_path):
            files.extend(glob.glob(os.path.join(year_path, f"bhavcopy_{year}*.csv")))
        else:
            print(f"Warning: Data folder for {year} not found.")
    
    files.sort()
    print(f"Found {len(files)} files. Reading...")
    
    cols_to_use = ['SYMBOL', 'SERIES', 'CLOSE_PRICE', 'DATE1']
    dfs = []
    
    for f in files:
        try:
            df = pd.read_csv(f, usecols=lambda c: c in cols_to_use)
            df = df[df['SERIES'] == 'EQ']
            dfs.append(df)
        except:
            pass
    
    full_df = pd.concat(dfs, ignore_index=True)
    full_df.rename(columns={'DATE1': 'TIMESTAMP', 'CLOSE_PRICE': 'CLOSE'}, inplace=True)
    full_df['TIMESTAMP'] = pd.to_datetime(full_df['TIMESTAMP'])
    
    # Apply Adjustments (Normalization)
    if adjustments_file:
        adjustments = load_adjustments(adjustments_file)
        print(f"Applying corporate action adjustments for {len(adjustments)} symbols...")
        
        for symbol, adj_list in adjustments.items():
            # Get data for this symbol
            mask = full_df['SYMBOL'] == symbol
            
            for adj in adj_list:
                # Apply factor to all dates BEFORE the ex-date
                date_mask = mask & (full_df['TIMESTAMP'] < adj['date'])
                full_df.loc[date_mask, 'CLOSE'] = full_df.loc[date_mask, 'CLOSE'] * adj['factor']
                
    print(f"Loaded {len(full_df)} rows. Processing...")
    return full_df

def calculate_rsi_fast(prices, window=14):
    """Fast RSI calculation using Wilder's smoothing."""
    delta = prices.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    
    avg_gain = gain.rolling(window=window, min_periods=window).mean()
    avg_loss = loss.rolling(window=window, min_periods=window).mean()
    
    for i in range(window, len(prices)):
        avg_gain.iloc[i] = (avg_gain.iloc[i-1] * 13 + gain.iloc[i]) / 14
        avg_loss.iloc[i] = (avg_loss.iloc[i-1] * 13 + loss.iloc[i]) / 14
    
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    
    return rsi

def calculate_rsi_for_symbol_fast(symbol_df):
    """Calculate RSI for a single symbol's data."""
    symbol_df = symbol_df.sort_values('TIMESTAMP').set_index('TIMESTAMP')
    
    daily_rsi = calculate_rsi_fast(symbol_df['CLOSE'])
    
    weekly_df = symbol_df.resample('W-FRI').agg({'CLOSE': 'last'}).dropna()
    weekly_rsi = calculate_rsi_fast(weekly_df['CLOSE'])
    
    monthly_df = symbol_df.resample('ME').agg({'CLOSE': 'last'}).dropna()
    monthly_rsi = calculate_rsi_fast(monthly_df['CLOSE'])
    
    return {
        'Close': symbol_df['CLOSE'].iloc[-1],
        'Daily_RSI': daily_rsi.iloc[-1] if not daily_rsi.isna().iloc[-1] else 0,
        'Weekly_RSI': weekly_rsi.iloc[-1] if not weekly_rsi.isna().iloc[-1] else 0,
        'Monthly_RSI': monthly_rsi.iloc[-1] if not monthly_rsi.isna().iloc[-1] else 0
    }

# Old upload function removed. Use update_all_google_sheets instead.

def main():
    print("="*80)
    print("Script RSI Calculator - Production Version with Google Sheets")
    print("="*80)
    
    # Load ALL data once with adjustments
    all_data = load_all_data(CONFIG['data_dir'], CONFIG['adjustments_file'])
    
    # Get last Bhavcopy date
    last_bhavcopy_date = all_data['TIMESTAMP'].max().strftime('%Y-%m-%d')
    print(f"Last Bhavcopy Date: {last_bhavcopy_date}")
    
    # Process each symbol
    results = []
    strategy_matches = []
    
    print(f"\nProcessing {len(ALL_SYMBOLS)} symbols (NIFTY 50 + NIFTY NEXT 50)...")
    
    for i, symbol in enumerate(ALL_SYMBOLS, 1):
        symbol_data = all_data[all_data['SYMBOL'] == symbol]
        
        if len(symbol_data) > 0:
            try:
                result = calculate_rsi_for_symbol_fast(symbol_data)
                
                # --- Strategy Logic ---
                d_rsi = result['Daily_RSI']
                w_rsi = result['Weekly_RSI']
                m_rsi = result['Monthly_RSI']
                
                matched_strategies = []
                
                # GFS Strategy: D[35-45], W[55-65], M[55-65]
                if (35 <= d_rsi <= 45) and (55 <= w_rsi <= 65) and (55 <= m_rsi <= 65):
                    matched_strategies.append("GFS")
                    
                # AGFS Strategy: D[55-65], W[55-65], M[55-65]
                if (55 <= d_rsi <= 65) and (55 <= w_rsi <= 65) and (55 <= m_rsi <= 65):
                    matched_strategies.append("AGFS")
                    
                # Value Buy Strategy: D[35-45], W[35-45], M[35-45]
                if (35 <= d_rsi <= 45) and (35 <= w_rsi <= 45) and (35 <= m_rsi <= 45):
                    matched_strategies.append("Value Buy")
                
                # Add to main results
                row = {
                    'Symbol': symbol,
                    **result,
                    'Last_Updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                results.append(row)
                
                # Add to strategy matches if any found
                for strat in matched_strategies:
                    strategy_matches.append({
                        'Strategy': strat,
                        'Symbol': symbol,
                        'Close': result['Close'],
                        'Daily_RSI': d_rsi,
                        'Weekly_RSI': w_rsi,
                        'Monthly_RSI': m_rsi
                    })
                
                status_msg = f"OK"
                if matched_strategies:
                    status_msg += f" [{', '.join(matched_strategies)}]"
                
                print(f"[{i}/{len(ALL_SYMBOLS)}] {symbol:<15} {status_msg} - D:{d_rsi:.1f} W:{w_rsi:.1f} M:{m_rsi:.1f}")
                
            except Exception as e:
                print(f"[{i}/{len(ALL_SYMBOLS)}] {symbol:<15} ERROR: {str(e)}")
        else:
            print(f"[{i}/{len(ALL_SYMBOLS)}] {symbol:<15} SKIPPED (No data)")
    
    # Create DataFrame
    df_results = pd.DataFrame(results)
    
    # Save to CSV
    df_results.to_csv(CONFIG['output_csv'], index=False)
    print("\n" + "="*80)
    print(f"[SUCCESS] CSV Report saved to: {CONFIG['output_csv']}")
    print(f"[SUCCESS] Total symbols processed: {len(results)}/{len(ALL_SYMBOLS)}")
    
    if strategy_matches:
        print(f"[INFO] Found {len(strategy_matches)} strategy matches.")
    else:
        print("[INFO] No stocks matched the strategy criteria today.")
    
    # Upload to Google Sheets
    if CONFIG['enable_google_sheets']:
        update_all_google_sheets(
            df_results,
            strategy_matches,
            CONFIG['service_account_file'],
            CONFIG['google_sheet_name'],
            last_bhavcopy_date
        )

    print("="*80)
    
    # Display summary
    if len(df_results) > 0:
        print("\nTop 10 by Daily RSI:")
        print(df_results.nlargest(10, 'Daily_RSI')[['Symbol', 'Close', 'Daily_RSI', 'Weekly_RSI', 'Monthly_RSI']].to_string(index=False))
        
        print("\nBottom 10 by Daily RSI:")
        print(df_results.nsmallest(10, 'Daily_RSI')[['Symbol', 'Close', 'Daily_RSI', 'Weekly_RSI', 'Monthly_RSI']].to_string(index=False))

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
        # Main report is always latest summary
        update_worksheet(spreadsheet, "RSI Data", df_main, is_main_report=True, last_date=last_bhavcopy_date)
        
        # 2. Update Strategy Sheet (History)
        execution_date = datetime.now().strftime('%Y-%m-%d')
        
        if strategy_matches:
            df_matches = pd.DataFrame(strategy_matches)
            
            # Sort by Strategy for better readability
            df_matches.sort_values(by=['Strategy', 'Symbol'], inplace=True)
            
            update_worksheet(spreadsheet, execution_date, df_matches, is_main_report=False, strategy_name="Consolidated")
        else:
            try:
                # Create/Update Date Sheet with "No Signals"
                # This confirms the script ran but found nothing
                ws = spreadsheet.worksheet(execution_date)
            except gspread.WorksheetNotFound:
                ws = spreadsheet.add_worksheet(title=execution_date, rows=20, cols=10)
            
            ws.clear()
            ws.update(range_name='A1', values=[["No signals for any strategy today."]])


    except Exception as e:
        print(f"[ERROR] Google Sheets Update Failed: {e}")

def update_worksheet(spreadsheet, title, df, is_main_report=False, last_date=None, strategy_name=None):
    """Helper to update a single worksheet."""
    try:
        try:
            worksheet = spreadsheet.worksheet(title)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=title, rows=100, cols=10)
        
        worksheet.clear()
        
        # Prepare Data
        headers = list(df.columns)
        data_rows = df.values.tolist()
        
        if is_main_report:
            metadata = [
                ["Script RSI Report"],
                [f"Last Bhavcopy Date: {last_date}"],
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

        worksheet.columns_auto_resize(0, len(headers))
        print(f"[SUCCESS] Updated sheet: {title}")
        
    except Exception as e:
        print(f"[ERROR] Failed to update sheet {title}: {e}")



if __name__ == "__main__":
    main()
