import os
import sys
# Set console encoding to UTF-8 to support ALL Emojis and special characters
if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

import paper_config
import pandas as pd
import numpy as np
import json
import asyncio
from telegram import Bot

from datetime import datetime, date
import xlsxwriter
import gspread
from google.oauth2.service_account import Credentials

# ==========================================
# Paper Trading Manager
# ==========================================
# This script manages the paper trading book by:
# 1. scanning for new entries based on RSI Logic.
# 2. updating open positions based on Exit Logic.
# 3. updating the 'paper_trade_book.csv' file.

# --- Configuration ---
# Constants are loaded from paper_config.py
# CAPITAL_PER_TRADE = paper_config.PER_TRADE_CAPITAL
# INITIAL_STOP_LOSS = paper_config.INITIAL_SL_PCT

# Paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
PAPER_BOOK_PATH = os.path.join(SCRIPT_DIR, "paper_trade_book.csv")
PAPER_BOOK_XLSX_PATH = os.path.join(SCRIPT_DIR, "paper_trade_book.xlsx")
DATA_DIR = os.path.join(PROJECT_ROOT, "NSE Bhavcopy", "NSE_Bhavcopy_Adjusted_Data")
MASTER_LIST_PATH = os.path.join(PROJECT_ROOT, "NSE Bhavcopy", "0_Script_Master_List.csv")
SIGNALS_PATH = os.path.join(PROJECT_ROOT, "Script RSI Calculation", "Script_RSI_Strategy_Signals.csv")
SERVICE_ACCOUNT_FILE = os.path.join(PROJECT_ROOT, "Script RSI Calculation", "service_account.json")
TELEGRAM_CREDS_FILE = os.path.join(PROJECT_ROOT, "Telegram Integration", "telegram_credentials.json")
GOOGLE_SHEET_NAME = "Script RSI Tracker"

def send_telegram_alert(message):
    """Sends a Telegram alert using credentials from the integration folder."""
    if not os.path.exists(TELEGRAM_CREDS_FILE):
        print(f"[WARN] Telegram credentials not found at {TELEGRAM_CREDS_FILE}")
        return

    try:
        with open(TELEGRAM_CREDS_FILE, "r") as f:
            creds = json.load(f)
            bot_token = creds.get("bot_token")
            # Support list or single string for chat_id
            chat_id_data = creds.get("chat_id")
            if isinstance(chat_id_data, list):
                chat_ids = chat_id_data
            elif chat_id_data:
                chat_ids = [str(chat_id_data)]
            else:
                chat_ids = []

        if not bot_token or not chat_ids:
            print("[WARN] Telegram credentials incomplete.")
            return

        async def _send():
            bot = Bot(token=bot_token)
            for chat_id in chat_ids:
                try:
                    await bot.send_message(chat_id=chat_id, text=message, parse_mode="Markdown")
                except Exception as e:
                    print(f"[ERROR] Failed to send Telegram to {chat_id}: {e}")

        # python-telegram-bot's Bot is async. We run it in a short loop.
        asyncio.run(_send())
        print("[INFO] Telegram Alert Sent.")

    except Exception as e:
        print(f"[ERROR] Failed to trigger Telegram Alert: {e}")

def calculate_rsi(series, window=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).fillna(0)
    loss = (-delta.where(delta < 0, 0)).fillna(0)
    avg_gain = gain.rolling(window=window, min_periods=window).mean()
    avg_loss = loss.rolling(window=window, min_periods=window).mean()
    for i in range(window, len(series)):
        avg_gain.iloc[i] = (avg_gain.iloc[i-1] * 13 + gain.iloc[i]) / 14
        avg_loss.iloc[i] = (avg_loss.iloc[i-1] * 13 + loss.iloc[i]) / 14
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def load_symbol_data(symbol):
    file_path = os.path.join(DATA_DIR, f"{symbol}.csv")
    if not os.path.exists(file_path):
        return pd.DataFrame()
    
    try:
        df = pd.read_csv(file_path)
        rename_map = {
            'date': 'TIMESTAMP', 'open_price': 'OPEN',
            'high_price': 'HIGH', 'low_price': 'LOW', 'close_price': 'CLOSE'
        }
        df.rename(columns=rename_map, inplace=True)
        df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
        df = df.sort_values('TIMESTAMP')
        df.set_index('TIMESTAMP', inplace=True)
        
        # Calculations
        df['Daily_RSI'] = calculate_rsi(df['CLOSE'])
        
        weekly_df = df['CLOSE'].resample('W-FRI').last()
        weekly_rsi = calculate_rsi(weekly_df)
        df['Weekly_RSI'] = df.index.map(lambda d: weekly_rsi.asof(d))
        
        monthly_df = df['CLOSE'].resample('ME').last()
        monthly_rsi = calculate_rsi(monthly_df)
        df['Monthly_RSI'] = df.index.map(lambda d: monthly_rsi.asof(d))
        
        # Trailing Base: Lowest Close of last 3 days (Shift 1 to use PAST 3 days relative to today)
        df['Trailing_Base'] = df['CLOSE'].shift(1).rolling(window=3).min()
        
        return df
    except Exception as e:
        print(f"Error loading {symbol}: {e}")
        return pd.DataFrame()

def load_paper_book():
    if not os.path.exists(PAPER_BOOK_PATH):
        # Create empty with headers
        columns = [
            'Buy Date', 'Symbol', 'Strategy', 'Status', 'Stage', 
            'Entry_Price', 'Initial_Quantity', 'Current_Quantity', 
            'Investment_Amount', 'Current_LTP', 'Sell Date', 
            'SL_Price', 'Exit_Price', 'Exit_Reason', 
            'Realized_PnL', 'Unrealized_PnL', 'Total_PnL', 'PnL_Percentage'
        ]
        df = pd.DataFrame(columns=columns)
        df.to_csv(PAPER_BOOK_PATH, index=False)
        return df
    df = pd.read_csv(PAPER_BOOK_PATH)
    # Enforce Float for numeric columns to prevent dtype warnings
    float_cols = ['Entry_Price', 'Current_LTP', 'SL_Price', 'Exit_Price', 
                  'Realized_PnL', 'Unrealized_PnL', 'Total_PnL', 'PnL_Percentage',
                  'Investment_Amount']
    for col in float_cols:
        if col in df.columns:
            df[col] = df[col].astype(float)
    return df

def save_paper_book(df):
    df.to_csv(PAPER_BOOK_PATH, index=False)
    print(f"Updated Paper Book: {PAPER_BOOK_PATH}")

def save_formatted_excel(df):
    """Saves the dataframe to Excel with formatting (Auto-fit, Filters)."""
    try:
        with pd.ExcelWriter(PAPER_BOOK_XLSX_PATH, engine='xlsxwriter') as writer:
            sheet_name = 'TradeBook'
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            workbook = writer.book
            worksheet = writer.sheets[sheet_name]
            
            # Formats
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'top',
                'fg_color': '#D7E4BC',
                'border': 1
            })
            
            # Iterate columns to set width
            for i, col in enumerate(df.columns):
                # Find max length of data or header
                # Convert to string, measure length, take max
                max_len = max(
                    df[col].astype(str).map(len).max(),
                    len(str(col))
                )
                # Cap max width to prevent huge columns
                if pd.isna(max_len): max_len = 10
                final_len = min(max_len + 2, 50) # Add padding, cap at 50
                
                worksheet.set_column(i, i, final_len)
                
                # Apply header format
                worksheet.write(0, i, col, header_format)
                
            # Add AutoFilter
            (max_row, max_col) = df.shape
            worksheet.autofilter(0, 0, max_row, max_col - 1)
            
            # Freeze Top Row
            worksheet.freeze_panes(1, 0)
            
        print(f"Saved Formatted Excel: {PAPER_BOOK_XLSX_PATH}")
        
    except Exception as e:
        print(f"Error saving formatted Excel: {e}")

def update_google_sheet(df):
    """Updates the Google Sheet tab 'Paper Trade Book'."""
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        print(f"[WARN] Service Account file not found: {SERVICE_ACCOUNT_FILE}")
        return

    print("\nUpdating Google Sheet...")
    try:
        # Authenticate
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
        client = gspread.authorize(creds)
        
        # Open Spreadsheet
        try:
            spreadsheet = client.open(GOOGLE_SHEET_NAME)
        except gspread.SpreadsheetNotFound:
            print(f"[WARN] Spreadsheet '{GOOGLE_SHEET_NAME}' not found. Please run Script 5 first to create it.")
            return

        title = "Paper Trade Book"
        try:
            worksheet = spreadsheet.worksheet(title)
            try:
                worksheet.update_index(1) # Ensure it is index 1
            except Exception: pass
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=title, rows=100, cols=20, index=1)
        
        worksheet.clear()
        
        # Prepare Data
        # Convert all NaNs to empty string or 0 for JSON serialization
        df_clean = df.fillna("")
        # Convert date objects to string
        for col in df_clean.columns:
            if df_clean[col].dtype == 'object':
                 df_clean[col] = df_clean[col].astype(str)
        
        headers = list(df_clean.columns)
        data_rows = df_clean.values.tolist()
        all_data = [headers] + data_rows
        
        # Update
        worksheet.update(range_name='A1', values=all_data)
        
        # Formatting (Basic)
        # Header Blue
        worksheet.format('A1:Z1', {
            "backgroundColor": {"red": 0.2, "green": 0.4, "blue": 0.8},
            "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}}
        })
        
        # Freeze Top Row
        try:
            worksheet.freeze(rows=1)
        except Exception as e:
            print(f"[WARN] Could not freeze top row: {e}")
        
        print(f"[SUCCESS] Updated Google Sheet tab: {title}")
        
    except Exception as e:
        print(f"[ERROR] Failed to update Google Sheet: {e}")

def get_latest_price(symbol, df_data):
    if df_data.empty: return None
    return df_data.iloc[-1]

def process_updates():
    print("Loading Paper Book...")
    book_df = load_paper_book()
    
    # Ensure Date columns are datetime objects for comparison (parsing can be tricky with string dates)
    # We will treat them as strings for storage but parse for logic if needed.
    
    # Load Master List for scanning new entries
    master_df = pd.read_csv(MASTER_LIST_PATH)
    all_symbols = master_df['Symbol'].dropna().unique().tolist()
    
    print(f"Total Symbols to scan: {len(all_symbols)}")
    
    # 1. Process OPEN Trades
    # -----------------------
    open_indices = book_df[book_df['Status'] == 'OPEN'].index
    
    print(f"Processing {len(open_indices)} open positions...")
    
    for idx in open_indices:
        row = book_df.loc[idx]
        symbol = row['Symbol']
        
        data_df = load_symbol_data(symbol)
        if data_df.empty: continue
        
        last_row = data_df.iloc[-1]
        current_date = last_row.name.date()
        
        # Prices
        high = last_row['HIGH']
        low = last_row['LOW']
        close = last_row['CLOSE']
        open_p = last_row['OPEN']
        
        # Read Trade State
        buy_price = float(row['Entry_Price'])
        sl_price = float(row['SL_Price']) if pd.notna(row['SL_Price']) else buy_price * 0.95
        stage = int(row['Stage'])
        initial_qty = int(row['Initial_Quantity'])
        current_qty = int(row['Current_Quantity'])
        
        # --- Exit Logic Checks ---
        
        # Check Targets (Based on High)
        target_8 = buy_price * (1 + paper_config.TARGET_1_PCT)
        target_10 = buy_price * (1 + paper_config.TARGET_2_PCT)
        target_15 = buy_price * (1 + paper_config.TARGET_3_PCT)
        
        new_stage = stage
        exit_triggered = False
        exit_reason = ""
        exit_price_exec = 0
        qty_to_sell = 0
        
        # Logic: We update SL and execute partials sequentially
        
        # Target 1: 8%
        if stage < 1 and high >= target_8:
            sl_price = max(sl_price, buy_price) # Move SL to Cost
            new_stage = 1
            print(f"[{symbol}] Target 8% Reached. SL moved to Breakeven.")
            # Alert for Stage 1
            msg = f"🚀 *PROFIT ALERT: Stage 1 Reached*\n\n📈 Symbol: {symbol}\n🎯 Level: 8% Target\n🛡️ Action: SL Moved to Breakeven\n💰 Current Price: {close}"
            send_telegram_alert(msg)
            
        # Target 2: 10%
        if new_stage < 2 and high >= target_10:
            # Partial Exit 50%
            qty_to_sell = int(initial_qty * 0.50)
            if qty_to_sell > 0 and current_qty >= qty_to_sell:
                exit_triggered = True
                exit_reason = "Target 10% Hit"
                exit_price_exec = target_10
                
            sl_price = max(sl_price, buy_price * 1.05) # Move SL to 5%
            new_stage = 2
            print(f"[{symbol}] Target 10% Hit. Partial Profit. SL -> 5%.")
            
            booked_pnl = (target_10 - buy_price) * qty_to_sell
            msg = f"💰 *PROFIT ALERT: Target 10% Hit*\n\n🚀 Symbol: {symbol}\n📦 Sold Qty: {qty_to_sell}\n💵 Price: {target_10:.2f}\n💰 PnL Booked: ₹{booked_pnl:.2f}\n🛡️ New SL: +5%"
            send_telegram_alert(msg)

        # Target 3: 15%
        if new_stage < 3 and high >= target_15:
            # Partial Exit next 25%
            qty_next = int(initial_qty * 0.25)
            # If we just triggered 10%, we handle that first. 
            # Ideally if high hits 15%, it hit 10% too.
            # In daily simulation, we can assume both happened if candle covers it.
            # Complex to handle both in one pass in this simple loop structure.
            # Let's execute the highest priority trigger or just one per day.
            # If 10% triggered this run, we wait next run for 15% unless we loop.
            # For simplicity: If 10% triggered, we stop check. Paper trade will likely catch 15% next day or if re-run.
            if not exit_triggered:
                qty_to_sell = qty_next
                if qty_to_sell > 0 and current_qty >= qty_to_sell:
                    exit_triggered = True
                    exit_reason = "Target 15% Hit"
                    exit_price_exec = target_15
                
                sl_price = max(sl_price, buy_price * 1.10) # SL to 10%
                new_stage = 3
                print(f"[{symbol}] Target 15% Hit. Partial Profit. SL -> 10%.")

                booked_pnl = (target_15 - buy_price) * qty_to_sell
                msg = f"💰 *PROFIT ALERT: Target 15% Hit*\n\n🚀 Symbol: {symbol}\n📦 Sold Qty: {qty_to_sell}\n💵 Price: {target_15:.2f}\n💰 PnL Booked: ₹{booked_pnl:.2f}\n🛡️ New SL: +10%"
                send_telegram_alert(msg)

        # Trailing SL Trigger (Stage 3+)
        if new_stage >= 3:
            trailing_base = last_row['Trailing_Base']
            if pd.notna(trailing_base):
                sl_price = max(sl_price, trailing_base)

        # Update State in DF (SL and Stage)
        book_df.at[idx, 'SL_Price'] = round(sl_price, 2)
        book_df.at[idx, 'Stage'] = new_stage
        
        # --- Execution of Exits ---
        
        # 1. Stop Loss Check (Overrides Targets if Hit on LOW)
        # Note: If high hit target, we assume target filled first?
        # Conservative: If Low <= SL, we are OUT.
        
        sl_hit = False
        if low <= sl_price:
             # Check Gap Down
            if open_p <= sl_price:
                exit_price_exec = open_p
                exit_reason = "SL Hit (Gap)"
            else:
                exit_price_exec = sl_price
                exit_reason = "SL Hit"
            
            exit_triggered = True
            qty_to_sell = current_qty # Sell ALL remaining
            sl_hit = True
            print(f"[{symbol}] STOP LOSS HIT at {exit_price_exec}")

            # Calculate PnL
            pnl = (exit_price_exec - buy_price) * qty_to_sell
            
            msg = f"🚨 *EXIT ALERT: STOP LOSS HIT*\n\n📉 Symbol: {symbol}\n💰 Exit Price: {exit_price_exec}\n📉 PnL Realized: ₹{pnl:.2f}\n📝 Reason: {exit_reason}"
            send_telegram_alert(msg)

        # 2. Process Trade
        if exit_triggered:
            # Calculate PnL for this chunk
            pnl = (exit_price_exec - buy_price) * qty_to_sell
            
            # If Partial
            if not sl_hit and qty_to_sell < current_qty:
                # We need to record this partial exit. 
                # The CSV format is one row per trade entry usually.
                # To record partials, we might need to split rows or just update Realized PnL.
                # The user format has "Realized_PnL". We can accumulate there.
                
                book_df.at[idx, 'Realized_PnL'] = book_df.at[idx, 'Realized_PnL'] + pnl
                book_df.at[idx, 'Current_Quantity'] = current_qty - qty_to_sell
                book_df.at[idx, 'Measurement_Date'] = str(pd.Timestamp.now().date()) # Optional Log
                
                # We don't close the row, just update
                print(f"   -> Sold {qty_to_sell}. Remaining: {current_qty - qty_to_sell}")
                
            else:
                # Full Exit
                book_df.at[idx, 'Realized_PnL'] = book_df.at[idx, 'Realized_PnL'] + pnl
                book_df.at[idx, 'Current_Quantity'] = 0
                book_df.at[idx, 'Status'] = 'CLOSED'
                book_df.at[idx, 'Sell Date'] = str(current_date)
                book_df.at[idx, 'Exit_Price'] = exit_price_exec
                book_df.at[idx, 'Exit_Reason'] = exit_reason
                print(f"   -> Trade CLOSED.")

        # 3. Update Mark-to-Market (Unrealized) for remaining qty
        rem_qty = book_df.at[idx, 'Current_Quantity']
        if rem_qty > 0:
            curr_val = rem_qty * close
            invested_val = rem_qty * buy_price
            unrealized = curr_val - invested_val
            
            book_df.at[idx, 'Current_LTP'] = close
            book_df.at[idx, 'Unrealized_PnL'] = round(unrealized, 2)
            book_df.at[idx, 'Total_PnL'] = round(book_df.at[idx, 'Realized_PnL'] + unrealized, 2)
            
            # PnL % on Original Investment Amount (Total)
            # Or PnL % on remaining? Usually on Total Investment.
            orig_inv = float(row['Investment_Amount'])
            if orig_inv > 0:
                book_df.at[idx, 'PnL_Percentage'] = round((book_df.at[idx, 'Total_PnL'] / orig_inv) * 100, 2)
        else:
            # Closed Trade PnL
            book_df.at[idx, 'Unrealized_PnL'] = 0
            book_df.at[idx, 'Total_PnL'] = book_df.at[idx, 'Realized_PnL']
            orig_inv = float(row['Investment_Amount'])
            if orig_inv > 0:
                book_df.at[idx, 'PnL_Percentage'] = round((book_df.at[idx, 'Total_PnL'] / orig_inv) * 100, 2)

    # 2. Scan For New Trades (From Signals CSV)
    # -----------------------
    print("\nScanning for New Entries from Signals File...")
    
    signals_df = pd.DataFrame()
    if os.path.exists(SIGNALS_PATH):
        try:
             signals_df = pd.read_csv(SIGNALS_PATH)
        except Exception as e:
            print(f"Error reading signals file: {e}")
    else:
        print(f"Signals file not found at: {SIGNALS_PATH}")

    if not signals_df.empty:
        # Get list of currently open symbols to avoid duplicates
        open_symbols = book_df[book_df['Status'] == 'OPEN']['Symbol'].unique().tolist()
        
        new_trades = []
        
        for idx, row in signals_df.iterrows():
            symbol = row['Symbol']
            
            # Skip if already open
            if symbol in open_symbols:
                continue
            
            # Skip if already added in this batch
            if symbol in [t['Symbol'] for t in new_trades]:
                continue
                
            strategy = row['Strategy']
            close_price = float(row['Close'])
            signal_date = row['Signal_Date']
            
            # Validate Date? 
            # If we want to strictly add TODAY's signals, we check signal_date.
            # But usually the signals CSV contains the LATEST signals. 
            # We assume if it's in the file, we want to take it (unless it's old?).
            # For now, we take all signals present in the file that aren't already open.
            
            # Parse Date to standard YYYY-MM-DD format
            try:
                dt_obj = pd.to_datetime(signal_date)
                formatted_date = dt_obj.strftime('%Y-%m-%d')
            except:
                formatted_date = str(signal_date)

            qty = int(paper_config.PER_TRADE_CAPITAL / close_price)
            if qty > 0:
                print(f"[SIGNAL] Found {strategy} for {symbol} at {close_price} on {formatted_date}")
                new_trade = {
                    'Buy Date': formatted_date,
                    'Symbol': symbol,
                    'Strategy': strategy,
                    'Status': 'OPEN',
                    'Stage': 0,
                    'Entry_Price': close_price,
                    'Initial_Quantity': qty,
                    'Current_Quantity': qty,
                    'Investment_Amount': qty * close_price,
                    'Current_LTP': close_price,
                    'Sell Date': '',
                    'SL_Price': round(close_price * (1 - paper_config.INITIAL_SL_PCT), 2),
                    'Exit_Price': 0,
                    'Exit_Reason': '',
                    'Realized_PnL': 0,
                    'Unrealized_PnL': 0,
                    'Total_PnL': 0,
                    'PnL_Percentage': 0
                }
                new_trades.append(new_trade)

        if new_trades:
            print(f"Adding {len(new_trades)} new trades to book.")
            new_df = pd.DataFrame(new_trades)
            # Ensure dtypes match for concat
            new_df['Entry_Price'] = new_df['Entry_Price'].astype(float)
            new_df['Investment_Amount'] = new_df['Investment_Amount'].astype(float)
            new_df['Current_LTP'] = new_df['Current_LTP'].astype(float)
            
            book_df = pd.concat([book_df, new_df], ignore_index=True)
            
        else:
            print("No new unique trades found in signals file.")

    else:
        print("Signals dataframe is empty or invalid.")

    # Always save to capture updates to existing trades (Exits, PnL)
    save_paper_book(book_df)
    save_formatted_excel(book_df)
    update_google_sheet(book_df)
    print("\nPaper Trade Book Updated Successfully (Local & Cloud).")

if __name__ == "__main__":
    process_updates()
