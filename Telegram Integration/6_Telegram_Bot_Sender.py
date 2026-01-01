import os
import sys
import json
import asyncio
import csv
from datetime import datetime
from telegram import Bot
from telegram.error import TelegramError

# Set paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CREDENTIALS_FILE = os.path.join(SCRIPT_DIR, "telegram_credentials.json")
# Report file is now the Signals CSV
REPORT_FILE = os.path.join(SCRIPT_DIR, "..", "Script RSI Calculation", "Script_RSI_Strategy_Signals.csv")
SHEET_URL = "https://docs.google.com/spreadsheets/d/1iTBqcMFPvJJIW0_nxErisK7GUbVnhJ23guAsmRiOCcY/edit"

def load_credentials():
    """Load bot token and chat ID(s) from JSON file."""
    if not os.path.exists(CREDENTIALS_FILE):
        print(f"Error: Credentials file not found at {CREDENTIALS_FILE}")
        return None, []
    
    try:
        with open(CREDENTIALS_FILE, "r") as f:
            creds = json.load(f)
            chat_id_data = creds.get("chat_id")
            
            # Normalize to list to support multiple groups
            if isinstance(chat_id_data, list):
                chat_ids = chat_id_data
            elif chat_id_data:
                chat_ids = [str(chat_id_data)]
            else:
                chat_ids = []
                
            return creds.get("bot_token"), chat_ids
    except Exception as e:
        print(f"[ERROR] Reading credentials: {e}")
        return None, []

def format_table(headers, rows):
    """Formats data into a simple aligned text table."""
    if not rows:
        return "No NEW signals found for today."

    # Calculate column widths
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, val in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(val)))

    # Create format string (e.g., "{:<10}  {:<10} ...")
    fmt = "  ".join([f"{{:<{w}}}" for w in col_widths])

    lines = []
    # Header
    lines.append(fmt.format(*headers))
    lines.append("-" * (sum(col_widths) + 2 * (len(col_widths) - 1)))
    
    # Rows
    for row in rows:
        lines.append(fmt.format(*row))
    
    return "\n".join(lines)

async def send_report():
    """Send the RSI report via Telegram. Returns True if successful, False otherwise."""
    bot_token, chat_ids = load_credentials()
    
    if not bot_token or not chat_ids:
        print("[ERROR] Invalid or missing credentials. Check telegram_credentials.json")
        return False

    if not os.path.exists(REPORT_FILE):
        print(f"[ERROR] Report file not found at {REPORT_FILE}")
        return False

    all_sent = True
    try:
        bot = Bot(token=bot_token)
        today_str = datetime.now().strftime("%Y-%m-%d")
        
        # Read CSV Data
        data_rows = []
        headers = ["Strategy", "Symbol", "Close", "Daily", "Wkly", "Mnthly"] # Shortened headers for mobile view
        signal_date = None

        with open(REPORT_FILE, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Capture signal date from the first available row
                if not signal_date:
                    signal_date = row.get('Signal_Date', 'Unknown')

                # Format numbers to 2 decimal places
                try:
                    close = f"{float(row.get('Close', 0)):.2f}"
                    d_rsi = f"{float(row.get('Daily_RSI', 0)):.2f}"
                    w_rsi = f"{float(row.get('Weekly_RSI', 0)):.2f}"
                    m_rsi = f"{float(row.get('Monthly_RSI', 0)):.2f}"
                except ValueError:
                    close = row.get('Close', '')
                    d_rsi = row.get('Daily_RSI', '')
                    w_rsi = row.get('Weekly_RSI', '')
                    m_rsi = row.get('Monthly_RSI', '')

                data_rows.append([
                    row.get('Strategy', ''),
                    row.get('Symbol', ''),
                    close,
                    d_rsi,
                    w_rsi,
                    m_rsi
                ])

        MAIN_REPORT_FILE = os.path.join(SCRIPT_DIR, "..", "Script RSI Calculation", "Script_RSI_Report_Adjusted.csv")
        
        # Sort by Strategy (Asc) and Symbol (Asc)
        data_rows.sort(key=lambda x: (x[0], x[1]))

        # Format Message
        # If no signal date found (empty signals), try to get it from the MAIN REPORT
        if not signal_date:
            try:
                if os.path.exists(MAIN_REPORT_FILE):
                    with open(MAIN_REPORT_FILE, "r") as mf:
                        mReader = csv.DictReader(mf)
                        for mRow in mReader:
                            # Typically 'Last_Date' or 'Date'
                            if 'Last_Date' in mRow:
                                signal_date = mRow['Last_Date']
                                break
                            elif 'Date' in mRow:
                                signal_date = mRow['Date']
                                break
            except Exception as e:
                print(f"[WARN] Could not read date from main report: {e}")

        # Final Fallback
        if not signal_date:
            signal_date = today_str

        table_text = format_table(headers, data_rows)
        message = f"🚀 *Daily Script RSI Signals*\n📅 Signal Date: {signal_date}\n\n```\n{table_text}\n```\n\n📊 [View Full Sheet & Paper Trade Data]({SHEET_URL})"

        # Send to all Chat IDs
        for chat_id in chat_ids:
            print(f"Sending signal report to Chat ID: {chat_id}...")
            try:
                await bot.send_message(
                    chat_id=chat_id,
                    text=message,
                    parse_mode="Markdown",
                    read_timeout=60,
                    write_timeout=60,
                    connect_timeout=60
                )
                print(f"[SUCCESS] Signals sent to {chat_id}!")
            except TelegramError as e:
                print(f"[ERROR] Failed to send to {chat_id}: {e}")
                all_sent = False
            except Exception as e:
                print(f"[ERROR] Unexpected error for {chat_id}: {e}")
                all_sent = False
        
        return all_sent
        
    except Exception as e:
        print(f"[ERROR] An unexpected global error occurred: {e}")
        return False

if __name__ == "__main__":
    success = asyncio.run(send_report())
    if success:
        sys.exit(0)
    else:
        sys.exit(1)
