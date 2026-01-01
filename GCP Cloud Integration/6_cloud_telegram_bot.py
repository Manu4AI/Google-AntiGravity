import os
import json
import asyncio
import csv
from datetime import datetime
from telegram import Bot
from telegram.error import TelegramError
from gcs_handler import GCSHandler
from io import StringIO

# Set paths
CREDENTIALS_FILE = "telegram_credentials.json"
REPORT_BLOB = "output/Script_RSI_Strategy_Signals.csv"

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
        return "No signals found for today."

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
    """Send the RSI report via Telegram."""
    bot_token, chat_ids = load_credentials()
    
    if not bot_token or not chat_ids:
        print("[ERROR] Invalid or missing credentials. Check telegram_credentials.json")
        return

    gcs = GCSHandler()
    if not gcs.file_exists(REPORT_BLOB):
        print(f"[ERROR] Report file not found in cloud: {REPORT_BLOB}")
        return

    try:
        bot = Bot(token=bot_token)
        today_str = datetime.now().strftime("%Y-%m-%d")
        
        # Read CSV Data from GCS using read_csv -> DataFrame -> DictReader equivalent or just standard usage
        df = gcs.read_csv(REPORT_BLOB)
        
        data_rows = []
        headers = ["Strategy", "Symbol", "Close", "Daily", "Wkly", "Mnthly"] 
        signal_date = None
        
        # Convert DF to list of dicts for compatibility with existing logic
        records = df.to_dict('records')

        for row in records:
            if not signal_date:
                signal_date = row.get('Signal_Date', 'Unknown')

            # Format numbers
            def fmt_num(v):
                try: return f"{float(v):.2f}"
                except: return str(v)

            close = fmt_num(row.get('Close', 0))
            d_rsi = fmt_num(row.get('Daily_RSI', 0))
            w_rsi = fmt_num(row.get('Weekly_RSI', 0))
            m_rsi = fmt_num(row.get('Monthly_RSI', 0))

            data_rows.append([
                str(row.get('Strategy', '')),
                str(row.get('Symbol', '')),
                close, d_rsi, w_rsi, m_rsi
            ])

        # Sort by Strategy (Asc) and Symbol (Asc)
        data_rows.sort(key=lambda x: (x[0], x[1]))

        if not signal_date or signal_date == "nan":
            signal_date = today_str
        else:
             # Clean up date if it has time
             signal_date = str(signal_date).split()[0]

        table_text = format_table(headers, data_rows)
        message = f"🚀 *Daily Script RSI Signals*\n📅 Signal Date: {signal_date}\n\n```\n{table_text}\n```"

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
            except Exception as e:
                print(f"[ERROR] Unexpected error for {chat_id}: {e}")
        
    except Exception as e:
        print(f"[ERROR] An unexpected global error occurred: {e}")

if __name__ == "__main__":
    asyncio.run(send_report())
