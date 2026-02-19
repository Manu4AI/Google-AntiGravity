import os
import sys
import json
import asyncio
import pandas as pd
from telegram import Bot
from telegram.error import TelegramError

# Set paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(SCRIPT_DIR, "Sensibull_Data.csv")
CREDENTIALS_FILE = os.path.join(SCRIPT_DIR, "..", "Telegram Integration", "telegram_credentials.json")

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

def format_table(df):
    """Formats DataFrame into a aligned text table string."""
    if df.empty:
        return "No stocks found with IVP >= 80."

    # Define columns and their specific display widths for better alignment
    # Stock (14), Fut Price (22), IVP (6), Result (10)
    header = f"{'Stock':<14} {'Fut Price':<22} {'IVP':<8} {'Result':<10}"
    separator = "-" * len(header)
    
    rows = []
    for _, row in df.iterrows():
        stock = str(row['Stock'])
        price = str(row['Fut Price'])
        
        # Ensure IVP is cleaned up (usually a number)
        try:
            ivp = str(int(float(row['IVP'])))
        except:
            ivp = str(row['IVP'])
            
        result_val = str(row['Result'])
        if result_val == 'nan' or not result_val.strip(): 
            result_val = "-"
        
        # Build line with specific alignment
        line = f"{stock:<14} {price:<22} {ivp:<8} {result_val:<10}"
        rows.append(line)
        
    return f"{header}\n{separator}\n" + "\n".join(rows)

async def send_alert():
    """Send the Sensibull alert via Telegram."""
    bot_token, chat_ids = load_credentials()
    
    if not bot_token or not chat_ids:
        print("[ERROR] Invalid or missing credentials.")
        return False

    if not os.path.exists(DATA_FILE):
        print(f"[ERROR] Data file not found at {DATA_FILE}")
        return False
        
    try:
        # Load Data
        df = pd.read_csv(DATA_FILE)
        
        # Filter: IVP >= 80
        # Ensure IVP is numeric, handle potential non-numeric data gracefully
        df['IVP'] = pd.to_numeric(df['IVP'], errors='coerce').fillna(0)
        filtered_df = df[df['IVP'] >= 80].copy()
        
        # Sort by IVP Descending just in case
        filtered_df = filtered_df.sort_values(by='IVP', ascending=False)
        
        # Format Message
        table_text = format_table(filtered_df)
        message = f"🚨 *Sensibull High IVP Alert (IVP >= 80)*\n\n```\n{table_text}\n```"

        bot = Bot(token=bot_token)
        all_sent = True

        # Send to all Chat IDs
        for chat_id in chat_ids:
            print(f"Sending alert to Chat ID: {chat_id}...")
            try:
                # 1. Send Text Message
                await bot.send_message(
                    chat_id=chat_id,
                    text=message,
                    parse_mode="Markdown",
                    read_timeout=60,
                    write_timeout=60,
                    connect_timeout=60
                )
                
                # 2. Send CSV File
                # with open(DATA_FILE, 'rb') as f:
                #    await bot.send_document(
                #        chat_id=chat_id,
                #        document=f,
                #        filename="Sensibull_Data.csv",
                #        read_timeout=60,
                #        write_timeout=60,
                #        connect_timeout=60
                #    )
                
                print(f"[SUCCESS] Alert sent to {chat_id}!")
            except TelegramError as e:
                print(f"[ERROR] Failed to send to {chat_id}: {e}")
                all_sent = False
            except Exception as e:
                print(f"[ERROR] Unexpected error for {chat_id}: {e}")
                all_sent = False
        
        return all_sent
        
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred: {e}")
        return False

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    success = asyncio.run(send_alert())
    if success:
        sys.exit(0)
    else:
        sys.exit(1)




