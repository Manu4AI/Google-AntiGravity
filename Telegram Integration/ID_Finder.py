import asyncio
import json
import os
from telegram import Bot

# Set paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CREDENTIALS_FILE = os.path.join(SCRIPT_DIR, "telegram_credentials.json")

def load_credentials():
    try:
        with open(CREDENTIALS_FILE, "r") as f:
            creds = json.load(f)
            return creds.get("bot_token")
    except Exception:
        return None

async def get_chat_ids():
    token = load_credentials()
    if not token or token == "YOUR_BOT_TOKEN_HERE":
        print("Error: Please set your bot_token in telegram_credentials.json first.")
        return

    bot = Bot(token=token)
    print("Checking for updates... (Press Ctrl+C to stop)")
    print("-" * 50)
    
    try:
        updates = await bot.get_updates()
        
        if not updates:
            print("No updates found. Please send a message to your bot first!")
            print("1. Go to your bot in Telegram.")
            print("2. Send 'Hello' or command '/start'.")
            print("3. If in a group, send a message tagging the bot.")
            return

        print(f"{'CHAT TYPE':<15} | {'CHAT TITLE/USER':<30} | {'CHAT ID'}")
        print("-" * 60)
        
        seen_ids = set()
        
        for update in updates:
            # Check different update types (message, channel_post, my_chat_member)
            chat = None
            if update.message:
                chat = update.message.chat
            elif update.channel_post:
                chat = update.channel_post.chat
            elif update.my_chat_member:
                chat = update.my_chat_member.chat
            
            if chat and chat.id not in seen_ids:
                chat_name = chat.title if chat.title else chat.username or chat.first_name
                print(f"{chat.type:<15} | {chat_name:<30} | {chat.id}")
                seen_ids.add(chat.id)
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(get_chat_ids())
