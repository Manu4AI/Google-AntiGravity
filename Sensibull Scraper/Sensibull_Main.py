import os
import sys
import subprocess
from datetime import datetime

# Set paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SCRAPER_SCRIPT = os.path.join(SCRIPT_DIR, "Sensibull_Scraper.py")
BOT_SCRIPT = os.path.join(SCRIPT_DIR, "Sensibull_Alert_Bot.py")

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def run_script(script_path):
    """Runs a python script and returns success status."""
    if not os.path.exists(script_path):
        log(f"ERROR: Script not found: {script_path}")
        return False
    
    log(f"Running {os.path.basename(script_path)}...")
    try:
        # Use sys.executable to ensure we use the same environment
        result = subprocess.run([sys.executable, "-u", script_path], check=True)
        return result.returncode == 0
    except subprocess.CalledProcessError as e:
        log(f"ERROR: Script {os.path.basename(script_path)} failed with code {e.returncode}")
        return False
    except Exception as e:
        log(f"CRITICAL: Unexpected error: {e}")
        return False

def main():
    log("="*60)
    log("SENSIBULL HOURLY ALERTS - ORCHESTRATOR")
    log("="*60)

    # --- MARKET HOURS CHECK ---
    # Convert UTC to IST (UTC + 5:30)
    # GitHub Runners are in UTC.
    now = datetime.utcnow()
    
    # IST Offset
    ist_hour = now.hour + 5
    ist_minute = now.minute + 30
    
    # Adjust for minute overflow
    if ist_minute >= 60:
        ist_minute -= 60
        ist_hour += 1
        
    # Adjust for day overflow (rare but possible in late UTC)
    if ist_hour >= 24:
        ist_hour -= 24
        
    current_time_str = f"{ist_hour:02d}:{ist_minute:02d}"
    log(f"Current Time (IST): {current_time_str}")

    # Define Market Hours: 09:00 to 15:35
    market_open = 9 * 60 + 0       # 09:00 -> 540 min
    market_close = 15 * 60 + 35    # 15:35 -> 935 min
    current_minutes = ist_hour * 60 + ist_minute
    
    # Allow a small buffer before 9:00 if needed, but definitely cut off after 15:35
    if current_minutes > market_close:
        log(f"[SKIP] Market is closed (After 15:35 IST). Skipping alert.")
        sys.exit(0)
        
    if current_minutes < market_open:
        log(f"[SKIP] Market not yet open (Before 09:00 IST). Skipping alert.")
        sys.exit(0)
    # --------------------------

    # 1. Run Scraper
    if not run_script(SCRAPER_SCRIPT):
        log("Scraping failed. Aborting alert.")
        sys.exit(1)
    
    # 2. Run Alert Bot
    if not run_script(BOT_SCRIPT):
        log("Alert sending failed.")
        sys.exit(1)

    log("="*60)
    log("SUCCESS: All steps completed.")
    log("="*60)

if __name__ == "__main__":
    main()
