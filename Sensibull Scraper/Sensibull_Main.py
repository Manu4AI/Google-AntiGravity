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
