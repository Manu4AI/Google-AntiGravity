import sys
import logging
import datetime
import subprocess
import os

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Ordered List of Cloud Scripts to Execute
SCRIPTS = [
    ("1_cloud_downloader.py", ["--mode", "check_today"], "Checking Bhavcopy"),
    ("2_cloud_script_updater.py", [], "Updating Cloud Script Data"),
    ("3_cloud_corporate_actions.py", [], "Checking Corporate Actions"),
    ("3.4_cloud_generate_adjustments.py", [], "Generating Adjustments"),
    ("4_cloud_update_adjustment_prices.py", [], "Applying Adjustments"),
    ("5_cloud_rsi_calculator.py", [], "Calculating RSI & Generating Signals"),
    ("6_cloud_telegram_bot.py", [], "Sending Telegram Notification")
]

def run_step(script_name, args, description, step_num, total_steps):
    logging.info(f"--- STEP {step_num}/{total_steps}: {description} ---")
    
    cmd = [sys.executable, script_name] + args
    
    try:
        # Stream output in real-time using Popen
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, # Merge stderr into stdout
            text=True,
            bufsize=1 # Line buffering
        )
        
        # Read line by line
        for line in process.stdout:
            print(line, end='', flush=True) 
            
        process.wait()
        return process.returncode
        
    except Exception as e:
        logging.error(f"Failed to run {script_name}: {e}")
        return 1 # Error

def main():
    start_time = datetime.datetime.now()
    logging.info(f"Starting Cloud RSI Workflow at {start_time}")
    
    total_steps = len(SCRIPTS)
    
    # State flags
    new_data = False
    new_ca = False
    
    # --- Step 1: Downloader ---
    ret = run_step(*SCRIPTS[0], 1, total_steps)
    if ret == 10: 
        new_data = True
        logging.info(">> New Data Found.")
    elif ret == 20:
        logging.info(">> No New Data.")
    else:
        logging.error(">> Downloader Failed. Exiting.")
        sys.exit(1)

    # --- Step 2: Updater ---
    if new_data:
        ret = run_step(*SCRIPTS[1], 2, total_steps)
    else:
        logging.info(">> Skipping Step 2 (Updater) - No new data.")

    # --- Step 3: Corporate Actions ---
    ret = run_step(*SCRIPTS[2], 3, total_steps)
    if ret == 10:
        new_ca = True
        logging.info(">> New Corporate Actions Found.")
    elif ret == 20: 
        logging.info(">> No New Corporate Actions.")
        
    # --- Step 4 & 5: Adjustments ---
    # We execute adjustments only if new CA found OR we just want to ensure consistency?
    # Optimization: Only if new_ca
    if new_ca:
         run_step(*SCRIPTS[3], 4, total_steps) # Gen Adjustments
         run_step(*SCRIPTS[4], 5, total_steps) # Apply Adjustments
    else:
         logging.info(">> Skipping Step 4 & 5 (Adjustments) - No new CA.")

    # --- Step 6 & 7: RSI & Telegram ---
    # Run if EITHER new data OR new CA (as prices might have changed due to adjustment)
    if new_data or new_ca:
        run_step(*SCRIPTS[5], 6, total_steps)
        run_step(*SCRIPTS[6], 7, total_steps)
    else:
        logging.info(">> Skipping RSI & Telegram - Nothing changed today.")

    end_time = datetime.datetime.now()
    duration = (end_time - start_time).total_seconds()
    logging.info(f"Workflow Completed in {duration:.2f} seconds.")

if __name__ == "__main__":
    main()
