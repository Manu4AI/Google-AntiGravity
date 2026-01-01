import subprocess
import os
import sys
import time
import pandas as pd
from datetime import datetime

# ==========================================
# RSI Tracker - Master Orchestrator
# ==========================================
# This script runs the entire end-to-end workflow related to RSI Tracking.
# It executes 7 distinct steps in sequence. 
# It intelligently skips steps if no new data is found.

# ================= LOGGING SETUP =================
class Logger:
    def __init__(self, log_dir="Log"):
        self.log_dir = log_dir
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)
            
        today_str = datetime.now().strftime('%Y-%m-%d')
        self.log_file = os.path.join(self.log_dir, f"Log_{today_str}.txt")
        
    def log(self, message):
        """Logs message to both console and file."""
        # Print to console
        print(message, flush=True)
        
        # Write to file
        try:
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(str(message) + "\n")
        except Exception as e:
            print(f"[LOG ERROR] Could not write to log file: {e}")

# Global Logger Instance
logger = Logger()

# Define the sequence (Path, Args, Description)
# Note: We don't loop this list blindly anymore, we access it by index in main()
SCRIPTS = [
    (os.path.join("NSE Bhavcopy", "1_NSE_bhavcopy_downloader.py"), ["--mode", "check_today"], "Downloading Daily Bhavcopy"),
    (os.path.join("NSE Bhavcopy", "2_Script_Wise_Updater.py"), [], "Updating Script-wise Data"),
    (os.path.join("NSE Bhavcopy", "3.1_NSE_corporate_actions.py"), [], "Checking Corporate Actions"),
    (os.path.join("NSE Bhavcopy", "3.4_generate_adjustments.py"), [], "Generating Adjustments"),
    (os.path.join("NSE Bhavcopy", "4_Update_Adjustment_Prices.py"), [], "Applying Price Adjustments"),
    (os.path.join("Script RSI Calculation", "5_Script_RSI_Calculator_Adjusted.py"), [], "Calculating RSI & Generating Signals"),
    (os.path.join("Paper Trading Simulator", "8_Paper_Trading_Manager.py"), [], "Updating Paper Trading Book"),
    (os.path.join("Telegram Integration", "6_Telegram_Bot_Sender.py"), [], "Sending Telegram Notification")
]

def run_step(script_rel_path, args, description, step_num, total_steps):
    """
    Executes a single python script.
    Returns: (success: bool, return_code: int)
    """
    script_path = os.path.abspath(script_rel_path)
    
    logger.log("=" * 60)
    logger.log(f"STEP {step_num}/{total_steps}: {description}")
    logger.log(f"Script: {script_path}")
    if args:
        logger.log(f"Arguments: {' '.join(args)}")
    logger.log("=" * 60)

    if not os.path.exists(script_path):
        logger.log(f"[ERROR] Script not found: {script_path}")
        return False, -1

    try:
        # Run from parent dir
        script_dir = os.path.dirname(script_path)
        script_name = os.path.basename(script_path)
        python_cmd = sys.executable

        # Use shell=False for better reliability and encoding handling
        command = [python_cmd, "-u", script_name] + args

        start_time = time.time()
        
        # Ensure children inherit UTF8 mode
        env = os.environ.copy()
        env["PYTHONUTF8"] = "1"
        env["PYTHONIOENCODING"] = "utf-8"

        # Capture return code, allow non-zero exit
        result = subprocess.run(
            command, 
            cwd=script_dir, 
            check=False,
            shell=False,
            env=env
        )
        duration = time.time() - start_time
        
        # Determine Success based on return code
        # We allow custom codes 10 and 20 as "Soft Success"
        # 0 = Standard Success
        # 10 = New Data
        # 20 = No Data / Processed Already
        # Anything else (e.g. 1) is Error
        
        is_success = result.returncode in [0, 10, 20]
        
        if is_success:
            logger.log(f"\n[SUCCESS] Step {step_num} completed in {duration:.2f} seconds.")
            logger.log(f"[RETURN CODE] {result.returncode}")
        else:
            logger.log(f"\n[FAILED] Step {step_num} failed with error code {result.returncode}.")
            
        logger.log("\n")
        return is_success, result.returncode

    except Exception as e:
        logger.log(f"\n[CRITICAL] Unexpected error: {e}")
        return False, -1

    except Exception as e:
        logger.log(f"[GIT ERROR] {e}")
    
    logger.log("-" * 40 + "\n")

    try:
        # 0. Configure Git Identity (if not set, avoids CI errors)
        subprocess.run(["git", "config", "user.email", "workflow@antigravity.bot"], check=False)
        subprocess.run(["git", "config", "user.name", "AntiGravity Bot"], check=False)

        # 1. Remove Secrets (Just in case)
        secrets = [
            "Script RSI Calculation/service_account.json",
            "Telegram Integration/telegram_credentials.json",
            "service_account.json",
            "telegram_credentials.json"
        ]
        for secret in secrets:
            subprocess.run(["git", "rm", "--cached", secret], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        # 2. Add all files
        subprocess.run(["git", "add", "."], check=True)

        # 3. Commit
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        commit_msg = f"Auto-Commit: After {step_name} - {timestamp}"
        result = subprocess.run(["git", "commit", "-m", commit_msg], capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.log(f"[GIT] Committed: {commit_msg}")
        else:
            if "nothing to commit" in result.stdout:
                 logger.log("[GIT] Nothing to commit.")
            else:
                 logger.log(f"[GIT] Commit failed: {result.stderr}")

        # 4. Push
        push_res = subprocess.run(["git", "push", "origin", "main"], capture_output=True, text=True)
        if push_res.returncode == 0:
            logger.log("[GIT] Pushed to GitHub successfully.")
        else:
            logger.log(f"[GIT] Push failed: {push_res.stderr}")

    except Exception as e:
        logger.log(f"[GIT ERROR] {e}")
    
    logger.log("-" * 40 + "\n")


def print_summary_log(step_results):
    """Prints a summary of what was updated during the run."""
    logger.log("\n" + "="*30)
    logger.log("      EXECUTION SUMMARY      ")
    logger.log("="*30)
    
    # Step Mapping for Display
    step_names = [
        "1. Bhavcopy Downloader",
        "2. Script-Wise Updater",
        "3. Corporate Actions",
        "4. Generate Adjustments",
        "5. Apply Adjustments",
        "6. RSI & Signals",
        "7. Paper Trading Manager",
        "8. Telegram Bot"
    ]

    for i, (status, detail) in enumerate(step_results):
        logger.log(f"{step_names[i]:<25} : [{status}] {detail}")

    logger.log("="*30 + "\n")


def check_for_missing_symbols():
    """Checks if any symbol in the Master List is missing its script-wise or adjusted CSV."""
    master_list = os.path.join("NSE Bhavcopy", "0_Script_Master_List.csv")
    script_dir = os.path.join("NSE Bhavcopy", "NSE_Bhavcopy_Scriptwsie_Data")
    adjusted_dir = os.path.join("NSE Bhavcopy", "NSE_Bhavcopy_Adjusted_Data")
    
    if not os.path.exists(master_list):
        return False
        
    try:
        df = pd.read_csv(master_list)
        symbols = df['Symbol'].dropna().unique().tolist()
        
        for sym in symbols:
            # Check script-wise raw data
            if not os.path.exists(os.path.join(script_dir, f"{sym}.csv")):
                return True 
            # Check adjusted data
            if not os.path.exists(os.path.join(adjusted_dir, f"{sym}.csv")):
                return True
    except Exception as e:
        logger.log(f"[ERROR] Error checking for missing symbols: {e}")
    return False

def main():
    logger.log(f"Starting RSI Workflow Orchestrator at {datetime.now()}")
    logger.log(f"Root Directory: {os.getcwd()}")
    
    total_steps = len(SCRIPTS)
    
    # Track state for Logic Flow
    process_data = False
    new_ca_found = False
    missing_symbols_found = check_for_missing_symbols()
    
    if missing_symbols_found:
        logger.log(">>> INFO: New symbols detected in Master List. Forcing data update.")

    # Track results for Summary Table
    # List of (Status, Detail)
    step_results = [("SKIPPED", "")] * total_steps

    def update_res(idx, status, detail=""):
        step_results[idx] = (status, detail)

    # ---------------------------------------------------------
    # STEP 1: DOWNLOADER
    # ---------------------------------------------------------
    step_idx = 0
    success, ret = run_step(*SCRIPTS[step_idx], step_idx + 1, total_steps)
    if not success: 
        update_res(step_idx, "FAILED", "Error in script")
        print_summary_log(step_results)
        sys.exit(1)
    
    if ret == 10:
        process_data = True
        update_res(step_idx, "SUCCESS", "New Data Downloaded")
        logger.log(">>> RESULT: New Data Downloaded.")
    elif ret == 20: 
        # MODIFIED: Even if data exists (20), we allow processing to continue 
        # in case previous run failed mid-way.
        process_data = True 
        update_res(step_idx, "DONE", "File Already Exists")
        logger.log(">>> RESULT: File already exists. Continuing to ensure consistency.")
    else:
        update_res(step_idx, "SUCCESS", f"Code {ret}")
    
    # GIT COMMIT 1
    run_git_commit("Step 1: Downloader")

    # ---------------------------------------------------------
    # STEP 2: UPDATER
    # ---------------------------------------------------------
    step_idx = 1
    if process_data or missing_symbols_found:
        success, ret = run_step(*SCRIPTS[step_idx], step_idx + 1, total_steps)
        if not success: 
            update_res(step_idx, "FAILED")
            print_summary_log(step_results)
            sys.exit(1)
        update_res(step_idx, "SUCCESS", "Updated stocks")
        
        # GIT COMMIT 2
        run_git_commit("Step 2: Script-Wise Updater")

        if missing_symbols_found:
            process_data = True 
    else:
        logger.log("=" * 60)
        logger.log(f"STEP {step_idx+1}/{total_steps}: Updating Script-wise Data")
        logger.log(">>> SKIPPING: No new Bhavcopy data to process.")
        logger.log("=" * 60 + "\n")
        update_res(step_idx, "SKIPPED", "No new Bhavcopy")

    # ---------------------------------------------------------
    # STEP 3: CORPORATE ACTIONS
    # ---------------------------------------------------------
    step_idx = 2
    success, ret = run_step(*SCRIPTS[step_idx], step_idx + 1, total_steps)
    if not success: 
        update_res(step_idx, "FAILED")
        print_summary_log(step_results)
        sys.exit(1)

    if ret == 10:
        new_ca_found = True
        update_res(step_idx, "SUCCESS", "New CA Found")
        logger.log(">>> RESULT: New Corporate Actions Found.")
    elif ret == 20:
        update_res(step_idx, "DONE", "No New CA Found")
        logger.log(">>> RESULT: No New Corporate Actions.")
    else:
        update_res(step_idx, "SUCCESS")

    # ---------------------------------------------------------
    # STEP 4: GENERATE ADJUSTMENTS
    # ---------------------------------------------------------
    step_idx = 3
    if new_ca_found or missing_symbols_found:
        success, ret = run_step(*SCRIPTS[step_idx], step_idx + 1, total_steps)
        if not success: 
            update_res(step_idx, "FAILED")
            print_summary_log(step_results)
            sys.exit(1)
        update_res(step_idx, "SUCCESS")
    else:
        logger.log("=" * 60)
        logger.log(f"STEP {step_idx+1}/{total_steps}: Generating Adjustments")
        logger.log(">>> SKIPPING: No new Corporate Actions.")
        logger.log("=" * 60 + "\n")
        update_res(step_idx, "SKIPPED", "No new CA")

    # ---------------------------------------------------------
    # STEP 5: APPLY ADJUSTMENTS
    # ---------------------------------------------------------
    step_idx = 4
    if new_ca_found or missing_symbols_found:
        success, ret = run_step(*SCRIPTS[step_idx], step_idx + 1, total_steps)
        if not success: 
            update_res(step_idx, "FAILED")
            print_summary_log(step_results)
            sys.exit(1)
        update_res(step_idx, "SUCCESS")
        
        # GIT COMMIT 3 (After CA steps)
        run_git_commit("Step 3-5: Corporate Actions & Adjustments")
        
    else:
        logger.log("=" * 60)
        logger.log(f"STEP {step_idx+1}/{total_steps}: Applying Price Adjustments")
        logger.log(">>> SKIPPING: No new Corporate Actions to apply.")
        logger.log("=" * 60 + "\n")
        update_res(step_idx, "SKIPPED", "No new CA")

    # ---------------------------------------------------------
    # STEP 6: RSI CALCULATION (If any data changed)
    # ---------------------------------------------------------
    step_idx = 5
    data_changed = process_data or new_ca_found
    
    if data_changed:
        success, ret = run_step(*SCRIPTS[step_idx], step_idx + 1, total_steps)
        if not success: 
            update_res(step_idx, "FAILED")
            print_summary_log(step_results)
            sys.exit(1)
        update_res(step_idx, "SUCCESS")
        
        # GIT COMMIT 4
        run_git_commit("Step 6: RSI Calculation")
        
    else:
        logger.log("=" * 60)
        logger.log(f"STEP {step_idx+1}/{total_steps}: Calculating RSI")
        logger.log(">>> SKIPPING: No new Data or Corporate Actions.")
        logger.log("=" * 60 + "\n")
        update_res(step_idx, "SKIPPED", "Data up to date")

    # ---------------------------------------------------------
    # STEP 7: PAPER TRADING MANAGER (If any data changed)
    # ---------------------------------------------------------
    step_idx = 6
    if data_changed:
        success, ret = run_step(*SCRIPTS[step_idx], step_idx + 1, total_steps)
        if not success: 
            update_res(step_idx, "FAILED")
            print_summary_log(step_results)
            sys.exit(1)
        update_res(step_idx, "SUCCESS")
        
        # GIT COMMIT 5 (Optional but good for history)
        run_git_commit("Step 7: Paper Trading Update")
        
    else:
        logger.log("=" * 60)
        logger.log(f"STEP {step_idx+1}/{total_steps}: Updating Paper Trading Book")
        logger.log(">>> SKIPPING: No new Data.")
        logger.log("=" * 60 + "\n")
        update_res(step_idx, "SKIPPED", "Data up to date")

    # ---------------------------------------------------------
    # STEP 8: TELEGRAM (If any data changed)
    # ---------------------------------------------------------
    step_idx = 7
    if data_changed:
        success, ret = run_step(*SCRIPTS[step_idx], step_idx + 1, total_steps)
        if not success: 
            update_res(step_idx, "FAILED")
            print_summary_log(step_results)
            sys.exit(1)
        update_res(step_idx, "SUCCESS")
    else:
        logger.log("=" * 60)
        logger.log(f"STEP {step_idx+1}/{total_steps}: Sending Telegram Notification")
        logger.log(">>> SKIPPING: No new report to send.")
        logger.log("=" * 60 + "\n")
        update_res(step_idx, "SKIPPED", "No signals to send")

    logger.log("=" * 60)
    logger.log("WORKFLOW COMPLETED")
    logger.log("=" * 60)
    
    print_summary_log(step_results)

if __name__ == "__main__":
    main()
