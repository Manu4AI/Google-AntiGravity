# Script RSI Calculator - Setup Guide

## 1. Prerequisites
- Python installed (3.x)
- Google Cloud Project with Sheets API & Drive API enabled
- `service_account.json` file in project directory

## 2. Installation
```bash
pip install pandas gspread google-auth
```

## 3. Configuration
The script uses `rsi_calculator_production.py`. Key settings are at the top:
- `data_dir`: Path to NSE Bhavcopy folder
- `output_csv`: Path for local report
- `service_account_file`: Path to JSON key
- `adjustments_file`: Path to `adjustments.csv` (for Splits/Bonuses)

## 4. Running the Script
```bash
cd "Script RSI Calculation"
python rsi_calculator_production.py
```

## 5. Automation (Windows Task Scheduler)
1. Open Task Scheduler (`Win + R` -> `taskschd.msc`)
2. Create Basic Task -> "Script RSI Automation"
3. Trigger: Daily at 17:00 (5:00 PM)
4. Action: Start a Program
   - Program: `i:\Other computers\My Mac\Shivendra -Mac Sync\Google AntiGravity\Script RSI Calculation\run_daily_automation.bat`
   - Start in: `i:\Other computers\My Mac\Shivendra -Mac Sync\Google AntiGravity\Script RSI Calculation`

## 6. Handling Corporate Actions (Splits/Bonuses)
To prevent RSI errors when a stock splits or issues a bonus (causing a large price drop), add the event to `adjustments.csv`.

**File Location:** `i:\Other computers\My Mac\Shivendra -Mac Sync\Google AntiGravity\Script RSI Calculation\adjustments.csv`

**Format:**
```csv
Symbol,ExDate,AdjustmentFactor,Note
TATASTEEL,2022-07-28,0.1,Split 1:10
RELIANCE,2023-07-20,0.908,Demerger
```

**How to calculate Factor:**
- **Split 1:10** (Price becomes 1/10th): Factor = `0.1`
- **Bonus 1:1** (Price becomes 1/2): Factor = `0.5`
- **Demerger**: Factor = `(PreExClose - DemergedValue) / PreExClose`
  - Example: Reliance closed 2840, opened ~2580. Factor = 2580/2840 ≈ 0.908.

The script will automatically adjust historical prices *in memory* without touching your original Bhavcopy files!
