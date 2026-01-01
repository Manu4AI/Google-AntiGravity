#!/bin/bash

# ========================================
# Script RSI Calculator (Adjusted) - Master Automation (Linux/Mac)
# ========================================

echo "========================================"
echo "[1/3] Starting Bhavcopy Downloader..."
echo "========================================"

# Get current script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARENT_DIR="$(dirname "$SCRIPT_DIR")"

# Downloader path
DOWNLOADER_DIR="$PARENT_DIR/nse_bhavcopy_downloader"

# Run Downloader
if [ -d "$DOWNLOADER_DIR" ]; then
    cd "$DOWNLOADER_DIR"
    if [ -f "downloader.py" ]; then
        echo "Running Downloader in 'Catch-Up' mode..."
        python3 downloader.py --mode check_today
    else
        echo "[ERROR] Downloader script not found at: $DOWNLOADER_DIR"
    fi
else
    echo "[ERROR] Downloader directory not found at: $DOWNLOADER_DIR"
fi

echo ""
echo "========================================"
echo "[2/3] Checking Corporate Actions (Splits/Bonuses)..."
echo "========================================"
cd "$SCRIPT_DIR"
python3 "script_corporate_actions.py"

echo ""
echo "========================================"
echo "[3/3] Calculating RSI (Adjusted) & Updating Google Sheets..."
echo "========================================"
python3 "Script_RSI_Calculator_Adjusted.py"

if [ $? -eq 0 ]; then
    echo ""
    echo "========================================"
    echo "SUCCESS: All tasks completed successfully."
    echo "========================================"
else
    echo ""
    echo "========================================"
    echo "ERROR: RSI Calculation failed."
    echo "========================================"
fi
