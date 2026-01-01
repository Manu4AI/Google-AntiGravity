@echo off
REM ========================================
REM Script RSI Calculator (Adjusted) - Master Automation
REM ========================================

echo ========================================
echo [1/3] Starting Bhavcopy Downloader...
echo ========================================

REM Get current script directory
set "SCRIPT_DIR=%~dp0"
REM Remove trailing backslash if present
if "%SCRIPT_DIR:~-1%"=="\" set "SCRIPT_DIR=%SCRIPT_DIR:~0,-1%"

REM Parent directory logic
for %%I in ("%SCRIPT_DIR%\..") do set "PARENT_DIR=%%~fI"

REM Downloader path (assuming it's in sibling folder relative to this script in Script RSI Calculation)
set "DOWNLOADER_DIR=%PARENT_DIR%\nse_bhavcopy_downloader"

REM Check if downloader exists in parent sibling or check inside script dir if we move it.
REM Current plan: Downloader stays in Root/nse_bhavcopy_downloader. 
REM Script moves to Root/Script RSI Calculation.
REM So Downloader is in ..\nse_bhavcopy_downloader

cd /d "%DOWNLOADER_DIR%"
if exist "downloader.py" (
    echo Running Downloader in 'Catch-Up' mode...
    python downloader.py --mode check_today
    if %ERRORLEVEL% NEQ 0 (
        echo [WARNING] Downloader reported an issue. Continuing...
    )
) else (
    echo [ERROR] Downloader script not found at: %DOWNLOADER_DIR%
    echo Please verify folder structure.
)

echo.
echo ========================================
echo [2/3] Checking Corporate Actions (Splits/Bonuses)...
echo ========================================
cd /d "%SCRIPT_DIR%"
python "script_corporate_actions.py"
if %ERRORLEVEL% NEQ 0 (
    echo [WARNING] Corporate action fetch failed. Using existing adjustments.
)

echo.
echo ========================================
echo [3/3] Calculating RSI (Adjusted) ^& Updating Google Sheets...
echo ========================================
python "5_Script_RSI_Calculator_Adjusted.py"

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ========================================
    echo SUCCESS: All tasks completed successfully.
    echo ========================================
) else (
    echo.
    echo ========================================
    echo ERROR: RSI Calculation failed.
    echo ========================================
)


REM Keep window open for 15 seconds
timeout /t 15
exit
