@echo off
REM ===============================================
REM Google AntiGravity - Sensibull Hourly Alerts
REM ===============================================

REM Get directory of this batch file
set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"

REM Generate Timestamp (Format: YYYYMMDD_HHMMSS)
for /f "tokens=2-4 delims=/ " %%a in ('echo %DATE%') do set "ds=%%c%%a%%b"
for /f "tokens=1-3 delims=:." %%a in ('echo %TIME: =0%') do set "ts=%%a%%b%%c"
set "TIMESTAMP=%ds%_%ts%"

set "LOG_FILE=%SCRIPT_DIR%Log\Sensibull_Execution_%TIMESTAMP%.log"

REM Ensure UTF-8 support for Python
set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"

echo Starting Sensibull Hourly Alerts Workflow...
echo Log file: %LOG_FILE%
echo.

REM Execute with PowerShell to see output in console AND save to file
powershell -NoProfile -Command "$env:PYTHONUTF8=1; & python -u 'Sensibull Scraper\Sensibull_Main.py' 2>&1 | ForEach-Object { $_.ToString() } | Tee-Object -FilePath '%LOG_FILE%'; exit $LASTEXITCODE"

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ========================================
    echo SUCCESS: Sensibull Alerts Sent.
    echo ========================================
) else (
    echo.
    echo ========================================
    echo ERROR: Sensibull Workflow Failed.
    echo ========================================
)

echo.
timeout /t 10
