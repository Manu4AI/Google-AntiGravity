@echo off
REM ===============================================
REM Google AntiGravity - RSI Workflow Launcher
REM ===============================================

REM Get directory of this batch file
set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"

REM Generate Timestamp (Format: YYYYMMDD_HHMMSS)
for /f "tokens=2-4 delims=/ " %%a in ('echo %DATE%') do set "ds=%%c%%a%%b"
for /f "tokens=1-3 delims=:." %%a in ('echo %TIME: =0%') do set "ts=%%a%%b%%c"
set "TIMESTAMP=%ds%_%ts%"

set "LOG_FILE=%SCRIPT_DIR%Log\Execution_Log_%TIMESTAMP%.log"

REM Ensure UTF-8 support for Python and Console
set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"
chcp 65001 >nul

echo Starting Daily RSI Workflow...
echo Log file: %LOG_FILE%
echo.

REM Execute with PowerShell to see output in console AND save to file
REM We use .ToString() on the objects to strip out messy PowerShell "NativeCommandError" metadata
powershell -NoProfile -Command "$env:PYTHONUTF8=1; [Console]::OutputEncoding = [System.Text.Encoding]::UTF8; & python -u 'main_orchestrator.py' 2>&1 | ForEach-Object { $_.ToString() } | Tee-Object -FilePath '%LOG_FILE%'; exit $LASTEXITCODE"

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ========================================
    echo SUCCESS: Workflow Completed Successfully.
    echo ========================================
) else (
    echo.
    echo ========================================
    echo ERROR: Workflow Failed. Check output in:
    echo %LOG_FILE%
    echo ========================================
)

echo.
echo Press any key to exit...
pause >nul
