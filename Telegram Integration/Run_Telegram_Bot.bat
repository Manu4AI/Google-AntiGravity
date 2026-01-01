@echo off
REM ========================================
REM Run Telegram Bot for RSI Report
REM ========================================

echo Sending Daily RSI Report via Telegram...

REM Get current script directory
set "SCRIPT_DIR=%~dp0"

cd /d "%SCRIPT_DIR%"
python "6_Telegram_Bot_Sender.py"

if %ERRORLEVEL% EQU 0 (
    echo.
    echo SUCCESS: Message sent.
) else (
    echo.
    echo ERROR: Failed to send message.
)

timeout /t 10
exit
