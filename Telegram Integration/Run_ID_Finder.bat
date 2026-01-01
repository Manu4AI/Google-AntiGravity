@echo off
REM ========================================
REM Run ID Finder to get Chat IDs
REM ========================================

echo Checking for new messages to find Chat IDs...
echo Please ensure you have sent a message to the bot recently!
echo.

REM Get current script directory
set "SCRIPT_DIR=%~dp0"

cd /d "%SCRIPT_DIR%"
python "ID_Finder.py"

echo.
echo If you see your Group ID above, copy it!
echo If not, send a message in the group: "@Manu_RSI_Bot Hello" and try again.
echo.
pause
