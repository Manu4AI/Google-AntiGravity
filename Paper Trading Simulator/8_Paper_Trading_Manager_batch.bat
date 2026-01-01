@echo off
REM ========================================
REM Google AntiGravity - Paper Trading Manager
REM ========================================

echo Starting Paper Trading Manager...
echo.

REM Get current script directory
set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"

python "8_Paper_Trading_Manager.py"

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ========================================
    echo SUCCESS: Paper Trading Book Updated.
    echo ========================================
) else (
    echo.
    echo ========================================
    echo ERROR: Failed to Update Paper Trading Book.
    echo ========================================
)

echo.
timeout /t 10
exit
