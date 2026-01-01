@echo off
pushd "%~dp0"
echo.
echo ===================================================
echo   STEP 1: Downloading NSE Corporate Actions (3.1)
echo ===================================================
python "3.1_NSE_corporate_actions.py"
if %ERRORLEVEL% NEQ 0 (
    echo Error running 3.1_NSE_corporate_actions.py
    pause
    exit /b %ERRORLEVEL%
)

echo.
echo ===================================================
echo   STEP 2: Generating Adjustments (3.4)
echo ===================================================
python "3.4_generate_adjustments.py"
if %ERRORLEVEL% NEQ 0 (
    echo Error running 3.4_generate_adjustments.py
    pause
    exit /b %ERRORLEVEL%
)

echo.
echo ===================================================
echo   Pipeline Completed Successfully!
echo ===================================================
pause
