@echo off
pushd "%~dp0"
echo.
echo ===================================================
echo   STEP 4: Updating Adjustment Prices
echo ===================================================
python "4_Update_Adjustment_Prices.py"
if %ERRORLEVEL% NEQ 0 (
    echo Error running 4_Update_Adjustment_Prices.py
    pause
    exit /b %ERRORLEVEL%
)

echo.
echo ===================================================
echo   Price Adjustment Completed Successfully!
echo ===================================================
pause
