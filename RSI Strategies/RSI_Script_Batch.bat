@echo off
echo Executing RSI_Script_Backtest_Adjusted...
python "RSI_Script_Backtest_Adjusted.py"
echo.
echo Executing RSI_Script_Backtest...
python "RSI_Script_Backtest.py"
echo.
echo All scripts execution completed.
pause
