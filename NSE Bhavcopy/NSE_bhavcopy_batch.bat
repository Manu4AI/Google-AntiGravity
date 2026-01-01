@echo off
echo Starting NSE Bhavcopy Daily Downloader...
echo This window will stay open to run the daily schedule at 18:00.
echo To run in background, you can minimize this window.

:: Navigate to the directory where this script is located
cd /d "%~dp0"

:: Run Python script (since we are in the same folder as downloader.py)
python 1_NSE_bhavcopy_downloader.py --mode scheduler

pause
