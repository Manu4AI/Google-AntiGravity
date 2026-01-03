@echo off
echo.
echo ==========================================
echo   STASHING AND PULLING DATA FROM GITHUB
echo ==========================================
echo.

:: 1. Stash local changes (this safely puts our local edits aside)
echo Saving your local changes temporarily...
git stash

:: 2. Pull the latest data from GitHub
echo Pulling latest CSVs and data from GitHub...
git pull origin main

:: 3. Pop the changes back (this reapplies our local edits on top of the fresh data)
echo Re-applying your local changes...
git stash pop

echo.
echo ==========================================
echo   SYNC COMPLETE - Local Folder is Updated
echo ==========================================
echo.
pause
