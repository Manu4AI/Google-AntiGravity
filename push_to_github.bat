@echo off
echo Syncing to GitHub...

:: Ensure Secrets are NOT tracked
git rm --cached "Script RSI Calculation/service_account.json" >nul 2>&1
git rm --cached "Telegram Integration/telegram_credentials.json" >nul 2>&1
git rm --cached "service_account.json" >nul 2>&1
git rm --cached "telegram_credentials.json" >nul 2>&1

:: Add & Commit
git add .
git commit -m "Manual Update: %DATE% %TIME%"

:: Push (Standard)
git push origin main

echo Done.
pause
