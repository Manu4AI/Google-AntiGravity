@echo off
echo Syncing to GitHub...

:: Consolidate cleaning secrets
git rm --cached "Script RSI Calculation/service_account.json" >nul 2>&1
git rm --cached "Telegram Integration/telegram_credentials.json" >nul 2>&1
git rm --cached "service_account.json" >nul 2>&1
git rm --cached "telegram_credentials.json" >nul 2>&1

:: 1. Save local changes
git add .
git commit -m "Manual Update: %DATE% %TIME%"

:: 2. Push to GitHub (Standard push, will fail if remote has new changes)
echo Pushing...
git push origin main

echo Done.
pause
