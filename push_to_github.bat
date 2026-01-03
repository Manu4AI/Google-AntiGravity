@echo off
echo Syncing to GitHub...

:: Consolidate cleaning secrets
git rm --cached "Script RSI Calculation/service_account.json" >nul 2>&1
git rm --cached "Telegram Integration/telegram_credentials.json" >nul 2>&1
git rm --cached "service_account.json" >nul 2>&1
git rm --cached "telegram_credentials.json" >nul 2>&1

:: 1. Save local changes
echo Staging and committing changes...
git add .
git commit -m "Manual Update: %DATE% %TIME%"

:: 2. Push to GitHub (Standard push - safer)
:: Using standard push ensures you don't accidentally overwrite bot data on GitHub.
echo Pushing to GitHub...
git push -u origin main

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [!] PUSH FAILED: It looks like GitHub has newer data than your local folder.
    echo Please run 'pull_from_github.bat' first to sync the bot's data, then try this again.
) else (
    echo Done.
)

pause
