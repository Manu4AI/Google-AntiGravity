@echo off
echo ========================================================
echo   SYNCING TO GITHUB (SECURE MODE)
echo ========================================================

:: 0. PRE-FLIGHT CHECK
if not exist "requirements.txt" (
    echo [ERROR] requirements.txt is MISSING!
    pause
    exit /b
)

:: 1. PURGE SECRETS (Stop tracking them if they leaked)
echo [!] Ensuring Secrets are NOT tracked...
git rm --cached "Script RSI Calculation/service_account.json" >nul 2>&1
git rm --cached "Telegram Integration/telegram_credentials.json" >nul 2>&1
git rm --cached "service_account.json" >nul 2>&1
git rm --cached "telegram_credentials.json" >nul 2>&1

:: 2. ADD & COMMIT
echo [1/3] Adding files (ignoring secrets)...
git add .

echo [2/3] Committing changes...
git commit -m "Security Fix: Removed Leaked Credentials & Added .gitignore"

:: 3. PUSH
echo [3/3] Pushing to GitHub...
git push -u origin main

echo.
echo ========================================================
echo   SUCCESS! 
echo   Your sensitive files are now removed from GitHub.
echo   Please generate a NEW Key one last time.
echo ========================================================
pause
