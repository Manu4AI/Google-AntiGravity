@echo off
echo ========================================================
echo   SYNCING TO GITHUB (SMART MODE)
echo ========================================================

:: 0. PRE-FLIGHT CHECK
if not exist "requirements.txt" (
    echo [ERROR] requirements.txt is MISSING! git cannot track it.
    echo Please restore the file and try again.
    pause
    exit /b
)
if not exist ".github\workflows\daily_rsi_run.yml" (
    echo [ERROR] Workflow file is MISSING!
    pause
    exit /b
)

:: 1. CHECK GIT
if not exist ".git" (
    echo [!] No .git folder found. Initializing...
    git init
    git branch -M main
    git remote add origin https://github.com/Manu4AI/Google-AntiGravity.git
    git pull origin main --allow-unrelated-histories
)

echo.
echo [1/3] Adding files...
:: Force add requirements to be safe
git add requirements.txt
git add .

echo [2/3] Committing changes...
set /p commit_msg="Enter commit message (Press Enter for 'Fix'): "
if "%commit_msg%"=="" set commit_msg=Fix Requirements
git commit -m "%commit_msg%"

echo [3/3] Pushing to GitHub...
git push -u origin main

echo.
echo ========================================================
if %ERRORLEVEL% EQU 0 (
    echo   SUCCESS!
) else (
    echo   PUSH FAILED.
)
echo ========================================================
pause
