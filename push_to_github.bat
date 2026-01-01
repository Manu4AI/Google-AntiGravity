@echo off
echo ========================================================
echo   FRESH UPLOAD TO GITHUB
echo ========================================================
echo   This will re-initialize the repository and push ALL files.
echo   Target: https://github.com/Manu4AI/Google-AntiGravity.git
echo ========================================================

:: 1. Clear old git history if exists (Clean Slate)
if exist ".git" (
    echo [!] Removing old .git folder for fresh start...
    rmdir /s /q .git
)

:: 2. Initialize
echo [1/4] Initializing new Repository...
git init
git branch -M main

:: 3. Add Remote
echo [2/4] Connecting to GitHub...
git remote add origin https://github.com/Manu4AI/Google-AntiGravity.git

:: 4. Add & Commit
echo [3/4] Adding all files...
git add .
git commit -m "Initial Commit: Complete Project Code"

:: 5. Push (Force)
echo [4/4] Pushing to GitHub...
git push -u origin main --force

echo.
echo ========================================================
if %ERRORLEVEL% EQU 0 (
    echo   SUCCESS! The repository has been re-uploaded.
) else (
    echo   FAILED. Please check:
    echo   1. Did you create the Empty Repo on GitHub?
    echo   2. Are your permissions correct?
)
echo ========================================================
pause
