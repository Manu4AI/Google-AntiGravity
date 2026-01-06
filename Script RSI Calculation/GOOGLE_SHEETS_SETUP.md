# Google Sheets Setup - Quick Guide

## Prerequisites
✅ You already have `service_account.json` - Great!

## Final Steps

### 1. Install Required Library
```bash
pip install gspread google-auth
```

### 2. Create Google Sheet
1. Go to: https://sheets.google.com
2. Create a new spreadsheet
3. Name it: **`[Git] Script RSI Tracker`** (exact name matters!)

### 3. Share Sheet with Service Account
1. Open your `service_account.json` file
2. Find the `client_email` field (looks like: `xxx@xxx.iam.gserviceaccount.com`)
3. Copy that email address
4. In your Google Sheet, click **"Share"** button (top right)
5. Paste the service account email
6. Give **"Editor"** permission
7. Click **"Send"**

### 4. Configure Script (Optional)
If you want to use a different sheet name, edit `rsi_calculator_production.py`:
```python
CONFIG = {
    ...
    'google_sheet_name': "Your Custom Name Here",
    'enable_google_sheets': True  # Set to False to disable
}
```

### 5. Test It!
```bash
python rsi_calculator_production.py
```

The script will:
1. Calculate RSI for all 50 stocks (~2 minutes)
2. Save to CSV file
3. Upload to Google Sheets automatically
4. Print the spreadsheet URL

## Troubleshooting

### "Spreadsheet not found"
- Make sure the sheet name in the script matches EXACTLY
- Ensure you shared the sheet with the service account email

### "Permission denied"
- Check that you gave "Editor" permission (not just "Viewer")
- Verify the service account email is correct

### "Module not found: gspread"
- Run: `pip install gspread google-auth`

## Automation
Once working, the Windows Task Scheduler will automatically:
1. Run daily at 4:30 PM
2. Update both CSV and Google Sheets
3. You can access the latest data anytime from Google Sheets on any device!
