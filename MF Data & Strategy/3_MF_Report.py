
import pandas as pd
import os

# Configuration
DATA_DIR = 'MF_Data'
ANALYSIS_FILE = 'ICICI_Pru_Gilt_Analysis.csv'
REPORT_FILE = 'MF_Strategy_Report.csv'
ANALYSIS_PATH = os.path.join(DATA_DIR, ANALYSIS_FILE)
REPORT_PATH = os.path.join(DATA_DIR, REPORT_FILE) # Or root? User said "generate the report". Let's put in the folder.

def generate_report():
    if not os.path.exists(ANALYSIS_PATH):
        print(f"Analysis file not found: {ANALYSIS_PATH}")
        return

    print("Generating report...")
    df = pd.read_csv(ANALYSIS_PATH)
    
    # Filter for drops
    df_report = df[df['Falls_1Pct_Or_More'] == True].copy()
    
    # Select Columns
    # "contains last date and the date when the price fallen more than eual to 1%"
    # We will show Date, NAV, Year_High, Drop%
    cols = ['Date', 'nav', 'Rolling_Max_1Y', 'Drawdown_Pct']
    df_report = df_report[cols]
    
    # Rename for better readability
    df_report.rename(columns={
        'nav': 'NAV',
        'Rolling_Max_1Y': '52_Week_High',
        'Drawdown_Pct': 'Drop_Percentage'
    }, inplace=True)
    
    # Format Drawdown
    df_report['Drop_Percentage'] = df_report['Drop_Percentage'].round(2)
    
    # Sort by Date descending (latest first)
    df_report.sort_values('Date', ascending=False, inplace=True)
    
    # Save
    df_report.to_csv(REPORT_PATH, index=False)
    print(f"Report generated: {REPORT_PATH}")
    print(df_report.head())

if __name__ == "__main__":
    generate_report()
