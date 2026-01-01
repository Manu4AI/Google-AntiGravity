
import os
import pandas as pd
import glob
from datetime import datetime

def get_strategy_description():
    return """
    <div class="section">
        <h2>Strategy Overview</h2>
        <p>This report summarizes the performance of the <strong>Adjusted RSI Multi-Stage Exit Strategy</strong>. 
        The strategy focuses on high-momentum setups using multi-timeframe RSI logic and manages risk through a unique tiered exit system.</p>
        
        <h3>Entry Criteria</h3>
        <table class="criteria-table">
            <tr>
                <th>Strategy Name</th>
                <th>Monthly RSI</th>
                <th>Weekly RSI</th>
                <th>Daily RSI</th>
            </tr>
            <tr>
                <td><strong>GFS (Good for Swing)</strong></td>
                <td>55 - 65</td>
                <td>55 - 65</td>
                <td>35 - 45</td>
            </tr>
            <tr>
                <td><strong>AGFS (Aggressive GFS)</strong></td>
                <td>55 - 65</td>
                <td>55 - 65</td>
                <td>55 - 65</td>
            </tr>
            <tr>
                <td><strong>Value Buy</strong></td>
                <td>35 - 45</td>
                <td>35 - 45</td>
                <td>35 - 45</td>
            </tr>
        </table>
        
        <h3>Exit Criteria (Multi-Stage)</h3>
        <ul>
            <li><strong>Initial Stop Loss:</strong> 5% below entry price.</li>
            <li><strong>Trigger 1 (Target ≥ 8%):</strong> Move Stop Loss to Entry Price (Cost).</li>
            <li><strong>Trigger 2 (Target ≥ 10%):</strong> Exit <strong>50%</strong> of position. Move Stop Loss to <strong>+5%</strong>.</li>
            <li><strong>Trigger 3 (Target ≥ 15%):</strong> Exit <strong>25%</strong> of position (75% total exited). Move Stop Loss to <strong>+10%</strong>.</li>
            <li><strong>Trailing (Target > 15%):</strong> Update Stop Loss to the <em>lowest close of the last 3 days</em> (if higher than current SL).</li>
        </ul>
    </div>
    """

def get_performance_summary(data_dir):
    years = [2021, 2022, 2023, 2024, 2025]
    summary_html = ""
    
    total_trades_all = 0
    total_avg_return_acc = 0
    total_sum_return_acc = 0
    years_count = 0
    
    rows = ""
    
    for year in years:
        file_path = os.path.join(data_dir, f"RSI_Exit_Adjusted_Report_{year}.xlsx")
        
        trades = 0
        win_rate = 0.0
        avg_ret = 0.0
        sum_ret = 0.0
        
        if os.path.exists(file_path):
            try:
                df = pd.read_excel(file_path)
                if not df.empty and 'Return %' in df.columns:
                    trades = len(df)
                    win_rate = len(df[df['Return %'] > 0]) / trades * 100
                    avg_ret = df['Return %'].mean()
                    sum_ret = df['Return %'].sum()
                    
                    total_trades_all += trades
                    total_avg_return_acc += avg_ret
                    total_sum_return_acc += sum_ret
                    years_count += 1
            except:
                pass
        
        row_class = "even" if year % 2 == 0 else "odd"
        rows += f"""
        <tr class="{row_class}">
            <td>{year}</td>
            <td>{trades}</td>
            <td>{win_rate:.1f}%</td>
            <td><strong>{avg_ret:.2f}%</strong></td>
            <td><strong>{sum_ret:.2f}%</strong></td>
        </tr>
        """
        
    avg_annual_avg_return = total_avg_return_acc / years_count if years_count > 0 else 0
    avg_annual_sum_return = total_sum_return_acc / years_count if years_count > 0 else 0
    
    summary_html += f"""
    <div class="section">
        <h2>Performance Summary (2021-2025)</h2>
        <table class="performance-table">
            <thead>
                <tr>
                    <th>Year</th>
                    <th>No. of Partial Trades</th>
                    <th>Win Rate (%)</th>
                    <th>Avg Return Per Trade (%)</th>
                    <th>Total Return (%)</th>
                </tr>
            </thead>
            <tbody>
                {rows}
            </tbody>
            <tfoot>
                <tr>
                    <td><strong>Average / Total</strong></td>
                    <td><strong>{int(total_trades_all/5)} (Avg)</strong></td>
                    <td>-</td>
                    <td><strong>{avg_annual_avg_return:.2f}%</strong></td>
                    <td><strong>{avg_annual_sum_return:.2f}% (Avg)</strong></td>
                </tr>
            </tfoot>
        </table>
    </div>
    """
    return summary_html

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_html_path = os.path.join(script_dir, "RSI_Strategy_Report.html")
    
    # CSS Styling
    css = """
    body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; color: #333; line-height: 1.6; max-width: 800px; margin: 0 auto; padding: 20px; }
    h1 { color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }
    h2 { color: #2980b9; margin-top: 30px; }
    h3 { color: #16a085; }
    .section { margin-bottom: 40px; background: #fff; padding: 20px; border-radius: 8px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
    table { width: 100%; border-collapse: collapse; margin: 20px 0; }
    th, td { padding: 12px; text-align: center; border-bottom: 1px solid #ddd; }
    th { background-color: #f2f2f2; color: #555; }
    .criteria-table th { background-color: #34495e; color: #fff; }
    .performance-table th { background-color: #2c3e50; color: #fff; }
    .performance-table tfoot { background-color: #ecf0f1; font-weight: bold; }
    tr:hover { background-color: #f5f5f5; }
    ul { list-style-type: none; padding: 0; }
    ul li { background: #e8f6f3; margin: 5px 0; padding: 10px; border-left: 5px solid #1abc9c; }
    .footer { text-align: center; font-size: 0.8em; color: #777; margin-top: 50px; }
    """
    
    content_body = get_strategy_description()
    content_perf = get_performance_summary(script_dir)
    
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>RSI Strategy Report</title>
        <style>{css}</style>
    </head>
    <body>
        <h1>RSI Multi-Stage Strategy Report</h1>
        <p><strong>Generated on:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
        
        {content_body}
        
        {content_perf}
        
        <div class="footer">
            <p>Generated by Google AntiGravity Agent</p>
        </div>
    </body>
    </html>
    """
    
    with open(output_html_path, "w", encoding='utf-8') as f:
        f.write(html_content)
        
    print(f"Report generated at: {output_html_path}")

if __name__ == "__main__":
    main()
