# NIFTY 50 Strategy Investigation Report (2021-2025)

## 1. Objective
To determine if a **"Buy the Dip" Strategy** (SIP + Top-up on Dips) can significantly outperform a **Normal Daily SIP** in the NIFTY 50 Index.

## 2. Executive Summary
After rigorous backtesting of multiple strategies (Percentage Drop, RSI, Moving Averages) and a theoretical maximum simulation, we conclude that **timing the NIFTY 50 index generates minimal Alpha (excess return)** compared to a standard SIP.

- **Normal SIP XIRR**: ~12.1%
- **Best Realistic Strategy (200-DMA)**: ~12.4% (**+0.3% Alpha**)
- **Theoretical Maximum (God Mode)**: ~13.2% (**+1.1% Alpha**)

## 3. Strategies Tested

### A. Percentage Drop Rule
*   **Logic**: Buy extra when NIFTY falls X% from 52-Week High.
*   **Observation**: NIFTY is consistently volatile.
    *   **1% Drop**: Triggered ~77% of trading days (Too frequent).
    *   **2% Drop**: Triggered ~40-80% of trading days.
*   **Result**: Acts as a "Leveraged SIP" (investing more capital) rather than a "Smart SIP". Returns improved marginally in 2021 but underperformed in 2024.

### B. Technical Indicators (RSI & SMA)
*   **Logic**: Buy when oversold (RSI < 40) or below trend (50/200 DMA).
*   **Results (2021-2025 Total)**:
    | Strategy | Triggers | Investment | Value | XIRR | Alpha |
    | :--- | :--- | :--- | :--- | :--- | :--- |
    | **Normal SIP** | 0 | ₹12.3L | ₹16.6L | **12.11%** | - |
    | **RSI < 40** | 147 | ₹13.8L | ₹18.6L | 12.27% | +0.16% |
    | **Price < 50-DMA** | 414 | ₹16.5L | ₹22.3L | 12.41% | +0.29% |
    | **Price < 200-DMA**| 190 | ₹14.2L | ₹19.2L | **12.41%** | **+0.30%** |

## 4. The "God Mode" Test (Theoretical Maximum)
To verify if the script was broken or if the market is just efficient, we simulated a "Perfect Foresight" strategy.
*   **Scenario 1**: Invest on 1st of every Month.
*   **Scenario 2 (God Mode)**: Invest on the **exact lowest price day** of every month.

**Results**:
- **Normal SIP**: 12.14% XIRR
- **God Mode**: 13.25% XIRR
- **Difference**: **+1.12%**

**Conclusion**: If knowing the future (buying the exact bottom) only adds 1.1% return, then no realistic strategy (which relies on lagging indicators) can possibly beat that. Our finding of +0.3% alpha for the 200-DMA strategy is consistent with this ceiling.

## 5. Source Code for Verification
You can provide this logic to other experts/AI to verify the XIRR calculation and logic.

### Backtest Logic (Python)
```python
def run_backtest(df):
    sip_amt = 1000
    topup_amt = 1000
    
    s1_inv = 0; s1_units = 0; s1_tx = []
    s2_inv = 0; s2_units = 0; s2_tx = []
    
    for idx, row in df.iterrows():
        date = row['Date']
        price = row['nav']
        
        # Condition: Example RSI < 40
        is_dip = row['RSI'] < 40 
        
        # Scenario 1: Normal SIP
        s1_units += sip_amt / price
        s1_inv += sip_amt
        s1_tx.append((date, -sip_amt))
        
        # Scenario 2: Strategy
        invest = sip_amt + (topup_amt if is_dip else 0)
        s2_units += invest / price
        s2_inv += invest
        s2_tx.append((date, -invest))
        
    # Final Value
    final_price = df.iloc[-1]['nav']
    
    s1_val = s1_units * final_price
    s2_val = s2_units * final_price
    
    # XIRR Calculation uses Newton-Raphson method on the cashflows
    # s1_xirr = xirr(s1_tx + [(last_date, s1_val)])
    # s2_xirr = xirr(s2_tx + [(last_date, s2_val)])
    
    return s1_xirr, s2_xirr
```

### Why is Alpha so low?
In a strong bull market (like NIFTY 2021-2025), "Time in the Market" beats "Timing the Market". Holding cash (or waiting to deploy extra capital) often means buying at higher prices later because the market trends upwards. The "God Mode" test proves that even perfect timing has limited upside in such an index.
