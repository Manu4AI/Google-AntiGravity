import pandas as pd
import numpy as np
import glob
import os
from datetime import time

# =========================================================
# STEP 1: GLOBAL PARAMETERS
# =========================================================
CAPITAL = 100_000
RISK_PER_TRADE = 1_000
ATR_MULT = 1.2
MIN_HOLD_SECONDS = 15 * 60  # 15 minutes
NO_NEW_TRADE_AFTER = time(15, 0)

print("Starting FINAL NIFTY MTF Strategy...")

# =========================================================
# STEP 2: LOAD 1-MINUTE MASTER DATA
# =========================================================
base_path = r"F:\Shivendra -PC Sync\Google AntiGravity\ICICI Breeze\NIFTY Spot Data\NIFTY Spot Master Data"
files = glob.glob(os.path.join(base_path, "**", "*.csv"), recursive=True)

if not files:
    raise FileNotFoundError("No CSV files found.")

df_list = []
for f in files:
    try:
        df_list.append(pd.read_csv(f, parse_dates=['datetime']))
    except Exception as e:
        print(f"Skipping {f}: {e}")

df_1m = (
    pd.concat(df_list, ignore_index=True)
    .sort_values("datetime")
    .set_index("datetime")
    [["open", "high", "low", "close", "volume"]]
)

# NSE session filter
df_1m = df_1m.between_time("09:15", "15:30")

print(f"Loaded {len(df_1m)} 1-minute bars")

# =========================================================
# STEP 3: RESAMPLING FUNCTION
# =========================================================
def resample_ohlc(df, tf):
    return (
        df.resample(tf, label="right", closed="right")
        .agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum"
        })
        .dropna()
    )

# =========================================================
# STEP 4: CREATE TIMEFRAMES
# =========================================================
df_5m  = resample_ohlc(df_1m, "5T")
df_15m = resample_ohlc(df_1m, "15T")
df_1h  = resample_ohlc(df_1m, "1H")
df_1d  = resample_ohlc(df_1m, "1D")

# =========================================================
# STEP 5: INDICATORS
# =========================================================
def RSI(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def DEMA(series, period=100):
    ema1 = series.ewm(span=period, adjust=False).mean()
    ema2 = ema1.ewm(span=period, adjust=False).mean()
    return 2 * ema1 - ema2

def ATR(df, period=14):
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - df["close"].shift()).abs(),
        (df["low"] - df["close"].shift()).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()

# =========================================================
# STEP 6: APPLY INDICATORS
# =========================================================
for d in [df_5m, df_15m, df_1h, df_1d]:
    d["rsi"] = RSI(d["close"])
    d["dema"] = DEMA(d["close"])

df_15m["atr"] = ATR(df_15m)
df_15m["atr_med"] = df_15m["atr"].rolling(20).median()

# =========================================================
# STEP 7: DAILY BIAS (STRONG ONLY)
# =========================================================
df_1d["bias"] = np.select(
    [
        (df_1d["close"] > df_1d["dema"]) & (df_1d["rsi"] > 60),
        (df_1d["close"] < df_1d["dema"]) & (df_1d["rsi"] < 40)
    ],
    [1, -1],
    default=0
)

df_5m["daily_bias"] = df_1d["bias"].reindex(df_5m.index, method="ffill")

# =========================================================
# STEP 8: 1H CONFIRMATION
# =========================================================
df_1h["trend_ok"] = (
    ((df_1h["close"] > df_1h["dema"]) & (df_1h["rsi"] > 50)) |
    ((df_1h["close"] < df_1h["dema"]) & (df_1h["rsi"] < 50))
)

df_5m["h1_ok"] = df_1h["trend_ok"].reindex(df_5m.index, method="ffill")

# =========================================================
# STEP 9: 15m CONTROLLED PULLBACK + MOMENTUM
# =========================================================
df_15m["pb_long"] = (
    df_15m["rsi"].between(50, 55) &
    (df_15m["close"] > df_15m["dema"]) &
    (df_15m["atr"] > df_15m["atr_med"])
)

df_15m["pb_short"] = (
    df_15m["rsi"].between(45, 50) &
    (df_15m["close"] < df_15m["dema"]) &
    (df_15m["atr"] > df_15m["atr_med"])
)

df_15m["mom_long"] = (df_15m["rsi"].shift(1) < 55) & (df_15m["rsi"] > 55)
df_15m["mom_short"] = (df_15m["rsi"].shift(1) > 45) & (df_15m["rsi"] < 45)

# Align 15m → 5m (FAST ENGINE REQUIREMENT)
df_5m["pb_long"]   = df_15m["pb_long"].reindex(df_5m.index, method="ffill")
df_5m["pb_short"]  = df_15m["pb_short"].reindex(df_5m.index, method="ffill")
df_5m["mom_long"]  = df_15m["mom_long"].reindex(df_5m.index, method="ffill")
df_5m["mom_short"] = df_15m["mom_short"].reindex(df_5m.index, method="ffill")
df_5m["atr_15m"]   = df_15m["atr"].reindex(df_5m.index, method="ffill")
df_5m["close_15m"] = df_15m["close"].reindex(df_5m.index, method="ffill")
df_5m["dema_15m"]  = df_15m["dema"].reindex(df_5m.index, method="ffill")

# =========================================================
# STEP 10: ENTRY SIGNALS (HIGH QUALITY ONLY)
# =========================================================
df_5m["long_entry"] = (
    (df_5m["daily_bias"] == 1) &
    (df_5m["h1_ok"]) &
    (df_5m["pb_long"]) &
    (df_5m["mom_long"]) &
    (df_5m["close"] > df_5m["dema"]) &
    (df_5m["rsi"].shift(1) < 50) &
    (df_5m["rsi"] > 50)
)

df_5m["short_entry"] = (
    (df_5m["daily_bias"] == -1) &
    (df_5m["h1_ok"]) &
    (df_5m["pb_short"]) &
    (df_5m["mom_short"]) &
    (df_5m["close"] < df_5m["dema"]) &
    (df_5m["rsi"].shift(1) > 50) &
    (df_5m["rsi"] < 50)
)

# =========================================================
# STEP 11: FAST TRADE ENGINE (FINAL)
# =========================================================
print(f"Daily Bias Longs: {df_5m['daily_bias'].eq(1).sum()}")
print(f"Daily Bias Shorts: {df_5m['daily_bias'].eq(-1).sum()}")
print(f"H1 Trend Ok: {df_5m['h1_ok'].sum()}")
print(f"PB Long: {df_5m['pb_long'].sum()}")
print(f"PB Short: {df_5m['pb_short'].sum()}")
print(f"Mom Long: {df_5m['mom_long'].sum()}")
print(f"Mom Short: {df_5m['mom_short'].sum()}")
print(f"Long Entries: {df_5m['long_entry'].sum()}")
print(f"Short Entries: {df_5m['short_entry'].sum()}")

print("Running backtest engine...")

trades = []
position = None

for row in df_5m.itertuples():
    t = row.Index
    
    # 🚫 Block late entries
    if position is None and t.time() >= NO_NEW_TRADE_AFTER:
        continue

    # ENTRY
    if position is None:
        if row.long_entry:
            sl = row.close - ATR_MULT * row.atr_15m
            qty = RISK_PER_TRADE / (row.close - sl)
            position = dict(dir="LONG", entry=row.close, sl=sl, qty=qty, entry_time=t)

        elif row.short_entry:
            sl = row.close + ATR_MULT * row.atr_15m
            qty = RISK_PER_TRADE / (sl - row.close)
            position = dict(dir="SHORT", entry=row.close, sl=sl, qty=qty, entry_time=t)

    # EXIT
    else:
        if (t - position["entry_time"]).total_seconds() < MIN_HOLD_SECONDS:
            continue

        if position["dir"] == "LONG":
            if row.low <= position["sl"]:
                exit_price, reason = position["sl"], "SL"
            elif row.close_15m < row.dema_15m:
                exit_price, reason = row.close, "15m Trend Break"
            else:
                continue
            pnl = (exit_price - position["entry"]) * position["qty"]

        else:
            if row.high >= position["sl"]:
                exit_price, reason = position["sl"], "SL"
            elif row.close_15m > row.dema_15m:
                exit_price, reason = row.close, "15m Trend Break"
            else:
                continue
            pnl = (position["entry"] - exit_price) * position["qty"]

        trades.append({**position, "exit_time": t, "exit": exit_price, "pnl": pnl, "exit_reason": reason})
        position = None

# =========================================================
# RESULTS
# =========================================================
trades_df = pd.DataFrame(trades)
print("\n================ FINAL RESULTS ================")
print(f"Total Trades : {len(trades_df)}")

if not trades_df.empty:
    print(f"Win Rate     : {(trades_df.pnl > 0).mean() * 100:.2f}%")
    print(f"Total PnL    : {trades_df.pnl.sum():.2f}")
    print(f"Max DD       : {(trades_df.pnl.cumsum().cummax() - trades_df.pnl.cumsum()).max():.2f}")

    output = os.path.join(base_path, "..", "Backtest_Results_FINAL.csv")
    trades_df.to_csv(output, index=False)
    print(f"\nSaved trade log to: {output}")
else:
    print("No trades generated. Check signal logic.")
