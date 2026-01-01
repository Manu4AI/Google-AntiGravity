import os
import sys
import time
import logging  # Added logging
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
# from breeze_connect import BreezeConnect # Commented out
import paper_config
import auto_login

# --- CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# Setup Logger
log_filename = f"paper_trader_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, log_filename)),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

TRADE_BOOK_FILE = os.path.join(BASE_DIR, "paper_trade_book.csv")
SIGNAL_FILE = os.path.join(BASE_DIR, "..", "Script RSI Calculation", "Script_RSI_Strategy_Signals.csv")

# breeze = None # Commented out

def init_breeze():
    """Initializes Breeze Connect with Auto-Login."""
    # global breeze
    try:
        # logger.info(" [Init] Attempting Auto-Login...")
        # session_token = auto_login.get_session_token()
        
        # if not session_token:
        #     logger.critical(" [Init] CRITICAL: Auto-Login Failed. Cannot proceed.")
        #     return False
            
        # breeze = BreezeConnect(api_key=paper_config.API_KEY)
        # breeze.generate_session(api_secret=paper_config.SECRET_KEY, session_token=session_token)
        # logger.info(" [Init] Breeze Session Connected Successfully!")
        logger.info(" [Init] Breeze Skipped. Using yfinance for data.")
        return True
    except Exception as e:
        logger.error(f" [Init] Error connecting to Breeze: {e}")
        return False

def load_trade_book():
    columns = ['Date','Symbol','Strategy','Status','Stage','Entry_Price','Initial_Quantity','Current_Quantity','Investment_Amount','Current_LTP','SL_Price','Exit_Price','Exit_Reason','Realized_PnL','Unrealized_PnL','Total_PnL','PnL_Percentage']
    if os.path.exists(TRADE_BOOK_FILE):
        df = pd.read_csv(TRADE_BOOK_FILE)
        # Ensure new columns exist
        for col in columns:
            if col not in df.columns:
                df[col] = 0.0 if 'PnL' in col or 'Price' in col or 'Amount' in col else (0 if 'Quantity' in col or 'Stage' in col else '')
        return df
    else:
        return pd.DataFrame(columns=columns)

def save_trade_book(df):
    df.to_csv(TRADE_BOOK_FILE, index=False)

def get_live_price(symbol):
    """Fetches LTP from yfinance."""
    try:
        ticker_symbol = f"{symbol}.NS"
        ticker = yf.Ticker(ticker_symbol)
        
        # fast_info is generally faster for current price
        price = ticker.fast_info.last_price
        
        if price and price > 0:
             return float(price)
        
        logger.warning(f"   [Price] No valid data for {ticker_symbol}")
        return None

    except Exception as e:
        logger.error(f"   [Price] Fetch failed for {symbol}: {e}")
        return None

def get_3_day_low(symbol):
    """Fetches historical daily data and returns the lowest low of the last 3 days."""
    try:
        ticker_symbol = f"{symbol}.NS"
        ticker = yf.Ticker(ticker_symbol)
        
        # Fetch 5 days history to be safe
        hist = ticker.history(period="5d")
        
        if not hist.empty and len(hist) >= 3:
            # We want the LAST 3 COMPLETED days. 
            # If market is open, history might include today.
            # Assuming we run this during market, we might want to exclude 'today' if we are looking for valid support 
            # or include it if we want trailing SL to tighten immediately.
            # Code logic said "lowest low of last 3 days". 
            # Let's take the last 3 rows.
            last_3 = hist.tail(3)
            min_low = last_3['Low'].min()
            return float(min_low)
            
        return None
            
    except Exception as e:
        logger.error(f"   [History] Fetch failed for {symbol}: {e}")
        return None

def process_new_signals():
    """Reads Signal CSV and adds new trades to Book."""
    logger.info(" [Signals] Checking for new signals...")
    
    if not os.path.exists(SIGNAL_FILE):
        logger.warning(f" [Signals] Signal file not found.")
        return

    try:
        df_signals = pd.read_csv(SIGNAL_FILE)
        df_book = load_trade_book()
        new_trades = []
        
        for index, row in df_signals.iterrows():
            symbol = row['Symbol']
            strategy = row['Strategy']
            signal_date = row['Signal_Date']
            close_price = float(row['Close'])
            
            duplicate = df_book[
                (df_book['Symbol'] == symbol) & 
                (df_book['Strategy'] == strategy) & 
                (df_book['Date'] == signal_date)
            ]
            
            if duplicate.empty:
                # Calculate Quantity based on Capital (10k)
                qty = int(paper_config.PER_TRADE_CAPITAL / close_price)
                if qty < 1: qty = 1
                
                investment = qty * close_price
                
                logger.info(f" [New Trade] {symbol} @ {close_price} x {qty} = {investment:.2f}")
                
                # Initial SL: 5% Hard Stop
                sl_price = close_price * (1 - paper_config.INITIAL_SL_PCT)
                
                new_trade = {
                    'Date': signal_date,
                    'Symbol': symbol,
                    'Strategy': strategy,
                    'Status': 'OPEN',
                    'Stage': 0,
                    'Entry_Price': close_price,
                    'Initial_Quantity': qty,
                    'Current_Quantity': qty,
                    'Investment_Amount': round(investment, 2),
                    'Current_LTP': close_price,
                    'SL_Price': round(sl_price, 2),
                    'Exit_Price': 0.0,
                    'Exit_Reason': '',
                    'Realized_PnL': 0.0,
                    'Unrealized_PnL': 0.0,
                    'Total_PnL': 0.0,
                    'PnL_Percentage': 0.0
                }
                new_trades.append(new_trade)
            
        if new_trades:
            if df_book.empty: df_book = pd.DataFrame(new_trades)
            else: df_book = pd.concat([df_book, pd.DataFrame(new_trades)], ignore_index=True)
            save_trade_book(df_book)
            logger.info(f" [Signals] Added {len(new_trades)} new trades.")
        else:
            logger.info(" [Signals] No new unique signals.")
            
    except Exception as e:
        logger.error(f" [Signals] Error: {e}")

def monitor_open_trades():
    """Loops through OPEN trades and applies Multi-Stage Exit Logic."""
    logger.info(" [Monitor] Checking Open Positions...")
    
    df_book = load_trade_book()
    open_indices = df_book[df_book['Status'] == 'OPEN'].index
    
    if len(open_indices) == 0:
        logger.info(" [Monitor] No open positions.")
        return

    changes = False
    
    for idx in open_indices:
        symbol = df_book.at[idx, 'Symbol']
        entry_price = float(df_book.at[idx, 'Entry_Price'])
        current_sl = float(df_book.at[idx, 'SL_Price'])
        stage = int(df_book.at[idx, 'Stage'])
        current_qty = int(df_book.at[idx, 'Current_Quantity'])
        initial_qty = int(df_book.at[idx, 'Initial_Quantity'])
        investment = float(df_book.at[idx, 'Investment_Amount'])
        realized_pnl = float(df_book.at[idx, 'Realized_PnL'])
        
        # 1. Get Live Price
        ltp = get_live_price(symbol)
        if ltp is None: continue
        
        # Calculate Metrics
        unrealized_pnl = (ltp - entry_price) * current_qty
        total_pnl = realized_pnl + unrealized_pnl
        pnl_pct = (ltp - entry_price) / entry_price # Price gain %
        total_pnl_pct = (total_pnl / investment) * 100 # Overall ROI %
        
        # Update Book (Display)
        df_book.at[idx, 'Current_LTP'] = ltp
        df_book.at[idx, 'Unrealized_PnL'] = round(unrealized_pnl, 2)
        df_book.at[idx, 'Total_PnL'] = round(total_pnl, 2)
        df_book.at[idx, 'PnL_Percentage'] = round(total_pnl_pct, 2)
        df_book.at[idx, 'PnL_Percentage'] = round(total_pnl_pct, 2)
        changes = True
        
        logger.info(f"   > {symbol} [Stg {stage}]: LTP {ltp} (ROI: {total_pnl_pct:.2f}%) | SL {current_sl}")
        
        # 2. CHECK STOP LOSS HIT FIRST
        if ltp <= current_sl:
            logger.info(f" -> HIT STOP LOSS at {ltp}! Closing remaining {current_qty} qty.")
            
            # Close trade
            final_pnl = (ltp - entry_price) * current_qty
            df_book.at[idx, 'Realized_PnL'] += round(final_pnl, 2)
            df_book.at[idx, 'Unrealized_PnL'] = 0
            df_book.at[idx, 'Total_PnL'] = round(df_book.at[idx, 'Realized_PnL'], 2)
            df_book.at[idx, 'PnL_Percentage'] = round((df_book.at[idx, 'Total_PnL']/investment)*100, 2)
            
            df_book.at[idx, 'Status'] = 'CLOSED'
            df_book.at[idx, 'Exit_Price'] = ltp
            df_book.at[idx, 'Current_Quantity'] = 0
            df_book.at[idx, 'Exit_Reason'] = f'SL Hit (Stage {stage})'
            changes = True
            continue

        # 3. STATE MACHINE UPGRADES
        
        # Stage 0 -> 1: Target 8% -> SL to Cost
        if stage < 1 and pnl_pct >= paper_config.TARGET_1_PCT:
            logger.info(" -> Target 8% Hit! Moving SL to Cost.")
            df_book.at[idx, 'SL_Price'] = entry_price
            df_book.at[idx, 'Stage'] = 1
            changes = True
            current_sl = entry_price 
            stage = 1
            
        # Stage 1 -> 2: Target 10% -> Exit 50%, SL to 5% Profit
        if stage < 2 and pnl_pct >= paper_config.TARGET_2_PCT:
            logger.info(" -> Target 10% Hit! Booking 50% Profit, SL to 5% Profit.")
            qty_to_book = int(initial_qty * 0.5)
            booked_pnl = (ltp - entry_price) * qty_to_book
            
            df_book.at[idx, 'Realized_PnL'] += round(booked_pnl, 2)
            df_book.at[idx, 'Current_Quantity'] = current_qty - qty_to_book
            
            new_sl = entry_price * 1.05
            df_book.at[idx, 'SL_Price'] = round(new_sl, 2)
            df_book.at[idx, 'Stage'] = 2
            changes = True
            stage = 2
            current_qty = df_book.at[idx, 'Current_Quantity']

        # Stage 2 -> 3: Target 15% -> Exit 25% (Next), SL to 10% Profit
        if stage < 3 and pnl_pct >= paper_config.TARGET_3_PCT:
            logger.info(" -> Target 15% Hit! Booking 25% Profit, SL to 10% Profit.")
            qty_to_book = int(initial_qty * 0.25)
            booked_pnl = (ltp - entry_price) * qty_to_book
            
            df_book.at[idx, 'Realized_PnL'] += round(booked_pnl, 2)
            df_book.at[idx, 'Current_Quantity'] = current_qty - qty_to_book
            
            new_sl = entry_price * 1.10
            df_book.at[idx, 'SL_Price'] = round(new_sl, 2)
            df_book.at[idx, 'Stage'] = 3
            changes = True
            stage = 3
            current_qty = df_book.at[idx, 'Current_Quantity']
     
        # Stage 3/4 -> Dynamic: Trail SL at Low of last 3 days
        if pnl_pct > 0.15: 
             low_3d = get_3_day_low(symbol)
             if low_3d:
                if low_3d > current_sl:
                    logger.info(f" -> Upgrading SL to 3-Day Low: {low_3d}")
                    df_book.at[idx, 'SL_Price'] = low_3d
                    df_book.at[idx, 'Stage'] = 4 
                    changes = True

    if changes:
        save_trade_book(df_book)

def is_market_open():
    """Checks if NS Equity Market is open (Mon-Fri, 09:15 - 15:30)."""
    now = datetime.now()
    
    # Check Weekend
    if now.weekday() >= 5: # 5=Sat, 6=Sun
        return False
        
    # Check Time
    current_time = now.time()
    market_start = datetime.strptime("09:15", "%H:%M").time()
    market_end = datetime.strptime("15:30", "%H:%M").time()
    
    return market_start <= current_time <= market_end

def main():
    print("="*60)
    logger.info(" Paper Trading Simulator (Capital Based: 10k/Trade)")
    print("="*60)
    
    if not init_breeze():
        logger.error("Failed to initialize. Exiting.")
        return

    logger.info(" [Main] Startup... Monitoring Signals & Trades.")
    
    try:
        while True:
            # Uncomment for live market logic
            # if is_market_open():
            logger.info(f"\n--- {datetime.now().strftime('%H:%M:%S')} ---")
            process_new_signals()
            monitor_open_trades()
            logger.info(" [Sleep] Waiting 60 seconds...")
            time.sleep(60)
            # else:
            #     logger.info(f" [Market Closed] {datetime.now().strftime('%H:%M:%S')} - Sleeping for 5 mins...")
            #     time.sleep(300)
            
    except KeyboardInterrupt:
        logger.info("\n [Main] Stopped by User.")

if __name__ == "__main__":
    main()
