import sys
import logging

# Setup basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    logger.info("Checking for yfinance...")
    try:
        import yfinance as yf
        logger.info("yfinance imported successfully.")
    except ImportError:
        logger.error("yfinance module NOT found. Please install it using 'pip install yfinance'.")
        return

    symbols = ["ADANIENT.NS", "UPL.NS", "GRASIM.NS"]
    
    for sym in symbols:
        logger.info(f"Fetching data for {sym}...")
        try:
            ticker = yf.Ticker(sym)
            # Try fetching current price
            # fast_info is faster
            price = ticker.fast_info.last_price
            logger.info(f"Price for {sym}: {price}")
            
            # Also try history just in case
            hist = ticker.history(period="1d")
            if not hist.empty:
                 logger.info(f"History for {sym}:\n{hist.tail(1)}")
            else:
                 logger.warning(f"History empty for {sym}")
                 
        except Exception as e:
            logger.error(f"Failed to fetch {sym}: {e}")

if __name__ == "__main__":
    main()
