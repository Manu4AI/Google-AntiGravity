import os
import sys
import logging
import time
from breeze_connect import BreezeConnect
import auto_login
import paper_config

# Setup basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting Debug Script...")

    # 1. Login
    session_token = auto_login.get_session_token()
    if not session_token:
        logger.error("Failed to get session token.")
        return

    breeze = BreezeConnect(api_key=paper_config.API_KEY)
    breeze.generate_session(api_secret=paper_config.SECRET_KEY, session_token=session_token)
    logger.info("Breeze Session Initialized.")
    
    # 2. Test specific symbols
    symbols = ["GRASIM", "UPL", "TECHM", "SIEMENS"]
    
    for sym in symbols:
        logger.info(f"\n--- Testing: {sym} ---")
        try:
            data = breeze.get_quotes(
                stock_code=sym, 
                exchange_code="NSE", 
                product_type="cash", 
                right="others", 
                strike_price="0"
            )
            
            if data and 'Success' in data and data['Success']:
                logger.info(f"SUCCESS: {data['Success']}")
            else:
                 logger.warning(f"FAILURE: {data}")
            
            time.sleep(2) # Delay to be safe

        except Exception as e:
            logger.error(f"EXCEPTION: {e}")

if __name__ == "__main__":
    main()
