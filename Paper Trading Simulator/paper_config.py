# Paper Trading Simulator Configuration

# --- ICICI Breeze API Keys (Existing) ---
API_KEY = "1%az344xABm70887K06g6Du76C59L197"
SECRET_KEY = "sz9KC97V1J4t795U12933063u5hl4&38"

# --- Auto-Login Credentials ---
USER_ID = "9532394326"
PASSWORD = "Antics@7180"
TOTP_KEY = "GVVUUSLJGRBWEVCTKZNECOBZPE"

# --- Trading Rules ---
PER_TRADE_CAPITAL = 10000  # Invest approx 10k per trade
INITIAL_SL_PCT = 0.05      # 5% Hard Stop Loss

# Milestones
TARGET_1_PCT = 0.08        # 8% Target -> Move SL to Cost
TARGET_2_PCT = 0.10        # 10% Target -> Book 50%, Move SL to 5% Profit
TARGET_3_PCT = 0.15        # 15% Target -> Book 25%, Move SL to 10% Profit
                           # > 15% -> Trail SL at "Low of last 3 days"
