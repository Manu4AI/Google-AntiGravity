import pandas as pd
import logging
from gcs_handler import GCSHandler

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CloudPriceAdjuster:
    def __init__(self):
        self.gcs = GCSHandler()
        self.adj_file = "config/Calculated_Adjustments.csv"
        self.script_prefix = "script_data"
        self.output_prefix = "adjusted_data"
        
    def run(self):
        # 1. Load Adjustments
        if not self.gcs.file_exists(self.adj_file):
            logging.error("Adjustment file missing.")
            return

        adj_df = self.gcs.read_csv(self.adj_file)
        if adj_df is None or adj_df.empty:
            logging.warning("No adjustments to apply.")
            # Just copy all files? Yes.
            adj_symbols = []
        else:
            adj_df['ex_date'] = pd.to_datetime(adj_df['ex_date'])
            adj_symbols = adj_df['symbol'].unique().tolist()
            
        # 2. List Raw Files
        files = self.gcs.list_files(prefix=self.script_prefix)
        csv_files = [f for f in files if f.endswith('.csv')]
        
        logging.info(f"Found {len(csv_files)} files. Adjusting for {len(adj_symbols)} symbols...")
        
        processed = 0
        adjusted = 0
        
        for file_path in csv_files:
            # file_path = script_data/RELIANCE.csv
            filename = file_path.split('/')[-1]
            symbol = filename.replace('.csv', '')
            dest_path = f"{self.output_prefix}/{filename}"
            
            try:
                if symbol not in adj_symbols:
                    # Direct Copy (Efficient)
                    self.gcs.copy_file(file_path, dest_path)
                else:
                    # Apply Logic
                    df = self.gcs.read_csv(file_path)
                    if df is None: continue
                    
                    df['date'] = pd.to_datetime(df['date'])
                    df = df.sort_values('date')
                    
                    # Filter adjustments for this symbol
                    sym_adjs = adj_df[adj_df['symbol'] == symbol]
                    
                    for _, row in sym_adjs.iterrows():
                        ex_date = row['ex_date']
                        factor = float(row['price_multiplier'])
                        
                        mask = df['date'] < ex_date
                        if mask.any():
                            # Adjust Prices
                            cols = ['open_price', 'high_price', 'low_price', 'close_price']
                            for c in cols:
                                if c in df.columns:
                                    df.loc[mask, c] = df.loc[mask, c] * factor
                            
                            # Adjust Volume (Inverse)
                            vol_cols = ['ttl_trd_qnty']
                            for c in vol_cols:
                                if c in df.columns:
                                     # Avoid float/int warnings
                                     df[c] = df[c].astype(float)
                                     df.loc[mask, c] = df.loc[mask, c] / factor
                    
                    # Round
                    price_cols = ['open_price', 'high_price', 'low_price', 'close_price', 'avg_price']
                    for c in price_cols:
                        if c in df.columns:
                            df[c] = df[c].round(2)
                            
                    self.gcs.write_csv(df, dest_path)
                    adjusted += 1
            
            except Exception as e:
                logging.error(f"Error {symbol}: {e}")
            
            processed += 1
            if processed % 50 == 0:
                logging.info(f"Processed {processed}...")

        logging.info(f"Complete. Processed: {processed}, Adjusted: {adjusted}")

if __name__ == "__main__":
    app = CloudPriceAdjuster()
    app.run()
