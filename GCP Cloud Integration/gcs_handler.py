import os
import io
import logging
from google.cloud import storage
import pandas as pd

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class GCSHandler:
    def __init__(self, bucket_name=None):
        """
        Initialize GCS Connection.
        If bucket_name is None, attempts to read from Environment Variable 'GCS_BUCKET_NAME'.
        """
        self.client = None
        self.bucket = None
        self.bucket_name = bucket_name or os.getenv("GCS_BUCKET_NAME")

        if not self.bucket_name:
            logging.warning("No Bucket Name provided. GCS operations will fail unless bucket_name is passed explicitly.")
            return

        try:
            self.client = storage.Client()
            self.bucket = self.client.bucket(self.bucket_name)
            logging.info(f"Connected to GCS Bucket: {self.bucket_name}")
        except Exception as e:
            logging.error(f"Failed to connect to GCS: {e}")

    def upload_file(self, local_path, blob_name):
        """Uploads a local file to GCS."""
        if not self.bucket: return False
        
        try:
            blob = self.bucket.blob(blob_name)
            blob.upload_from_filename(local_path)
            logging.info(f"Uploaded {local_path} to gs://{self.bucket_name}/{blob_name}")
            return True
        except Exception as e:
            logging.error(f"Upload failed: {e}")
            return False

    def download_file(self, blob_name, local_path):
        """Downloads a file from GCS to local path."""
        if not self.bucket: return False
        
        try:
            blob = self.bucket.blob(blob_name)
            
            # Ensure local dir exists
            local_dir = os.path.dirname(local_path)
            if local_dir and not os.path.exists(local_dir):
                os.makedirs(local_dir)
                
            blob.download_to_filename(local_path)
            logging.info(f"Downloaded gs://{self.bucket_name}/{blob_name} to {local_path}")
            return True
        except Exception as e:
            logging.error(f"Download failed: {e}")
            return False

    def read_csv(self, blob_name):
        """Reads a CSV directly from GCS into a Pandas DataFrame."""
        if not self.bucket: return None
        
        try:
            blob = self.bucket.blob(blob_name)
            content = blob.download_as_string()
            return pd.read_csv(io.BytesIO(content))
        except Exception as e:
            logging.error(f"Failed to read CSV from GCS ({blob_name}): {e}")
            return None

    def write_csv(self, df, blob_name, index=False):
        """Writes a Pandas DataFrame directly to GCS as a CSV."""
        if not self.bucket: return False
        
        try:
            blob = self.bucket.blob(blob_name)
            blob.upload_from_string(df.to_csv(index=index), 'text/csv')
            logging.info(f"Saved DataFrame to gs://{self.bucket_name}/{blob_name}")
            return True
        except Exception as e:
            logging.error(f"Failed to write CSV to GCS ({blob_name}): {e}")
            return False

    def list_files(self, prefix=None):
        """Lists files in the bucket with a given prefix."""
        if not self.bucket: return []
        
        try:
            blobs = self.client.list_blobs(self.bucket_name, prefix=prefix)
            return [blob.name for blob in blobs]
        except Exception as e:
            logging.error(f"List files failed: {e}")
            return []

    def file_exists(self, blob_name):
        """Checks if a file exists in GCS."""
        if not self.bucket: return False
        blob = self.bucket.blob(blob_name)
        return blob.exists()

    def copy_file(self, source_blob_name, dest_blob_name):
        """Copies a file within the same bucket efficiently."""
        if not self.bucket: return False
        try:
            source_blob = self.bucket.blob(source_blob_name)
            self.bucket.copy_blob(source_blob, self.bucket, dest_blob_name)
            logging.info(f"Copied {source_blob_name} to {dest_blob_name}")
            return True
        except Exception as e:
            logging.error(f"Copy failed: {e}")
            return False


# Test usage (if run directly)
if __name__ == "__main__":
    # Mock behavior for local testing if env var is missing
    print("GCSHandler Module. Import this in other scripts.")
