import polars as pl
import logging
import os

def save_to_parquet(lf: pl.LazyFrame, filename: str):
    """
    Executes the LazyFrame transformation plan and streams the result to a Parquet file.
    
    Key Optimization:
    Uses 'sink_parquet' instead of 'collect().write_parquet()'. 
    This allows processing datasets larger than available RAM by streaming batches.
    
    Args:
        lf (pl.LazyFrame): The unexecuted Polars LazyFrame.
        filename (str): Output filename (e.g., 'fact_transactions.parquet').
    """

    output_dir = os.path.join("data", "processed")
    output_path = os.path.join(output_dir, filename)
    
    # Ensure the 'processed' folder exists
    os.makedirs(output_dir, exist_ok=True)
    
    logging.info(f"Stream-loading data to {output_path}...")
    
    try:
        lf.sink_parquet(
            output_path, 
            compression="snappy", 
            row_group_size=100_000
        )
        
        # Performance Logging
        # verifying file creation and logging its size
        file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
        logging.info(f"SUCCESS: Saved {filename} | Size: {file_size_mb:.2f} MB")
        
    except Exception as e:
        logging.error(f"CRITICAL ERROR: Failed to load {filename}. Reason: {e}")
        raise