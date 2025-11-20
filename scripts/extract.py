import logging
from pathlib import Path
import polars as pl

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"

def scan_dataset(file_name: str) -> pl.LazyFrame:
    """
    Lazily scans a CSV file from the configured raw data directory.

    Args:
        file_name (str): The name of the CSV file (e.g., 'transactions.csv').

    Returns:
        pl.LazyFrame: A Polars LazyFrame representing the dataset plan.

    Raises:
        FileNotFoundError: If the file does not exist at the constructed path.
    """

    file_path = RAW_DATA_DIR / file_name
    
    if not file_path.exists():
        error_msg = f"Source file missing: {file_path}"
        logging.error(error_msg)
        raise FileNotFoundError(error_msg)

    try:
        logging.info(f"Scanning source: {file_name}")
        return pl.scan_csv(file_path)
        
    except Exception as e:
        logging.error(f"Failed to scan {file_name}. Reason: {e}")
        raise