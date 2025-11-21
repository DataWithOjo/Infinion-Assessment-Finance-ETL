import logging
from pathlib import Path
from typing import List, Optional
import polars as pl

def save_to_parquet(
    lf: pl.LazyFrame, 
    filename: str, 
    partition_cols: Optional[List[str]] = None
) -> None:
    """
    Executes the LazyFrame plan and saves the result to Parquet.
    Supports optional Hive-style partitioning.

    Args:
        lf (pl.LazyFrame): The unexecuted Polars LazyFrame.
        filename (str): Output filename (e.g., 'fact_transactions.parquet'). 
                        If partitioning is used, this becomes the directory name.
        partition_cols (List[str]): Columns to partition by (e.g., ['year', 'month']).
    """
    # Setup Paths
    processed_dir = Path("data") / "processed"
    output_path = processed_dir / filename

    # Ensure parent directory exists
    processed_dir.mkdir(parents=True, exist_ok=True)

    logging.info(f"Starting load for {filename}...")

    try:
        if partition_cols:
            logging.info(f"Writing partitioned dataset to {output_path} based on {partition_cols}...")
            
            # We strip the extension for the folder name if it exists
            folder_path = output_path.with_suffix('') 
            
            lf.collect().write_parquet(
                folder_path,
                compression="snappy",
                use_pyarrow=True,
                pyarrow_options={"partition_cols": partition_cols}
            )
            logging.info(f"SUCCESS: Partitioned data saved to {folder_path}/")
            
        else:
            # STANDARD STREAMING WRITE (Single File)
            # Uses sink_parquet for maximum memory efficiency on large single files
            lf.sink_parquet(
                output_path,
                compression="snappy",
                row_group_size=100_000
            )
            
            # Check size
            file_size_mb = output_path.stat().st_size / (1024 * 1024)
            logging.info(f"SUCCESS: Saved {filename} | Size: {file_size_mb:.2f} MB")

    except Exception as e:
        logging.error(f"CRITICAL ERROR: Failed to load {filename}. Reason: {e}")
        raise