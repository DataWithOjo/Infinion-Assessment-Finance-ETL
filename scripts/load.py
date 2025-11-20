import logging
from pathlib import Path
import polars as pl

def save_to_parquet(lf: pl.LazyFrame, filename: str) -> None:
    """
    Executes the LazyFrame transformation plan and streams the result to a Parquet file.

    Key Optimization:
        Uses 'sink_parquet' instead of 'collect().write_parquet()'. 
        This allows processing datasets larger than available RAM by streaming batches.

    Args:
        lf (pl.LazyFrame): The unexecuted Polars LazyFrame.
        filename (str): Output filename (e.g., 'fact_transactions.parquet').

    Raises:
        Exception: Propagates any error that occurs during the write process.
    """

    processed_dir = Path("data") / "processed"
    output_path = processed_dir / filename

    processed_dir.mkdir(parents=True, exist_ok=True)

    logging.info(f"Stream-loading data to {output_path}...")

    try:
        lf.sink_parquet(
            output_path,
            compression="snappy",
            row_group_size=100_000
        )

        # Performance Logging
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        logging.info(f"SUCCESS: Saved {filename} | Size: {file_size_mb:.2f} MB")

    except Exception as e:
        logging.error(f"CRITICAL ERROR: Failed to load {filename}. Reason: {e}")
        raise