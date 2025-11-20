import logging
import polars as pl
from typing import List, Optional

class DataValidationError(Exception):
    """Custom exception raised when data validation fails."""
    pass

def validate_schema(lf: pl.LazyFrame, expected_columns: List[str], file_name: str = "Unknown") -> pl.LazyFrame:
    """
    Validates that the LazyFrame contains the expected columns.

    Args:
        lf (pl.LazyFrame): The dataset to check.
        expected_columns (List[str]): List of column names that MUST exist.
        file_name (str): Name of the file for logging purposes.

    Returns:
        pl.LazyFrame: The original LazyFrame if validation passes.

    Raises:
        DataValidationError: If required columns are missing.
    """
    try:
        current_columns = lf.collect_schema().names()
    except Exception as e:
        logging.error(f"Failed to read schema for {file_name}: {e}")
        raise DataValidationError(f"Could not read schema for {file_name}") from e

    missing_columns = [col for col in expected_columns if col not in current_columns]

    if missing_columns:
        error_msg = (
            f"Schema Validation Failed for '{file_name}'. "
            f"Missing columns: {missing_columns}. "
            f"Found: {current_columns}"
        )
        logging.error(error_msg)
        raise DataValidationError(error_msg)
    
    logging.info(f"Schema validation passed for '{file_name}'")
    return lf

def validate_data_quality(lf: pl.LazyFrame, file_name: str) -> pl.LazyFrame:
    """
    Runs basic data quality checks suitable for financial data.
    
    Checks implemented:
    - Transactions/Loans: Ensure amounts are not negative.
    
    Args:
        lf (pl.LazyFrame): The dataset to validate.
        file_name (str): Context name (e.g., 'transactions', 'loans').

    Returns:
        pl.LazyFrame: The LazyFrame with invalid rows filtered out (or just validated).
    """
    
    schema = lf.collect_schema().names()
    
    amount_cols = [col for col in ["Amount", "PrincipalAmount", "Balance"] if col in schema]
    
    if amount_cols:
        for col in amount_cols:

            lf = lf.filter(pl.col(col) >= 0)
            logging.info(f"Applied non-negative filter for column '{col}' in {file_name}")

    return lf