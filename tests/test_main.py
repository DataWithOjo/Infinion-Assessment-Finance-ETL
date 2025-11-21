import pytest
import logging
from unittest.mock import patch
import sys
from main import main

# ==========================================
# HAPPY PATH: Full Pipeline Success
# ==========================================
@patch("main.validate_schema")   
@patch("main.save_to_parquet")
@patch("main.clean_and_enrich")
@patch("main.scan_dataset")
def test_main_success(mock_extract, mock_transform, mock_load, mock_validate, caplog):
    """
    Verifies that main() correctly orchestrates the ETL flow.
    """
    # Capture INFO logs so we can see "PIPELINE SUCCESSFUL"
    caplog.set_level(logging.INFO)

    # ARRANGE
    # We return a string, which is fine because we mocked validation & transform
    mock_extract.return_value = "dummy_lazyframe"
    
    mock_transform.return_value = {
        "transactions": "clean_txn_lf",
        "loans": "clean_loan_lf"
    }

    # ACT
    main()

    # ASSERT
    # Verify Extraction (11 files)
    assert mock_extract.call_count == 11
    
    # Verify Transformation
    mock_transform.assert_called_once()
    
    # Verify Loading (Transactions + Loans)
    assert mock_load.call_count == 2
    
    # --- Update expectations to match Partitioning logic ---
    # Transaction load has partition_cols
    mock_load.assert_any_call(
        "clean_txn_lf", 
        "fact_transactions", 
        partition_cols=["txn_year", "txn_month"]
    )
    
    # Loans load is standard
    mock_load.assert_any_call("clean_loan_lf", "fact_loans.parquet")

    # Verify Success Log
    assert "PIPELINE SUCCESSFUL" in caplog.text

# ==========================================
# ERROR PATH: Missing File
# ==========================================
@patch("main.scan_dataset")
@patch("sys.exit")
def test_main_missing_file_error(mock_exit, mock_extract, caplog):
    """
    Verifies that the pipeline catches FileNotFoundError and exits.
    """
    mock_extract.side_effect = FileNotFoundError("transactions.csv not found")

    main()

    assert "CRITICAL: Missing file" in caplog.text
    mock_exit.assert_called_once_with(1)

# ==========================================
# ERROR PATH: Generic Crash
# ==========================================
@patch("main.scan_dataset")
@patch("sys.exit")
def test_main_generic_crash(mock_exit, mock_extract, caplog):
    """
    Verifies that unexpected errors are caught and logged.
    """
    mock_extract.side_effect = Exception("Out of Memory")

    main()

    assert "CRITICAL: Pipeline failed" in caplog.text
    mock_exit.assert_called_once_with(1)