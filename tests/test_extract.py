import pytest
from unittest.mock import patch, MagicMock
import polars as pl
from scripts.extract import scan_dataset

# HAPPY PATH: When the file exists

@patch("scripts.extract.pl.scan_csv")
@patch("pathlib.Path.exists")  
def test_scan_dataset_success(mock_exists, mock_scan_csv):
    """
    Test that scan_dataset returns a LazyFrame when file exists.
    """
    mock_exists.return_value = True 
    
    dummy_lf = pl.LazyFrame({"a": [1, 2, 3]}) 
    mock_scan_csv.return_value = dummy_lf

    result = scan_dataset("transactions.csv")

    assert isinstance(result, pl.LazyFrame)
    mock_exists.assert_called_once() 
    mock_scan_csv.assert_called_once() 

# ERROR PATH: When the file is missing
@patch("pathlib.Path.exists") 
def test_scan_dataset_file_not_found(mock_exists):
    """
    Test that scan_dataset raises FileNotFoundError when file is missing.
    """

    mock_exists.return_value = False 

    with pytest.raises(FileNotFoundError) as excinfo:
        scan_dataset("ghost_file.csv")
    
    assert "ghost_file.csv" in str(excinfo.value)

# LOGGING CHECK: Verify we are logging errors
@patch("pathlib.Path.exists") 
def test_scan_dataset_logs_error(mock_exists, caplog):
    """
    Test that the function logs an error message before raising exception.
    'caplog' is a pytest fixture that captures logging.
    """
    mock_exists.return_value = False

    with pytest.raises(FileNotFoundError):
        scan_dataset("missing.csv")

    assert "Source file missing" in caplog.text
