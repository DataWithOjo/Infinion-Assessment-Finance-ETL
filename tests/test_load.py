import pytest
import logging
from unittest.mock import patch, MagicMock
import polars as pl
from scripts.load import save_to_parquet

# HAPPY PATH TEST
@patch("scripts.load.pl.LazyFrame.sink_parquet")
@patch("pathlib.Path.mkdir")
@patch("pathlib.Path.stat")
def test_save_to_parquet_success(mock_stat, mock_mkdir, mock_sink, caplog):
    """
    Verifies that save_to_parquet calls sink_parquet and logs success.
    """
    # pytest to capture INFO logs
    caplog.set_level(logging.INFO)

    # ARRANGE
    # Mock file size (5MB)
    mock_stat_obj = MagicMock()
    mock_stat_obj.st_size = 5 * 1024 * 1024
    mock_stat.return_value = mock_stat_obj
    
    lf = pl.LazyFrame({"col": [1, 2, 3]})

    # ACT
    save_to_parquet(lf, "test_file.parquet")

    # ASSERT
    mock_mkdir.assert_called_once()
    mock_sink.assert_called_once()

    # Verify arguments
    _, kwargs = mock_sink.call_args
    assert kwargs["compression"] == "snappy"
    assert kwargs["row_group_size"] == 100_000
    
    # Verify logging
    assert "SUCCESS: Saved test_file.parquet" in caplog.text
    assert "5.00 MB" in caplog.text

# ERROR PATH TEST
@patch("scripts.load.pl.LazyFrame.sink_parquet")
@patch("pathlib.Path.mkdir")
def test_save_to_parquet_failure(mock_mkdir, mock_sink, caplog):
    """
    Verifies that exceptions are caught and logged as CRITICAL ERROR.
    """
    # ARRANGE
    mock_sink.side_effect = PermissionError("Access Denied")
    lf = pl.LazyFrame({"col": [1]})

    # ACT & ASSERT
    with pytest.raises(PermissionError):
        save_to_parquet(lf, "fail.parquet")
    
    assert "CRITICAL ERROR" in caplog.text
    assert "Access Denied" in caplog.text