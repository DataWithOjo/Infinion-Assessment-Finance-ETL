import pytest
import polars as pl
from datetime import date
from scripts.transform import clean_and_enrich

# FIXTURES (Fake Data)
@pytest.fixture
def mock_inputs():
    """Creates minimal LazyFrames for all 11 inputs to test logic."""
    
    # Core Data
    txn = pl.LazyFrame({
        "TransactionID": [1], 
        "AccountOriginID": [100],
        "TransactionTypeID": [1],
        "BranchID": [10],
        "Amount": [50.0],
        "TransactionDate": ["2023-01-01 00:00:00"]
    })
    loans = pl.LazyFrame({
        "LoanID": [1], "AccountID": [100], "LoanStatusID": [1], 
        "PrincipalAmount": [1000.0], "InterestRate": [0.05],
        "StartDate": ["2023-01-01"], "EstimatedEndDate": ["2024-01-01"]
    })
    accts = pl.LazyFrame({
        "AccountID": [100], "CustomerID": [500], 
        "AccountTypeID": [1], "AccountStatusID": [1],
        "Balance": [100.0], "OpeningDate": ["2020-01-01"]
    })
    cust = pl.LazyFrame({
        "CustomerID": [500], "FirstName": [" john "], "LastName": ["doe"], # Dirty names
        "DateOfBirth": ["1990-01-01"], "AddressID": [50], "CustomerTypeID": [1]
    })
    addr = pl.LazyFrame({
        "AddressID": [50], "Street": ["123 Main"], "City": ["Lagos"], "Country": ["Nigeria"]
    })
    branches = pl.LazyFrame({
        "BranchID": [10], "BranchName": [" Main Branch "]
    })

    # Reference Data
    acc_type = pl.LazyFrame({"AccountTypeID": [1], "TypeName": ["Savings"]})
    acc_stat = pl.LazyFrame({"AccountStatusID": [1], "StatusName": ["Active"]})
    txn_type = pl.LazyFrame({"TransactionTypeID": [1], "TypeName": ["Deposit"]})
    loan_stat = pl.LazyFrame({"LoanStatusID": [1], "StatusName": ["Active"]})
    cust_type = pl.LazyFrame({"CustomerTypeID": [1], "TypeName": ["Individual"]})

    return (txn, loans, accts, cust, addr, branches, acc_type, acc_stat, txn_type, loan_stat, cust_type)

# TESTS

def test_clean_and_enrich_logic(mock_inputs):
    """Test if joins work and names are cleaned."""
    
    results = clean_and_enrich(*mock_inputs)
    
    df_txn = results['transactions'].collect() 
    
    assert df_txn.height == 1
    assert "full_name" in df_txn.columns
    assert df_txn["full_name"][0] == "John Doe" 
    assert df_txn["city"][0] == "Lagos"        
    assert df_txn["txn_type"][0] == "Deposit"  

def test_loan_logic_check(mock_inputs):
    """Test that loans with EndDate < StartDate are filtered out."""
    (txn, loans, *rest) = mock_inputs
    
    bad_loan = pl.LazyFrame({
        "LoanID": [2], "AccountID": [100], "LoanStatusID": [1],
        "PrincipalAmount": [1000.0], "InterestRate": [0.05],
        "StartDate": ["2023-01-01"], 
        "EstimatedEndDate": ["2022-01-01"]
    })
    
    results = clean_and_enrich(txn, bad_loan, *rest)
    
    df_loan = results['loans'].collect()
    
    assert df_loan.height == 0