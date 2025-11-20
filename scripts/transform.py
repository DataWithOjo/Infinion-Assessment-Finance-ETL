import logging
from typing import Dict
from datetime import date
import polars as pl

# HELPER FUNCTIONS

def _clean_cols(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Standardizes column names to lowercase to prevent case-sensitivity issues.
    """
    return lf.select(pl.all().name.to_lowercase())

def _smart_date(col_name: str) -> pl.Expr:
    """
    Parses date columns with mixed formats (ISO, DD-MM-YYYY, etc.).
    Returns a Polars Expression.
    """
    return pl.coalesce([
        pl.col(col_name).str.to_datetime("%Y-%m-%d %H:%M:%S.%f", strict=False),
        pl.col(col_name).str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False),
        pl.col(col_name).str.to_datetime("%d-%m-%Y %H:%M:%S", strict=False),
        pl.col(col_name).str.to_datetime("%Y-%m-%d", strict=False)
    ])

# MAIN TRANSFORMATION LOGIC

def clean_and_enrich(
    # Raw LazyFrames
    txn_lf: pl.LazyFrame,
    loans_lf: pl.LazyFrame,
    accounts_lf: pl.LazyFrame,
    cust_lf: pl.LazyFrame,
    addr_lf: pl.LazyFrame,
    branches_lf: pl.LazyFrame,
    # Reference LazyFrames
    acc_types_lf: pl.LazyFrame,
    acc_stat_lf: pl.LazyFrame,
    txn_types_lf: pl.LazyFrame,
    loan_stat_lf: pl.LazyFrame,
    cust_types_lf: pl.LazyFrame
) -> Dict[str, pl.LazyFrame]:
    """
    Orchestrates the cleaning, normalization, and joining of raw financial data.
    
    Implements the 'Wide Table' (OBT) architecture by denormalizing 
    Accounts, Customers, and Reference data into the Transaction and Loan facts.

    Returns:
        Dict[str, pl.LazyFrame]: A dictionary containing the final 'transactions' 
                                 and 'loans' datasets ready for loading.
    """
    logging.info("Starting transformation pipeline...")

    # CLEANING INDIVIDUAL TABLES

    # Clean ADDRESSES
    clean_addr = _clean_cols(addr_lf).with_columns([
        pl.col("street").str.strip_chars().fill_null("Unknown Street"),
        pl.col("city").str.strip_chars().fill_null("Unknown City"),
        pl.col("country").str.strip_chars().fill_null("Unknown Country")
    ])

    # Clean CUSTOMERS
    # Standardize names and Parse DOB using smart date logic
    clean_cust = _clean_cols(cust_lf).with_columns([
        pl.col("firstname").str.strip_chars().fill_null("Unknown"),
        pl.col("lastname").str.strip_chars().fill_null("Unknown"),
        _smart_date("dateofbirth").alias("dob")
    ]).with_columns([
        pl.concat_str([pl.col("firstname"), pl.col("lastname")], separator=" ").alias("full_name")
    ])

    # Clean ACCOUNTS
    # Ensure numeric balance
    clean_acc = _clean_cols(accounts_lf).with_columns([
        pl.col("balance").cast(pl.Float64).fill_null(0.0),
        _smart_date("openingdate").alias("open_date")
    ])

    # Clean LOANS
    # Logical check: Start Date vs End Date
    clean_loans = _clean_cols(loans_lf).with_columns([
        _smart_date("startdate").alias("loan_start"),
        _smart_date("estimatedenddate").alias("loan_end"),
        pl.col("principalamount").cast(pl.Float64).fill_null(0.0),
        pl.col("interestrate").cast(pl.Float64).fill_null(0.0)
    ]).filter(
        pl.col("loan_end") >= pl.col("loan_start")
    )

    # Clean TRANSACTIONS
    # Deduplicate IDs and Remove future dates
    clean_txn = _clean_cols(txn_lf).unique(subset=["transactionid"]).with_columns([
        _smart_date("transactiondate").alias("txn_date"),
        pl.col("amount").cast(pl.Float64).fill_null(0.0)
    ]).filter(
        pl.col("txn_date").is_not_null()
    ).filter(
        pl.col("txn_date") <= date.today()
    )

    # Clean BRANCHES
    clean_branches = _clean_cols(branches_lf).with_columns([
        pl.col("branchname").str.strip_chars()
    ])

    # Clean REFERENCES
    # Rename generic 'TypeName' columns to be specific
    ref_acc_type = _clean_cols(acc_types_lf).rename({"typename": "account_type"})
    ref_acc_stat = _clean_cols(acc_stat_lf).rename({"statusname": "account_status"})
    ref_txn_type = _clean_cols(txn_types_lf).rename({"typename": "txn_type"})
    ref_loan_stat = _clean_cols(loan_stat_lf).rename({"statusname": "loan_status"})
    ref_cust_type = _clean_cols(cust_types_lf).rename({"typename": "customer_type"})


    # DIMENSIONAL MODELING

    logging.info("Linking dimensions...")

    # Create Rich Customer Dimension
    dim_customers = (
        clean_cust
        .join(ref_cust_type, on="customertypeid", how="left")
        .join(clean_addr, on="addressid", how="left")
        .select(["customerid", "full_name", "dob", "customer_type", "city", "country"])
    )

    # Create Rich Account Dimension (links to Customers)
    dim_accounts = (
        clean_acc
        .join(ref_acc_type, on="accounttypeid", how="left")
        .join(ref_acc_stat, on="accountstatusid", how="left")
        .join(dim_customers, on="customerid", how="left")
    )

    # FACT TABLE CREATION

    logging.info("Building Fact Tables...")

    # Transactions (Joined with Accounts and Branches)
    fact_transactions = (
        clean_txn
        .join(ref_txn_type, on="transactiontypeid", how="left")
        .join(dim_accounts, left_on="accountoriginid", right_on="accountid", how="left")
        .join(clean_branches, on="branchid", how="left")
        .drop(["transactiondate", "transactiontypeid", "branchid", "accountoriginid"])
    )

    # Loans (Joined with Accounts)
    fact_loans = (
        clean_loans
        .join(ref_loan_stat, on="loanstatusid", how="left")
        .join(dim_accounts, on="accountid", how="left")
        .drop(["startdate", "estimatedenddate", "loanstatusid", "accountid"])
    )

    return {
        "transactions": fact_transactions,
        "loans": fact_loans
    }