import logging
from typing import Dict
from datetime import date
import polars as pl
from utils.common import clean_cols, smart_date

# ==========================================
# MAIN TRANSFORMATION LOGIC
# ==========================================

def clean_and_enrich(
    txn_lf: pl.LazyFrame,
    loans_lf: pl.LazyFrame,
    accounts_lf: pl.LazyFrame,
    cust_lf: pl.LazyFrame,
    addr_lf: pl.LazyFrame,
    branches_lf: pl.LazyFrame,
    acc_types_lf: pl.LazyFrame,
    acc_stat_lf: pl.LazyFrame,
    txn_types_lf: pl.LazyFrame,
    loan_stat_lf: pl.LazyFrame,
    cust_types_lf: pl.LazyFrame
) -> Dict[str, pl.LazyFrame]:
    
    logging.info("Starting transformation pipeline...")

    # CLEANING
    clean_addr = clean_cols(addr_lf).with_columns([
        pl.col("street").str.strip_chars().fill_null("Unknown"),
        pl.col("city").str.strip_chars().fill_null("Unknown"),
        pl.col("country").str.strip_chars().fill_null("Unknown")
    ])

    clean_cust = clean_cols(cust_lf).with_columns([
        pl.col("firstname").str.strip_chars().fill_null("Unknown").str.to_titlecase(),
        pl.col("lastname").str.strip_chars().fill_null("Unknown").str.to_titlecase(),
        smart_date("dateofbirth").alias("dob")
    ]).with_columns([
        pl.concat_str([pl.col("firstname"), pl.col("lastname")], separator=" ").alias("full_name")
    ])

    clean_acc = clean_cols(accounts_lf).with_columns([
        pl.col("balance").cast(pl.Float64).fill_null(0.0),
        smart_date("openingdate").alias("open_date")
    ])

    clean_loans = clean_cols(loans_lf).with_columns([
        smart_date("startdate").alias("loan_start"),
        smart_date("estimatedenddate").alias("loan_end"),
        pl.col("principalamount").cast(pl.Float64).fill_null(0.0),
        pl.col("interestrate").cast(pl.Float64).fill_null(0.0)
    ]).filter(
        pl.col("loan_end") >= pl.col("loan_start")
    ).with_columns([
        # Partitioning Columns for Loans
        pl.col("loan_start").dt.year().alias("loan_year")
    ])

    clean_txn = clean_cols(txn_lf).unique(subset=["transactionid"]).with_columns([
        smart_date("transactiondate").alias("txn_date"),
        pl.col("amount").cast(pl.Float64).fill_null(0.0)
    ]).filter(
        pl.col("txn_date").is_not_null()
    ).filter(
        pl.col("txn_date") <= date.today()
    ).with_columns([
        # Partitioning Columns for Transactions
        pl.col("txn_date").dt.year().alias("txn_year"),
        pl.col("txn_date").dt.month().alias("txn_month")
    ])

    clean_branches = clean_cols(branches_lf).with_columns([
        pl.col("branchname").str.strip_chars()
    ])

    # Reference Cleaning
    ref_acc_type = clean_cols(acc_types_lf).rename({"typename": "account_type"})
    ref_acc_stat = clean_cols(acc_stat_lf).rename({"statusname": "account_status"})
    ref_txn_type = clean_cols(txn_types_lf).rename({"typename": "txn_type"})
    ref_loan_stat = clean_cols(loan_stat_lf).rename({"statusname": "loan_status"})
    ref_cust_type = clean_cols(cust_types_lf).rename({"typename": "customer_type"})

    # JOINING
    dim_customers = (
        clean_cust
        .join(ref_cust_type, on="customertypeid", how="left")
        .join(clean_addr, on="addressid", how="left")
        .select(["customerid", "full_name", "dob", "customer_type", "city", "country"])
    )

    dim_accounts = (
        clean_acc
        .join(ref_acc_type, on="accounttypeid", how="left")
        .join(ref_acc_stat, on="accountstatusid", how="left")
        .join(dim_customers, on="customerid", how="left")
    )

    # FACTS
    fact_transactions = (
        clean_txn
        .join(ref_txn_type, on="transactiontypeid", how="left")
        .join(dim_accounts, left_on="accountoriginid", right_on="accountid", how="left")
        .join(clean_branches, on="branchid", how="left")
        .drop(["transactiondate", "transactiontypeid", "branchid", "accountoriginid"])
    )

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