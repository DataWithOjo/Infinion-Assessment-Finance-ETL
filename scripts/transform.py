import polars as pl
import logging
from datetime import date

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
):
    logging.info("Starting transformation pipeline...")

    # Converts all column names to lowercase
    def clean_cols(lf):
        return lf.select(pl.all().name.to_lowercase())

    # Handles "2018-06-12 00:00:00.000000" and mixed formats
    def smart_date(col_name):
        return pl.coalesce([
            pl.col(col_name).str.to_datetime("%Y-%m-%d %H:%M:%S.%f", strict=False),
            pl.col(col_name).str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False),
            pl.col(col_name).str.to_datetime("%d-%m-%Y %H:%M:%S", strict=False),
            pl.col(col_name).str.to_datetime("%Y-%m-%d", strict=False)
        ])

    # Clean ADDRESSES
    clean_addr = clean_cols(addr_lf).with_columns([
        pl.col("street").str.strip_chars().fill_null("Unknown Street"),
        pl.col("city").str.strip_chars().fill_null("Unknown City"),
        pl.col("country").str.strip_chars().fill_null("Unknown Country")
    ])

    # Clean CUSTOMERS
    clean_cust = clean_cols(cust_lf).with_columns([
        pl.col("firstname").str.strip_chars().fill_null("Unknown"),
        pl.col("lastname").str.strip_chars().fill_null("Unknown"),
        smart_date("dateofbirth").alias("dob")
    ]).with_columns([
        pl.concat_str([pl.col("firstname"), pl.col("lastname")], separator=" ").alias("full_name")
    ])

    # Clean ACCOUNTS
    clean_acc = clean_cols(accounts_lf).with_columns([
        pl.col("balance").cast(pl.Float64).fill_null(0.0),
        smart_date("openingdate").alias("open_date")
    ])

    # Clean LOANS
    clean_loans = clean_cols(loans_lf).with_columns([
        smart_date("startdate").alias("loan_start"),
        smart_date("estimatedenddate").alias("loan_end"),
        pl.col("principalamount").cast(pl.Float64).fill_null(0.0),
        pl.col("interestrate").cast(pl.Float64).fill_null(0.0)
    ]).filter(
        pl.col("loan_end") >= pl.col("loan_start")
    )

    # Clean TRANSACTIONS
    clean_txn = clean_cols(txn_lf).unique(subset=["transactionid"]).with_columns([
        smart_date("transactiondate").alias("txn_date"),
        pl.col("amount").cast(pl.Float64).fill_null(0.0)
    ]).filter(
        pl.col("txn_date").is_not_null()
    ).filter(
        pl.col("txn_date") <= date.today()
    )

    # Clean BRANCHES
    clean_branches = clean_cols(branches_lf).with_columns([
        pl.col("branchname").str.strip_chars()
    ])

    # Clean REFERENCES
    ref_acc_type = clean_cols(acc_types_lf).rename({"typename": "account_type"})
    ref_acc_stat = clean_cols(acc_stat_lf).rename({"statusname": "account_status"})
    ref_txn_type = clean_cols(txn_types_lf).rename({"typename": "txn_type"})
    ref_loan_stat = clean_cols(loan_stat_lf).rename({"statusname": "loan_status"})
    ref_cust_type = clean_cols(cust_types_lf).rename({"typename": "customer_type"})
    

    logging.info("Linking dimensions...")

    # CUSTOMER DIMENSION
    dim_customers = (
        clean_cust
        .join(ref_cust_type, on="customertypeid", how="left")
        .join(clean_addr, on="addressid", how="left")
        .select(["customerid", "full_name", "dob", "customer_type", "city", "country"])
    )

    # ACCOUNT DIMENSION
    dim_accounts = (
        clean_acc
        .join(ref_acc_type, on="accounttypeid", how="left")
        .join(ref_acc_stat, on="accountstatusid", how="left")
        .join(dim_customers, on="customerid", how="left")
    )

    
    logging.info("Building Fact Tables...")

    # TRANSACTIONS 
    fact_transactions = (
        clean_txn
        .join(ref_txn_type, on="transactiontypeid", how="left")
        .join(dim_accounts, left_on="accountoriginid", right_on="accountid", how="left")
        .join(clean_branches, on="branchid", how="left")
        .drop(["transactiondate", "transactiontypeid", "branchid", "accountoriginid"])
    )

    # LOANS
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