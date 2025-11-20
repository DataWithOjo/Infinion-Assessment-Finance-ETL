import logging
import time
import sys
from pathlib import Path

from scripts.extract import scan_dataset
from scripts.transform import clean_and_enrich
from scripts.load import save_to_parquet

# CONFIGURATION

DATA_SOURCES = {
    "transactions": "transactions.csv",
    "loans": "loans.csv",
    "accounts": "accounts.csv",
    "customers": "customers.csv",
    "addresses": "addresses.csv",
    "branches": "branches.csv",
    "account_types": "account_types.csv",
    "account_statuses": "account_statuses.csv",
    "transaction_types": "transaction_types.csv",
    "loan_statuses": "loan_statuses.csv",
    "customer_types": "customer_types.csv"
}

# LOGGING SETUP

def setup_logging():
    """Configures the logging format and level."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

# MAIN PIPELINE EXECUTION

def main():
    """
    Orchestrates the ETL Pipeline: Extract -> Transform -> Load.
    """
    setup_logging()
    start_total = time.time()

    logging.info("   STARTING FINANCE DATA PIPELINE (OBT)   ")

    try:
        # EXTRACT (Lazy Loading)

        logging.info(">>> EXTRACTING RAW DATA")
        
        # Helper to get LazyFrame by key
        def get_source(key):
            return scan_dataset(DATA_SOURCES[key])

        # Extract Core Entities
        txn_lf = get_source("transactions")
        loans_lf = get_source("loans")
        accts_lf = get_source("accounts")
        cust_lf = get_source("customers")
        addr_lf = get_source("addresses")
        branches_lf = get_source("branches")

        # Extract Reference Tables
        acc_types_lf = get_source("account_types")
        acc_stats_lf = get_source("account_statuses")
        txn_types_lf = get_source("transaction_types")
        loan_stats_lf = get_source("loan_statuses")
        cust_types_lf = get_source("customer_types")

        logging.info(f"    Extraction plan created for {len(DATA_SOURCES)} sources.")

        # TRANSFORM (Polars Query Optimization)

        logging.info(">>> TRANSFORMING & DENORMALIZING")
        
        etl_results = clean_and_enrich(
            txn_lf, loans_lf, accts_lf, cust_lf, addr_lf, branches_lf,
            acc_types_lf, acc_stats_lf, txn_types_lf, loan_stats_lf, cust_types_lf
        )
        
        logging.info("    Transformation logic applied. Ready to stream.")

        # LOAD (Streaming to Parquet)

        logging.info(">>> PHASE 3: LOADING TO ANALYTICS STORE")

        # Save the "Wide Tables"
        save_to_parquet(etl_results['transactions'], "fact_transactions.parquet")
        save_to_parquet(etl_results['loans'], "fact_loans.parquet")

        # SUMMARY

        duration = time.time() - start_total

        logging.info(f"   PIPELINE SUCCESSFUL IN {duration:.2f} SECONDS")
        logging.info("   Outputs available in: data/processed/")


    except FileNotFoundError as fnf_error:
        logging.error(f"CRITICAL: Missing file. {fnf_error}")
        logging.error("Please ensure all CSV files are in 'data/raw/'")
        sys.exit(1)
        
    except Exception as e:
        logging.error(f"CRITICAL: Pipeline failed. Reason: {e}")
        # Print full traceback for debugging
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()