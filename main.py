import logging
import time
import sys
from pathlib import Path


from scripts.extract import scan_dataset
from scripts.transform import clean_and_enrich
from scripts.load import save_to_parquet

from scripts.validate import validate_schema, validate_data_quality

# ==========================================
# CONFIGURATION
# ==========================================

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

EXPECTED_SCHEMAS = {
    "transactions": ["TransactionID", "AccountOriginID", "Amount", "TransactionDate"],
    "loans": ["LoanID", "AccountID", "PrincipalAmount", "StartDate"]
}

# ==========================================
# LOGGING SETUP (Dual Logging)
# ==========================================

def setup_logging():
    """
    Configures logging to write to BOTH console and a timestamped file.
    """
    # Create a logs directory if it doesn't exist
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    # Define the log filename
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    log_filename = log_dir / f"etl_run_{timestamp}.log"

    # Configure the logger
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_filename), 
            logging.StreamHandler(sys.stdout)  
        ]
    )
    
    logging.info(f"Logging initialized. Writing to: {log_filename}")

# ==========================================
# MAIN PIPELINE EXECUTION
# ==========================================

def main():
    """
    Orchestrates the ETL Pipeline: Extract -> Transform -> Load.
    """
    setup_logging()
    start_total = time.time()

    logging.info("==========================================")
    logging.info("   STARTING FINANCE DATA PIPELINE (OBT)   ")
    logging.info("==========================================")

    try:
        # ---------------------------------------------------------
        # EXTRACT (Lazy Loading)
        # ---------------------------------------------------------
        logging.info(">>> PHASE 1: EXTRACTING RAW DATA")
        
        def get_source(key):
            lf = scan_dataset(DATA_SOURCES[key])
            
            # Schema Validation for critical sources
            if key in EXPECTED_SCHEMAS:
                validate_schema(lf, EXPECTED_SCHEMAS[key])
                
            return lf

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

        logging.info(f"    Extraction and Validation successful for {len(DATA_SOURCES)} sources.")

        # ---------------------------------------------------------
        # TRANSFORM (Polars Query Optimization)
        # ---------------------------------------------------------
        logging.info(">>> PHASE 2: TRANSFORMING & DENORMALIZING")
        
        etl_results = clean_and_enrich(
            txn_lf, loans_lf, accts_lf, cust_lf, addr_lf, branches_lf,
            acc_types_lf, acc_stats_lf, txn_types_lf, loan_stats_lf, cust_types_lf
        )
        
        logging.info("    Transformation logic applied. Ready to stream.")

        # ---------------------------------------------------------
        # LOAD (Streaming to Parquet)
        # ---------------------------------------------------------
        logging.info(">>> PHASE 3: LOADING TO ANALYTICS STORE")

        save_to_parquet(etl_results['transactions'], "fact_transactions.parquet")
        save_to_parquet(etl_results['loans'], "fact_loans.parquet")

        # ---------------------------------------------------------
        # SUMMARY
        # ---------------------------------------------------------
        duration = time.time() - start_total
        logging.info("==========================================")
        logging.info(f"   PIPELINE SUCCESSFUL IN {duration:.2f} SECONDS")
        logging.info("   Outputs available in: data/processed/")
        logging.info("==========================================")

    except FileNotFoundError as fnf_error:
        logging.error(f"CRITICAL: Missing file. {fnf_error}")
        logging.error("Please ensure all CSV files are in 'data/raw/'")
        sys.exit(1)
        
    except ValueError as val_error: 
        logging.error(f"CRITICAL: Data Validation Failed. {val_error}")
        sys.exit(1)

    except Exception as e:
        logging.error(f"CRITICAL: Pipeline failed. Reason: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()