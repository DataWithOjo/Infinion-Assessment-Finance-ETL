import logging
import time
import os
import sys

sys.path.append(os.getcwd())

from scripts.extract import scan_dataset
from scripts.transform import clean_and_enrich
from scripts.load import save_to_parquet

# Configure professional logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def main():
    """
    Main execution entry point for the Finance ETL Pipeline.
    Orchestrates Extract, Transform, and Load phases.
    """
    start_total = time.time()

    logging.info("   STARTING FINANCE DATA PIPELINE (OBT)   ")


    try:

        logging.info("EXTRACTING RAW DATA")
        
        # Core Business Entities
        txn_lf = scan_dataset("transactions.csv")
        loans_lf = scan_dataset("loans.csv")
        accts_lf = scan_dataset("accounts.csv")
        cust_lf = scan_dataset("customers.csv")
        addr_lf = scan_dataset("addresses.csv")
        branches_lf = scan_dataset("branches.csv")

        # Reference Tables
        acc_types_lf = scan_dataset("account_types.csv")
        acc_stats_lf = scan_dataset("account_statuses.csv")
        txn_types_lf = scan_dataset("transaction_types.csv")
        loan_stats_lf = scan_dataset("loan_statuses.csv")
        cust_types_lf = scan_dataset("customer_types.csv")

        logging.info("    Extraction plan created for all 11 sources.")

        logging.info("TRANSFORMING & DENORMALIZING")
        
        # This function builds the execution graph but DOES NOT run it yet (Lazy)
        # It joins all the reference tables and creates the Wide Table schema
        etl_results = clean_and_enrich(
            txn_lf, loans_lf, accts_lf, cust_lf, addr_lf, branches_lf,
            acc_types_lf, acc_stats_lf, txn_types_lf, loan_stats_lf, cust_types_lf
        )
        
        logging.info("    Transformation logic applied. Ready to stream.")

        logging.info(">>> PHASE 3: LOADING TO ANALYTICS STORE")

        # Save the "Wide Tables" (OBT Architecture)
        # This triggers the actual computation
        save_to_parquet(etl_results['transactions'], "fact_transactions.parquet")
        save_to_parquet(etl_results['loans'], "fact_loans.parquet")

        duration = time.time() - start_total

        logging.info(f"   PIPELINE SUCCESSFUL IN {duration:.2f} SECONDS")
        logging.info("   Outputs available in: data/processed/")

    except FileNotFoundError as fnf_error:
        logging.error(f"CRITICAL: Missing file. {fnf_error}")
        logging.error("Please ensure all 11 CSV files are in 'data/raw/'")
        sys.exit(1)
        
    except Exception as e:
        logging.error(f"CRITICAL: Pipeline failed. Reason: {e}")
        # Print full traceback for debugging
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()