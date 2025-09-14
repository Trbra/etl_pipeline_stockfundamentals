from datetime import date
import logging
import psycopg2
from dotenv import load_dotenv
import os

# --- Load environment ---
load_dotenv()

DB_CONFIG = {
    'dbname': os.getenv("DB_NAME"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'host': os.getenv("DB_HOST"),
    'port': os.getenv("DB_PORT")
}

# --- Configure logging ---
logging.basicConfig(
    filename='data_freshness.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

try:
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM companies c
                USING fundamentals f
                WHERE c.company_id = f.company_id
                  AND f.report_date <= CURRENT_DATE - INTERVAL '90 days'
                RETURNING c.ticker;
            """)
            deleted = cur.fetchall()
            conn.commit()

            if deleted:
                tickers = [row[0] for row in deleted]
                logging.info(f"Deleted {len(tickers)} companies: {', '.join(tickers)}")
            else:
                logging.info("No companies deleted (all are up to date).")

except Exception as e:
    logging.error(f"Database operation failed: {e}")

finally:
    logging.info("Database cleanup finished.")
