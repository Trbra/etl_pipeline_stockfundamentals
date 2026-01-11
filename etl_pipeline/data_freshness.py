import os
import logging
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = os.getenv("DATABASE_URL")

logging.basicConfig(
    filename="data_freshness.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Tune these to your storage preference
KEEP_PRICE_DAYS = 730        # ~2 years
KEEP_FUNDAMENTAL_YEARS = 3   # ~3 years


def main():
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM metrics WHERE price_date < CURRENT_DATE - INTERVAL %s;",
                (f"{KEEP_PRICE_DAYS} days",),
            )
            metrics_deleted = cur.rowcount

            cur.execute(
                "DELETE FROM prices WHERE price_date < CURRENT_DATE - INTERVAL %s;",
                (f"{KEEP_PRICE_DAYS} days",),
            )
            prices_deleted = cur.rowcount

            cur.execute(
                "DELETE FROM fundamentals WHERE report_date < CURRENT_DATE - INTERVAL %s;",
                (f"{KEEP_FUNDAMENTAL_YEARS} years",),
            )
            fundamentals_deleted = cur.rowcount

            cur.execute(
                "DELETE FROM financials WHERE report_date < CURRENT_DATE - INTERVAL %s;",
                (f"{KEEP_FUNDAMENTAL_YEARS} years",),
            )
            financials_deleted = cur.rowcount

            conn.commit()

    logging.info(
        f"freshness done. Deleted prices={prices_deleted}, metrics={metrics_deleted}, "
        f"fundamentals={fundamentals_deleted}, financials={financials_deleted}"
    )


if __name__ == "__main__":
    main()
