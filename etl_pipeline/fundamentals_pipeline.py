import os
import time
import logging
from datetime import datetime, date

import psycopg2
import yfinance as yf
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = os.getenv("DATABASE_URL")

logging.basicConfig(
    filename="etl_fundamentals.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

UNIVERSE_CODES = ("SP500", "TSX60")


def fetch_active_universe_tickers(cur, as_of: date) -> list[tuple[str, str]]:
    """
    Returns list of (ticker_raw, ticker_yf) for tickers in today's SP500/TSX60 snapshot.
    """
    cur.execute(
        """
        SELECT DISTINCT um.ticker_raw, tm.ticker_yf
        FROM universe_membership_daily um
        JOIN ticker_map tm ON tm.ticker_raw = um.ticker_raw
        WHERE um.as_of_date = %s
          AND um.is_member = TRUE
          AND um.universe_code = ANY(%s);
        """,
        (as_of, list(UNIVERSE_CODES)),
    )
    return cur.fetchall()


def insert_data(cur, conn, ticker_raw: str, ticker_yf: str, max_retries=3, delay=5):
    attempt = 1
    while attempt <= max_retries:
        try:
            info = yf.Ticker(ticker_yf).info

            name = info.get("shortName") or info.get("longName")
            sector = info.get("sector")
            industry = info.get("industry")
            report_date = datetime.now().date()

            market_cap = info.get("marketCap")
            trailing_eps = info.get("trailingEps")
            forward_eps = info.get("forwardEps")
            dividend_yield = info.get("dividendYield")

            revenue = info.get("totalRevenue")
            net_income = info.get("netIncomeToCommon")
            free_cash_flow = info.get("freeCashflow")
            debt_to_equity = info.get("debtToEquity")
            roe = info.get("returnOnEquity")

            pe_ratio = None
            try:
                current_price = info.get("currentPrice")
                pe_ratio = (current_price / trailing_eps) if (current_price and trailing_eps) else None
            except Exception:
                pe_ratio = None

            # Single transaction per ticker (faster + cleaner)
            try:
                # Upsert company
                cur.execute(
                    """
                    INSERT INTO companies (ticker, name, sector, industry)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (ticker) DO UPDATE
                    SET name = EXCLUDED.name,
                        sector = EXCLUDED.sector,
                        industry = EXCLUDED.industry
                    RETURNING company_id;
                    """,
                    (ticker_raw, name, sector, industry),
                )
                company_id = cur.fetchone()[0]

                # Upsert fundamentals
                cur.execute(
                    """
                    INSERT INTO fundamentals
                        (company_id, report_date, market_cap, pe_ratio, trailing_eps, forward_eps, dividend_yield)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (company_id, report_date) DO UPDATE
                    SET market_cap = EXCLUDED.market_cap,
                        pe_ratio = EXCLUDED.pe_ratio,
                        trailing_eps = EXCLUDED.trailing_eps,
                        forward_eps = EXCLUDED.forward_eps,
                        dividend_yield = EXCLUDED.dividend_yield;
                    """,
                    (company_id, report_date, market_cap, pe_ratio, trailing_eps, forward_eps, dividend_yield),
                )

                # Upsert financials
                cur.execute(
                    """
                    INSERT INTO financials
                        (company_id, report_date, revenue, net_income, free_cash_flow, debt_to_equity, roe)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (company_id, report_date) DO UPDATE
                    SET revenue = EXCLUDED.revenue,
                        net_income = EXCLUDED.net_income,
                        free_cash_flow = EXCLUDED.free_cash_flow,
                        debt_to_equity = EXCLUDED.debt_to_equity,
                        roe = EXCLUDED.roe;
                    """,
                    (company_id, report_date, revenue, net_income, free_cash_flow, debt_to_equity, roe),
                )

                conn.commit()
                logging.info(f"Inserted fundamentals/financials for {ticker_raw} ({ticker_yf})")
                return

            except Exception as e:
                conn.rollback()
                logging.error(f"DB upsert failed for {ticker_raw} ({ticker_yf}): {e}")
                return

        except Exception as e:
            logging.warning(f"Attempt {attempt} failed for {ticker_raw} ({ticker_yf}): {e}")
            attempt += 1
            if attempt <= max_retries:
                time.sleep(delay)
            else:
                logging.error(f"Failed to insert {ticker_raw} after {max_retries} attempts")
                return


def main():
    as_of = date.today()

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        tickers = fetch_active_universe_tickers(cur, as_of)
        logging.info(f"Loaded {len(tickers)} active universe tickers for {as_of}")

        for i, (ticker_raw, ticker_yf) in enumerate(tickers, start=1):
            insert_data(cur, conn, ticker_raw, ticker_yf)
            if i % 25 == 0:
                logging.info(f"Processed {i}/{len(tickers)} tickers")

    except Exception as e:
        logging.error(f"Fundamentals pipeline failed: {e}")
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass
        logging.info("Fundamentals pipeline complete.")


if __name__ == "__main__":
    main()
