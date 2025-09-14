import io
import requests
import pandas as pd
import yfinance as yf
import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime
import logging
import time

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
    filename='etl_fundamentals.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- Insert or update company + fundamentals safely ---
def insertData(cur, conn, ticker, max_retries=3, delay=5):
    attempt = 1
    while attempt <= max_retries:
        try:
            info = yf.Ticker(ticker).info
            name = info.get("shortName")
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
                pe_ratio = info.get("currentPrice") / trailing_eps if trailing_eps else None
            except Exception:
                pe_ratio = None

            # --- Insert or update company ---
            try:
                cur.execute("""
                    INSERT INTO companies (ticker, name, sector, industry)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (ticker) DO UPDATE
                    SET name = EXCLUDED.name,
                        sector = EXCLUDED.sector,
                        industry = EXCLUDED.industry
                    RETURNING company_id;
                """, (ticker, name, sector, industry))
                company_id = cur.fetchone()[0]
                conn.commit()
            except Exception as e:
                conn.rollback()
                logging.error(f"Company insert/update failed for {ticker}: {e}")
                return

            # --- Insert fundamentals safely ---
            try:
                cur.execute("""
                    INSERT INTO fundamentals (company_id, report_date, market_cap, pe_ratio, trailing_eps, forward_eps, dividend_yield)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (company_id, report_date) DO UPDATE
                    SET market_cap = EXCLUDED.market_cap,
                        pe_ratio = EXCLUDED.pe_ratio,
                        trailing_eps = EXCLUDED.trailing_eps,
                        forward_eps = EXCLUDED.forward_eps,
                        dividend_yield = EXCLUDED.dividend_yield;
                """, (company_id, report_date, market_cap, pe_ratio, trailing_eps, forward_eps, dividend_yield))
                conn.commit()
                logging.info(f"Inserted fundamentals for {ticker}")
            except Exception as e:
                conn.rollback()
                logging.error(f"Fundamentals insert/update failed for {ticker}: {e}")


            try:
                cur.execute("""
                    INSERT INTO financials (company_id, report_date, revenue, net_income, free_cash_flow, debt_to_equity, roe)
                    VALUES ( %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (company_id, report_date) DO UPDATE
                    SET revenue = EXCLUDED.revenue,
                        net_income = EXCLUDED.net_income,
                        free_cash_flow = EXCLUDED.free_cash_flow,
                        debt_to_equity = EXCLUDED.debt_to_equity,
                        roe = EXCLUDED.roe;
                """, (company_id, report_date, revenue, net_income, free_cash_flow, debt_to_equity, roe))
                conn.commit()
                logging.info(f"Inserted financials for {ticker}")
            except Exception as e:
                conn.rollback()
                logging.error(f"Financials insert/update failed for {ticker}: {e}")

            return # Success

        except Exception as e:
            logging.warning(f"Attempt {attempt} failed for {ticker}: {e}")
            attempt += 1
            if attempt <= max_retries:
                time.sleep(delay)
            else:
                logging.error(f"Failed to insert {ticker} after {max_retries} attempts")
                return

# --- Get all S&P 500 tickers ---
try:
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    sp500 = pd.read_html(io.StringIO(response.text))[0]  # FutureWarning safe
    tickers = sp500['Symbol'].tolist()
    logging.info(f"Fetched {len(tickers)} S&P 500 tickers successfully.")
except Exception as e:
    logging.error(f"Failed to fetch tickers from Wikipedia: {e}")
    tickers = []

# --- Insert into DB ---
try:
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for i, ticker in enumerate(tickers, start=1):
        insertData(cur, conn, ticker)
        if i % 10 == 0:
            logging.info(f"Processed {i}/{len(tickers)} tickers")

except Exception as e:
    logging.error(f"Database connection or commit failed: {e}")

finally:
    cur.close()
    conn.close()
    logging.info("Database connection closed")

logging.info("Process Complete")



