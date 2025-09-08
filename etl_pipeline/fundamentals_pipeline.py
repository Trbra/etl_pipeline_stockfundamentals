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
load_dotenv(dotenv_path=r"C:\Users\trbra\OneDrive\Desktop\projects\etl_pipeline_stockfundamentals\etl_pipeline\.env")

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

# --- Insert data function with retries ---
def insertData(cur, ticker, max_retries=3, delay=5):
    attempt = 1
    while attempt <= max_retries:
        try:
            info = yf.Ticker(ticker).info
            name = info.get("shortName")
            sector = info.get("sector")
            industry = info.get("industry")
            reportDate = datetime.now().date()
            marketCap = info.get("marketCap")
            trailingEps = info.get("trailingEps")
            forwardEps = info.get("forwardEps")
            divYield = info.get("dividendYield")

            peRatio = None
            try:
                peRatio = info.get("currentPrice") / trailingEps if trailingEps else None
            except Exception:
                peRatio = None

            # Insert into companies
            cur.execute("""
                INSERT INTO companies (ticker, name, sector, industry)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (ticker) DO UPDATE SET
                    name = EXCLUDED.name,
                    sector = EXCLUDED.sector,
                    industry = EXCLUDED.industry
                RETURNING company_ID;
            """, (ticker, name, sector, industry))
            company_id = cur.fetchone()[0]

            # Insert into fundamentals
            cur.execute("""
                INSERT INTO fundamentals (company_id, report_date, market_cap, pe_ratio, trailing_eps, forward_eps, dividend_yield)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """, (company_id, reportDate, marketCap, peRatio, trailingEps, forwardEps, divYield))

            logging.info(f"Inserted {ticker} on attempt {attempt}")
            return  # Success, exit function

        except Exception as e:
            logging.warning(f"Attempt {attempt} failed for {ticker}: {e}")
            attempt += 1
            if attempt <= max_retries:
                time.sleep(delay)  # wait before retrying
            else:
                logging.error(f"Failed to insert {ticker} after {max_retries} attempts")
                return

# --- Get top 100 S&P 500 tickers ---
try:
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    sp500 = pd.read_html(response.text)[0]
    tickers = sp500['Symbol'].tolist()
except Exception as e:
    logging.error(f"Failed to fetch tickers from Wikipedia: {e}")
    tickers = []

# --- Fetch market caps ---
data = []
for i, ticker in enumerate(tickers, start=1):
    try:
        cap = yf.Ticker(ticker).fast_info.get("marketCap")
        if cap:
            data.append({"Ticker": ticker, "MarketCap": cap})
        logging.info(f"[{i}/{len(tickers)}] Processed {ticker}")
    except Exception as e:
        logging.error(f"Error fetching market cap for {ticker}: {e}")

# --- Get top 100 ---
df = pd.DataFrame(data)
top100 = df.sort_values("MarketCap", ascending=False).head(100).reset_index(drop=True)
logging.info(f"Top 100 tickers: {top100['Ticker'].tolist()}")

# --- Insert into DB with retries ---
try:
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for i, ticker in enumerate(top100["Ticker"], start=1):
        insertData(cur, ticker)
        # Optional: commit every 10 inserts
        if i % 10 == 0:
            conn.commit()
            logging.info(f"Committed batch of {i} tickers")

    conn.commit()
    logging.info("All inserts committed successfully")

except Exception as e:
    logging.error(f"Database connection or commit failed: {e}")

finally:
    cur.close()
    conn.close()
    logging.info("Database connection closed")

logging.info("Process Complete")