import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import psycopg2
import yfinance as yf
import pandas as pd

# === Load .env ===
load_dotenv()

DB_CONFIG = {
    'dbname': os.getenv("DB_NAME"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'host': os.getenv("DB_HOST"),
    'port': os.getenv("DB_PORT")
}

# === Helper functions ===
def get_close_price(prices, target_date):
    """
    Return the closing price for the target_date.
    If exact date is missing, return the last available price before the date.
    """
    past_data = prices.loc[:target_date]
    if past_data.empty:
        return None
    return float(past_data.iloc[-1])

def calc_change(prices, today_date, offset_days):
    """Return % change between today and a past date (nearest available)."""
    past_date = today_date - timedelta(days=offset_days)
    today_price = get_close_price(prices, today_date)
    past_price = get_close_price(prices, past_date)

    if today_price is not None and past_price is not None:
        return round(((today_price - past_price) / past_price) * 100, 4)
    return None

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("SELECT company_id, ticker FROM companies;")
    companies = cur.fetchall()
    tickers = [t[1] for t in companies]  # list of all tickers
    company_map = {t[1]: t[0] for t in companies}  # ticker -> company_id

    # --- Batch download all tickers ---
    try:
        all_data = yf.download(
            tickers, period="1y", interval="1d", progress=False, group_by='ticker', auto_adjust=False
        )
    except Exception as e:
        print(f"Batch download failed: {e}")
        conn.close()
        return

    for ticker in tickers:
        try:
            df = all_data[ticker][['Close']].dropna()
            if df.empty:
                print(f"No data for {ticker}")
                continue

            df.index = pd.to_datetime(df.index.date)  # normalize dates
            prices = df['Close']

            today_date = prices.index[-1]
            today_close = get_close_price(prices, today_date)

            # calculate changes
            change_1m = calc_change(prices, today_date, 30)
            change_3m = calc_change(prices, today_date, 90)
            change_6m = calc_change(prices, today_date, 180)
            change_1y = calc_change(prices, today_date, 364)


            # insert/update prices
            cur.execute("""
                INSERT INTO prices (
                    company_id, price_date, close_price, change_1m, change_3m, change_6m, change_1y
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (company_id, price_date) DO UPDATE
                SET close_price = EXCLUDED.close_price,
                    change_1m = EXCLUDED.change_1m,
                    change_3m = EXCLUDED.change_3m,
                    change_6m = EXCLUDED.change_6m,
                    change_1y = EXCLUDED.change_1y;
            """, (
                company_map[ticker], today_date, today_close,
                change_1m, change_3m, change_6m, change_1y
            ))

            print(f"Updated prices for {ticker} ({today_date})")

        except Exception as e:
            print(f"Error processing {ticker}: {e}")

    conn.commit()
    cur.close()
    conn.close()
    print("All prices updated successfully.")

if __name__ == "__main__":
    main()
