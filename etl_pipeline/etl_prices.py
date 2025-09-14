import os
from dotenv import load_dotenv
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

# === Helper: Calculate RSI ===
def compute_rsi(series: pd.Series, period: int = 14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)

    # standard RSI: need period values before it becomes non-null
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # === Load tickers ===
    cur.execute("SELECT company_id, ticker FROM companies;")
    companies = cur.fetchall()
    tickers = [t[1] for t in companies]
    company_map = {t[1]: t[0] for t in companies}

    # --- Download today's prices (1 day) ---
    try:
        all_data = yf.download(
            tickers, period="1d", interval="1d",
            progress=False, group_by='ticker', auto_adjust=False
        )
    except Exception as e:
        print(f"Batch download failed: {e}")
        conn.close()
        return

    for ticker in tickers:
        try:
            # handle case where group_by='ticker' produced multiindex columns
            if ticker not in all_data.columns.levels[0]:
                print(f"No data for {ticker}")
                continue

            df = all_data[ticker][['Close', 'Volume']].dropna()
            if df.empty:
                print(f"No price data for {ticker}")
                continue

            # normalize index to date (midnight) for consistent DB date keys
            df.index = pd.to_datetime(df.index.date)

            # === Insert today's price(s) into prices table ===
            inserted_dates = []
            for date, row in df.iterrows():
                try:
                    cur.execute("""
                        INSERT INTO prices (company_id, price_date, close_price, volume)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (company_id, price_date) DO UPDATE
                        SET close_price = EXCLUDED.close_price,
                            volume = EXCLUDED.volume;
                    """, (company_map[ticker], date, float(row['Close']), int(row['Volume'])))
                    inserted_dates.append(pd.to_datetime(date))
                except Exception as e:
                    # keep going if one date fails
                    print(f"Failed to insert price for {ticker} on {date}: {e}")

            # commit so the newly inserted prices are available for the subsequent SELECT
            conn.commit()

            if not inserted_dates:
                print(f"No new/updated price rows inserted for {ticker}")
                continue

            # --- Pull recent history from DB (including today) to compute indicators ---
            cur.execute("""
                SELECT price_date, close_price, volume
                FROM prices
                WHERE company_id = %s
                ORDER BY price_date DESC
                LIMIT 500
            """, (company_map[ticker],))
            rows = cur.fetchall()

            if not rows:
                print(f"No historical prices found in DB for {ticker}")
                continue

            hist_df = pd.DataFrame(rows, columns=['price_date', 'Close', 'Volume'])
            # ensure datetime index and ascending order for rolling calculations
            hist_df['price_date'] = pd.to_datetime(hist_df['price_date'])
            hist_df.set_index('price_date', inplace=True)
            hist_df.sort_index(inplace=True)

            # === Calculate metrics on DB history (so we have enough lookback) ===
            hist_df['ma50'] = hist_df['Close'].rolling(window=50, min_periods=50).mean()
            hist_df['ma200'] = hist_df['Close'].rolling(window=200, min_periods=200).mean()
            hist_df['rsi14'] = compute_rsi(hist_df['Close'], 14)

            # === Insert metrics only for the date(s) we just inserted/updated from Yahoo ===
            for dt in inserted_dates:
                # normalize dt to match index dtype (Timestamp)
                dt_ts = pd.to_datetime(dt)

                if dt_ts not in hist_df.index:
                    # possible if DB stores different timezone / date format - try matching by date portion
                    matches = hist_df[hist_df.index.normalize() == dt_ts.normalize()]
                    if matches.empty:
                        print(f"No matching historical row for {ticker} on {dt_ts.date()}, skipping metrics insert")
                        continue
                    metrics_row = matches.iloc[-1]
                    metrics_date = matches.index[-1]
                else:
                    metrics_row = hist_df.loc[dt_ts]
                    metrics_date = dt_ts

                ma50 = float(metrics_row['ma50']) if not pd.isna(metrics_row['ma50']) else None
                ma200 = float(metrics_row['ma200']) if not pd.isna(metrics_row['ma200']) else None
                rsi14 = float(metrics_row['rsi14']) if not pd.isna(metrics_row['rsi14']) else None

                try:
                    cur.execute("""
                        INSERT INTO metrics (company_id, price_date, ma50, ma200, rsi14)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (company_id, price_date) DO UPDATE
                        SET ma50 = EXCLUDED.ma50,
                            ma200 = EXCLUDED.ma200,
                            rsi14 = EXCLUDED.rsi14
                    """, (
                        company_map[ticker], metrics_date,
                        ma50, ma200, rsi14
                    ))
                except Exception as e:
                    print(f"Failed to insert metrics for {ticker} on {metrics_date}: {e}")

            print(f"Inserted/updated {len(inserted_dates)} price(s) and metrics for {ticker}")

        except Exception as e:
            print(f"Error processing {ticker}: {e}")

    conn.commit()
    cur.close()
    conn.close()
    print("All prices & metrics updated successfully.")

if __name__ == "__main__":
    main()





