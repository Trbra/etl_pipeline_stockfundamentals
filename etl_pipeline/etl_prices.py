import os
from datetime import date as dt_date

from dotenv import load_dotenv
import psycopg2
import yfinance as yf
import pandas as pd

load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}

UNIVERSE_CODES = ("SP500", "TSX60")


def compute_rsi(series: pd.Series, period: int = 14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def fetch_active_universe(cur, as_of: dt_date) -> list[tuple[int, str, str]]:
    """
    Returns list of (company_id, ticker_raw, ticker_yf) for today's universe.
    """
    cur.execute(
        """
        SELECT c.company_id, c.ticker AS ticker_raw, tm.ticker_yf
        FROM universe_membership_daily um
        JOIN companies c ON c.ticker = um.ticker_raw
        JOIN ticker_map tm ON tm.ticker_raw = um.ticker_raw
        WHERE um.as_of_date = %s
          AND um.is_member = TRUE
          AND um.universe_code = ANY(%s);
        """,
        (as_of, list(UNIVERSE_CODES)),
    )
    return cur.fetchall()


def main():
    as_of = dt_date.today()

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    universe_rows = fetch_active_universe(cur, as_of)
    if not universe_rows:
        print("No active universe tickers found for today. Did you run refresh_universes.py?")
        cur.close()
        conn.close()
        return

    # Download in batch using yfinance tickers
    yf_tickers = [row[2] for row in universe_rows]

    try:
        all_data = yf.download(
            yf_tickers,
            period="7d",          # safer than 1d (covers weekends/holidays)
            interval="1d",
            progress=False,
            group_by="ticker",
            auto_adjust=False
        )
    except Exception as e:
        print(f"Batch download failed: {e}")
        cur.close()
        conn.close()
        return

    # Map yfinance ticker -> (company_id, ticker_raw)
    yf_to_company = {row[2]: (row[0], row[1]) for row in universe_rows}

    for yf_sym in yf_tickers:
        company_id, ticker_raw = yf_to_company[yf_sym]

        try:
            # yfinance returns multiindex columns when multiple tickers
            if isinstance(all_data.columns, pd.MultiIndex):
                if yf_sym not in all_data.columns.levels[0]:
                    print(f"No data for {ticker_raw} ({yf_sym})")
                    continue
                df = all_data[yf_sym][["Close", "Volume"]].dropna()
            else:
                # single ticker case
                df = all_data[["Close", "Volume"]].dropna()

            if df.empty:
                print(f"No price data for {ticker_raw} ({yf_sym})")
                continue

            # normalize to date keys
            df.index = pd.to_datetime(df.index.date)

            inserted_dates = []
            for d, row in df.iterrows():
                cur.execute(
                    """
                    INSERT INTO prices (company_id, price_date, close_price, volume)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (company_id, price_date) DO UPDATE
                    SET close_price = EXCLUDED.close_price,
                        volume = EXCLUDED.volume;
                    """,
                    (company_id, d, float(row["Close"]), None if pd.isna(row["Volume"]) else int(row["Volume"])),
                )
                inserted_dates.append(pd.to_datetime(d))

            conn.commit()

            # Pull history for indicators
            cur.execute(
                """
                SELECT price_date, close_price, volume
                FROM prices
                WHERE company_id = %s
                ORDER BY price_date DESC
                LIMIT 500;
                """,
                (company_id,),
            )
            rows = cur.fetchall()
            if not rows:
                print(f"No historical prices found in DB for {ticker_raw}")
                continue

            hist_df = pd.DataFrame(rows, columns=["price_date", "Close", "Volume"])
            hist_df["price_date"] = pd.to_datetime(hist_df["price_date"])
            hist_df.set_index("price_date", inplace=True)
            hist_df.sort_index(inplace=True)

            hist_df["ma50"] = hist_df["Close"].rolling(window=50, min_periods=50).mean()
            hist_df["ma200"] = hist_df["Close"].rolling(window=200, min_periods=200).mean()
            hist_df["rsi14"] = compute_rsi(hist_df["Close"], 14)

            for d in inserted_dates:
                d = pd.to_datetime(d)

                if d not in hist_df.index:
                    matches = hist_df[hist_df.index.normalize() == d.normalize()]
                    if matches.empty:
                        continue
                    metrics_row = matches.iloc[-1]
                    metrics_date = matches.index[-1]
                else:
                    metrics_row = hist_df.loc[d]
                    metrics_date = d

                ma50 = None if pd.isna(metrics_row["ma50"]) else float(metrics_row["ma50"])
                ma200 = None if pd.isna(metrics_row["ma200"]) else float(metrics_row["ma200"])
                rsi14 = None if pd.isna(metrics_row["rsi14"]) else float(metrics_row["rsi14"])

                cur.execute(
                    """
                    INSERT INTO metrics (company_id, price_date, ma50, ma200, rsi14)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (company_id, price_date) DO UPDATE
                    SET ma50 = EXCLUDED.ma50,
                        ma200 = EXCLUDED.ma200,
                        rsi14 = EXCLUDED.rsi14;
                    """,
                    (company_id, metrics_date, ma50, ma200, rsi14),
                )

            conn.commit()
            print(f"Updated prices+metrics for {ticker_raw} ({yf_sym}) [{len(inserted_dates)} day(s)]")

        except Exception as e:
            conn.rollback()
            print(f"Error processing {ticker_raw} ({yf_sym}): {e}")

    cur.close()
    conn.close()
    print("All prices & metrics updated successfully.")


if __name__ == "__main__":
    main()
