import os
from datetime import date as dt_date

from dotenv import load_dotenv
import psycopg2
import yfinance as yf
import pandas as pd

load_dotenv()

DB_CONFIG = os.getenv("DATABASE_URL")

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


def tsx_dash_fallback(yf_symbol: str) -> str | None:
    """
    If a TSX symbol uses dot-class notation, Yahoo sometimes expects dash notation.
    Examples:
      CCL.B.TO -> CCL-B.TO
      BIP.UN.TO -> BIP-UN.TO
    Returns a fallback symbol or None if no transformation applies.
    """
    if not yf_symbol.endswith(".TO"):
        return None

    base = yf_symbol[:-3]  # strip ".TO"
    if "." not in base:
        return None

    alt_base = base.replace(".", "-")
    alt = f"{alt_base}.TO"
    return alt if alt != yf_symbol else None


def _coerce_download_to_ohlcv(df: pd.DataFrame, yf_symbol: str) -> pd.DataFrame:
    """
    yfinance can return either:
      - SingleIndex columns: Open, High, Low, Close, Adj Close, Volume
      - MultiIndex columns: (TICKER, Open), (TICKER, Close), ...
    This function returns a frame with columns ['Close', 'Volume'].
    """
    if df is None or df.empty:
        raise ValueError("Empty download dataframe")

    # Handle MultiIndex (common when group_by='ticker' OR even sometimes in general)
    if isinstance(df.columns, pd.MultiIndex):
        # Typical case: top level = ticker
        if yf_symbol in df.columns.get_level_values(0):
            df = df[yf_symbol]
        else:
            # Sometimes the ticker isn't exactly matched (rare), fallback: pick first ticker group
            first_ticker = df.columns.get_level_values(0)[0]
            df = df[first_ticker]

    # Now expect flat columns
    cols = [c.strip() for c in df.columns.astype(str).tolist()]
    df.columns = cols

    if "Close" not in df.columns:
        raise ValueError(f"Missing Close column. Columns: {df.columns.tolist()}")

    out = df[["Close", "Volume"]] if "Volume" in df.columns else df[["Close"]].copy()
    out = out.dropna(subset=["Close"])
    if out.empty:
        raise ValueError("No close price rows after dropna")
    if "Volume" not in out.columns:
        out["Volume"] = None
    return out


def download_one_symbol(yf_symbol: str, period: str = "7d") -> pd.DataFrame:
    """
    Download a single ticker's daily OHLCV frame and return Close/Volume only.
    Handles both single-index and multi-index columns.
    """
    df = yf.download(
        yf_symbol,
        period=period,
        interval="1d",
        progress=False,
        auto_adjust=False,
        group_by="ticker",  # OK now that we handle MultiIndex
    )
    return _coerce_download_to_ohlcv(df, yf_symbol)


def update_ticker_map(cur, ticker_raw: str, new_ticker_yf: str):
    cur.execute(
        """
        UPDATE ticker_map
        SET ticker_yf = %s,
            updated_at = NOW()
        WHERE ticker_raw = %s;
        """,
        (new_ticker_yf, ticker_raw),
    )


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

    # Map yfinance ticker -> (company_id, ticker_raw)
    yf_to_company = {row[2]: (row[0], row[1]) for row in universe_rows}
    yf_tickers = list(yf_to_company.keys())

    success = 0
    failed = []

    for yf_sym in yf_tickers:
        company_id, ticker_raw = yf_to_company[yf_sym]

        try:
            # Try primary symbol first
            try:
                df = download_one_symbol(yf_sym, period="7d")
                used_symbol = yf_sym
            except Exception as primary_err:
                # If TSX-style dot notation, try dash fallback
                alt = tsx_dash_fallback(yf_sym)
                if not alt:
                    raise primary_err

                df = download_one_symbol(alt, period="7d")
                used_symbol = alt

                # Persist the working Yahoo symbol so next run is cheaper
                update_ticker_map(cur, ticker_raw, used_symbol)
                conn.commit()
                print(f"[MAP FIX] {ticker_raw}: {yf_sym} -> {used_symbol}")

            # Normalize index to DATE (midnight) for consistent DB keys
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
            print(f"Updated prices+metrics for {ticker_raw} ({used_symbol}) [{len(inserted_dates)} day(s)]")
            success += 1

        except Exception as e:
            conn.rollback()
            failed.append((ticker_raw, yf_sym, str(e)))
            print(f"FAILED {ticker_raw} ({yf_sym}): {e}")

    cur.close()
    conn.close()

    print(f"\nDone. Success={success} Failed={len(failed)}")
    if failed:
        print("Failures:")
        for fr in failed:
            print(" -", fr)


if __name__ == "__main__":
    main()
