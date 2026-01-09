import io
import os
import logging
from datetime import date, datetime

import pandas as pd
import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}

logging.basicConfig(
    filename="refresh_universes.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

WIKI_SP500 = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
WIKI_TSX60 = "https://en.wikipedia.org/wiki/S%26P/TSX_60"

HEADERS = {"User-Agent": "Mozilla/5.0"}


def _fetch_table(url: str) -> pd.DataFrame:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    tables = pd.read_html(io.StringIO(r.text))
    if not tables:
        raise ValueError(f"No tables found at {url}")
    return tables[0]


def fetch_sp500_symbols() -> list[str]:
    df = _fetch_table(WIKI_SP500)
    # Wikipedia table uses "Symbol"
    symbols = df["Symbol"].astype(str).str.strip().tolist()
    return [s for s in symbols if s and s != "nan"]


def fetch_tsx60_symbols() -> list[str]:
    df = _fetch_table(WIKI_TSX60)
    # Constituents table uses "Symbol"
    symbols = df["Symbol"].astype(str).str.strip().tolist()
    return [s for s in symbols if s and s != "nan"]


def normalize_yfinance_symbol(raw: str, universe_code: str) -> tuple[str, str, str]:
    """
    Returns (ticker_yf, exchange, currency)
    """
    raw = raw.strip()

    # Common US dot-ticker conversions for Yahoo
    # (You can extend as you discover more)
    dot_to_dash = {
        "BRK.B": "BRK-B",
        "BF.B": "BF-B",
    }
    if raw in dot_to_dash:
        return dot_to_dash[raw], "US", "USD"

    if universe_code == "TSX60":
        # TSX symbols generally need .TO for Yahoo Finance
        return f"{raw}.TO", "TSX", "CAD"

    # Default for SP500 / US listings
    return raw, "US", "USD"


def upsert_company(cur, ticker_raw: str):
    cur.execute(
        """
        INSERT INTO companies (ticker)
        VALUES (%s)
        ON CONFLICT (ticker) DO NOTHING;
        """,
        (ticker_raw,),
    )


def upsert_ticker_map(cur, ticker_raw: str, ticker_yf: str, exchange: str, currency: str):
    cur.execute(
        """
        INSERT INTO ticker_map (ticker_raw, ticker_yf, exchange, currency, updated_at)
        VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT (ticker_raw) DO UPDATE
        SET ticker_yf = EXCLUDED.ticker_yf,
            exchange = EXCLUDED.exchange,
            currency = EXCLUDED.currency,
            updated_at = NOW();
        """,
        (ticker_raw, ticker_yf, exchange, currency),
    )


def insert_membership_snapshot(cur, universe_code: str, as_of: date, tickers: list[str]):
    rows = [(universe_code, as_of, t, "wikipedia", True) for t in tickers]
    # Avoid per-row INSERT loops (fast)
    from psycopg2.extras import execute_values

    execute_values(
        cur,
        """
        INSERT INTO universe_membership_daily
            (universe_code, as_of_date, ticker_raw, source, is_member)
        VALUES %s
        ON CONFLICT (universe_code, as_of_date, ticker_raw) DO UPDATE
        SET is_member = EXCLUDED.is_member;
        """,
        rows,
        page_size=1000,
    )


def main():
    as_of = date.today()

    try:
        sp500 = fetch_sp500_symbols()
        tsx60 = fetch_tsx60_symbols()
        logging.info(f"Fetched SP500={len(sp500)} TSX60={len(tsx60)} symbols for {as_of}.")
    except Exception as e:
        logging.error(f"Failed to fetch universes: {e}")
        return

    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Ensure universes exist
                cur.execute(
                    """
                    INSERT INTO universe(universe_code, name)
                    VALUES ('SP500','S&P 500'), ('TSX60','S&P/TSX 60')
                    ON CONFLICT DO NOTHING;
                    """
                )

                # Insert snapshots
                insert_membership_snapshot(cur, "SP500", as_of, sp500)
                insert_membership_snapshot(cur, "TSX60", as_of, tsx60)

                # Upsert companies + ticker_map for BOTH lists
                for universe_code, tickers in (("SP500", sp500), ("TSX60", tsx60)):
                    for raw in tickers:
                        upsert_company(cur, raw)
                        yf_sym, exch, ccy = normalize_yfinance_symbol(raw, universe_code)
                        upsert_ticker_map(cur, raw, yf_sym, exch, ccy)

                conn.commit()

        logging.info("Universe refresh complete.")
    except Exception as e:
        logging.error(f"DB failure in refresh_universes: {e}")


if __name__ == "__main__":
    main()
