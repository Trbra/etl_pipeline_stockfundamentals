import io
import os
import logging
from datetime import date

import pandas as pd
import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = os.getenv("DATABASE_URL")

logging.basicConfig(
    filename="refresh_universes.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

WIKI_SP500 = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
WIKI_TSX60 = "https://en.wikipedia.org/wiki/S%26P/TSX_60"

HEADERS = {"User-Agent": "Mozilla/5.0"}


def _fetch_wiki_html_tables(url: str) -> list[pd.DataFrame]:
    """
    Fetches and parses all HTML tables from a Wikipedia page.
    Returns a list of DataFrames (possibly empty).
    """
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    tables = pd.read_html(io.StringIO(r.text))
    # Normalize column names (strip whitespace)
    for t in tables:
        t.columns = [str(c).strip() for c in t.columns]
    return tables


def _pick_table_by_columns(tables: list[pd.DataFrame], required_cols: set[str], url: str) -> pd.DataFrame:
    """
    Picks the first table that contains all required columns.
    Raises a KeyError with helpful debug output if not found.
    """
    for t in tables:
        if required_cols.issubset(set(t.columns)):
            return t

    sample_cols = [list(t.columns) for t in tables[:8]]
    raise KeyError(
        f"Could not find required columns {sorted(required_cols)} at {url}. "
        f"Example table columns (first {len(sample_cols)} tables): {sample_cols}"
    )


def _extract_symbol_series(df: pd.DataFrame) -> pd.Series:
    """
    Returns the best-guess symbol column from a wikipedia constituents table.
    Supports common variants: Symbol / Ticker / S&amp;P 500 Symbol, etc.
    """
    # Build a map of lower->actual
    col_map = {str(c).strip().lower(): c for c in df.columns}

    candidates = [
        "symbol",
        "ticker",
        "s&p 500 symbol",
        "s&p/tsx 60 symbol",
        "tsx symbol",
        "sp500 symbol",
    ]
    for c in candidates:
        if c in col_map:
            return df[col_map[c]]

    raise KeyError(f"No Symbol/Ticker-like column found. Columns: {list(df.columns)}")


def fetch_sp500_symbols() -> list[str]:
    tables = _fetch_wiki_html_tables(WIKI_SP500)

    # Prefer tables that actually have Symbol
    try:
        df = _pick_table_by_columns(tables, {"Symbol"}, WIKI_SP500)
        sym = df["Symbol"]
    except KeyError:
        # Fallback: find any plausible symbol/ticker column
        df = tables[0] if tables else pd.DataFrame()
        sym = _extract_symbol_series(df) if not df.empty else pd.Series([], dtype=str)

    symbols = sym.astype(str).str.strip()
    # Remove blanks / nan text / footnote artifacts
    symbols = symbols[symbols.notna() & (symbols != "") & (symbols.str.lower() != "nan")]
    return symbols.tolist()


def fetch_tsx60_symbols() -> list[str]:
    tables = _fetch_wiki_html_tables(WIKI_TSX60)

    # TSX60 page sometimes changes table ordering; pick by columns, not index.
    try:
        df = _pick_table_by_columns(tables, {"Symbol"}, WIKI_TSX60)
        sym = df["Symbol"]
    except KeyError:
        # Fallback: scan all tables for a symbol-like column
        sym = None
        for t in tables:
            try:
                sym = _extract_symbol_series(t)
                break
            except KeyError:
                continue
        if sym is None:
            # Provide helpful debug context
            sample_cols = [list(t.columns) for t in tables[:8]]
            raise KeyError(
                f"Could not locate TSX60 Symbol column. Example table columns: {sample_cols}"
            )

    symbols = sym.astype(str).str.strip()
    symbols = symbols[symbols.notna() & (symbols != "") & (symbols.str.lower() != "nan")]
    return symbols.tolist()


def normalize_yfinance_symbol(raw: str, universe_code: str) -> tuple[str, str, str]:
    """
    Returns (ticker_yf, exchange, currency)
    """
    raw = raw.strip()

    # Common US dot-ticker conversions for Yahoo Finance
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
        with psycopg2.connect(DB_CONFIG) as conn:
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
