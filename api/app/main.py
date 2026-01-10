from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import date, datetime
import json

import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")  # api/.env should define this
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "http://localhost:3000")

if not DATABASE_URL:
    # fallback if you still want to use DB_* (not recommended long term)
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    if DB_NAME and DB_USER and DB_PASSWORD:
        DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

app = FastAPI(title="Market Screener API", version="1.0")

origins = [o.strip() for o in CORS_ORIGINS.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL not set (api/.env missing DATABASE_URL)")
        _pool = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=1, max_size=10)
    return _pool


async def fetch(sql: str, *args):
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch(sql, *args)


async def fetchrow(sql: str, *args):
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchrow(sql, *args)


async def execute(sql: str, *args):
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.execute(sql, *args)


def _normalize_json(val: Any) -> Dict[str, Any]:
    """
    asyncpg usually returns json/jsonb as dict, but some environments return str.
    Normalize to dict for pydantic.
    """
    if val is None:
        return {}
    if isinstance(val, dict):
        return val
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            return {"raw": val}
    return {"raw": str(val)}


# -------------------------
# Models
# -------------------------
class ScreenerRow(BaseModel):
    ticker: str
    name: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    price_date: Optional[date] = None
    close_price: Optional[float] = None
    volume: Optional[int] = None
    ma50: Optional[float] = None
    ma200: Optional[float] = None
    rsi14: Optional[float] = None
    market_cap: Optional[int] = None
    pe_ratio: Optional[float] = None
    dividend_yield: Optional[float] = None
    trend_bullish: Optional[bool] = None
    rsi_oversold: Optional[bool] = None
    rsi_overbought: Optional[bool] = None


class SeriesPoint(BaseModel):
    date: date
    close: Optional[float] = None
    volume: Optional[int] = None
    ma50: Optional[float] = None
    ma200: Optional[float] = None
    rsi14: Optional[float] = None


class RankingRow(BaseModel):
    ticker: str
    name: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    price_date: Optional[date] = None
    close_price: Optional[float] = None
    rsi14: Optional[float] = None
    pe_ratio: Optional[float] = None
    dividend_yield: Optional[float] = None
    market_cap: Optional[int] = None

    score: float
    trend_score: float
    rsi_score: float
    value_score: float
    size_score: float
    yield_score: float

    reasons: Dict[str, Any] = Field(default_factory=dict)


class RankingConfig(BaseModel):
    name: str = "default"
    weights: Dict[str, float] = Field(default_factory=dict)
    params: Dict[str, Any] = Field(default_factory=dict)
    active: bool = True


class StatusResponse(BaseModel):
    ok: bool
    db_connected: bool
    server_time: datetime
    screener_rows: int
    dim_company_rows: int
    fact_prices_rows: int
    fact_metrics_rows: int
    latest_price_date: Optional[date] = None
    latest_metrics_date: Optional[date] = None
    latest_fundamentals_date: Optional[date] = None
    notes: Optional[str] = None

class DataQualitySnapshot(BaseModel):
    dq_date: date
    created_at: datetime

    universe_companies: int
    companies_in_dim: int

    tickers_with_price_today: int
    tickers_missing_price_today: int
    pct_with_price_today: float

    tickers_with_metrics_today: int
    tickers_missing_metrics_today: int
    pct_with_metrics_today: float

    tickers_with_ma200_today: int
    pct_with_ma200_today: float

    tickers_with_rsi_today: int
    pct_with_rsi_today: float

    duplicates_fact_prices: int
    duplicates_fact_metrics: int

    nonpositive_prices_today: int
    zero_volume_today: int

    notes: Optional[str] = None
    


# -------------------------
# Debug / Health
# -------------------------
@app.get("/health")
async def health():
    return {"ok": True}


@app.get("/debug/routes")
async def debug_routes():
    return [getattr(r, "path", None) for r in app.router.routes]


@app.get("/debug/db_counts")
async def debug_db_counts():
    rows = await fetch(
        """
        SELECT
          (SELECT COUNT(*) FROM warehouse.v_screener_latest) AS screener,
          (SELECT COUNT(*) FROM warehouse.fact_prices) AS fact_prices,
          (SELECT COUNT(*) FROM warehouse.fact_metrics) AS fact_metrics,
          (SELECT COUNT(*) FROM warehouse.dim_company) AS dim_company;
        """
    )
    return dict(rows[0])


# -------------------------
# Status API (NEW)
# -------------------------
@app.get("/api/status", response_model=StatusResponse)
async def status():
    """
    Status endpoint for the GUI status page.
    Returns pipeline freshness + row counts.
    """
    try:
        row = await fetchrow(
            """
            SELECT
              (SELECT COUNT(*) FROM warehouse.v_screener_latest) AS screener_rows,
              (SELECT COUNT(*) FROM warehouse.dim_company)       AS dim_company_rows,
              (SELECT COUNT(*) FROM warehouse.fact_prices)       AS fact_prices_rows,
              (SELECT COUNT(*) FROM warehouse.fact_metrics)      AS fact_metrics_rows,
              (SELECT MAX(full_date) FROM warehouse.v_price_series)   AS latest_price_date,
              (SELECT MAX(full_date) FROM warehouse.v_metrics_series) AS latest_metrics_date,
              (SELECT MAX(d.full_date)
               FROM warehouse.fact_fundamentals ff
               JOIN warehouse.dim_date d ON d.date_id = ff.date_id)   AS latest_fundamentals_date;
            """
        )
        if not row:
            return StatusResponse(
                ok=False,
                db_connected=True,
                server_time=datetime.utcnow(),
                screener_rows=0,
                dim_company_rows=0,
                fact_prices_rows=0,
                fact_metrics_rows=0,
                latest_price_date=None,
                latest_metrics_date=None,
                latest_fundamentals_date=None,
                notes="Status query returned no rows (unexpected)",
            )

        r = dict(row)
        return StatusResponse(
            ok=True,
            db_connected=True,
            server_time=datetime.utcnow(),
            screener_rows=int(r.get("screener_rows") or 0),
            dim_company_rows=int(r.get("dim_company_rows") or 0),
            fact_prices_rows=int(r.get("fact_prices_rows") or 0),
            fact_metrics_rows=int(r.get("fact_metrics_rows") or 0),
            latest_price_date=r.get("latest_price_date"),
            latest_metrics_date=r.get("latest_metrics_date"),
            latest_fundamentals_date=r.get("latest_fundamentals_date"),
            notes=None,
        )
    except Exception as e:
        # Keep response JSON so UI can still render a failure state
        return StatusResponse(
            ok=False,
            db_connected=False,
            server_time=datetime.utcnow(),
            screener_rows=0,
            dim_company_rows=0,
            fact_prices_rows=0,
            fact_metrics_rows=0,
            latest_price_date=None,
            latest_metrics_date=None,
            latest_fundamentals_date=None,
            notes=str(e),
        )


# -------------------------
# Ranking Config API
# -------------------------
@app.get("/api/ranking-config", response_model=RankingConfig)
async def get_ranking_config():
    row = await fetchrow(
        """
        SELECT name, weights, params, active
        FROM warehouse.ranking_config
        WHERE active = TRUE
        ORDER BY updated_at DESC
        LIMIT 1;
        """
    )
    if not row:
        raise HTTPException(status_code=404, detail="No active ranking config found")

    d = dict(row)
    d["weights"] = _normalize_json(d.get("weights"))
    d["params"] = _normalize_json(d.get("params"))
    return RankingConfig(**d)


@app.put("/api/ranking-config", response_model=RankingConfig)
async def update_ranking_config(cfg: RankingConfig):
    # Validate: weights sum to ~1
    if not cfg.weights:
        raise HTTPException(status_code=400, detail="weights cannot be empty")

    s = float(sum(cfg.weights.values()))
    if s <= 0.99 or s >= 1.01:
        raise HTTPException(status_code=400, detail=f"Weights must sum to 1.0 (got {s})")

    # Ensure the keys exist (optional but nice)
    required = {"trend", "rsi", "value", "size", "yield"}
    missing = required - set(cfg.weights.keys())
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing weight keys: {sorted(list(missing))}")

    # Update active config row
    await execute(
        """
        UPDATE warehouse.ranking_config
        SET weights = $1::jsonb,
            params = $2::jsonb,
            updated_at = NOW()
        WHERE active = TRUE;
        """,
        json.dumps(cfg.weights),
        json.dumps(cfg.params),
    )

    return cfg


# -------------------------
# Screener API
# -------------------------
@app.get("/api/screener", response_model=List[ScreenerRow])
async def screener(
    q: Optional[str] = None,
    sector: Optional[str] = None,
    rsi_lte: Optional[float] = None,
    bullish: Optional[bool] = None,
    limit: int = Query(200, ge=1, le=2000),
):
    where = []
    args = []
    i = 1

    if q:
        where.append(f"(ticker ILIKE ${i} OR name ILIKE ${i})")
        args.append(f"%{q}%")
        i += 1
    if sector:
        where.append(f"sector = ${i}")
        args.append(sector)
        i += 1
    if rsi_lte is not None:
        where.append(f"rsi14 <= ${i}")
        args.append(rsi_lte)
        i += 1
    if bullish is not None:
        where.append(f"trend_bullish = ${i}")
        args.append(bullish)
        i += 1

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    sql = f"""
      SELECT
        ticker, name, sector, industry, price_date, close_price, volume,
        ma50, ma200, rsi14, market_cap, pe_ratio, dividend_yield,
        trend_bullish, rsi_oversold, rsi_overbought
      FROM warehouse.v_screener_latest
      {where_sql}
      ORDER BY market_cap DESC NULLS LAST
      LIMIT {limit};
    """

    rows = await fetch(sql, *args)
    return [ScreenerRow(**dict(r)) for r in rows]


@app.get("/api/company/{ticker}/series", response_model=List[SeriesPoint])
async def company_series(ticker: str, days: int = Query(365, ge=7, le=5000)):
    rows = await fetch(
        """
      SELECT
        ps.full_date AS date,
        ps.close_price AS close,
        ps.volume,
        ms.ma50,
        ms.ma200,
        ms.rsi14
      FROM warehouse.v_price_series ps
      LEFT JOIN warehouse.v_metrics_series ms
        ON ms.ticker = ps.ticker AND ms.full_date = ps.full_date
      WHERE ps.ticker = $1
      ORDER BY ps.full_date DESC
      LIMIT $2;
    """,
        ticker,
        days,
    )

    if not rows:
        raise HTTPException(status_code=404, detail="Ticker not found or no data")

    rows = list(reversed(rows))  # ascending for chart
    return [SeriesPoint(**dict(r)) for r in rows]


# -------------------------
# Rankings API (uses config weights)
# -------------------------
@app.get("/api/rankings", response_model=List[RankingRow])
async def rankings(
    sector: Optional[str] = None,
    limit: int = Query(50, ge=1, le=500),
):
    cfg = await get_ranking_config()

    w_trend = float(cfg.weights.get("trend", 0.35))
    w_rsi = float(cfg.weights.get("rsi", 0.25))
    w_value = float(cfg.weights.get("value", 0.20))
    w_size = float(cfg.weights.get("size", 0.10))
    w_yield = float(cfg.weights.get("yield", 0.10))

    where = []
    args: List[Any] = [w_trend, w_rsi, w_value, w_size, w_yield]
    i = 6  # because $1-$5 are weights

    if sector:
        where.append(f"sector = ${i}")
        args.append(sector)
        i += 1

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    rows = await fetch(
        f"""
        SELECT
          ticker, name, sector, industry, price_date, close_price, rsi14, pe_ratio,
          dividend_yield, market_cap,

          ROUND((
            $1*trend_score
            + $2*rsi_score
            + $3*value_score
            + $4*size_score
            + $5*yield_score
          )::numeric, 4) AS score,

          trend_score, rsi_score, value_score, size_score, yield_score,
          reasons
        FROM warehouse.v_rankings_latest
        {where_sql}
        ORDER BY score DESC NULLS LAST
        LIMIT {limit};
        """,
        *args,
    )

    out: List[RankingRow] = []
    for r in rows:
        d = dict(r)
        d["reasons"] = _normalize_json(d.get("reasons"))
        out.append(RankingRow(**d))
    return out

# -------------------------
# Data Quality Snapshot API
# -------------------------

async def _upsert_dq_snapshot(d: Dict[str, Any]) -> None:
    # Upsert daily snapshot
    await execute(
        """
        INSERT INTO warehouse.data_quality_daily (
          dq_date, universe_companies, companies_in_dim,
          tickers_with_price_today, tickers_missing_price_today, pct_with_price_today,
          tickers_with_metrics_today, tickers_missing_metrics_today, pct_with_metrics_today,
          tickers_with_ma200_today, pct_with_ma200_today,
          tickers_with_rsi_today, pct_with_rsi_today,
          duplicates_fact_prices, duplicates_fact_metrics,
          nonpositive_prices_today, zero_volume_today,
          notes
        )
        VALUES (
          $1, $2, $3,
          $4, $5, $6,
          $7, $8, $9,
          $10, $11,
          $12, $13,
          $14, $15,
          $16, $17,
          $18
        )
        ON CONFLICT (dq_date) DO UPDATE SET
          created_at = NOW(),
          universe_companies = EXCLUDED.universe_companies,
          companies_in_dim = EXCLUDED.companies_in_dim,
          tickers_with_price_today = EXCLUDED.tickers_with_price_today,
          tickers_missing_price_today = EXCLUDED.tickers_missing_price_today,
          pct_with_price_today = EXCLUDED.pct_with_price_today,
          tickers_with_metrics_today = EXCLUDED.tickers_with_metrics_today,
          tickers_missing_metrics_today = EXCLUDED.tickers_missing_metrics_today,
          pct_with_metrics_today = EXCLUDED.pct_with_metrics_today,
          tickers_with_ma200_today = EXCLUDED.tickers_with_ma200_today,
          pct_with_ma200_today = EXCLUDED.pct_with_ma200_today,
          tickers_with_rsi_today = EXCLUDED.tickers_with_rsi_today,
          pct_with_rsi_today = EXCLUDED.pct_with_rsi_today,
          duplicates_fact_prices = EXCLUDED.duplicates_fact_prices,
          duplicates_fact_metrics = EXCLUDED.duplicates_fact_metrics,
          nonpositive_prices_today = EXCLUDED.nonpositive_prices_today,
          zero_volume_today = EXCLUDED.zero_volume_today,
          notes = EXCLUDED.notes;
        """,
        d["dq_date"],
        d["universe_companies"],
        d["companies_in_dim"],
        d["tickers_with_price_today"],
        d["tickers_missing_price_today"],
        d["pct_with_price_today"],
        d["tickers_with_metrics_today"],
        d["tickers_missing_metrics_today"],
        d["pct_with_metrics_today"],
        d["tickers_with_ma200_today"],
        d["pct_with_ma200_today"],
        d["tickers_with_rsi_today"],
        d["pct_with_rsi_today"],
        d["duplicates_fact_prices"],
        d["duplicates_fact_metrics"],
        d["nonpositive_prices_today"],
        d["zero_volume_today"],
        d.get("notes"),
    )


@app.post("/api/dq/run", response_model=DataQualitySnapshot)
async def dq_run():
    """
    Computes today's data quality snapshot and stores it in warehouse.data_quality_daily.
    """
    today = date.today()

    row = await fetchrow(
        """
        WITH base AS (
          SELECT COUNT(*)::int AS universe_companies
          FROM warehouse.dim_company
        ),
        latest_price_day AS (
          SELECT MAX(full_date) AS latest_price_date
          FROM warehouse.v_price_series
        ),
        latest_metrics_day AS (
          SELECT MAX(full_date) AS latest_metrics_date
          FROM warehouse.v_metrics_series
        ),
        price_cov AS (
          SELECT
            COUNT(DISTINCT ticker)::int AS tickers_with_price_today,
            SUM(CASE WHEN close_price IS NULL THEN 1 ELSE 0 END)::int AS null_price_rows
          FROM warehouse.v_price_series
          WHERE full_date = (SELECT latest_price_date FROM latest_price_day)
        ),
        metrics_cov AS (
          SELECT
            COUNT(DISTINCT ticker)::int AS tickers_with_metrics_today
          FROM warehouse.v_metrics_series
          WHERE full_date = (SELECT latest_metrics_date FROM latest_metrics_day)
        ),
        ma_cov AS (
          SELECT
            COUNT(DISTINCT ticker)::int AS tickers_with_ma200_today
          FROM warehouse.v_metrics_series
          WHERE full_date = (SELECT latest_metrics_date FROM latest_metrics_day)
            AND ma200 IS NOT NULL
        ),
        rsi_cov AS (
          SELECT
            COUNT(DISTINCT ticker)::int AS tickers_with_rsi_today
          FROM warehouse.v_metrics_series
          WHERE full_date = (SELECT latest_metrics_date FROM latest_metrics_day)
            AND rsi14 IS NOT NULL
        ),
        sanity AS (
          SELECT
            SUM(CASE WHEN close_price <= 0 THEN 1 ELSE 0 END)::int AS nonpositive_prices_today,
            SUM(CASE WHEN volume = 0 THEN 1 ELSE 0 END)::int AS zero_volume_today
          FROM warehouse.v_price_series
          WHERE full_date = (SELECT latest_price_date FROM latest_price_day)
        ),
        dups_prices AS (
          SELECT COALESCE(SUM(cnt-1),0)::int AS duplicates_fact_prices
          FROM (
            SELECT company_id, date_id, COUNT(*) AS cnt
            FROM warehouse.fact_prices
            GROUP BY company_id, date_id
            HAVING COUNT(*) > 1
          ) t
        ),
        dups_metrics AS (
          SELECT COALESCE(SUM(cnt-1),0)::int AS duplicates_fact_metrics
          FROM (
            SELECT company_id, date_id, COUNT(*) AS cnt
            FROM warehouse.fact_metrics
            GROUP BY company_id, date_id
            HAVING COUNT(*) > 1
          ) t
        )
        SELECT
          (SELECT universe_companies FROM base) AS universe_companies,
          (SELECT COUNT(*)::int FROM warehouse.dim_company) AS companies_in_dim,

          (SELECT tickers_with_price_today FROM price_cov) AS tickers_with_price_today,
          ((SELECT universe_companies FROM base) - (SELECT tickers_with_price_today FROM price_cov))::int
            AS tickers_missing_price_today,

          (SELECT tickers_with_metrics_today FROM metrics_cov) AS tickers_with_metrics_today,
          ((SELECT universe_companies FROM base) - (SELECT tickers_with_metrics_today FROM metrics_cov))::int
            AS tickers_missing_metrics_today,

          (SELECT tickers_with_ma200_today FROM ma_cov) AS tickers_with_ma200_today,
          (SELECT tickers_with_rsi_today FROM rsi_cov) AS tickers_with_rsi_today,

          (SELECT duplicates_fact_prices FROM dups_prices) AS duplicates_fact_prices,
          (SELECT duplicates_fact_metrics FROM dups_metrics) AS duplicates_fact_metrics,

          (SELECT nonpositive_prices_today FROM sanity) AS nonpositive_prices_today,
          (SELECT zero_volume_today FROM sanity) AS zero_volume_today;
        """
    )

    if not row:
        raise HTTPException(status_code=500, detail="DQ query returned no results")

    r = dict(row)
    universe = int(r["universe_companies"] or 0)

    with_price = int(r["tickers_with_price_today"] or 0)
    with_metrics = int(r["tickers_with_metrics_today"] or 0)
    with_ma200 = int(r["tickers_with_ma200_today"] or 0)
    with_rsi = int(r["tickers_with_rsi_today"] or 0)

    pct_price = round((with_price / universe * 100.0), 2) if universe else 0.0
    pct_metrics = round((with_metrics / universe * 100.0), 2) if universe else 0.0
    pct_ma200 = round((with_ma200 / universe * 100.0), 2) if universe else 0.0
    pct_rsi = round((with_rsi / universe * 100.0), 2) if universe else 0.0

    payload = {
        "dq_date": today,
        "universe_companies": universe,
        "companies_in_dim": int(r["companies_in_dim"] or 0),
        "tickers_with_price_today": with_price,
        "tickers_missing_price_today": int(r["tickers_missing_price_today"] or 0),
        "pct_with_price_today": pct_price,
        "tickers_with_metrics_today": with_metrics,
        "tickers_missing_metrics_today": int(r["tickers_missing_metrics_today"] or 0),
        "pct_with_metrics_today": pct_metrics,
        "tickers_with_ma200_today": with_ma200,
        "pct_with_ma200_today": pct_ma200,
        "tickers_with_rsi_today": with_rsi,
        "pct_with_rsi_today": pct_rsi,
        "duplicates_fact_prices": int(r["duplicates_fact_prices"] or 0),
        "duplicates_fact_metrics": int(r["duplicates_fact_metrics"] or 0),
        "nonpositive_prices_today": int(r["nonpositive_prices_today"] or 0),
        "zero_volume_today": int(r["zero_volume_today"] or 0),
        "notes": None,
    }

    await _upsert_dq_snapshot(payload)

    snap = await fetchrow(
        """
        SELECT *
        FROM warehouse.data_quality_daily
        WHERE dq_date = $1
        """,
        today,
    )

    d = dict(snap)
    return DataQualitySnapshot(
        dq_date=d["dq_date"],
        created_at=d["created_at"],
        universe_companies=d["universe_companies"],
        companies_in_dim=d["companies_in_dim"],
        tickers_with_price_today=d["tickers_with_price_today"],
        tickers_missing_price_today=d["tickers_missing_price_today"],
        pct_with_price_today=float(d["pct_with_price_today"]),
        tickers_with_metrics_today=d["tickers_with_metrics_today"],
        tickers_missing_metrics_today=d["tickers_missing_metrics_today"],
        pct_with_metrics_today=float(d["pct_with_metrics_today"]),
        tickers_with_ma200_today=d["tickers_with_ma200_today"],
        pct_with_ma200_today=float(d["pct_with_ma200_today"]),
        tickers_with_rsi_today=d["tickers_with_rsi_today"],
        pct_with_rsi_today=float(d["pct_with_rsi_today"]),
        duplicates_fact_prices=d["duplicates_fact_prices"],
        duplicates_fact_metrics=d["duplicates_fact_metrics"],
        nonpositive_prices_today=d["nonpositive_prices_today"],
        zero_volume_today=d["zero_volume_today"],
        notes=d.get("notes"),
    )


@app.get("/api/dq/latest", response_model=List[DataQualitySnapshot])
async def dq_latest(limit: int = Query(30, ge=1, le=365)):
    rows = await fetch(
        """
        SELECT *
        FROM warehouse.data_quality_daily
        ORDER BY dq_date DESC
        LIMIT $1;
        """,
        limit,
    )

    out: List[DataQualitySnapshot] = []
    for r in rows:
        d = dict(r)
        out.append(
            DataQualitySnapshot(
                dq_date=d["dq_date"],
                created_at=d["created_at"],
                universe_companies=d["universe_companies"],
                companies_in_dim=d["companies_in_dim"],
                tickers_with_price_today=d["tickers_with_price_today"],
                tickers_missing_price_today=d["tickers_missing_price_today"],
                pct_with_price_today=float(d["pct_with_price_today"]),
                tickers_with_metrics_today=d["tickers_with_metrics_today"],
                tickers_missing_metrics_today=d["tickers_missing_metrics_today"],
                pct_with_metrics_today=float(d["pct_with_metrics_today"]),
                tickers_with_ma200_today=d["tickers_with_ma200_today"],
                pct_with_ma200_today=float(d["pct_with_ma200_today"]),
                tickers_with_rsi_today=d["tickers_with_rsi_today"],
                pct_with_rsi_today=float(d["pct_with_rsi_today"]),
                duplicates_fact_prices=d["duplicates_fact_prices"],
                duplicates_fact_metrics=d["duplicates_fact_metrics"],
                nonpositive_prices_today=d["nonpositive_prices_today"],
                zero_volume_today=d["zero_volume_today"],
                notes=d.get("notes"),
            )
        )
    return out
