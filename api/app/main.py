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

DATABASE_URL = os.getenv("DATABASE_URL")
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "http://localhost:3000")

if not DATABASE_URL:
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
            raise RuntimeError("DATABASE_URL not set")
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
    trend_score: Optional[float] = None
    rsi_score: Optional[float] = None
    value_score: Optional[float] = None
    size_score: Optional[float] = None
    yield_score: Optional[float] = None
    quality_score: Optional[float] = None
    quality_factors: Optional[Dict[str, Optional[float]]] = None
    score_raw: Optional[float] = None
    base_score: Optional[float] = None
    penalty_total: Optional[float] = None
    final_score_raw: Optional[float] = None
    final_after_penalties: Optional[float] = None
    final_score: Optional[float] = None
    penalties: Optional[Dict[str, float]] = None
    factor_percentiles: Optional[Dict[str, Optional[float]]] = None
    missing_factors: Optional[List[str]] = None
    sector_cap_applied: Optional[bool] = None
    effective_weights: Optional[Dict[str, float]] = None
    raw_values: Optional[Dict[str, Any]] = None

    normalized_weights: Optional[Dict[str, float]] = None
    contributions: Optional[Dict[str, float]] = None
    trend_source: Optional[str] = None
    avg_volume_60d: Optional[float] = None
    profit_margin: Optional[float] = None
    roe: Optional[float] = None
    debt_to_equity: Optional[float] = None

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


@app.get("/api/status", response_model=StatusResponse)
async def status():
    try:
        row = await fetchrow(
            """
            SELECT
              (SELECT COUNT(*) FROM warehouse.v_screener_latest) AS screener_rows,
              (SELECT COUNT(*) FROM warehouse.dim_company) AS dim_company_rows,
              (SELECT COUNT(*) FROM warehouse.fact_prices) AS fact_prices_rows,
              (SELECT COUNT(*) FROM warehouse.fact_metrics) AS fact_metrics_rows,
              (SELECT MAX(full_date) FROM warehouse.v_price_series) AS latest_price_date,
              (SELECT MAX(full_date) FROM warehouse.v_metrics_series) AS latest_metrics_date,
              (SELECT MAX(d.full_date)
               FROM warehouse.fact_fundamentals ff
               JOIN warehouse.dim_date d ON d.date_id = ff.date_id) AS latest_fundamentals_date;
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
                notes="Status query returned no rows",
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
        return RankingConfig(
            name="default",
            weights={
                "trend": 0.40,
                "quality": 0.25,
                "value": 0.20,
                "rsi": 0.10,
                "size": 0.05,
                "yield": 0.00,
            },
            params={
                "min_market_cap": 2000000000,
                "min_avg_volume": 250000,
                "exclude_negative_eps": False,
                "rsi_min": 0.0,
                "rsi_max": 100.0,
                "max_per_sector": 3,
                "sector_cap_top_n": 20,
                "compression_gamma": 1.0,
                "penalty_rsi_high": 0.03,
                "penalty_rsi_low": 0.03,
                "penalty_pe_50": 0.04,
                "penalty_pe_80": 0.12,
                "penalty_negative_eps": 0.35,
                "penalty_revenue_decline": 0.05,
                "penalty_eps_decline": 0.08,
                "penalty_high_debt": 0.06,
                "penalty_missing_quality": 0.03,
                "disable_yield_before_ma200": True,
            },
            active=True,
        )

    d = dict(row)
    d["weights"] = _normalize_json(d.get("weights"))
    d["params"] = _normalize_json(d.get("params"))
    return RankingConfig(**d)


@app.put("/api/ranking-config", response_model=RankingConfig)
async def update_ranking_config(cfg: RankingConfig):
    if not cfg.weights:
        raise HTTPException(status_code=400, detail="weights cannot be empty")

    s = float(sum(cfg.weights.values()))
    if s <= 0.99 or s >= 1.01:
        raise HTTPException(status_code=400, detail=f"Weights must sum to 1.0 (got {s})")

    required = {"trend", "rsi", "value", "size", "yield"}
    missing = required - set(cfg.weights.keys())
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing weight keys: {sorted(list(missing))}")

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


@app.get("/")
async def root():
    return {
        "service": "Market Screener API",
        "ok": True,
        "docs": "/docs",
        "health": "/health",
    }


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

    rows = list(reversed(rows))
    return [SeriesPoint(**dict(r)) for r in rows]


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (float, int)):
        return float(value)
    try:
        return float(value)
    except Exception:
        return None


def _normalize_dividend_yield(raw: Optional[float]) -> Optional[float]:
    if raw is None:
        return None
    value = _to_float(raw)
    if value is None:
        return None
    return (value / 100.0) if abs(value) > 5 else value * 100.0


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def _compute_percentiles(values: list[Optional[float]], invert: bool = False) -> list[Optional[float]]:
    import bisect

    clean = [v for v in values if v is not None]
    if not clean:
        return [None] * len(values)

    sorted_vals = sorted(clean)
    n = len(sorted_vals)
    out = []

    for v in values:
        if v is None:
            out.append(None)
            continue

        rank = bisect.bisect_left(sorted_vals, v)
        pct = rank / (n - 1) if n > 1 else 1.0

        if invert:
            pct = 1.0 - pct

        out.append(_clamp(pct, 0.0, 1.0))

    return out


def _score_rsi(rsi: Optional[float]) -> Optional[float]:
    r = _to_float(rsi)
    if r is None or r < 0:
        return None
    if r <= 30:
        return 0.3 + 0.7 * (r / 30.0)
    if r <= 40:
        return 0.8 + 0.2 * ((r - 30.0) / 10.0)
    if r <= 60:
        return 1.0
    if r <= 70:
        return 1.0 - 0.4 * ((r - 60.0) / 10.0)
    if r <= 80:
        return 0.6 - 0.4 * ((r - 70.0) / 10.0)
    return 0.1


def _score_value(
    pe_ratio: Optional[float],
    trailing_eps: Optional[float],
    exclude_negative_eps: bool,
) -> Optional[float]:
    pe = _to_float(pe_ratio)
    eps = _to_float(trailing_eps)

    if pe is None:
        return None

    if eps is not None and eps < 0:
        if exclude_negative_eps:
            return None
        return 0.0

    if pe <= 15.0:
        return 1.0
    if pe <= 25.0:
        return 0.8 - 0.02 * (pe - 15.0)
    if pe <= 40.0:
        return 0.6 - 0.013333333333333334 * (pe - 25.0)
    if pe <= 60.0:
        return 0.4 - 0.01 * (pe - 40.0)
    if pe <= 100.0:
        return 0.2 - 0.005 * (pe - 60.0)
    return 0.0


def _score_size(market_cap: Optional[float]) -> Optional[float]:
    m = _to_float(market_cap)
    if m is None:
        return None
    if m >= 100_000_000_000.0:
        return 1.0
    if m >= 50_000_000_000.0:
        return 0.9
    if m >= 20_000_000_000.0:
        return 0.8
    if m >= 10_000_000_000.0:
        return 0.65
    if m >= 5_000_000_000.0:
        return 0.45
    if m >= 2_000_000_000.0:
        return 0.25
    return 0.0


def _score_yield(dividend_pct: Optional[float]) -> Optional[float]:
    if dividend_pct is None:
        return None
    if dividend_pct >= 6.0:
        return 1.0
    if dividend_pct >= 4.0:
        return 0.8
    if dividend_pct >= 2.0:
        return 0.6
    if dividend_pct > 0:
        return 0.3
    return 0.0


def _score_trend(
    close_price: Optional[float],
    ma50: Optional[float],
    ma200: Optional[float],
    p20: Optional[float],
    p60: Optional[float],
) -> tuple[Optional[float], Optional[str]]:
    close = _to_float(close_price)
    if close is None:
        return None, None

    ma50_val = _to_float(ma50)
    ma200_val = _to_float(ma200)
    p20_val = _to_float(p20)
    p60_val = _to_float(p60)

    if ma200_val is not None and ma50_val is not None:
        if ma50_val > ma200_val:
            return 1.0, "long-term"
        if close > ma50_val:
            return 0.5, "long-term"
        return 0.1, "long-term"

    if ma50_val is None and p20_val is None and p60_val is None:
        return None, None

    score = 0.5

    if ma50_val is not None:
        score += 0.2 if close > ma50_val else -0.2

    if p20_val is not None and p20_val > 0:
        score += _clamp(((close / p20_val - 1.0) / 0.1) * 0.2, -0.2, 0.2)

    if p60_val is not None and p60_val > 0:
        score += _clamp(((close / p60_val - 1.0) / 0.25) * 0.1, -0.1, 0.1)

    return _clamp(score, 0.0, 1.0), "short-term"


def _normalize_weights(weights: Dict[str, float], available: Dict[str, bool]) -> Dict[str, float]:
    active = {k: float(weights.get(k, 0.0)) for k, ok in available.items() if ok}
    total = sum(active.values())

    if total <= 0.0:
        count = len(active)
        return {k: 1.0 / count for k in active} if count else {}

    return {k: v / total for k, v in active.items()}


def _compute_profit_margin(revenue: Optional[float], net_income: Optional[float]) -> Optional[float]:
    r = _to_float(revenue)
    n = _to_float(net_income)

    if r is None or n is None or r == 0:
        return None

    return n / r


def _score_quality(
    revenue: Optional[float],
    revenue_prev: Optional[float],
    net_income: Optional[float],
    net_income_prev: Optional[float],
    free_cash_flow: Optional[float],
    debt_to_equity: Optional[float],
    roe: Optional[float],
) -> tuple[Optional[float], Dict[str, Optional[float]]]:

    def norm_growth(curr: Optional[float], prev: Optional[float]) -> Optional[float]:
        c = _to_float(curr)
        p = _to_float(prev)

        if c is None or p is None or p == 0:
            return None

        g = (c - p) / abs(p)
        return _clamp((g + 0.5) / 1.0, 0.0, 1.0)

    def norm_positive(val: Optional[float]) -> Optional[float]:
        v = _to_float(val)
        if v is None:
            return None
        return 1.0 if v > 0 else 0.0

    factors: Dict[str, Optional[float]] = {}

    factors["revenue_growth"] = norm_growth(revenue, revenue_prev)
    factors["net_income_growth"] = norm_growth(net_income, net_income_prev)

    pm = _compute_profit_margin(revenue, net_income)
    if pm is None:
        factors["profit_margin"] = None
    elif pm >= 0.2:
        factors["profit_margin"] = 1.0
    elif pm >= 0.1:
        factors["profit_margin"] = 0.8
    elif pm >= 0.05:
        factors["profit_margin"] = 0.6
    elif pm > 0:
        factors["profit_margin"] = 0.4
    else:
        factors["profit_margin"] = 0.0

    if roe is None:
        factors["roe"] = None
    elif roe >= 0.25:
        factors["roe"] = 1.0
    elif roe >= 0.15:
        factors["roe"] = 0.8
    elif roe >= 0.08:
        factors["roe"] = 0.6
    elif roe >= 0:
        factors["roe"] = 0.4
    else:
        factors["roe"] = 0.0

    if debt_to_equity is None:
        factors["debt_to_equity"] = None
    else:
        d = _to_float(debt_to_equity)
        if d <= 0.5:
            factors["debt_to_equity"] = 1.0
        elif d <= 1.0:
            factors["debt_to_equity"] = 0.8
        elif d <= 2.0:
            factors["debt_to_equity"] = 0.6
        elif d <= 3.0:
            factors["debt_to_equity"] = 0.4
        else:
            factors["debt_to_equity"] = 0.2

    factors["free_cash_flow_positive"] = norm_positive(free_cash_flow)

    vals = [v for v in factors.values() if v is not None]
    if not vals:
        return None, factors

    score = sum(vals) / len(vals)
    return _clamp(score, 0.0, 1.0), factors


@app.get("/api/rankings", response_model=List[RankingRow])
async def rankings(
    sector: Optional[str] = None,
    limit: int = Query(50, ge=1, le=500),
):
    cfg = await get_ranking_config()
    params = cfg.params or {}

    min_market_cap = int(params.get("min_market_cap", 2000000000))
    min_avg_volume = int(params.get("min_avg_volume", 250000))
    exclude_negative_eps = bool(params.get("exclude_negative_eps", False))
    rsi_min = float(params.get("rsi_min", 0.0))
    rsi_max = float(params.get("rsi_max", 100.0))
    disable_yield_before_ma200 = bool(params.get("disable_yield_before_ma200", True))

    where = [
        "s.market_cap >= $1",
        "s.close_price IS NOT NULL",
        "s.rsi14 IS NOT NULL",
        "s.pe_ratio IS NOT NULL",
    ]

    args: List[Any] = [min_market_cap]
    i = 2

    if min_avg_volume > 0:
        where.append(f"p.avg_volume_60d >= ${i}")
        args.append(min_avg_volume)
        i += 1

    if exclude_negative_eps:
        where.append("(s.trailing_eps IS NULL OR s.trailing_eps >= 0)")

    if rsi_min > 0.0:
        where.append(f"s.rsi14 >= ${i}")
        args.append(rsi_min)
        i += 1

    if rsi_max < 100.0:
        where.append(f"s.rsi14 <= ${i}")
        args.append(rsi_max)
        i += 1

    if sector:
        where.append(f"s.sector = ${i}")
        args.append(sector)
        i += 1

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    query_limit = max(limit * 20, 200)

    rows = await fetch(
        f"""
        SELECT
          s.ticker,
          s.name,
          s.sector,
          s.industry,
          s.price_date,
          s.close_price,
          s.rsi14,
          s.pe_ratio,
          s.dividend_yield,
          s.market_cap,
          s.trailing_eps,
          s.ma50,
          s.ma200,
          s.trend_bullish,
          s.rsi_oversold,
          s.rsi_overbought,
          p.p20_price,
          p.p60_price,
          p.avg_volume_60d,
          f.revenue,
          f.net_income,
          fprev.revenue AS revenue_prev,
          fprev.net_income AS net_income_prev,
          f.free_cash_flow,
          f.debt_to_equity,
          f.roe
        FROM warehouse.v_screener_latest s
        LEFT JOIN LATERAL (
          SELECT
            max(close_price) FILTER (WHERE rn = 20) AS p20_price,
            max(close_price) FILTER (WHERE rn = 60) AS p60_price,
            avg(volume) FILTER (WHERE rn <= 60) AS avg_volume_60d
          FROM (
            SELECT close_price, volume,
              row_number() OVER (ORDER BY full_date DESC) AS rn
            FROM warehouse.v_price_series ps
            WHERE ps.ticker = s.ticker
          ) p
        ) p ON TRUE
        LEFT JOIN LATERAL (
          SELECT ff.revenue, ff.net_income, ff.free_cash_flow, ff.debt_to_equity, ff.roe
          FROM warehouse.fact_financials ff
          JOIN warehouse.dim_company c2 ON c2.company_id = ff.company_id
          JOIN warehouse.dim_date d2 ON d2.date_id = ff.date_id
          WHERE c2.ticker = s.ticker
          ORDER BY d2.full_date DESC
          LIMIT 1
        ) f ON TRUE
        LEFT JOIN LATERAL (
          SELECT ff2.revenue, ff2.net_income
          FROM warehouse.fact_financials ff2
          JOIN warehouse.dim_company c3 ON c3.company_id = ff2.company_id
          JOIN warehouse.dim_date d3 ON d3.date_id = ff2.date_id
          WHERE c3.ticker = s.ticker
          ORDER BY d3.full_date DESC
          OFFSET 1 LIMIT 1
        ) fprev ON TRUE
        {where_sql}
        ORDER BY s.market_cap DESC NULLS LAST
        LIMIT {query_limit};
        """,
        *args,
    )

    out: List[RankingRow] = []

    for r in rows:
        row = dict(r)

        dividend_yield_pct = _normalize_dividend_yield(row.get("dividend_yield"))

        trend_score, trend_source = _score_trend(
            row.get("close_price"),
            row.get("ma50"),
            row.get("ma200"),
            row.get("p20_price"),
            row.get("p60_price"),
        )

        yield_score = _score_yield(dividend_yield_pct)
        if disable_yield_before_ma200 and row.get("ma200") is None:
            yield_score = None

        quality_score, quality_factors = _score_quality(
            row.get("revenue"),
            row.get("revenue_prev"),
            row.get("net_income"),
            row.get("net_income_prev"),
            row.get("free_cash_flow"),
            row.get("debt_to_equity"),
            row.get("roe"),
        )

        scores = {
            "trend": trend_score,
            "rsi": _score_rsi(row.get("rsi14")),
            "value": _score_value(row.get("pe_ratio"), row.get("trailing_eps"), exclude_negative_eps),
            "size": _score_size(row.get("market_cap")),
            "yield": yield_score,
            "quality": quality_score,
        }

        available = {k: v is not None for k, v in scores.items()}
        if disable_yield_before_ma200 and row.get("ma200") is None:
            available["yield"] = False

        normalized_weights = _normalize_weights(cfg.weights, available)

        contributions = {
            k: normalized_weights.get(k, 0.0) * scores[k]
            for k in scores
            if scores[k] is not None and k in normalized_weights
        }

        final_score_raw = sum(contributions.values())
        missing = [k for k, v in scores.items() if v is None]

        out.append(
            RankingRow(
                ticker=row.get("ticker"),
                name=row.get("name"),
                sector=row.get("sector"),
                price_date=row.get("price_date"),
                close_price=_to_float(row.get("close_price")),
                rsi14=_to_float(row.get("rsi14")),
                pe_ratio=_to_float(row.get("pe_ratio")),
                dividend_yield=_to_float(row.get("dividend_yield")),
                market_cap=_to_float(row.get("market_cap")),
                score=final_score_raw,
                score_raw=final_score_raw,
                trend_score=trend_score,
                rsi_score=scores["rsi"],
                value_score=scores["value"],
                size_score=scores["size"],
                yield_score=yield_score,
                quality_score=quality_score,
                quality_factors=quality_factors,
                normalized_weights=normalized_weights,
                contributions=contributions,
                trend_source=trend_source,
                avg_volume_60d=_to_float(row.get("avg_volume_60d")),
                profit_margin=_compute_profit_margin(row.get("revenue"), row.get("net_income")),
                roe=_to_float(row.get("roe")),
                debt_to_equity=_to_float(row.get("debt_to_equity")),
                missing_factors=missing,
                effective_weights=normalized_weights,
                raw_values={
                    "revenue": _to_float(row.get("revenue")),
                    "revenue_prev": _to_float(row.get("revenue_prev")),
                    "net_income": _to_float(row.get("net_income")),
                    "net_income_prev": _to_float(row.get("net_income_prev")),
                    "free_cash_flow": _to_float(row.get("free_cash_flow")),
                    "trailing_eps": _to_float(row.get("trailing_eps")),
                },
                reasons={
                    "trend_bullish": row.get("trend_bullish"),
                    "trend_source": trend_source,
                },
            )
        )

    trend_vals = [r.trend_score for r in out]
    quality_vals = [r.quality_score for r in out]
    value_vals = [r.pe_ratio for r in out]
    rsi_vals = [r.rsi14 for r in out]
    size_vals = [r.market_cap for r in out]
    yield_vals = [r.dividend_yield for r in out]

    trend_pcts = _compute_percentiles(trend_vals, invert=False)
    quality_pcts = _compute_percentiles(quality_vals, invert=False)
    value_clean = [v if (v is not None and v > 0) else None for v in value_vals]
    value_pcts = _compute_percentiles(value_clean, invert=True)
    rsi_pcts = _compute_percentiles(rsi_vals, invert=False)
    size_pcts = _compute_percentiles(size_vals, invert=False)
    yield_pcts = _compute_percentiles(yield_vals, invert=False)

    p_rsi_high = float(params.get("penalty_rsi_high", 0.03))
    p_rsi_low = float(params.get("penalty_rsi_low", 0.03))
    p_pe_50 = float(params.get("penalty_pe_50", 0.04))
    p_pe_80 = float(params.get("penalty_pe_80", 0.12))
    p_neg_eps = float(params.get("penalty_negative_eps", 0.35))
    p_rev_decline = float(params.get("penalty_revenue_decline", 0.05))
    p_eps_decline = float(params.get("penalty_eps_decline", 0.08))
    p_high_debt = float(params.get("penalty_high_debt", 0.06))
    p_missing_quality = float(params.get("penalty_missing_quality", 0.03))

    for i, r in enumerate(out):
        factor_pcts = {
            "trend": trend_pcts[i],
            "quality": quality_pcts[i],
            "value": value_pcts[i],
            "rsi": rsi_pcts[i],
            "size": size_pcts[i],
            "yield": yield_pcts[i],
        }

        avail = {k: factor_pcts.get(k) is not None for k in factor_pcts}
        effective = _normalize_weights(cfg.weights, avail)

        contribs = {}
        for k, pct in factor_pcts.items():
            if pct is None:
                continue
            contribs[k] = effective.get(k, 0.0) * pct

        final_raw = sum(contribs.values())

        penalties: Dict[str, float] = {}

        rsi_val = r.rsi14
        if rsi_val is not None:
            if rsi_val > 75:
                penalties["rsi_high"] = p_rsi_high
            if rsi_val < 30:
                penalties["rsi_low"] = p_rsi_low

        pe = r.pe_ratio
        if pe is not None:
            if pe > 80:
                penalties["pe_80"] = p_pe_80
            elif pe > 50:
                penalties["pe_50"] = p_pe_50
        else:
            penalties["pe_missing"] = 0.01

        if r.raw_values and r.raw_values.get("trailing_eps") is not None:
            if r.raw_values.get("trailing_eps") < 0:
                penalties["negative_eps"] = p_neg_eps

        rev = r.raw_values.get("revenue") if r.raw_values else None
        rev_prev = r.raw_values.get("revenue_prev") if r.raw_values else None
        if rev is not None and rev_prev is not None and rev_prev != 0 and rev < rev_prev:
            penalties["revenue_decline"] = p_rev_decline

        ni = r.raw_values.get("net_income") if r.raw_values else None
        ni_prev = r.raw_values.get("net_income_prev") if r.raw_values else None
        if ni is not None and ni_prev is not None and ni_prev != 0 and ni < ni_prev:
            penalties["eps_decline"] = p_eps_decline

        dte = r.debt_to_equity
        if dte is not None and dte > 3.0:
            penalties["high_debt"] = p_high_debt

        if r.quality_score is None:
            penalties["missing_quality"] = p_missing_quality

        total_penalty = sum(penalties.values())

        r.normalized_weights = effective
        r.contributions = contribs
        r.score_raw = final_raw
        r.base_score = final_raw
        r.penalties = penalties
        r.penalty_total = total_penalty
        r.factor_percentiles = factor_pcts
        r.final_after_penalties = _clamp(final_raw - total_penalty, 0.0, 1.0)
        r.final_score_raw = r.final_after_penalties

    final_vals = [r.final_after_penalties for r in out]
    final_pcts = _compute_percentiles(final_vals, invert=False)

    for i, r in enumerate(out):
        adjusted_score = r.final_after_penalties or 0.0

        r.score = adjusted_score
        r.final_score = adjusted_score

        if r.factor_percentiles is None:
            r.factor_percentiles = {}

        r.factor_percentiles["final_percentile"] = final_pcts[i]

    max_per_sector = int(params.get("max_per_sector", 3))

    selected: List[RankingRow] = []
    excluded: List[RankingRow] = []
    counts: Dict[str, int] = {}

    sorted_rows = sorted(out, key=lambda rr: rr.score or 0.0, reverse=True)

    for r in sorted_rows:
        key = r.sector or "UNKNOWN"
        cnt = counts.get(key, 0)

        if cnt < max_per_sector:
            selected.append(r)
            counts[key] = cnt + 1
            r.sector_cap_applied = False
        else:
            excluded.append(r)
            r.sector_cap_applied = True

    final_rows: List[RankingRow] = selected[:limit]

    if len(final_rows) < limit:
        need = limit - len(final_rows)
        final_rows.extend(excluded[:need])

    return final_rows


async def _upsert_dq_snapshot(d: Dict[str, Any]) -> None:
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
          ((SELECT universe_companies FROM base) - (SELECT tickers_with_price_today FROM price_cov))::int AS tickers_missing_price_today,
          (SELECT tickers_with_metrics_today FROM metrics_cov) AS tickers_with_metrics_today,
          ((SELECT universe_companies FROM base) - (SELECT tickers_with_metrics_today FROM metrics_cov))::int AS tickers_missing_metrics_today,
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