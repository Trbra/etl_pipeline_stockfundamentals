-- ============================================================
-- GUI Views + Pipeline Run Logging (Resume-grade)
-- ============================================================

-- -------------------------
-- 1) Run logging tables
-- -------------------------
CREATE TABLE IF NOT EXISTS public.etl_run_history (
    run_id BIGSERIAL PRIMARY KEY,
    job_name TEXT NOT NULL,
    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMP,
    status TEXT NOT NULL DEFAULT 'running',  -- running|success|failed
    rows_inserted BIGINT DEFAULT 0,
    rows_updated BIGINT DEFAULT 0,
    rows_failed BIGINT DEFAULT 0,
    message TEXT
);

CREATE TABLE IF NOT EXISTS public.etl_failures (
    failure_id BIGSERIAL PRIMARY KEY,
    run_id BIGINT REFERENCES public.etl_run_history(run_id) ON DELETE CASCADE,
    ticker_raw VARCHAR(50),
    ticker_yf VARCHAR(60),
    stage TEXT,         -- download|insert_prices|insert_metrics|etc
    error TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- -------------------------
-- 2) “Gold” / API-ready views
--    These query from warehouse.* (fast & consistent)
-- -------------------------

-- Latest date_id per company in fact_prices
CREATE OR REPLACE VIEW warehouse.v_latest_price_date AS
SELECT
  company_id,
  MAX(date_id) AS latest_date_id
FROM warehouse.fact_prices
GROUP BY company_id;

-- Latest price snapshot per company
CREATE OR REPLACE VIEW warehouse.v_latest_prices AS
SELECT
  fp.company_id,
  fp.date_id,
  fp.close_price,
  fp.volume,
  fp.created_at
FROM warehouse.fact_prices fp
JOIN warehouse.v_latest_price_date l
  ON l.company_id = fp.company_id
 AND l.latest_date_id = fp.date_id;

-- Latest metrics snapshot aligned to latest price date
CREATE OR REPLACE VIEW warehouse.v_latest_metrics AS
SELECT
  fm.company_id,
  fm.date_id,
  fm.ma50,
  fm.ma200,
  fm.rsi14
FROM warehouse.fact_metrics fm
JOIN warehouse.v_latest_price_date l
  ON l.company_id = fm.company_id
 AND l.latest_date_id = fm.date_id;

-- Latest fundamentals date_id per company
CREATE OR REPLACE VIEW warehouse.v_latest_fund_date AS
SELECT
  company_id,
  MAX(date_id) AS latest_date_id
FROM warehouse.fact_fundamentals
GROUP BY company_id;

-- Latest fundamentals snapshot
CREATE OR REPLACE VIEW warehouse.v_latest_fundamentals AS
SELECT
  ff.company_id,
  ff.date_id,
  ff.market_cap,
  ff.pe_ratio,
  ff.trailing_eps,
  ff.forward_eps,
  ff.dividend_yield
FROM warehouse.fact_fundamentals ff
JOIN warehouse.v_latest_fund_date l
  ON l.company_id = ff.company_id
 AND l.latest_date_id = ff.date_id;

-- Main screener view: one row per company (latest price/metrics/fundamentals)
CREATE OR REPLACE VIEW warehouse.v_screener_latest AS
SELECT
  c.company_id,
  c.ticker,
  c.name,
  c.sector,
  c.industry,

  d.full_date AS price_date,
  p.close_price,
  p.volume,

  m.ma50,
  m.ma200,
  m.rsi14,

  fd.full_date AS fundamentals_date,
  f.market_cap,
  f.pe_ratio,
  f.trailing_eps,
  f.forward_eps,
  f.dividend_yield,

  -- derived “signals” (simple but useful)
  CASE WHEN m.ma50 IS NOT NULL AND m.ma200 IS NOT NULL AND m.ma50 > m.ma200 THEN TRUE ELSE FALSE END AS trend_bullish,
  CASE WHEN m.rsi14 IS NOT NULL AND m.rsi14 <= 30 THEN TRUE ELSE FALSE END AS rsi_oversold,
  CASE WHEN m.rsi14 IS NOT NULL AND m.rsi14 >= 70 THEN TRUE ELSE FALSE END AS rsi_overbought

FROM warehouse.dim_company c
LEFT JOIN warehouse.v_latest_prices p ON p.company_id = c.company_id
LEFT JOIN warehouse.dim_date d ON d.date_id = p.date_id
LEFT JOIN warehouse.v_latest_metrics m ON m.company_id = c.company_id AND m.date_id = p.date_id
LEFT JOIN warehouse.v_latest_fundamentals f ON f.company_id = c.company_id
LEFT JOIN warehouse.dim_date fd ON fd.date_id = f.date_id;

-- Price series view (for chart endpoint)
CREATE OR REPLACE VIEW warehouse.v_price_series AS
SELECT
  c.ticker,
  d.full_date,
  fp.close_price,
  fp.volume
FROM warehouse.fact_prices fp
JOIN warehouse.dim_company c ON c.company_id = fp.company_id
JOIN warehouse.dim_date d ON d.date_id = fp.date_id;

-- Metrics series view (for RSI/MA overlays)
CREATE OR REPLACE VIEW warehouse.v_metrics_series AS
SELECT
  c.ticker,
  d.full_date,
  fm.ma50,
  fm.ma200,
  fm.rsi14
FROM warehouse.fact_metrics fm
JOIN warehouse.dim_company c ON c.company_id = fm.company_id
JOIN warehouse.dim_date d ON d.date_id = fm.date_id;

-- Watchlists (simple product feature)
CREATE TABLE IF NOT EXISTS public.watchlists (
    watchlist_id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.watchlist_items (
    watchlist_id BIGINT NOT NULL REFERENCES public.watchlists(watchlist_id) ON DELETE CASCADE,
    ticker VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (watchlist_id, ticker)
);
