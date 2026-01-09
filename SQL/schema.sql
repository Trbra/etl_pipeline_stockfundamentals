-- SQL Schema for Financial Data Warehouse
-- -------------------------
-- Public (raw/operational)
-- -------------------------

CREATE TABLE IF NOT EXISTS companies (
    company_id SERIAL PRIMARY KEY,
    ticker VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(255),
    sector VARCHAR(255),
    industry VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS fundamentals (
    fundamental_id SERIAL PRIMARY KEY,
    company_id INT NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    report_date DATE NOT NULL,
    market_cap BIGINT,
    pe_ratio FLOAT,
    trailing_eps FLOAT,
    forward_eps FLOAT,
    dividend_yield FLOAT,
    CONSTRAINT uq_fundamentals_company_date UNIQUE (company_id, report_date)
);

CREATE TABLE IF NOT EXISTS prices (
    price_id SERIAL PRIMARY KEY,
    company_id INT NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    price_date DATE NOT NULL,
    close_price NUMERIC(12,4) NOT NULL,
    volume BIGINT,
    created_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_company_date UNIQUE (company_id, price_date)
);

CREATE TABLE IF NOT EXISTS financials (
    financial_id SERIAL PRIMARY KEY,
    company_id INT NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    report_date DATE NOT NULL,
    revenue BIGINT,
    net_income BIGINT,
    free_cash_flow BIGINT,
    debt_to_equity FLOAT,
    roe FLOAT,
    CONSTRAINT uq_financials_company_date UNIQUE (company_id, report_date)
);

CREATE TABLE IF NOT EXISTS metrics (
    metric_id SERIAL PRIMARY KEY,
    company_id INT NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    price_date DATE NOT NULL,
    ma50 NUMERIC(12,4),
    ma200 NUMERIC(12,4),
    rsi14 NUMERIC(6,2),
    CONSTRAINT uq_metrics UNIQUE (company_id, price_date)
);

CREATE INDEX IF NOT EXISTS idx_prices_company_date ON prices(company_id, price_date);
CREATE INDEX IF NOT EXISTS idx_metrics_company_date ON metrics(company_id, price_date);
CREATE INDEX IF NOT EXISTS idx_fundamentals_company_date ON fundamentals(company_id, report_date);
CREATE INDEX IF NOT EXISTS idx_financials_company_date ON financials(company_id, report_date);

-- -------------------------
-- Universe + Ticker Mapping
-- -------------------------

CREATE TABLE IF NOT EXISTS universe (
    universe_code TEXT PRIMARY KEY,     -- e.g. 'SP500', 'TSX60'
    name TEXT NOT NULL
);

INSERT INTO universe(universe_code, name) VALUES
('SP500', 'S&P 500'),
('TSX60', 'S&P/TSX 60')
ON CONFLICT (universe_code) DO NOTHING;

-- Daily snapshot of membership
CREATE TABLE IF NOT EXISTS universe_membership_daily (
    universe_code TEXT NOT NULL REFERENCES universe(universe_code),
    as_of_date DATE NOT NULL,
    ticker_raw VARCHAR(50) NOT NULL,
    source TEXT NOT NULL DEFAULT 'wikipedia',
    is_member BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (universe_code, as_of_date, ticker_raw)
);

CREATE INDEX IF NOT EXISTS idx_universe_membership_asof
ON universe_membership_daily(as_of_date, universe_code);

-- Raw ticker -> yfinance ticker mapping (TSX symbols generally need .TO)
CREATE TABLE IF NOT EXISTS ticker_map (
    ticker_raw VARCHAR(50) PRIMARY KEY,
    ticker_yf VARCHAR(60) NOT NULL,
    exchange TEXT,             -- 'US', 'TSX', etc.
    currency TEXT,             -- 'USD', 'CAD', etc.
    updated_at TIMESTAMP DEFAULT NOW()
);

-- -------------------------
-- Warehouse schema (star)
-- -------------------------

CREATE SCHEMA IF NOT EXISTS warehouse;

-- Dimension: Company
CREATE TABLE IF NOT EXISTS warehouse.dim_company (
    company_id SERIAL PRIMARY KEY,
    ticker VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(255),
    sector VARCHAR(255),
    industry VARCHAR(255)
);

-- Dimension: Date
CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_id SERIAL PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    year INT,
    quarter INT,
    month INT,
    week INT,
    day INT,
    day_of_week VARCHAR(10)
);

-- Fact: Prices
CREATE TABLE IF NOT EXISTS warehouse.fact_prices (
    fact_id BIGSERIAL PRIMARY KEY,
    company_id INT REFERENCES warehouse.dim_company(company_id),
    date_id INT REFERENCES warehouse.dim_date(date_id),
    close_price NUMERIC(12,4),
    volume BIGINT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Fact: Fundamentals
CREATE TABLE IF NOT EXISTS warehouse.fact_fundamentals (
    fact_id BIGSERIAL PRIMARY KEY,
    company_id INT REFERENCES warehouse.dim_company(company_id),
    date_id INT REFERENCES warehouse.dim_date(date_id),
    market_cap BIGINT,
    pe_ratio FLOAT,
    trailing_eps FLOAT,
    forward_eps FLOAT,
    dividend_yield FLOAT
);

-- Fact: Financials
CREATE TABLE IF NOT EXISTS warehouse.fact_financials (
    fact_id BIGSERIAL PRIMARY KEY,
    company_id INT REFERENCES warehouse.dim_company(company_id),
    date_id INT REFERENCES warehouse.dim_date(date_id),
    revenue BIGINT,
    net_income BIGINT,
    free_cash_flow BIGINT,
    debt_to_equity FLOAT,
    roe FLOAT
);

-- Fact: Metrics
CREATE TABLE IF NOT EXISTS warehouse.fact_metrics (
    fact_id BIGSERIAL PRIMARY KEY,
    company_id INT REFERENCES warehouse.dim_company(company_id),
    date_id INT REFERENCES warehouse.dim_date(date_id),
    ma50 NUMERIC(12,4),
    ma200 NUMERIC(12,4),
    rsi14 NUMERIC(6,2)
);


CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_prices ON warehouse.fact_prices(company_id, date_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_fundamentals ON warehouse.fact_fundamentals(company_id, date_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_financials ON warehouse.fact_financials(company_id, date_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_metrics ON warehouse.fact_metrics(company_id, date_id);
