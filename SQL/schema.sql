CREATE TABLE companies (
    company_ID SERIAL PRIMARY KEY,
    ticker VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(50),
    sector VARCHAR(255),
    industry VARCHAR(255)
);

CREATE TABLE fundamentals (
    fundamental_id SERIAL PRIMARY KEY,
    company_id INT REFERENCES companies(company_ID) ON DELETE CASCADE,
    report_date DATE,
    market_cap BIGINT,
    pe_ratio FLOAT,
    trailing_eps FLOAT,
    forward_eps FLOAT,
    dividend_yield FLOAT,
    CONSTRAINT uq_fundamentals_company_date UNIQUE (company_id, report_date)
);

CREATE TABLE prices (
    price_id SERIAL PRIMARY KEY,
    company_id INT REFERENCES companies(company_id) ON DELETE CASCADE,
    price_date DATE NOT NULL,
    close_price NUMERIC(12,4) NOT NULL,
	volume BIGINT,
    created_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_company_date UNIQUE (company_id, price_date)
);

CREATE TABLE financials (
    financial_id SERIAL PRIMARY KEY,
    company_id INT REFERENCES companies(company_id) ON DELETE CASCADE,
    report_date DATE NOT NULL,
    revenue BIGINT,
    net_income BIGINT,
    free_cash_flow BIGINT,
    debt_to_equity FLOAT,
    roe FLOAT,
	volume BIGINT,
    CONSTRAINT uq_financials_company_date UNIQUE (company_id, report_date)
);

CREATE TABLE metrics (
    metric_id SERIAL PRIMARY KEY,
    company_id INT REFERENCES companies(company_id) ON DELETE CASCADE,
    price_date DATE NOT NULL,
    ma50 NUMERIC(12,4),
    ma200 NUMERIC(12,4),
    rsi14 NUMERIC(6,2),
    CONSTRAINT uq_metrics UNIQUE (company_id, price_date)
);


-- Create warehouse schema
CREATE SCHEMA warehouse;

-- Dimension: Company
CREATE TABLE warehouse.dim_company (
    company_id SERIAL PRIMARY KEY,
    ticker VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(255),
    sector VARCHAR(255),
    industry VARCHAR(255)
);

-- Dimension: Date (standard calendar dimension)
CREATE TABLE warehouse.dim_date (
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
CREATE TABLE warehouse.fact_prices (
    fact_id BIGSERIAL PRIMARY KEY,
    company_id INT REFERENCES warehouse.dim_company(company_id),
    date_id INT REFERENCES warehouse.dim_date(date_id),
    close_price NUMERIC(12,4),
    volume BIGINT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Fact: Fundamentals
CREATE TABLE warehouse.fact_fundamentals (
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
CREATE TABLE warehouse.fact_financials (
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
CREATE TABLE warehouse.fact_metrics (
    fact_id BIGSERIAL PRIMARY KEY,
    company_id INT REFERENCES warehouse.dim_company(company_id),
    date_id INT REFERENCES warehouse.dim_date(date_id),
    ma50 NUMERIC(12,4),
    ma200 NUMERIC(12,4),
    rsi14 NUMERIC(6,2)
);






