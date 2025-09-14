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





