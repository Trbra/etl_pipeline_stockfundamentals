CREATE TABLE companies (
    company_ID SERIAL PRIMARY KEY,
    ticker VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(50),
    sector VARCHAR(255),
    industry VARCHAR(255)
);

CREATE TABLE fundamentals (
    fundamental_id SERIAL PRIMARY KEY,
    company_id INT REFERENCES companies(company_ID),
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
    change_1m NUMERIC(8,4),
    change_3m NUMERIC(8,4),
    change_6m NUMERIC(8,4),
    change_1y NUMERIC(8,4),
    created_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_company_date UNIQUE (company_id, price_date)
);






