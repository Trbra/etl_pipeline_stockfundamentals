CREATE TABLE companies (
	company_ID SERIAL PRIMARY KEY,
	ticker varchar(50) UNIQUE NOT NULL,
	name varchar(50),
	sector varchar(255),
	industry varchar(255)
);

CREATE TABLE fundamentals (
	fundamental_id SERIAL PRIMARY KEY,
	company_id INT REFERENCES companies(company_ID),
	report_date DATE,
	market_cap BIGINT,
	pe_ratio FLOAT,
	trailing_eps FLOAT,
	forward_eps FLOAT,
	dividend_yield FLOAT
);