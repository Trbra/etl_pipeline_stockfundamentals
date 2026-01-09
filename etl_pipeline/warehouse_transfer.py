import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}

conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

try:
    # Dim Company
    cur.execute(
        """
        INSERT INTO warehouse.dim_company (ticker, name, sector, industry)
        SELECT ticker, name, sector, industry
        FROM public.companies
        ON CONFLICT (ticker) DO NOTHING;
        """
    )
    conn.commit()

    # Dim Date
    cur.execute(
        """
        INSERT INTO warehouse.dim_date (full_date, year, quarter, month, week, day, day_of_week)
        SELECT d::date,
            EXTRACT(YEAR FROM d)::INT,
            EXTRACT(QUARTER FROM d)::INT,
            EXTRACT(MONTH FROM d)::INT,
            EXTRACT(WEEK FROM d)::INT,
            EXTRACT(DAY FROM d)::INT,
            TRIM(TO_CHAR(d, 'Day'))
        FROM generate_series('2015-01-01'::date, '2030-12-31'::date, interval '1 day') d
        ON CONFLICT (full_date) DO NOTHING;
        """
    )
    conn.commit()

    # Fact Prices
    cur.execute(
        """
        INSERT INTO warehouse.fact_prices (company_id, date_id, close_price, volume, created_at)
        SELECT c.company_id,
               d.date_id,
               p.close_price,
               p.volume,
               p.created_at
        FROM public.prices p
        JOIN public.companies pc ON pc.company_id = p.company_id
        JOIN warehouse.dim_company c ON c.ticker = pc.ticker
        JOIN warehouse.dim_date d ON d.full_date = p.price_date
        ON CONFLICT DO NOTHING;
        """
    )
    conn.commit()

    # Fact Fundamentals
    cur.execute(
        """
        INSERT INTO warehouse.fact_fundamentals (company_id, date_id, market_cap, pe_ratio, trailing_eps, forward_eps, dividend_yield)
        SELECT c.company_id,
               d.date_id,
               f.market_cap,
               f.pe_ratio,
               f.trailing_eps,
               f.forward_eps,
               f.dividend_yield
        FROM public.fundamentals f
        JOIN public.companies pc ON pc.company_id = f.company_id
        JOIN warehouse.dim_company c ON c.ticker = pc.ticker
        JOIN warehouse.dim_date d ON d.full_date = f.report_date
        ON CONFLICT DO NOTHING;
        """
    )
    conn.commit()

    # Fact Financials
    cur.execute(
        """
        INSERT INTO warehouse.fact_financials (company_id, date_id, revenue, net_income, free_cash_flow, debt_to_equity, roe)
        SELECT c.company_id,
               d.date_id,
               f.revenue,
               f.net_income,
               f.free_cash_flow,
               f.debt_to_equity,
               f.roe
        FROM public.financials f
        JOIN public.companies pc ON pc.company_id = f.company_id
        JOIN warehouse.dim_company c ON c.ticker = pc.ticker
        JOIN warehouse.dim_date d ON d.full_date = f.report_date
        ON CONFLICT DO NOTHING;
        """
    )
    conn.commit()

    # Fact Metrics
    cur.execute(
        """
        INSERT INTO warehouse.fact_metrics (company_id, date_id, ma50, ma200, rsi14)
        SELECT c.company_id,
               d.date_id,
               m.ma50,
               m.ma200,
               m.rsi14
        FROM public.metrics m
        JOIN public.companies pc ON pc.company_id = m.company_id
        JOIN warehouse.dim_company c ON c.ticker = pc.ticker
        JOIN warehouse.dim_date d ON d.full_date = m.price_date
        ON CONFLICT DO NOTHING;
        """
    )
    conn.commit()

finally:
    cur.close()
    conn.close()
    print("ETL process complete")
