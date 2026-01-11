📊 Market Screener & Ranking Platform

A full-stack, production-style financial data platform that ingests, processes, ranks, and visualizes equity market data for U.S. and Canadian stocks.
Built to demonstrate data engineering, backend API design, analytics modeling, and frontend visualization.

🚀 Live Demo

Frontend (Vercel):
https://etl-pipeline-stockfundamentals.vercel.app

API (Render):
https://etl-pipeline-stockfundamentals.onrender.com

🏗️ Architecture Overview

Frontend

Next.js (App Router)

TypeScript

TailwindCSS

Recharts (interactive financial charts)

Hosted on Vercel

Backend

FastAPI (async)

PostgreSQL access via asyncpg

REST API for screener, rankings, charts, and system status

Hosted on Render

Data Platform

PostgreSQL (Neon)

Raw operational tables + analytics warehouse (star schema)

Daily universe snapshots (S&P 500 + S&P/TSX 60)

Derived metrics (MA50, MA200, RSI, trends)

Configurable ranking engine

Automation

GitHub Actions (cron-based ETL scheduling)

Environment-agnostic DB connectivity via DATABASE_URL

🔄 Data Pipeline (ETL)

The pipeline is designed to be idempotent, automated, and production-safe.

Steps

Universe Refresh

Pulls S&P 500 and TSX 60 constituents from Wikipedia

Tracks daily membership snapshots

Maps raw tickers to Yahoo Finance symbols

Fundamentals Ingestion

Company metadata

Market cap, EPS, P/E, dividends

Financial statements (revenue, FCF, ROE, debt)

Price & Metrics Ingestion

Daily OHLC close + volume

Rolling MA50 / MA200

RSI(14)

Warehouse Load

Star schema (dim_company, dim_date)

Fact tables (fact_prices, fact_metrics, etc.)

Data Quality & Status

Latest data timestamps

Row counts per table

Health endpoint exposed to frontend

🧠 Ranking Engine

Stocks are scored using a weighted multi-factor model, fully configurable at runtime.

Factors

Trend (MA50 vs MA200)

Momentum (RSI)

Value (P/E)

Size (market cap)

Yield (dividends)

Features

Adjustable weights (sum to 1.0)

Transparent sub-scores

Human-readable “reasons” per ranking

Rankings exposed via API and UI

📈 Frontend Features
Screener

Search by ticker or name

Filter by sector, RSI, trend

Live data from warehouse views

Rankings

Sort by composite score

Explainable ranking breakdown

Adjustable weighting (future extension ready)

Compare

Multi-ticker comparison

Overlayed price charts (normalized)

RSI comparison

Volume bars

Moving averages

Color-coded tickers with legends

Hoverable info tooltips for novice users

Company Detail

Individual stock charts

Price, volume, RSI, trend indicators

Status

Backend health

Database connectivity

Latest data freshness

Row counts per fact table

🗄️ Database Design
Operational (Raw)

companies

prices

fundamentals

financials

metrics

universe_membership_daily

ticker_map

Warehouse (Star Schema)

Dimensions: dim_company, dim_date

Facts: fact_prices, fact_metrics, fact_fundamentals, fact_financials

Views for analytics and API consumption

⚙️ Deployment
Backend (Render)

FastAPI app

Environment variables:

DATABASE_URL

CORS_ORIGINS

Database (Neon)

Managed PostgreSQL

SSL-secured connections

Shared across API + ETL

Frontend (Vercel)

Next.js app

Environment variables:

NEXT_PUBLIC_API_BASE

Automation (GitHub Actions)

Scheduled ETL runs via cron

Secure secrets management

Logs retained as workflow artifacts

🧪 Local Development
# Backend
cd api
uvicorn app.main:app --reload

# Frontend
cd web
npm install
npm run dev

🧩 Why This Project

This project was built to simulate real-world data engineering and analytics workflows, not just a demo app.

It demonstrates:

Production-style ETL design

Star-schema data modeling

Async API development

Cross-cloud deployment

Automated scheduling

Explainable analytics

Clean UI for technical & non-technical users

📌 Future Enhancements

Alerting (email / Slack)

Portfolio tracking (paper trading)

Historical ranking backtests

Authentication

Caching layer (Redis)

Feature flags for ranking models

👤 Author

Built by Troy Brajcic
Computer Science / Data Engineering focused
🇨🇦 Canada
