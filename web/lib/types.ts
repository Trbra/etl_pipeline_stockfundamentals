export type ScreenerRow = {
  ticker: string;
  name?: string | null;
  sector?: string | null;
  industry?: string | null;

  price_date?: string | null;
  close_price?: number | null;
  volume?: number | null;

  ma50?: number | null;
  ma200?: number | null;
  rsi14?: number | null;

  market_cap?: number | null;
  pe_ratio?: number | null;
  dividend_yield?: number | null;

  trend_bullish?: boolean | null;
  rsi_oversold?: boolean | null;
  rsi_overbought?: boolean | null;
};

export type SeriesPoint = {
  date: string;
  close?: number | null;
  volume?: number | null;
  ma50?: number | null;
  ma200?: number | null;
  rsi14?: number | null;
};

export type StatusRow = {
  job_name: string;
  started_at: string;
  finished_at?: string | null;
  status: string;
  rows_inserted: number;
  rows_updated: number;
  rows_failed: number;
  message?: string | null;
};

export type Watchlist = {
  watchlist_id: number;
  name: string;
};
