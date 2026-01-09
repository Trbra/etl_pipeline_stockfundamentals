from pydantic import BaseModel
from datetime import date, datetime
from typing import Optional

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

class StatusRow(BaseModel):
    job_name: str
    started_at: datetime
    finished_at: Optional[datetime] = None
    status: str
    rows_inserted: int
    rows_updated: int
    rows_failed: int
    message: Optional[str] = None

class Watchlist(BaseModel):
    watchlist_id: int
    name: str
