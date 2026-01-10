"use client";

import { useEffect, useMemo, useState } from "react";
import { apiGet } from "../../lib/api";
import CompareTickerPicker, { PickedTicker } from "../../components/CompareTickerPicker";
import MultiTickerCharts from "../../components/MultiTickerCharts";
import FundamentalsCompare from "../../components/FundamentalsCompare";

export type SeriesPoint = {
  date: string; // ISO date
  close?: number | null;
  volume?: number | null;
  ma50?: number | null;
  ma200?: number | null;
  rsi14?: number | null;
};

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
};

async function fetchTickerSeries(ticker: string, days: number) {
  return apiGet<SeriesPoint[]>(`/api/company/${encodeURIComponent(ticker)}/series?days=${days}`);
}

async function fetchTickerSnapshot(ticker: string) {
  // screener returns list; we pick exact ticker match if present
  const rows = await apiGet<ScreenerRow[]>(
    `/api/screener?q=${encodeURIComponent(ticker)}&limit=20`
  );
  const exact = rows.find((r) => r.ticker?.toUpperCase() === ticker.toUpperCase());
  return exact ?? rows[0] ?? null;
}

export default function ComparePage() {
  const [picked, setPicked] = useState<PickedTicker[]>([
    { ticker: "AAPL", name: "Apple Inc." },
    { ticker: "MSFT", name: "Microsoft" },
  ]);
  const [days, setDays] = useState(365);
  const [normalize, setNormalize] = useState(true);

  const [seriesByTicker, setSeriesByTicker] = useState<Record<string, SeriesPoint[]>>({});
  const [snapByTicker, setSnapByTicker] = useState<Record<string, ScreenerRow | null>>({});
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const tickers = useMemo(
    () => picked.map((p) => p.ticker.toUpperCase()).filter(Boolean),
    [picked]
  );

  useEffect(() => {
    let cancelled = false;

    async function run() {
      if (!tickers.length) return;
      setLoading(true);
      setErr(null);

      try {
        // fetch series in parallel
        const seriesPairs = await Promise.all(
          tickers.map(async (t) => [t, await fetchTickerSeries(t, days)] as const)
        );
        const nextSeries: Record<string, SeriesPoint[]> = {};
        for (const [t, s] of seriesPairs) nextSeries[t] = s;

        // fetch snapshots in parallel (light)
        const snapPairs = await Promise.all(
          tickers.map(async (t) => [t, await fetchTickerSnapshot(t)] as const)
        );
        const nextSnaps: Record<string, ScreenerRow | null> = {};
        for (const [t, s] of snapPairs) nextSnaps[t] = s;

        if (!cancelled) {
          setSeriesByTicker(nextSeries);
          setSnapByTicker(nextSnaps);
        }
      } catch (e: any) {
        if (!cancelled) setErr(String(e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    run();
    return () => {
      cancelled = true;
    };
  }, [days, tickers.join("|")]); // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <div className="space-y-4">
      <div className="bg-zinc-900/40 border border-zinc-800 rounded-2xl p-4">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
          <div>
            <div className="text-xl font-semibold">Compare</div>
            <div className="text-sm text-zinc-400 mt-1">
              Overlay multiple tickers across price, performance, RSI, and volume.
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-3">
            <label className="text-sm text-zinc-300 flex items-center gap-2">
              Lookback
              <select
                className="bg-zinc-950/40 border border-zinc-800 rounded-xl px-3 py-2 text-sm"
                value={days}
                onChange={(e) => setDays(Number(e.target.value))}
              >
                <option value={90}>90d</option>
                <option value={180}>180d</option>
                <option value={365}>1y</option>
                <option value={730}>2y</option>
                <option value={1825}>5y</option>
              </select>
            </label>

            <label className="text-sm text-zinc-300 flex items-center gap-2">
              <input
                type="checkbox"
                checked={normalize}
                onChange={(e) => setNormalize(e.target.checked)}
              />
              Normalize (base 100)
            </label>
          </div>
        </div>

        <div className="mt-4">
          <CompareTickerPicker picked={picked} onChange={setPicked} />
        </div>

        {loading ? <div className="mt-3 text-zinc-400 text-sm">Loading…</div> : null}
        {err ? <div className="mt-3 text-red-300 text-sm whitespace-pre-wrap">{err}</div> : null}
      </div>

      <FundamentalsCompare tickers={tickers} snapshots={snapByTicker} />

      <MultiTickerCharts
        tickers={tickers}
        seriesByTicker={seriesByTicker}
        normalize={normalize}
      />
    </div>
  );
}
