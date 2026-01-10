"use client";

import { useMemo } from "react";
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  Cell,
} from "recharts";

type ScreenerRow = {
  ticker: string;
  name?: string | null;
  sector?: string | null;
  market_cap?: number | null;
  pe_ratio?: number | null;
  dividend_yield?: number | null;
};

const PALETTE = [
  "#60a5fa", // blue
  "#f87171", // red
  "#34d399", // green
  "#fbbf24", // amber
  "#a78bfa", // purple
  "#fb7185", // pink
  "#22d3ee", // cyan
  "#c084fc", // violet
  "#4ade80", // lime
  "#f97316", // orange
  "#eab308", // yellow
  "#93c5fd", // light blue
];

function hashString(s: string) {
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) >>> 0;
  return h;
}

function colorForTicker(ticker: string) {
  return PALETTE[hashString(ticker) % PALETTE.length];
}

export default function FundamentalsCompare({
  tickers,
  snapshots,
}: {
  tickers: string[];
  snapshots: Record<string, ScreenerRow | null>;
}) {
  const data = useMemo(() => {
    return tickers.map((t) => {
      const s = snapshots[t];
      return {
        ticker: t,
        pe_ratio: s?.pe_ratio ?? null,
        dividend_yield_pct: s?.dividend_yield != null ? s.dividend_yield * 100 : null,
        market_cap_b: s?.market_cap != null ? s.market_cap / 1e9 : null,
      };
    });
  }, [tickers, snapshots]);

  if (!tickers.length) return null;

  return (
    <div className="bg-zinc-900/40 border border-zinc-800 rounded-2xl p-4 min-w-0">
      <div className="flex items-center justify-between gap-3 min-w-0">
        <div className="font-semibold truncate">Fundamentals Snapshot</div>
        <div className="text-xs text-zinc-500 truncate">
          Latest values from the warehouse screener view
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 mt-3 min-w-0">
        <ChartCard title="P/E Ratio">
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={data}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="ticker" />
              <YAxis domain={["auto", "auto"]} />
              <Tooltip />
              <Legend />
              <Bar dataKey="pe_ratio" isAnimationActive={false}>
                {data.map((row) => (
                  <Cell key={`pe-${row.ticker}`} fill={colorForTicker(row.ticker)} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Dividend Yield (%)">
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={data}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="ticker" />
              <YAxis domain={["auto", "auto"]} />
              <Tooltip />
              <Legend />
              <Bar dataKey="dividend_yield_pct" isAnimationActive={false}>
                {data.map((row) => (
                  <Cell key={`y-${row.ticker}`} fill={colorForTicker(row.ticker)} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Market Cap (B)">
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={data}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="ticker" />
              <YAxis domain={["auto", "auto"]} />
              <Tooltip />
              <Legend />
              <Bar dataKey="market_cap_b" isAnimationActive={false}>
                {data.map((row) => (
                  <Cell key={`m-${row.ticker}`} fill={colorForTicker(row.ticker)} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>
    </div>
  );
}

function ChartCard({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="border border-zinc-800 rounded-2xl p-3 bg-zinc-950/30 min-w-0">
      <div className="text-sm font-semibold mb-2">{title}</div>
      {children}
    </div>
  );
}
