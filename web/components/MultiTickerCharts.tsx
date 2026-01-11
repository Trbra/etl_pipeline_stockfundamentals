"use client";

import { useMemo, Fragment, useState } from "react";
import CompareLegend from "./CompareLegend";
import InfoTooltip from "./InfoToolTip";
import {
  ResponsiveContainer,
  LineChart,
  Line,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  BarChart,
  Bar,
} from "recharts";

type SeriesPoint = {
  date: string;
  close?: number | null;
  volume?: number | null;
  ma50?: number | null;
  ma200?: number | null;
  rsi14?: number | null;
};

// -------------------------
// Stable color mapping
// -------------------------
const PALETTE = [
  "#60a5fa",
  "#f87171",
  "#34d399",
  "#fbbf24",
  "#a78bfa",
  "#fb7185",
  "#22d3ee",
  "#c084fc",
  "#4ade80",
  "#f97316",
  "#eab308",
  "#93c5fd",
];

function hashString(s: string) {
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) >>> 0;
  return h;
}

function colorForTicker(ticker: string) {
  return PALETTE[hashString(ticker) % PALETTE.length];
}

// -------------------------
// Data shaping
// -------------------------
function mergeSeries(
  tickers: string[],
  seriesByTicker: Record<string, SeriesPoint[]>
): Array<Record<string, any>> {
  const map = new Map<string, Record<string, any>>();

  for (const t of tickers) {
    const s = seriesByTicker[t] ?? [];
    for (const p of s) {
      const d = p.date;
      if (!map.has(d)) map.set(d, { date: d });
      const row = map.get(d)!;
      row[`${t}:close`] = p.close ?? null;
      row[`${t}:rsi14`] = p.rsi14 ?? null;
      row[`${t}:volume`] = p.volume ?? null;
      row[`${t}:ma50`] = p.ma50 ?? null;
      row[`${t}:ma200`] = p.ma200 ?? null;
    }
  }

  return Array.from(map.values()).sort((a, b) => (a.date < b.date ? -1 : 1));
}

function computeNormalized(
  rows: Array<Record<string, any>>,
  tickers: string[]
): Array<Record<string, any>> {
  const base: Record<string, number | null> = {};
  for (const t of tickers) {
    base[t] = null;
    for (const r of rows) {
      const v = r[`${t}:close`];
      if (typeof v === "number" && v > 0) {
        base[t] = v;
        break;
      }
    }
  }

  return rows.map((r) => {
    const out: Record<string, any> = { date: r.date };
    for (const t of tickers) {
      const v = r[`${t}:close`];
      const b = base[t];
      out[`${t}:norm`] = typeof v === "number" && typeof b === "number" ? (v / b) * 100 : null;
    }
    return out;
  });
}

function fmtDate(d: string) {
  if (!d) return "";
  const parts = d.split("-");
  if (parts.length !== 3) return d;
  const [, m, day] = parts;
  return `${m}/${day}`;
}

// -------------------------
// Layout helpers
// -------------------------
function ChartShell({
  title,
  subtitle,
  heightClass,
  right,
  info,
  children,
}: {
  title: string;
  subtitle?: string;
  heightClass: string;
  right?: React.ReactNode;
  info?: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <div className="bg-zinc-900/40 border border-zinc-800 rounded-2xl p-4 min-w-0">
      <div className="flex items-start justify-between gap-3 min-w-0">
        <div className="min-w-0">
          <div className="flex items-center gap-2 min-w-0">
            <div className="font-semibold truncate">{title}</div>
            {info ? info : null}
          </div>
          {subtitle ? <div className="text-xs text-zinc-500 truncate mt-0.5">{subtitle}</div> : null}
        </div>
        {right ? <div className="shrink-0">{right}</div> : null}
      </div>

      <div className={`mt-3 w-full min-w-0 ${heightClass}`}>{children}</div>
    </div>
  );
}

export default function MultiTickerCharts({
  tickers,
  seriesByTicker,
  normalize,
}: {
  tickers: string[];
  seriesByTicker: Record<string, SeriesPoint[]>;
  normalize: boolean;
}) {
  const [showMAs, setShowMAs] = useState(true);

  const merged = useMemo(() => mergeSeries(tickers, seriesByTicker), [tickers, seriesByTicker]);
  const normalized = useMemo(() => computeNormalized(merged, tickers), [merged, tickers]);

  if (!tickers.length) {
    return (
      <div className="bg-zinc-900/40 border border-zinc-800 rounded-2xl p-4 text-zinc-400">
        Add tickers to compare.
      </div>
    );
  }

  return (
    <div className="space-y-4 min-w-0">
      <CompareLegend tickers={tickers} colorForTicker={colorForTicker} />

      <ChartShell
        title={normalize ? "Performance (Base 100)" : "Close Price"}
        subtitle="Overlay multiple tickers"
        heightClass="h-[360px]"
        info={
          <InfoTooltip title="Performance vs Close Price">
            <div className="space-y-2">
              <p>
                <b>Close Price</b> is the stock’s price at the end of each trading day.
              </p>
              <p>
                <b>Performance (Base 100)</b> rescales each ticker so the first day = 100. This
                makes different-priced stocks comparable by showing % growth over time.
              </p>
              <p>
                Example: if a line goes from 100 to 120, that stock is up about <b>20%</b> over
                the selected period.
              </p>
            </div>
          </InfoTooltip>
        }
      >
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={normalize ? normalized : merged}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="date" tickFormatter={fmtDate} minTickGap={24} />
            <YAxis domain={["auto", "auto"]} />
            <Tooltip />
            <Legend />
            {tickers.map((t) => (
              <Line
                key={`perf-${t}`}
                type="monotone"
                dataKey={normalize ? `${t}:norm` : `${t}:close`}
                dot={false}
                strokeWidth={2}
                stroke={colorForTicker(t)}
                isAnimationActive={false}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </ChartShell>

      <ChartShell
        title="RSI (14)"
        subtitle="Momentum / overbought-oversold indicator"
        heightClass="h-[280px]"
        info={
          <InfoTooltip title="RSI (Relative Strength Index)">
            <div className="space-y-2">
              <p>
                <b>RSI</b> is a momentum indicator that ranges from <b>0 to 100</b>.
              </p>
              <p>
                Common interpretation:
                <br />• <b>Below 30</b>: “oversold” (price has fallen a lot recently)
                <br />• <b>Above 70</b>: “overbought” (price has risen a lot recently)
              </p>
              <p>
                RSI is not a guarantee — it’s a hint about recent buying/selling pressure.
              </p>
            </div>
          </InfoTooltip>
        }
      >
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={merged}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="date" tickFormatter={fmtDate} minTickGap={24} />
            <YAxis domain={[0, 100]} />
            <Tooltip />
            <Legend />
            {tickers.map((t) => (
              <Line
                key={`rsi-${t}`}
                type="monotone"
                dataKey={`${t}:rsi14`}
                dot={false}
                strokeWidth={2}
                stroke={colorForTicker(t)}
                isAnimationActive={false}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </ChartShell>

      <ChartShell
        title="Volume"
        subtitle="How many shares traded each day"
        heightClass="h-[320px]"
        info={
          <InfoTooltip title="Volume">
            <div className="space-y-2">
              <p>
                <b>Volume</b> is the number of shares traded in a day.
              </p>
              <p>
                Higher volume often means more interest and liquidity. Sudden spikes can happen
                around news, earnings, or big market moves.
              </p>
              <p>
                Volume is most useful when comparing a stock to <b>its own history</b> (not
                always across different tickers/exchanges).
              </p>
            </div>
          </InfoTooltip>
        }
      >
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={merged}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="date" tickFormatter={fmtDate} minTickGap={24} />
            <YAxis domain={["auto", "auto"]} />
            <Tooltip />
            <Legend />
            {tickers.map((t) => (
              <Bar
                key={`vol-${t}`}
                dataKey={`${t}:volume`}
                fill={colorForTicker(t)}
                fillOpacity={0.75}
                isAnimationActive={false}
              />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </ChartShell>

      <ChartShell
        title="Trend (Close + MA50/MA200)"
        subtitle="All tickers overlaid. Toggle moving averages to reduce clutter."
        heightClass="h-[360px]"
        right={
          <label className="text-xs text-zinc-300 flex items-center gap-2 select-none">
            <input
              type="checkbox"
              checked={showMAs}
              onChange={(e) => setShowMAs(e.target.checked)}
            />
            Show MAs
          </label>
        }
        info={
          <InfoTooltip title="Moving Averages (MA50 / MA200)">
            <div className="space-y-2">
              <p>
                A <b>moving average</b> smooths the price over time to highlight the trend.
              </p>
              <p>
                • <b>MA50</b> = average of the last 50 trading days (short/medium trend)
                <br />• <b>MA200</b> = average of the last 200 trading days (long trend)
              </p>
              <p>
                A common idea: if MA50 is above MA200, the trend is often considered more
                “bullish”. If below, more “bearish”.
              </p>
            </div>
          </InfoTooltip>
        }
      >
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={merged}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="date" tickFormatter={fmtDate} minTickGap={24} />
            <YAxis domain={["auto", "auto"]} />
            <Tooltip />
            <Legend />
            {tickers.map((t) => {
              const c = colorForTicker(t);
              return (
                <Fragment key={`trend-${t}`}>
                  <Line
                    key={`trend-close-${t}`}
                    type="monotone"
                    dataKey={`${t}:close`}
                    dot={false}
                    strokeWidth={2}
                    stroke={c}
                    isAnimationActive={false}
                  />
                  {showMAs ? (
                    <>
                      <Line
                        key={`trend-ma50-${t}`}
                        type="monotone"
                        dataKey={`${t}:ma50`}
                        dot={false}
                        strokeWidth={2}
                        stroke={c}
                        strokeOpacity={0.55}
                        isAnimationActive={false}
                      />
                      <Line
                        key={`trend-ma200-${t}`}
                        type="monotone"
                        dataKey={`${t}:ma200`}
                        dot={false}
                        strokeWidth={2}
                        stroke={c}
                        strokeOpacity={0.35}
                        isAnimationActive={false}
                      />
                    </>
                  ) : null}
                </Fragment>
              );
            })}
          </LineChart>
        </ResponsiveContainer>
      </ChartShell>
    </div>
  );
}
