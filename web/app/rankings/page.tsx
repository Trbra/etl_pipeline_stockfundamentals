"use client";

import { useEffect, useState } from "react";
import { apiGet } from "../../lib/api";

type RankingRow = {
  ticker: string;
  name?: string | null;
  sector?: string | null;
  price_date?: string | null;
  close_price?: number | null;
  rsi14?: number | null;
  pe_ratio?: number | null;
  dividend_yield?: number | null;
  market_cap?: number | null;

  score: number;
  trend_score?: number | null;
  rsi_score?: number | null;
  value_score?: number | null;
  size_score?: number | null;
  yield_score?: number | null;
  quality_score?: number | null;

  normalized_weights?: Record<string, number>;
  contributions?: Record<string, number>;
  trend_source?: string | null;
  avg_volume_60d?: number | null;
  profit_margin?: number | null;
  roe?: number | null;
  debt_to_equity?: number | null;
  factor_percentiles?: Record<string, number | null>;
  penalties?: Record<string, number>;
  base_score?: number | null;
  penalty_total?: number | null;
  final_after_penalties?: number | null;
  final_score?: number | null;

  reasons: Record<string, any>;
  sector_cap_applied?: boolean;
};

function formatDividendYield(value?: number | null) {
  if (value == null) return "N/A";
  const absValue = Math.abs(value);
  let pct = value;
  if (absValue <= 0.2) {
    pct = value * 100;
  } else if (absValue >= 5) {
    pct = value / 100;
  }
  return `${pct.toFixed(2)}%`;
}

function trendLabel(score?: number | null) {
  if (score == null) return "Pending";
  if (score >= 0.75) return "Strong";
  if (score >= 0.45) return "Neutral";
  return "Weak";
}

export default function RankingsPage() {
  const [rows, setRows] = useState<RankingRow[]>([]);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    apiGet<RankingRow[]>("/api/rankings?limit=100")
      .then(setRows)
      .catch((e) => setErr(String(e)));
  }, []);

  return (
    <div className="bg-zinc-900/40 border border-zinc-800 rounded-2xl p-4">
      <div className="text-xl font-semibold">Top Ranked</div>
      {err ? <div className="mt-3 text-red-300 whitespace-pre-wrap">{err}</div> : null}

      <div className="mt-4 overflow-auto border border-zinc-800 rounded-2xl">
        <table className="min-w-full text-sm">
          <thead className="bg-zinc-900/60 text-left">
            <tr>
              <th className="p-3">Rank</th>
              <th className="p-3">Ticker</th>
              <th className="p-3">Name</th>
              <th className="p-3">Score</th>
              <th className="p-3">Quality</th>
              <th className="p-3">Trend</th>
              <th className="p-3">RSI</th>
              <th className="p-3">P/E</th>
              <th className="p-3">Yield</th>
              <th className="p-3">Why</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r, idx) => (
              <tr key={r.ticker} className="border-t border-zinc-800 hover:bg-zinc-900/30">
                <td className="p-3 text-zinc-400">{idx + 1}</td>
                <td className="p-3 font-semibold">{r.ticker}</td>
                <td className="p-3 text-zinc-300">{r.name ?? ""}</td>
                <td className="p-3 font-semibold">{r.score != null ? r.score.toFixed(3) : "N/A"}</td>
                <td className="p-3">{r.quality_score != null ? `${Math.round(r.quality_score * 100)}%` : "N/A"}</td>
                <td className="p-3">
                  {trendLabel(r.trend_score)}
                  {r.factor_percentiles?.trend != null ? (
                    <span className="text-zinc-400">, {Math.round((r.factor_percentiles.trend ?? 0) * 100)}th</span>
                  ) : null}
                </td>
                <td className="p-3">{r.rsi14 != null ? r.rsi14.toFixed(1) : "N/A"}</td>
                <td className="p-3">{r.pe_ratio != null ? r.pe_ratio.toFixed(1) : "N/A"}</td>
                <td className="p-3">{formatDividendYield(r.dividend_yield)}</td>
                <td className="p-3 text-zinc-400 max-w-[420px]">
                  {r.trend_source ? (
                    <div className="text-xs text-zinc-500">
                      Trend type: {r.trend_source === "long-term" ? "MA50/MA200" : "Short-term"}
                    </div>
                  ) : null}
                  <div className="text-xs">
                    <div className="text-zinc-400">Sector: {r.sector ?? "Unknown"}</div>
                    <div>
                      {r.trend_score == null ? "Trend pending; " : r.reasons?.trend_bullish ? "MA50>MA200; " : ""}
                      {r.rsi14 != null ? `RSI=${r.rsi14.toFixed(1)}; ` : ""}
                      {r.pe_ratio != null ? `P/E=${r.pe_ratio.toFixed(1)}; ` : ""}
                      {r.dividend_yield != null ? `Yield=${formatDividendYield(r.dividend_yield)}; ` : ""}
                      {r.quality_score != null ? `Quality=${Math.round(r.quality_score * 100)}%; ` : ""}
                    </div>
                  </div>
                  {r.normalized_weights ? (
                    <div className="mt-1 text-xs text-zinc-500">
                      W: T{(r.normalized_weights.trend ?? 0).toFixed(2)}
                      &nbsp;Q{(r.normalized_weights.quality ?? 0).toFixed(2)}
                      &nbsp;R{(r.normalized_weights.rsi ?? 0).toFixed(2)}
                      &nbsp;V{(r.normalized_weights.value ?? 0).toFixed(2)}
                      &nbsp;S{(r.normalized_weights.size ?? 0).toFixed(2)}
                      &nbsp;Y{(r.normalized_weights.yield ?? 0).toFixed(2)}
                    </div>
                  ) : null}
                  {r.contributions ? (
                    <div className="text-xs text-zinc-500">
                      C: T{(r.contributions.trend ?? 0).toFixed(2)}
                      &nbsp;Q{(r.contributions.quality ?? 0).toFixed(2)}
                      &nbsp;R{(r.contributions.rsi ?? 0).toFixed(2)}
                      &nbsp;V{(r.contributions.value ?? 0).toFixed(2)}
                      &nbsp;S{(r.contributions.size ?? 0).toFixed(2)}
                      &nbsp;Y{(r.contributions.yield ?? 0).toFixed(2)}
                    </div>
                  ) : null}
                  {r.sector_cap_applied ? (
                    <div className="mt-1 text-xs text-amber-300">Notes: Sector cap applied</div>
                  ) : null}
                  {r.base_score != null ? (
                    <div className="mt-1 text-xs text-zinc-400">Base score: {r.base_score.toFixed(3)}</div>
                  ) : null}
                  {r.penalties && Object.keys(r.penalties).length ? (
                    <div className="mt-1 text-xs text-rose-300">
                      Penalties: {Object.entries(r.penalties).map(([k, v]) => `${k}=${v.toFixed(2)}`).join(", ")}
                    </div>
                  ) : null}
                  {r.penalty_total != null ? (
                    <div className="mt-1 text-xs text-rose-300">Penalty total: -{r.penalty_total.toFixed(3)}</div>
                  ) : null}
                  {r.final_after_penalties != null ? (
                    <div className="mt-1 text-xs text-zinc-400">After penalties (raw): {r.final_after_penalties.toFixed(3)}</div>
                  ) : null}
                  {r.final_score != null ? (
                    <div className="mt-1 text-xs font-semibold">Final score: {r.final_score.toFixed(3)}</div>
                  ) : null}
                </td>
              </tr>
            ))}
            {!rows.length && !err ? (
              <tr>
                <td colSpan={9} className="p-4 text-zinc-400">No rankings returned.</td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
    </div>
  );
}
