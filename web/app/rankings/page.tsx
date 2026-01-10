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
  trend_score: number;
  rsi_score: number;
  value_score: number;
  size_score: number;
  yield_score: number;

  reasons: Record<string, any>;
};

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
                <td className="p-3 font-semibold">{r.score.toFixed(3)}</td>
                <td className="p-3">{r.trend_score > 0 ? "Bull" : "—"}</td>
                <td className="p-3">{r.rsi14 != null ? r.rsi14.toFixed(1) : "N/A"}</td>
                <td className="p-3">{r.pe_ratio != null ? r.pe_ratio.toFixed(1) : "N/A"}</td>
                <td className="p-3">
                  {r.dividend_yield != null ? (r.dividend_yield * 100).toFixed(2) + "%" : "N/A"}
                </td>
                <td className="p-3 text-zinc-400 max-w-[420px]">
                  {r.reasons?.trend_bullish ? "MA50>MA200; " : ""}
                  {r.rsi14 != null ? `RSI=${r.rsi14.toFixed(1)}; ` : ""}
                  {r.pe_ratio != null ? `P/E=${r.pe_ratio.toFixed(1)}; ` : ""}
                  {r.dividend_yield != null ? `Yield=${(r.dividend_yield * 100).toFixed(2)}%` : ""}
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
