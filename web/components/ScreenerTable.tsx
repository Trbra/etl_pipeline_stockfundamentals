"use client";

import Link from "next/link";
import type { ScreenerRow } from "@/lib/types";

export default function ScreenerTable({ rows }: { rows: ScreenerRow[] }) {
  return (
    <div className="mt-4 overflow-auto border border-zinc-800 rounded-2xl">
      <table className="min-w-full text-sm">
        <thead className="bg-zinc-900/60">
          <tr className="text-left">
            <th className="p-3">Ticker</th>
            <th className="p-3">Name</th>
            <th className="p-3">Sector</th>
            <th className="p-3">Date</th>
            <th className="p-3">Close</th>
            <th className="p-3">RSI</th>
            <th className="p-3">Trend</th>
            <th className="p-3">Mkt Cap</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr
              key={r.ticker}
              className="border-t border-zinc-800 hover:bg-zinc-900/30"
            >
              <td className="p-3 font-semibold">
                <Link className="hover:underline" href={`/company/${r.ticker}`}>
                  {r.ticker}
                </Link>
              </td>
              <td className="p-3 text-zinc-300">{r.name ?? ""}</td>
              <td className="p-3 text-zinc-300">{r.sector ?? ""}</td>
              <td className="p-3 text-zinc-300">{r.price_date ?? ""}</td>
              <td className="p-3">{r.close_price != null ? Number(r.close_price).toFixed(2) : ""}</td>
              <td className="p-3">{r.rsi14 != null ? Number(r.rsi14).toFixed(1) : ""}</td>
              <td className="p-3">
                {r.trend_bullish ? (
                  <span className="text-emerald-400">Bull</span>
                ) : (
                  <span className="text-zinc-400">—</span>
                )}
                {r.rsi_oversold ? <span className="ml-2 text-cyan-300">Oversold</span> : null}
                {r.rsi_overbought ? <span className="ml-2 text-amber-300">Overbought</span> : null}
              </td>
              <td className="p-3 text-zinc-300">
                {r.market_cap ? Intl.NumberFormat().format(r.market_cap) : ""}
              </td>
            </tr>
          ))}
          {!rows.length ? (
            <tr>
              <td className="p-4 text-zinc-400" colSpan={8}>
                No results (check API connection / filters).
              </td>
            </tr>
          ) : null}
        </tbody>
      </table>
    </div>
  );
}
