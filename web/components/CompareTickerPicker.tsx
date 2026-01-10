"use client";

import { useEffect, useMemo, useState } from "react";
import { apiGet } from "../lib/api";

export type PickedTicker = { ticker: string; name?: string | null };

type ScreenerRow = {
  ticker: string;
  name?: string | null;
  sector?: string | null;
  market_cap?: number | null;
};

export default function CompareTickerPicker({
  picked,
  onChange,
}: {
  picked: PickedTicker[];
  onChange: (v: PickedTicker[]) => void;
}) {
  const [q, setQ] = useState("");
  const [options, setOptions] = useState<ScreenerRow[]>([]);
  const [open, setOpen] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const pickedSet = useMemo(
    () => new Set(picked.map((p) => p.ticker.toUpperCase())),
    [picked]
  );

  useEffect(() => {
    let cancelled = false;

    async function run() {
      setErr(null);
      const query = q.trim();
      if (!query) {
        setOptions([]);
        return;
      }

      try {
        const rows = await apiGet<ScreenerRow[]>(
          `/api/screener?q=${encodeURIComponent(query)}&limit=25`
        );
        if (!cancelled) setOptions(rows);
      } catch (e: any) {
        if (!cancelled) setErr(String(e));
      }
    }

    const t = setTimeout(run, 200);
    return () => {
      cancelled = true;
      clearTimeout(t);
    };
  }, [q]);

  function add(ticker: string, name?: string | null) {
    const T = ticker.toUpperCase();
    if (pickedSet.has(T)) return;
    onChange([...picked, { ticker: T, name }]);
    setQ("");
    setOptions([]);
    setOpen(false);
  }

  function remove(ticker: string) {
    const T = ticker.toUpperCase();
    onChange(picked.filter((p) => p.ticker.toUpperCase() !== T));
  }

  return (
    <div className="relative">
      <div className="flex flex-wrap gap-2">
        {picked.map((p) => (
          <div
            key={p.ticker}
            className="flex items-center gap-2 px-3 py-1.5 rounded-full border border-zinc-800 bg-zinc-950/30"
          >
            <span className="font-semibold">{p.ticker}</span>
            <button
              className="text-zinc-400 hover:text-zinc-200"
              onClick={() => remove(p.ticker)}
              aria-label={`Remove ${p.ticker}`}
              type="button"
            >
              ✕
            </button>
          </div>
        ))}
      </div>

      <div className="mt-3 flex items-center gap-2">
        <input
          value={q}
          onChange={(e) => {
            setQ(e.target.value);
            setOpen(true);
          }}
          onFocus={() => setOpen(true)}
          placeholder="Add ticker… (e.g., NVDA, TD.TO)"
          className="w-full bg-zinc-950/40 border border-zinc-800 rounded-2xl px-4 py-3 text-sm text-zinc-200 outline-none"
        />
        <button
          className="px-4 py-3 rounded-2xl border border-zinc-800 bg-zinc-900/30 hover:bg-zinc-900/50 text-sm"
          type="button"
          onClick={() => {
            // allow manual add when user types exact symbol
            const t = q.trim();
            if (t) add(t, null);
          }}
        >
          Add
        </button>
      </div>

      {err ? <div className="mt-2 text-sm text-red-300">{err}</div> : null}

      {open && options.length ? (
        <div className="absolute z-20 mt-2 w-full border border-zinc-800 bg-zinc-950 rounded-2xl overflow-hidden shadow-xl">
          {options.map((o) => {
            const T = o.ticker.toUpperCase();
            const disabled = pickedSet.has(T);
            return (
              <button
                type="button"
                key={o.ticker}
                disabled={disabled}
                onClick={() => add(o.ticker, o.name)}
                className={`w-full text-left px-4 py-3 border-b border-zinc-900 last:border-b-0
                  ${disabled ? "opacity-50 cursor-not-allowed" : "hover:bg-zinc-900/40"}`}
              >
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <div className="font-semibold">{o.ticker}</div>
                    <div className="text-xs text-zinc-400">{o.name ?? ""}</div>
                  </div>
                  <div className="text-xs text-zinc-500">
                    {o.market_cap ? `MCap ${(o.market_cap / 1e9).toFixed(0)}B` : ""}
                  </div>
                </div>
              </button>
            );
          })}
        </div>
      ) : null}
    </div>
  );
}
