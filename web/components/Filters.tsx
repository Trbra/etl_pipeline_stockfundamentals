"use client";

import { useState } from "react";

export default function Filters({ onChange }: { onChange: (qs: string) => void }) {
  const [q, setQ] = useState("");
  const [sector, setSector] = useState("");
  const [rsiLte, setRsiLte] = useState("");
  const [bullish, setBullish] = useState<"" | "true" | "false">("");

  function apply() {
    const params = new URLSearchParams();
    if (q.trim()) params.set("q", q.trim());
    if (sector.trim()) params.set("sector", sector.trim());
    if (rsiLte.trim()) params.set("rsi_lte", rsiLte.trim());
    if (bullish) params.set("bullish", bullish);

    const s = params.toString();
    onChange(s ? `?${s}` : "");
  }

  return (
    <div className="bg-zinc-900/40 border border-zinc-800 rounded-2xl p-4">
      <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
        <input
          className="bg-zinc-950 border border-zinc-800 rounded-xl px-3 py-2"
          placeholder="Search ticker or name"
          value={q}
          onChange={(e) => setQ(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && apply()}
        />
        <input
          className="bg-zinc-950 border border-zinc-800 rounded-xl px-3 py-2"
          placeholder="Sector (exact, from Wikipedia/YF)"
          value={sector}
          onChange={(e) => setSector(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && apply()}
        />
        <input
          className="bg-zinc-950 border border-zinc-800 rounded-xl px-3 py-2"
          placeholder="RSI <= (e.g. 30)"
          value={rsiLte}
          onChange={(e) => setRsiLte(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && apply()}
        />
        <select
          className="bg-zinc-950 border border-zinc-800 rounded-xl px-3 py-2"
          value={bullish}
          onChange={(e) => setBullish(e.target.value as any)}
        >
          <option value="">Trend (any)</option>
          <option value="true">Bullish (MA50 &gt; MA200)</option>
          <option value="false">Not bullish</option>
        </select>
      </div>

      <div className="mt-3 flex justify-end">
        <button
          onClick={apply}
          className="bg-white text-black rounded-xl px-4 py-2 font-medium"
        >
          Apply
        </button>
      </div>
    </div>
  );
}
