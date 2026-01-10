"use client";

type Props = {
  tickers: string[];
  colorForTicker: (t: string) => string;
  title?: string;
};

export default function CompareLegend({ tickers, colorForTicker, title = "Legend" }: Props) {
  if (!tickers?.length) return null;

  return (
    <div className="bg-zinc-900/40 border border-zinc-800 rounded-2xl p-4 min-w-0">
      <div className="flex items-center justify-between gap-3 min-w-0">
        <div className="font-semibold truncate">{title}</div>
        <div className="text-xs text-zinc-500 truncate">Ticker → color</div>
      </div>

      <div className="mt-3 flex flex-wrap gap-2">
        {tickers.map((t) => (
          <div
            key={`legend-${t}`}
            className="flex items-center gap-2 px-3 py-1.5 rounded-full border border-zinc-800 bg-zinc-950/30"
          >
            <span
              className="inline-block w-3 h-3 rounded-full"
              style={{ backgroundColor: colorForTicker(t) }}
              aria-hidden
            />
            <span className="text-sm font-semibold text-zinc-200">{t}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
