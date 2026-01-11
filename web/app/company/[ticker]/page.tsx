"use client";

import { useEffect, useState } from "react";
import { apiGet } from "@/lib/api";
import PriceChart from "@/components/PriceChart";
import type { SeriesPoint } from "@/lib/types";

type PageProps = {
  params: Promise<{ ticker: string }>;
};

export default function CompanyPage({ params }: PageProps) {
  const [ticker, setTicker] = useState<string>("");
  const [series, setSeries] = useState<SeriesPoint[]>([]);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    (async () => {
      try {
        const p = await params;
        if (cancelled) return;

        setTicker(p.ticker);

        setErr(null);
        const data = await apiGet<SeriesPoint[]>(`/api/company/${p.ticker}/series?days=365`);
        if (!cancelled) setSeries(data);
      } catch (e) {
        if (!cancelled) setErr(String(e));
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [params]);

  return (
    <div>
      <div className="text-2xl font-semibold">{ticker || "..."}</div>
      <div className="mt-3">
        {err ? (
          <div className="text-red-300 whitespace-pre-wrap">{err}</div>
        ) : (
          <PriceChart series={series} />
        )}
      </div>
    </div>
  );
}
