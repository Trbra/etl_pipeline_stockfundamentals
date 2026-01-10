"use client";

import { useEffect, useState } from "react";
import { apiGet } from "@/lib/api";
import PriceChart from "@/components/PriceChart";
import type { SeriesPoint } from "@/lib/types";

export default function CompanyPage({ params }: { params: { ticker: string } }) {
  const ticker = params.ticker;
  const [series, setSeries] = useState<SeriesPoint[]>([]);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    setErr(null);
    apiGet<SeriesPoint[]>(`/api/company/${ticker}/series?days=365`)
      .then(setSeries)
      .catch((e) => setErr(String(e)));
  }, [ticker]);

  return (
    <div>
      <div className="text-2xl font-semibold">{ticker}</div>
      <div className="mt-3">
        {err ? <div className="text-red-300 whitespace-pre-wrap">{err}</div> : <PriceChart series={series} />}
      </div>
    </div>
  );
}
