"use client";

import { useEffect, useState } from "react";
import Filters from "@/components/Filters";
import ScreenerTable from "@/components/ScreenerTable";
import { apiGet } from "@/lib/api";
import type { ScreenerRow } from "@/lib/types";

export default function Page() {
  const [qs, setQs] = useState("");
  const [rows, setRows] = useState<ScreenerRow[]>([]);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    setErr(null);
    apiGet<ScreenerRow[]>(`/api/screener${qs}`)
      .then(setRows)
      .catch((e) => {
        setRows([]);
        setErr(String(e));
      });
  }, [qs]);

  return (
    <div>
      <Filters onChange={setQs} />

      {err ? (
        <div className="mt-3 text-red-300 whitespace-pre-wrap">
          {err}
          {"\n\n"}If your API is running on 0.0.0.0:8000, use NEXT_PUBLIC_API_BASE=http://localhost:8000 in web/.env.local
        </div>
      ) : null}

      <ScreenerTable rows={rows} />
    </div>
  );
}
