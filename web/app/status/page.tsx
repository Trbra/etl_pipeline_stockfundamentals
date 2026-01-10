"use client";

import { useEffect, useMemo, useState } from "react";

type StatusResponse = {
  ok: boolean;
  db_connected: boolean;
  server_time: string;
  screener_rows: number;
  dim_company_rows: number;
  fact_prices_rows: number;
  fact_metrics_rows: number;
  latest_price_date?: string | null;
  latest_metrics_date?: string | null;
  latest_fundamentals_date?: string | null;
  notes?: string | null;
};

type DQSnapshot = {
  dq_date: string;
  created_at: string;

  universe_companies: number;
  companies_in_dim: number;

  tickers_with_price_today: number;
  tickers_missing_price_today: number;
  pct_with_price_today: number;

  tickers_with_metrics_today: number;
  tickers_missing_metrics_today: number;
  pct_with_metrics_today: number;

  tickers_with_ma200_today: number;
  pct_with_ma200_today: number;

  tickers_with_rsi_today: number;
  pct_with_rsi_today: number;

  duplicates_fact_prices: number;
  duplicates_fact_metrics: number;

  nonpositive_prices_today: number;
  zero_volume_today: number;

  notes?: string | null;
};

const API_BASE = process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:8000";

function classNames(...xs: Array<string | false | null | undefined>) {
  return xs.filter(Boolean).join(" ");
}

function Badge({
  tone,
  children,
}: {
  tone: "ok" | "warn" | "bad" | "neutral";
  children: React.ReactNode;
}) {
  const cls =
    tone === "ok"
      ? "bg-emerald-500/10 text-emerald-200 border-emerald-500/20"
      : tone === "warn"
      ? "bg-amber-500/10 text-amber-200 border-amber-500/20"
      : tone === "bad"
      ? "bg-red-500/10 text-red-200 border-red-500/20"
      : "bg-zinc-500/10 text-zinc-200 border-zinc-500/20";

  return (
    <span className={classNames("inline-flex items-center px-3 py-1 rounded-full text-xs font-semibold border", cls)}>
      {children}
    </span>
  );
}

function Stat({ label, value }: { label: string; value: any }) {
  return (
    <div className="border border-zinc-800 bg-zinc-950/30 rounded-2xl p-4">
      <div className="text-xs text-zinc-500">{label}</div>
      <div className="text-lg font-semibold text-zinc-100 mt-1">{value ?? "-"}</div>
    </div>
  );
}

function SectionCard({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="border border-zinc-800 bg-zinc-950/30 rounded-2xl p-4">
      <div className="font-semibold text-zinc-100">{title}</div>
      <div className="mt-3">{children}</div>
    </div>
  );
}

function pctTone(pct: number) {
  if (pct >= 97) return "ok";
  if (pct >= 90) return "warn";
  return "bad";
}

export default function StatusPage() {
  const [status, setStatus] = useState<StatusResponse | null>(null);
  const [dq, setDq] = useState<DQSnapshot | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const load = async () => {
    setErr(null);
    try {
      // status
      const sRes = await fetch(`${API_BASE}/api/status`, { cache: "no-store" });
      const sText = await sRes.text();
      if (!sRes.ok) throw new Error(`GET /api/status failed: ${sRes.status} ${sText}`);
      const sJson = JSON.parse(sText) as StatusResponse;
      setStatus(sJson);

      // dq latest (may not exist yet)
      const dqRes = await fetch(`${API_BASE}/api/dq/latest?limit=1`, { cache: "no-store" });
      const dqText = await dqRes.text();
      if (!dqRes.ok) {
        // if endpoint exists but returns error, show it
        throw new Error(`GET /api/dq/latest failed: ${dqRes.status} ${dqText}`);
      }
      const dqJson = JSON.parse(dqText) as DQSnapshot[];
      setDq(dqJson?.[0] ?? null);
    } catch (e: any) {
      setErr(e?.message ?? String(e));
    }
  };

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const warnings = useMemo(() => {
    const w: string[] = [];
    if (!dq) return w;

    if (dq.duplicates_fact_prices > 0) w.push(`Duplicate rows found in warehouse.fact_prices: +${dq.duplicates_fact_prices}`);
    if (dq.duplicates_fact_metrics > 0) w.push(`Duplicate rows found in warehouse.fact_metrics: +${dq.duplicates_fact_metrics}`);
    if (dq.nonpositive_prices_today > 0) w.push(`Non-positive prices detected on latest day: ${dq.nonpositive_prices_today}`);
    if (dq.zero_volume_today > 0) w.push(`Zero volume rows detected on latest day: ${dq.zero_volume_today}`);

    if (dq.pct_with_price_today < 90) w.push(`Price coverage is low: ${dq.pct_with_price_today.toFixed(2)}%`);
    if (dq.pct_with_metrics_today < 90) w.push(`Metrics coverage is low: ${dq.pct_with_metrics_today.toFixed(2)}%`);

    // MA200/RSI can naturally be lower early; warn only if *very* low.
    if (dq.pct_with_ma200_today < 60) w.push(`MA200 coverage is low: ${dq.pct_with_ma200_today.toFixed(2)}% (needs ~200 days history)`);
    if (dq.pct_with_rsi_today < 85) w.push(`RSI coverage is low: ${dq.pct_with_rsi_today.toFixed(2)}% (needs ~14 days history)`);

    return w;
  }, [dq]);

  const runDQ = async () => {
    setBusy(true);
    setErr(null);
    try {
      const res = await fetch(`${API_BASE}/api/dq/run`, { method: "POST" });
      const text = await res.text();
      if (!res.ok) throw new Error(`POST /api/dq/run failed: ${res.status} ${text}`);
      await load();
    } catch (e: any) {
      setErr(e?.message ?? String(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="max-w-6xl mx-auto px-4 py-8">
      <div className="flex items-start justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold">System Status</h1>
          <p className="text-zinc-400 mt-1">
            Pipeline + warehouse freshness + data quality checks (API: {API_BASE})
          </p>
        </div>

        <div className="flex items-center gap-2">
          {status ? (
            <>
              <Badge tone={status.ok ? "ok" : "bad"}>{status.ok ? "API OK" : "API ERROR"}</Badge>
              <Badge tone={status.db_connected ? "ok" : "bad"}>{status.db_connected ? "DB Connected" : "DB Disconnected"}</Badge>
            </>
          ) : (
            <Badge tone="neutral">Loading…</Badge>
          )}
        </div>
      </div>

      {err ? (
        <div className="mt-6 border border-red-500/30 bg-red-500/10 rounded-2xl p-4 text-red-200">
          <div className="font-semibold">Status error</div>
          <div className="text-sm mt-1 whitespace-pre-wrap">{err}</div>
        </div>
      ) : null}

      {!status && !err ? <div className="mt-6 text-zinc-400">Loading status…</div> : null}

      {status ? (
        <>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mt-6">
            <Stat label="Screener rows" value={status.screener_rows} />
            <Stat label="Dim companies" value={status.dim_company_rows} />
            <Stat label="Fact prices rows" value={status.fact_prices_rows} />
            <Stat label="Fact metrics rows" value={status.fact_metrics_rows} />
          </div>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mt-4">
            <Stat label="Latest price date" value={status.latest_price_date ?? "-"} />
            <Stat label="Latest metrics date" value={status.latest_metrics_date ?? "-"} />
            <Stat label="Latest fundamentals date" value={status.latest_fundamentals_date ?? "-"} />
          </div>

          <div className="mt-4 grid grid-cols-1 lg:grid-cols-3 gap-4">
            <SectionCard title="Server">
              <div className="text-xs text-zinc-500">Server time (UTC)</div>
              <div className="text-sm text-zinc-200 mt-1">{status.server_time}</div>
              {status.notes ? (
                <>
                  <div className="text-xs text-zinc-500 mt-3">Notes</div>
                  <pre className="whitespace-pre-wrap text-xs text-zinc-300 mt-1">{status.notes}</pre>
                </>
              ) : null}
            </SectionCard>

            <SectionCard title="Data Quality (latest snapshot)">
              <div className="flex items-center justify-between gap-3">
                <div className="text-sm text-zinc-200">
                  {dq ? (
                    <>
                      <div className="font-semibold">DQ Date: {dq.dq_date}</div>
                      <div className="text-xs text-zinc-500 mt-1">Created: {dq.created_at}</div>
                    </>
                  ) : (
                    <div className="text-zinc-400">No DQ snapshot found yet.</div>
                  )}
                </div>

                <button
                  onClick={runDQ}
                  disabled={busy}
                  className={classNames(
                    "px-3 py-2 rounded-xl text-sm font-semibold border",
                    busy
                      ? "opacity-60 cursor-not-allowed border-zinc-800 bg-zinc-900/40 text-zinc-300"
                      : "border-zinc-700 bg-zinc-900/60 hover:bg-zinc-900 text-zinc-100"
                  )}
                >
                  {busy ? "Running…" : "Run DQ now"}
                </button>
              </div>

              {dq ? (
                <div className="mt-4 flex flex-wrap gap-2">
                  <Badge tone={pctTone(dq.pct_with_price_today)}>
                    Prices: {dq.pct_with_price_today.toFixed(2)}% ({dq.tickers_with_price_today}/{dq.universe_companies})
                  </Badge>
                  <Badge tone={pctTone(dq.pct_with_metrics_today)}>
                    Metrics: {dq.pct_with_metrics_today.toFixed(2)}% ({dq.tickers_with_metrics_today}/{dq.universe_companies})
                  </Badge>
                  <Badge tone={pctTone(dq.pct_with_ma200_today)}>
                    MA200: {dq.pct_with_ma200_today.toFixed(2)}%
                  </Badge>
                  <Badge tone={pctTone(dq.pct_with_rsi_today)}>
                    RSI: {dq.pct_with_rsi_today.toFixed(2)}%
                  </Badge>

                  {dq.duplicates_fact_prices > 0 ? (
                    <Badge tone="bad">Duplicates Prices: {dq.duplicates_fact_prices}</Badge>
                  ) : (
                    <Badge tone="ok">Duplicates Prices: 0</Badge>
                  )}

                  {dq.duplicates_fact_metrics > 0 ? (
                    <Badge tone="bad">Duplicates Metrics: {dq.duplicates_fact_metrics}</Badge>
                  ) : (
                    <Badge tone="ok">Duplicates Metrics: 0</Badge>
                  )}
                </div>
              ) : null}

              {dq?.notes ? (
                <>
                  <div className="text-xs text-zinc-500 mt-3">DQ Notes</div>
                  <pre className="whitespace-pre-wrap text-xs text-zinc-300 mt-1">{dq.notes}</pre>
                </>
              ) : null}
            </SectionCard>

            <SectionCard title="Warnings">
              {warnings.length ? (
                <ul className="space-y-2 text-sm text-amber-200">
                  {warnings.map((w, idx) => (
                    <li key={idx} className="border border-amber-500/20 bg-amber-500/10 rounded-xl p-2">
                      {w}
                    </li>
                  ))}
                </ul>
              ) : (
                <div className="text-sm text-zinc-400">No warnings detected.</div>
              )}
              <div className="text-xs text-zinc-500 mt-3">
                Notes: MA200 needs ~200 trading days, RSI needs ~14 — lower coverage is normal early on.
              </div>
            </SectionCard>
          </div>
        </>
      ) : null}
    </div>
  );
}
