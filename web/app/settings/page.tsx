"use client";

import { useEffect, useMemo, useState } from "react";
import { apiGet, apiPut } from "../../lib/api";

type RankingConfig = {
  name: string;
  weights: {
    trend: number;
    rsi: number;
    value: number;
    size: number;
    yield: number;
    [k: string]: number;
  };
  params: Record<string, any>;
  active: boolean;
};

const DEFAULTS: RankingConfig = {
  name: "default",
  active: true,
  weights: {
    trend: 0.35,
    rsi: 0.25,
    value: 0.2,
    size: 0.1,
    yield: 0.1,
  },
  params: {
    rsi_target_low: 35,
    rsi_target_high: 45,
    rsi_soft_low: 30,
    rsi_soft_high: 50,
  },
};

function clamp(n: number, lo: number, hi: number) {
  return Math.max(lo, Math.min(hi, n));
}

export default function SettingsPage() {
  const [cfg, setCfg] = useState<RankingConfig>(DEFAULTS);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    setErr(null);
    setMsg(null);
    apiGet<RankingConfig>("/api/ranking-config")
      .then((data) => setCfg(data))
      .catch((e) => setErr(String(e)))
      .finally(() => setLoading(false));
  }, []);

  const sum = useMemo(() => {
    const w = cfg.weights;
    return (w.trend || 0) + (w.rsi || 0) + (w.value || 0) + (w.size || 0) + (w.yield || 0);
  }, [cfg.weights]);

  const sumOk = useMemo(() => Math.abs(sum - 1.0) < 0.001, [sum]);

  function setWeight(key: keyof RankingConfig["weights"], value: number) {
    setCfg((prev) => ({
      ...prev,
      weights: {
        ...prev.weights,
        [key]: clamp(value, 0, 1),
      },
    }));
  }

  function setParam(key: string, value: any) {
    setCfg((prev) => ({
      ...prev,
      params: {
        ...prev.params,
        [key]: value,
      },
    }));
  }

  async function save() {
    setSaving(true);
    setErr(null);
    setMsg(null);
    try {
      const payload: RankingConfig = {
        ...cfg,
        weights: {
          trend: Number(cfg.weights.trend),
          rsi: Number(cfg.weights.rsi),
          value: Number(cfg.weights.value),
          size: Number(cfg.weights.size),
          yield: Number(cfg.weights.yield),
        },
      };

      const updated = await apiPut<RankingConfig>("/api/ranking-config", payload);
      setCfg(updated);
      setMsg("Saved. Rankings will update immediately.");
    } catch (e: any) {
      setErr(String(e));
    } finally {
      setSaving(false);
    }
  }

  function reset() {
    setCfg(DEFAULTS);
    setMsg("Reset locally (click Save to persist).");
    setErr(null);
  }

  if (loading) {
    return <div className="text-zinc-300">Loading settings...</div>;
  }

  return (
    <div className="bg-zinc-900/40 border border-zinc-800 rounded-2xl p-5">
      <div className="flex items-start justify-between gap-4">
        <div>
          <div className="text-xl font-semibold">Ranking Model Settings</div>
          <div className="text-sm text-zinc-400 mt-1">
            Adjust weights and parameters used by <code>/api/rankings</code>.
          </div>
        </div>

        <div className="text-right">
          <div className="text-sm text-zinc-400">Weights sum</div>
          <div className={`text-lg font-semibold ${sumOk ? "text-emerald-300" : "text-amber-300"}`}>
            {sum.toFixed(3)}
          </div>
        </div>
      </div>

      {msg ? <div className="mt-3 text-emerald-300 text-sm">{msg}</div> : null}
      {err ? <div className="mt-3 text-red-300 text-sm whitespace-pre-wrap">{err}</div> : null}

      {/* Weights */}
      <div className="mt-6 grid grid-cols-1 lg:grid-cols-2 gap-4">
        <WeightRow
          label="Trend (MA50 > MA200)"
          value={cfg.weights.trend}
          onChange={(v) => setWeight("trend", v)}
        />
        <WeightRow
          label="RSI (mean reversion)"
          value={cfg.weights.rsi}
          onChange={(v) => setWeight("rsi", v)}
        />
        <WeightRow
          label="Value (P/E)"
          value={cfg.weights.value}
          onChange={(v) => setWeight("value", v)}
        />
        <WeightRow
          label="Size (market cap)"
          value={cfg.weights.size}
          onChange={(v) => setWeight("size", v)}
        />
        <WeightRow
          label="Yield (dividend)"
          value={cfg.weights.yield}
          onChange={(v) => setWeight("yield", v)}
        />

        <div className="border border-zinc-800 rounded-2xl p-4 bg-zinc-950/30">
          <div className="font-semibold">Notes</div>
          <div className="text-sm text-zinc-400 mt-1 leading-relaxed">
            Keep the weights sum at <span className="text-zinc-200">1.0</span>. If RSI/MA values are missing due to
            insufficient history, those components contribute less (or zero) until enough data exists.
          </div>
        </div>
      </div>

      {/* Params */}
      <div className="mt-8 border border-zinc-800 rounded-2xl p-4 bg-zinc-950/30">
        <div className="flex items-center justify-between gap-4">
          <div>
            <div className="font-semibold">Parameters</div>
            <div className="text-sm text-zinc-400 mt-1">
              Optional thresholds used in the scoring logic (you can extend these later).
            </div>
          </div>
        </div>

        <div className="mt-4 grid grid-cols-1 md:grid-cols-2 gap-3">
          <ParamNumber
            label="RSI target low"
            value={Number(cfg.params.rsi_target_low ?? 35)}
            onChange={(v) => setParam("rsi_target_low", v)}
          />
          <ParamNumber
            label="RSI target high"
            value={Number(cfg.params.rsi_target_high ?? 45)}
            onChange={(v) => setParam("rsi_target_high", v)}
          />
          <ParamNumber
            label="RSI soft low"
            value={Number(cfg.params.rsi_soft_low ?? 30)}
            onChange={(v) => setParam("rsi_soft_low", v)}
          />
          <ParamNumber
            label="RSI soft high"
            value={Number(cfg.params.rsi_soft_high ?? 50)}
            onChange={(v) => setParam("rsi_soft_high", v)}
          />
        </div>

        <div className="text-xs text-zinc-500 mt-3">
          (Right now your SQL view uses a fixed RSI scoring band. Next step is wiring these params into SQL or computing
          RSI score in Python.)
        </div>
      </div>

      {/* Actions */}
      <div className="mt-6 flex flex-wrap items-center gap-3">
        <button
          className={`px-4 py-2 rounded-xl border border-zinc-700 ${
            sumOk ? "bg-white/10 hover:bg-white/15" : "bg-zinc-800/40 cursor-not-allowed"
          }`}
          disabled={!sumOk || saving}
          onClick={save}
        >
          {saving ? "Saving..." : "Save Settings"}
        </button>

        <button
          className="px-4 py-2 rounded-xl border border-zinc-800 bg-zinc-900/40 hover:bg-zinc-900/60"
          onClick={reset}
          disabled={saving}
        >
          Reset to Defaults
        </button>

        <a
          className="px-4 py-2 rounded-xl border border-zinc-800 bg-zinc-900/20 hover:bg-zinc-900/40 text-zinc-200"
          href="/rankings"
        >
          View Rankings →
        </a>

        {!sumOk ? (
          <div className="text-sm text-amber-300">Weights must sum to 1.0 to save.</div>
        ) : null}
      </div>
    </div>
  );
}

function WeightRow({
  label,
  value,
  onChange,
}: {
  label: string;
  value: number;
  onChange: (v: number) => void;
}) {
  return (
    <div className="border border-zinc-800 rounded-2xl p-4 bg-zinc-950/30">
      <div className="flex items-center justify-between gap-3">
        <div className="font-semibold">{label}</div>
        <div className="flex items-center gap-2">
          <input
            className="w-20 px-2 py-1 rounded-lg bg-zinc-900 border border-zinc-800 text-zinc-200"
            type="number"
            step="0.01"
            min="0"
            max="1"
            value={Number.isFinite(value) ? value : 0}
            onChange={(e) => onChange(Number(e.target.value))}
          />
          <div className="text-sm text-zinc-400">{(value * 100).toFixed(0)}%</div>
        </div>
      </div>

      <input
        className="mt-3 w-full"
        type="range"
        min="0"
        max="1"
        step="0.01"
        value={Number.isFinite(value) ? value : 0}
        onChange={(e) => onChange(Number(e.target.value))}
      />
    </div>
  );
}

function ParamNumber({
  label,
  value,
  onChange,
}: {
  label: string;
  value: number;
  onChange: (v: number) => void;
}) {
  return (
    <div className="border border-zinc-800 rounded-2xl p-3 bg-zinc-950/20">
      <div className="text-sm text-zinc-300">{label}</div>
      <input
        className="mt-2 w-full px-3 py-2 rounded-xl bg-zinc-900 border border-zinc-800 text-zinc-200"
        type="number"
        value={Number.isFinite(value) ? value : 0}
        onChange={(e) => onChange(Number(e.target.value))}
      />
    </div>
  );
}
