"use client";

import dynamic from "next/dynamic";
import type { SeriesPoint } from "@/lib/types";

const ReactECharts = dynamic(() => import("echarts-for-react"), { ssr: false });

export default function PriceChart({ series }: { series: SeriesPoint[] }) {
  const dates = series.map((p) => p.date);
  const close = series.map((p) => (p.close ?? null));
  const ma50 = series.map((p) => (p.ma50 ?? null));
  const ma200 = series.map((p) => (p.ma200 ?? null));
  const rsi = series.map((p) => (p.rsi14 ?? null));

  const option = {
    tooltip: { trigger: "axis" },
    legend: { data: ["Close", "MA50", "MA200", "RSI"], textStyle: { color: "#ddd" } },
    grid: [
      { left: 50, right: 20, top: 40, height: "55%" },
      { left: 50, right: 20, top: "72%", height: "20%" }
    ],
    xAxis: [
      { type: "category", data: dates, axisLabel: { color: "#aaa" } },
      { type: "category", data: dates, gridIndex: 1, axisLabel: { color: "#aaa" } }
    ],
    yAxis: [
      { type: "value", axisLabel: { color: "#aaa" } },
      { type: "value", gridIndex: 1, min: 0, max: 100, axisLabel: { color: "#aaa" } }
    ],
    series: [
      { name: "Close", type: "line", data: close, smooth: true, showSymbol: false },
      { name: "MA50", type: "line", data: ma50, smooth: true, showSymbol: false },
      { name: "MA200", type: "line", data: ma200, smooth: true, showSymbol: false },
      { name: "RSI", type: "line", xAxisIndex: 1, yAxisIndex: 1, data: rsi, smooth: true, showSymbol: false }
    ]
  };

  return (
    <div className="bg-zinc-900/40 border border-zinc-800 rounded-2xl p-4">
      <ReactECharts option={option} style={{ height: 420 }} />
    </div>
  );
}
