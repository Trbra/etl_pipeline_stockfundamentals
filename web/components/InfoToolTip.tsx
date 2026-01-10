"use client";

import { useId, useState } from "react";

export default function InfoTooltip({
  title,
  children,
  className = "",
}: {
  title: string;
  children: React.ReactNode;
  className?: string;
}) {
  const id = useId();
  const [open, setOpen] = useState(false);

  return (
    <div className={`relative inline-flex items-center ${className}`}>
      <button
        type="button"
        aria-describedby={id}
        aria-label={`Info: ${title}`}
        onMouseEnter={() => setOpen(true)}
        onMouseLeave={() => setOpen(false)}
        onFocus={() => setOpen(true)}
        onBlur={() => setOpen(false)}
        className="inline-flex items-center justify-center w-7 h-7 rounded-full border border-zinc-800 bg-zinc-950/30 text-zinc-300 hover:text-white hover:bg-zinc-900/40 transition"
      >
        <span className="text-sm font-bold">i</span>
      </button>

      {open ? (
        <div
          id={id}
          role="tooltip"
          className="absolute right-0 top-9 z-50 w-[320px] max-w-[80vw] rounded-2xl border border-zinc-800 bg-zinc-950 p-3 shadow-xl"
        >
          <div className="text-sm font-semibold text-zinc-100">{title}</div>
          <div className="mt-1 text-xs leading-relaxed text-zinc-300">{children}</div>
        </div>
      ) : null}
    </div>
  );
}
