export default function Nav() {
  return (
    <div className="flex items-center justify-between">
      <div className="text-xl font-semibold tracking-tight">Market Screener</div>
      <div className="flex gap-3 text-sm">
        <a className="text-zinc-300 hover:text-white" href="/">Screener</a>
        <a className="text-zinc-300 hover:text-white" href="/rankings">Rankings</a>
        <a className="text-zinc-300 hover:text-white" href="/settings">Settings</a>
        <a className="text-zinc-300 hover:text-white" href="/compare">Compare</a>
        <a className="text-zinc-300 hover:text-white" href="/status">Status</a>
      </div>
    </div>
  );
}
