import type { Metadata } from "next";

import { CycleBacktester } from "@/components/terminal";

export const metadata: Metadata = {
  title: "Election & Cycle Backtesting",
};

// A server component: CycleBacktester carries its own "use client" boundary and is rendered with
// its default props, so it opens on the ±180-day window over the server's default event dates.
// The wrapper keeps `space-y-4` rather than a flex gap so every standalone page shares one heading
// pattern (ipo and screener use the same classes).
export default function CyclesPage() {
  return (
    <div className="space-y-4">
      <h1 className="text-xl font-semibold text-slate-100">Election &amp; Cycle Backtesting</h1>
      <p className="text-sm text-slate-400">
        Price, valuation, turnover and foreign-flow paths around Kenyan general elections, aligned
        on days from the event.
      </p>
      <CycleBacktester />
    </div>
  );
}
