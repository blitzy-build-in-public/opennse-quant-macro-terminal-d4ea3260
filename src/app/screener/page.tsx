"use client";

import { useState } from "react";

import {
  AnomalyFeed,
  EquityScreener,
  NetFlowChart,
  ValuationBandsChart,
  WatchlistManager,
} from "@/components/terminal";

// A client page because it owns the selected ticker and hands its setter to the screener and the
// anomaly feed; a server component cannot pass functions as props. The widgets own all fetching
// and their loading, empty and error states, so the page adds only layout and this one string.
// Before a selection, ValuationBandsChart shows its own "select a counter" prompt and NetFlowChart
// shows market-wide flows.
export default function ScreenerPage() {
  const [selectedTicker, setSelectedTicker] = useState<string | undefined>(undefined);

  return (
    <div className="space-y-4">
      <h1 className="text-xl font-semibold text-slate-100">Custom Watchlists &amp; Screener</h1>
      <p className="text-sm text-slate-400">
        Screen NSE counters by valuation multiples, dividend yield and volume anomalies; select a
        counter to see its valuation bands and foreign-versus-local flows.
      </p>
      {/* grid-cols-1 (minmax(0, 1fr)) on both grids keeps a single column at the page width, so
          the screener's wide table scrolls inside its card below xl instead of widening the page. */}
      <div className="grid grid-cols-1 gap-4 xl:grid-cols-3">
        <div className="xl:col-span-2">
          <EquityScreener onSelectTicker={setSelectedTicker} />
        </div>
        <div className="space-y-4">
          <WatchlistManager />
          <AnomalyFeed onSelectTicker={setSelectedTicker} />
        </div>
      </div>
      <section aria-labelledby="selected-counter-heading" className="space-y-4">
        {/* aria-live announces the new selection, since the ticker buttons sit far above. */}
        <h2
          id="selected-counter-heading"
          data-testid="screener-selected-ticker"
          aria-live="polite"
          className="text-lg font-semibold text-slate-100"
        >
          {selectedTicker
            ? `Selected counter: ${selectedTicker}`
            : "Select a counter in the screener or anomaly feed"}
        </h2>
        <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
          <ValuationBandsChart ticker={selectedTicker} />
          <NetFlowChart ticker={selectedTicker} />
        </div>
      </section>
    </div>
  );
}
