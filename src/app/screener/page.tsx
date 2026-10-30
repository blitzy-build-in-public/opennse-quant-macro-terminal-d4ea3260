"use client";

import { useCallback, useRef, useState } from "react";

import {
  AnomalyFeed,
  EquityScreener,
  NetFlowChart,
  ValuationBandsChart,
  WatchlistManager,
  type Watchlist,
} from "@/components/terminal";

// A client page because it owns the selected ticker and hands a selection handler to the screener
// and the anomaly feed; a server component cannot pass functions as props. The widgets own all
// fetching and their loading, empty and error states, so the page adds only layout, this one
// string and the watchlists relayed from WatchlistManager to the screener. Before a selection,
// ValuationBandsChart shows its own "select a counter" prompt and NetFlowChart shows market-wide
// flows.
export default function ScreenerPage() {
  const [selectedTicker, setSelectedTicker] = useState<string | undefined>(undefined);
  // The lists WatchlistManager reports after each create, delete, ticker addition or removal,
  // handed to the screener so its watchlist filter, options and rows follow them on this page.
  const [watchlists, setWatchlists] = useState<Watchlist[] | undefined>(undefined);
  const selectedSectionRef = useRef<HTMLElement>(null);

  // The selected counter's heading and charts sit below the screener and the feed, usually below
  // the fold, so a selection also scrolls them into view; "nearest" leaves the page still when the
  // section is already visible. The motion preference is read at click time, never during
  // render, so the server and hydration output stay the same. Focus stays on the pressed ticker,
  // so keyboard users keep their place in the table or feed; the heading's aria-live region
  // announces the change.
  const handleSelectTicker = useCallback((ticker: string) => {
    setSelectedTicker(ticker);
    const reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    selectedSectionRef.current?.scrollIntoView({
      block: "nearest",
      behavior: reduceMotion ? "auto" : "smooth",
    });
  }, []);

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
          <EquityScreener
            onSelectTicker={handleSelectTicker}
            selectedTicker={selectedTicker}
            watchlists={watchlists}
          />
        </div>
        <div className="space-y-4">
          <WatchlistManager onWatchlistsChange={setWatchlists} />
          <AnomalyFeed onSelectTicker={handleSelectTicker} selectedTicker={selectedTicker} />
        </div>
      </div>
      {/* scroll-my-4 keeps the scrolled-to section 16px clear of the viewport edge. */}
      <section
        ref={selectedSectionRef}
        aria-labelledby="selected-counter-heading"
        className="scroll-my-4 space-y-4"
      >
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
        {/* items-start keeps each chart card at its own height, so the bands card's short prompt
            or empty state is not stretched to the flows chart beside it. */}
        <div className="grid grid-cols-1 items-start gap-4 lg:grid-cols-2">
          <ValuationBandsChart ticker={selectedTicker} />
          <NetFlowChart ticker={selectedTicker} />
        </div>
      </section>
    </div>
  );
}
