"use client";

import type { ReactElement } from "react";

import { formatDate, formatDecimal, formatInteger } from "./lib/format";
import { useTerminalQuery } from "./lib/useTerminalQuery";
import { WidgetFrame } from "./shared/WidgetFrame";
import type {
  AnomaliesResponse,
  AnomalyItem,
  AnomalyMetric,
  DecimalString,
  TerminalWidgetProps,
} from "./types";

/** Props of {@link AnomalyFeed}. */
export interface AnomalyFeedProps extends TerminalWidgetProps {
  /**
   * Metric to scan: `"volume"` (shares), `"turnover"` (KES) or `"both"`. Default `"both"`.
   */
  metric?: AnomalyMetric;
  /**
   * Spike threshold in standard deviations, 0.5–10 (validated by the API). Default `2`. A row is
   * a spike when its rolling Z-score is at or above this value; negative Z-scores never are.
   */
  threshold?: number;
  /**
   * Called with the ticker when its button is pressed, for example to load that counter's
   * valuation bands and flows. Without it tickers render as plain text.
   */
  onSelectTicker?: (ticker: string) => void;
}

const DEFAULT_METRIC: AnomalyMetric = "both";
const DEFAULT_THRESHOLD = 2;

// An empty feed can mean a quiet market as well as an empty database, so the copy names both
// rather than only the generic "No data yet" message of WidgetFrame.
const EMPTY_FEED_MESSAGE =
  "No spikes in the latest sessions. With no data loaded yet, run the seeder or an ingestion job.";

// Every class below is a complete static string, never assembled from fragments, so Tailwind's
// source scan and a host's `@source` directive or `content` glob find each one.
const SUMMARY_CLASS = "mb-2 text-xs text-slate-400";
// The list scrolls inside the card so a busy session does not stretch the host's layout; the
// region is focusable so the scroll is keyboard-reachable when the tickers are not buttons. Its
// p-1, and the tickers' scroll-my-1 when focus scrolls one into view, equal the focus outline's
// width plus offset, which the scroll box would otherwise clip. `relative` makes the box the
// containing block of each row's absolutely positioned `sr-only` span; without it those spans
// escape the overflow clip and stretch the host page below the feed.
const SCROLL_REGION_CLASS =
  "relative max-h-96 overflow-y-auto rounded p-1 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400";
const LIST_CLASS = "m-0 list-none p-0";
const ITEM_CLASS =
  "flex flex-wrap items-center gap-x-3 gap-y-1 border-b border-slate-800 py-1 last:border-b-0";
const ITEM_LEAD_CLASS = "flex min-w-0 items-center gap-2";
const ITEM_FIGURES_CLASS = "ms-auto flex items-center gap-3 text-end tabular-nums";
// The button keeps the 44×44 px minimum target; its look matches plain text apart from colour,
// even in a host without a CSS reset. Hover is a Tailwind 4 `hover:` variant, which applies only
// under `(hover: hover)`, so it never sticks on touch screens. Button and plain ticker share
// min-w-16, which fits the usual 3–5 letter NSE ticker, and the date's min-w-20 fits
// "29 Sep 2026", so the date and badge columns align across rows.
const TICKER_BUTTON_CLASS =
  "inline-flex min-h-11 min-w-16 scroll-my-1 cursor-pointer items-center justify-start rounded border-0 bg-transparent px-2 font-mono text-sm font-semibold text-sky-300 hover:bg-slate-800 hover:text-sky-200 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400 motion-safe:transition-colors motion-safe:duration-150 motion-safe:ease-out";
const TICKER_TEXT_CLASS =
  "inline-flex min-w-16 items-center px-2 font-mono text-sm font-semibold text-slate-100";
const DATE_CLASS = "min-w-20 whitespace-nowrap text-xs text-slate-400";
const VALUE_CLASS = "block text-sm text-slate-100";
const MEAN_CLASS = "block text-xs text-slate-400";
const Z_CLASS = "whitespace-nowrap text-sm font-semibold text-slate-50";

/** Label and badge classes of each metric; the label text carries the meaning, not the colour. */
const METRIC_BADGES: Record<AnomalyItem["metric"], { label: string; className: string }> = {
  volume: {
    label: "Volume",
    className: "rounded bg-amber-900/40 px-1.5 py-0.5 text-xs font-medium text-amber-200",
  },
  turnover: {
    label: "Turnover",
    className: "rounded bg-sky-900/40 px-1.5 py-0.5 text-xs font-medium text-sky-200",
  },
};

/**
 * An amount in the spiking metric's unit: a share count for volume, KES for turnover. The `z`
 * kind has no unit affixes, so with `unit: false` it yields the exact grouped digits of the count
 * (a 2 dp window mean keeps its fraction).
 */
function formatAmount(value: DecimalString, metric: AnomalyItem["metric"]): string {
  return metric === "volume"
    ? `${formatDecimal(value, "z", { unit: false })} shares`
    : formatDecimal(value, "kes");
}

/** Window details of an item, shown as its tooltip and read by screen readers. */
function itemDetails(item: AnomalyItem): string {
  const noun = item.observations_used === 1 ? "observation" : "observations";
  const stdev = formatAmount(item.rolling_stdev, item.metric);
  return `${formatInteger(item.observations_used)} ${noun}, sd ${stdev}`;
}

interface AnomalyRowProps {
  item: AnomalyItem;
  onSelectTicker?: (ticker: string) => void;
}

function AnomalyRow({ item, onSelectTicker }: AnomalyRowProps): ReactElement {
  const badge = METRIC_BADGES[item.metric];
  const details = itemDetails(item);
  return (
    <li
      data-testid="anomaly-item"
      data-ticker={item.ticker}
      data-trade-date={item.trade_date}
      data-metric={item.metric}
      title={details}
      className={ITEM_CLASS}
    >
      <span className={ITEM_LEAD_CLASS}>
        {onSelectTicker ? (
          <button
            type="button"
            onClick={() => onSelectTicker(item.ticker)}
            className={TICKER_BUTTON_CLASS}
          >
            <span className="sr-only">Select </span>
            {item.ticker}
          </button>
        ) : (
          <span className={TICKER_TEXT_CLASS}>{item.ticker}</span>
        )}
        <span className={DATE_CLASS}>{formatDate(item.trade_date)}</span>
        <span className={badge.className}>{badge.label}</span>
      </span>
      <span className={ITEM_FIGURES_CLASS}>
        <span>
          <span className={VALUE_CLASS}>{formatAmount(item.value, item.metric)}</span>
          <span className={MEAN_CLASS}>mean {formatAmount(item.rolling_mean, item.metric)}</span>
        </span>
        <strong className={Z_CLASS}>z = {formatDecimal(item.z_score, "z")}</strong>
      </span>
      <span className="sr-only">{`, ${details}`}</span>
    </li>
  );
}

/**
 * Feed of recent positive volume and turnover spikes from `GET /market/anomalies`: each row is a
 * ticker's session whose rolling Z-score (default 20-row window, sessions without a row skipped)
 * is at or above `threshold`. Rows keep the server's order, newest session first and then the
 * highest Z-score, and every value is shown from its exact decimal string.
 *
 * Works with no props. The root carries `data-testid="terminal-anomaly-feed"` and
 * `data-state`, and each row `data-testid="anomaly-item"` with its ticker, date and metric.
 */
export function AnomalyFeed({
  metric = DEFAULT_METRIC,
  threshold = DEFAULT_THRESHOLD,
  onSelectTicker,
  apiBaseUrl,
  getHeaders,
  className,
}: AnomalyFeedProps): ReactElement {
  const { state, data, error, refetch } = useTerminalQuery<AnomaliesResponse>({
    path: "/market/anomalies",
    query: { metric, threshold },
    apiBaseUrl,
    getHeaders,
    isEmpty: (response) => response.items.length === 0,
  });

  return (
    <WidgetFrame
      testId="terminal-anomaly-feed"
      title="Volume & turnover spikes"
      state={state}
      error={error}
      onRetry={refetch}
      emptyMessage={EMPTY_FEED_MESSAGE}
      skeletonClassName="h-48"
      className={className}
    >
      {data === undefined ? null : (
        <>
          {/* The server's echo of its window and threshold describes the rows on screen, even
              while a request for new props is still in flight. */}
          <p className={SUMMARY_CLASS}>
            z ≥ {formatDecimal(data.threshold, "z")} over a {formatInteger(data.window)}-session
            window
          </p>
          <div
            role="region"
            aria-label="Spikes, newest first"
            tabIndex={0}
            className={SCROLL_REGION_CLASS}
          >
            <ul className={LIST_CLASS}>
              {data.items.map((item) => (
                <AnomalyRow
                  key={`${item.ticker}|${item.trade_date}|${item.metric}`}
                  item={item}
                  onSelectTicker={onSelectTicker}
                />
              ))}
            </ul>
          </div>
        </>
      )}
    </WidgetFrame>
  );
}
