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
  /**
   * The ticker the caller has selected, for example the last one passed to `onSelectTicker`.
   * Every row of that ticker is tinted, marked with a sky accent at its start edge and
   * `data-selected="true"`, and its ticker button carries `aria-current="true"`. Without it, or
   * when no row matches, no row is marked.
   */
  selectedTicker?: string;
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
// While a request for new props, or a refetch, is in flight, the previous feed stays on screen,
// dimmed and marked busy; the opacity change runs only when the user has not asked for reduced
// motion.
const BODY_CLASS = "motion-safe:transition-opacity motion-safe:duration-200 motion-safe:ease-out";
const BODY_DIMMED_CLASS =
  "opacity-60 motion-safe:transition-opacity motion-safe:duration-200 motion-safe:ease-out";
const UPDATING_CLASS = "text-xs text-slate-400";
// The list scrolls inside the card so a busy session does not stretch the host's layout; the
// region is focusable so the scroll is keyboard-reachable when the tickers are not buttons. Its
// p-1, and the tickers' scroll-my-1 when focus scrolls one into view, equal the focus outline's
// width plus offset, which the scroll box would otherwise clip. `relative` makes the box the
// containing block of each row's absolutely positioned `sr-only` span; without it those spans
// escape the overflow clip and stretch the host page below the feed.
const SCROLL_REGION_CLASS =
  "relative max-h-96 overflow-y-auto rounded p-1 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400";
const LIST_CLASS = "m-0 list-none p-0";
// Every row reserves a 2px start border, transparent unless the row's ticker is the selected one;
// a selected row turns it sky-400 (about 6.7:1 on its tint) and tints itself slate-800, so it is
// marked beyond the tint alone and moving the selection shifts nothing. Every text colour in the
// row stays at least 5.5:1 on the tint (slate-400, the lowest).
const ITEM_CLASS =
  "flex flex-wrap items-center gap-x-3 gap-y-1 border-b border-s-2 border-slate-800 border-s-transparent py-1 last:border-b-0";
const ITEM_SELECTED_CLASS =
  "flex flex-wrap items-center gap-x-3 gap-y-1 border-b border-s-2 border-slate-800 border-s-sky-400 bg-slate-800 py-1 last:border-b-0";
const ITEM_LEAD_CLASS = "flex min-w-0 items-center gap-2";
const ITEM_FIGURES_CLASS = "ms-auto flex items-center gap-3 text-end tabular-nums";
// The button keeps the 44×44 px minimum target; its look matches plain text apart from colour,
// even in a host without a CSS reset. Hover is a Tailwind 4 `hover:` variant, which applies only
// under `(hover: hover)`, so it never sticks on touch screens. The pressed `active:` state, which
// touch screens apply without `:hover`, deepens the background one step past hover to slate-700
// and sets its own sky-200 text, 7.8:1. Button and plain ticker share min-w-16, which fits the
// usual 3–5 letter NSE ticker, and the date's min-w-20 fits "29 Sep 2026", so the date and badge
// columns align across rows.
const TICKER_BUTTON_CLASS =
  "inline-flex min-h-11 min-w-16 scroll-my-1 cursor-pointer items-center justify-start rounded border-0 bg-transparent px-2 font-mono text-sm font-semibold text-sky-300 hover:bg-slate-800 hover:text-sky-200 active:bg-slate-700 active:text-sky-200 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400 motion-safe:transition-colors motion-safe:duration-150 motion-safe:ease-out";
// On a selected row, already slate-800, hover and press each step one shade further, to slate-700
// (sky-200 at 7.8:1) and slate-600 (sky-200 at about 5.7:1), so both stay visible.
const TICKER_BUTTON_SELECTED_CLASS =
  "inline-flex min-h-11 min-w-16 scroll-my-1 cursor-pointer items-center justify-start rounded border-0 bg-transparent px-2 font-mono text-sm font-semibold text-sky-300 hover:bg-slate-700 hover:text-sky-200 active:bg-slate-600 active:text-sky-200 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400 motion-safe:transition-colors motion-safe:duration-150 motion-safe:ease-out";
const TICKER_TEXT_CLASS =
  "inline-flex min-w-16 items-center px-2 font-mono text-sm font-semibold text-slate-100";
const DATE_CLASS = "min-w-20 whitespace-nowrap text-xs text-slate-400";
const VALUE_CLASS = "block text-sm text-slate-100";
const MEAN_CLASS = "block text-xs text-slate-400";
const Z_CLASS = "whitespace-nowrap text-sm font-semibold text-slate-50";

/**
 * Label and badge classes of each metric; the label text carries the meaning, not the colour.
 * Both class strings mirror EquityScreener's VOLUME_BADGE_CLASS and TURNOVER_BADGE_CLASS, which
 * badge the same spikes on the same page, and must stay identical to them in colour, opacity,
 * padding, radius and text. Only the screener's whitespace-nowrap is left out: it keeps that
 * table's two-word labels on one line, and these one-word labels cannot wrap.
 */
const METRIC_BADGES: Record<AnomalyItem["metric"], { label: string; className: string }> = {
  volume: {
    label: "Volume",
    className: "rounded bg-amber-900/60 px-1.5 py-0.5 text-xs font-medium text-amber-200",
  },
  turnover: {
    label: "Turnover",
    className: "rounded bg-sky-900/60 px-1.5 py-0.5 text-xs font-medium text-sky-200",
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

/** Read out by the header's live status region while a request runs over a retained feed. */
const UPDATING_STATUS_TEXT = "Updating spikes…";

/**
 * The settled feed as the header's live status region announces it, built from the server's
 * echo of its threshold and window, for example "29 spikes at z ≥ 2 over a 20-session window",
 * or "No spikes at …" for an empty feed.
 */
function settledStatusText(data: AnomaliesResponse): string {
  const count = data.items.length;
  const threshold = formatDecimal(data.threshold, "z");
  const criteria = `at z ≥ ${threshold} over a ${formatInteger(data.window)}-session window`;
  if (count === 0) {
    return `No spikes ${criteria}`;
  }
  return `${formatInteger(count)} ${count === 1 ? "spike" : "spikes"} ${criteria}`;
}

interface AnomalyRowProps {
  item: AnomalyItem;
  onSelectTicker?: (ticker: string) => void;
  /** Whether this row's ticker is the caller's `selectedTicker`. */
  selected: boolean;
}

function AnomalyRow({ item, onSelectTicker, selected }: AnomalyRowProps): ReactElement {
  const badge = METRIC_BADGES[item.metric];
  const details = itemDetails(item);
  return (
    <li
      data-testid="anomaly-item"
      data-ticker={item.ticker}
      data-trade-date={item.trade_date}
      data-metric={item.metric}
      data-selected={selected ? "true" : "false"}
      title={details}
      className={selected ? ITEM_SELECTED_CLASS : ITEM_CLASS}
    >
      <span className={ITEM_LEAD_CLASS}>
        {onSelectTicker ? (
          <button
            type="button"
            onClick={() => onSelectTicker(item.ticker)}
            aria-current={selected ? "true" : undefined}
            className={selected ? TICKER_BUTTON_SELECTED_CLASS : TICKER_BUTTON_CLASS}
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
 * When `metric` or `threshold` changes, or a refetch runs, the previous feed stays on screen,
 * dimmed and marked `aria-busy="true"`, with a visible "Updating…" in the header until the new
 * response arrives. A polite live status region in the header, mounted in every state, reads
 * "Updating spikes…" meanwhile and then announces the settled feed from the server's echo, for
 * example "29 spikes at z ≥ 2 over a 20-session window", or "No spikes at …" when it is empty.
 *
 * Works with no props. The root carries `data-testid="terminal-anomaly-feed"` and
 * `data-state`, and each row `data-testid="anomaly-item"` with its ticker, date, metric and
 * `data-selected`. The status region is `data-testid="anomaly-feed-status"` and the visible
 * indicator `data-testid="anomaly-feed-updating"`.
 */
export function AnomalyFeed({
  metric = DEFAULT_METRIC,
  threshold = DEFAULT_THRESHOLD,
  onSelectTicker,
  selectedTicker,
  apiBaseUrl,
  getHeaders,
  className,
}: AnomalyFeedProps): ReactElement {
  const { state, data, error, isFetching, refetch } = useTerminalQuery<AnomaliesResponse>({
    path: "/market/anomalies",
    query: { metric, threshold },
    apiBaseUrl,
    getHeaders,
    isEmpty: (response) => response.items.length === 0,
  });

  // A request for new props, or a refetch, is pending while the previous feed stays on screen.
  const updating = isFetching && data !== undefined;
  // Left empty during the first load, which WidgetFrame announces as "Loading…", and in the error
  // state, whose alert announces itself.
  let statusText = "";
  if (updating) {
    statusText = UPDATING_STATUS_TEXT;
  } else if (data !== undefined && (state === "ready" || state === "empty")) {
    statusText = settledStatusText(data);
  }

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
      actions={
        // The header renders in every state, so the status region stays mounted, and keeps its
        // announcement, when the empty state replaces the feed. The visible indicator is hidden
        // from screen readers, which hear the region instead.
        <>
          {updating ? (
            <span aria-hidden="true" data-testid="anomaly-feed-updating" className={UPDATING_CLASS}>
              Updating…
            </span>
          ) : null}
          <span
            role="status"
            aria-live="polite"
            data-testid="anomaly-feed-status"
            className="sr-only"
          >
            {statusText}
          </span>
        </>
      }
    >
      {data === undefined ? null : (
        <div className={updating ? BODY_DIMMED_CLASS : BODY_CLASS} aria-busy={updating}>
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
                  selected={item.ticker === selectedTicker}
                />
              ))}
            </ul>
          </div>
        </div>
      )}
    </WidgetFrame>
  );
}
