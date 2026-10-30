"use client";

import { useCallback, useEffect, useId, useLayoutEffect, useRef, useState } from "react";
import type { KeyboardEvent, ReactElement } from "react";

import type { TerminalApiError } from "./lib/apiClient";
import {
  EMPTY_VALUE,
  compareDecimal,
  formatDate,
  formatDecimal,
  formatInteger,
} from "./lib/format";
import type { DecimalKind } from "./lib/format";
import { useTerminalQuery } from "./lib/useTerminalQuery";
import {
  FIELD_LABEL_CLASS,
  SECONDARY_BUTTON_CLASS,
  TEXT_FIELD_CLASS,
  TICKER_BUTTON_CLASS as SHARED_TICKER_BUTTON_CLASS,
  TICKER_BUTTON_SELECTED_CLASS as SHARED_TICKER_BUTTON_SELECTED_CLASS,
} from "./shared/controlStyles";
import { FieldErrorText, WidgetFrame } from "./shared/WidgetFrame";
import { SECTION_HEADING_CLASS } from "./shared/headingAndTileClasses";
import type {
  DecimalString,
  ScreenerFilters,
  ScreenerResponse,
  ScreenerRow,
  ScreenerSort,
  SectorAggregate,
  SortOrder,
  TerminalWidgetProps,
  Watchlist,
  WidgetState,
} from "./types";

/** Props of {@link EquityScreener}. */
export interface EquityScreenerProps extends TerminalWidgetProps {
  /**
   * Filters applied when the widget mounts, as `GET /market/screener` query values (for example
   * `{ sector: "Banking", anomalies_only: true, as_of: "2026-09-29" }`). Reset restores them.
   * Changing the prop after mount does not re-filter; the user's own edits stay in place.
   */
  initialFilters?: ScreenerFilters;
  /**
   * Called with the ticker when the user activates a counter's ticker button, for example to
   * show its valuation bands and flows. Without it, tickers render as plain text.
   */
  onSelectTicker?: (ticker: string) => void;
  /**
   * The ticker the caller has selected, for example the last one passed to `onSelectTicker`. Its
   * row is tinted, marked with a sky accent at its start edge and `data-selected="true"`, and its
   * ticker button carries `aria-current="true"`. Without it, or when no row matches, no row is
   * marked.
   */
  selectedTicker?: string;
  /**
   * The caller's current watchlists, for example as a sibling `WatchlistManager` reports them
   * through `onWatchlistsChange`. They are a change signal, never the option list: the selector's
   * options and the names it shows always come from the widget's own `GET /watchlists`. An array
   * whose lists differ from the previous array's, compared by id, name and `updated_at` in any
   * order, fetches `GET /watchlists` again; a new array with the same content changes nothing.
   * When the applied watchlist was in the previous array and is missing from the new one, the
   * caller deleted it, so the filter returns to all counters at once with a notice naming it. No
   * other conclusion is drawn from the array, since a caller may not know a list created
   * elsewhere. Without it, the widget relies on its own requests alone.
   */
  watchlists?: Watchlist[];
}

/** The eight typed numeric filters, in panel order, with their visible labels. */
const NUMERIC_FILTERS = [
  { key: "min_pe", label: "Min P/E" },
  { key: "max_pe", label: "Max P/E" },
  { key: "min_pb", label: "Min P/B" },
  { key: "max_pb", label: "Max P/B" },
  { key: "min_ev_ebitda", label: "Min EV/EBITDA" },
  { key: "max_ev_ebitda", label: "Max EV/EBITDA" },
  { key: "min_dividend_yield", label: "Min dividend yield (%)" },
  { key: "min_volume_z", label: "Min volume Z" },
] as const satisfies ReadonlyArray<{ key: keyof ScreenerFilters; label: string }>;

type NumericFilterKey = (typeof NUMERIC_FILTERS)[number]["key"];

/** The text currently typed into each numeric filter, applied after a pause in typing. */
type Drafts = Record<NumericFilterKey, string>;

/** Request fields that have an input in the panel, so their errors are shown beneath it. */
const INPUT_FIELDS: ReadonlySet<string> = new Set<string>([
  "sector",
  "watchlist_id",
  ...NUMERIC_FILTERS.map(({ key }) => key),
]);

/** The bounded pairs the server checks with `min_exceeds_max`, whose `field` is the minimum. */
const BOUNDED_FILTERS = [
  { min: "min_pe", max: "max_pe" },
  { min: "min_pb", max: "max_pb" },
  { min: "min_ev_ebitda", max: "max_ev_ebitda" },
] as const satisfies ReadonlyArray<{ min: NumericFilterKey; max: NumericFilterKey }>;

/** Typing pause after which the numeric drafts are applied; Enter and blur apply at once. */
const DRAFT_DEBOUNCE_MS = 400;

/** Server defaults of `sort` and `order`, used to mark the active column before any click. */
const DEFAULT_SORT: ScreenerSort = "market_cap_kes";
const DEFAULT_ORDER: SortOrder = "desc";

/** Empty-state copy when the filters exclude every counter of a universe that has data. */
const NO_MATCH_MESSAGE = "No counters match these filters";

/** The screener's nullable metrics, each explained by its own flag codes when null. */
type NullableMetric =
  | "change_pct"
  | "market_cap_kes"
  | "eps_kes"
  | "pe"
  | "pb"
  | "ev_ebitda"
  | "dividend_yield_pct"
  | "volume_z"
  | "turnover_z";

/**
 * Flag-code prefixes that explain a null value of each metric (the reason codes of the valuation,
 * session-change and Z-score engines). A metric whose row carries none of them falls back to
 * every row flag.
 */
const METRIC_FLAG_PREFIXES: Record<NullableMetric, readonly string[]> = {
  change_pct: ["change_pct:"],
  market_cap_kes: ["currency_mismatch"],
  eps_kes: ["pe_", "zero_shares", "no_fundamentals", "currency_mismatch"],
  pe: ["pe_", "zero_shares", "no_fundamentals", "currency_mismatch"],
  pb: ["pb_", "zero_shares", "no_fundamentals", "currency_mismatch"],
  ev_ebitda: ["ev_", "zero_shares", "no_fundamentals", "currency_mismatch"],
  dividend_yield_pct: ["dy_", "no_fundamentals", "currency_mismatch"],
  volume_z: ["volume_z:"],
  turnover_z: ["turnover_z:"],
};

/** Tooltip of a null value that has no reason code. */
const NO_REASON_TITLE = "Not available";

/** One column of the counter table; `sort` marks the columns the server can sort by. */
interface CounterColumn {
  /** The JSON field shown, also written to each cell's `data-column`. */
  id: string;
  label: string;
  sort?: ScreenerSort;
  numeric: boolean;
  /** Pinned to the inline start while the table scrolls sideways. */
  sticky?: boolean;
}

const COUNTER_COLUMNS: readonly CounterColumn[] = [
  { id: "ticker", label: "Ticker", sort: "ticker", numeric: false, sticky: true },
  { id: "company_name", label: "Company", numeric: false },
  { id: "sector", label: "Sector", numeric: false },
  { id: "close_price", label: "Close", numeric: true },
  { id: "change_pct", label: "Change %", numeric: true },
  { id: "market_cap_kes", label: "Market cap", sort: "market_cap_kes", numeric: true },
  { id: "eps_kes", label: "EPS", numeric: true },
  { id: "pe", label: "P/E", sort: "pe", numeric: true },
  { id: "pb", label: "P/B", sort: "pb", numeric: true },
  { id: "ev_ebitda", label: "EV/EBITDA", sort: "ev_ebitda", numeric: true },
  { id: "dividend_yield_pct", label: "Div. yield", sort: "dividend_yield_pct", numeric: true },
  { id: "volume", label: "Volume", numeric: true },
  { id: "turnover_kes", label: "Turnover", numeric: true },
  { id: "volume_z", label: "Vol Z", sort: "volume_z", numeric: true },
  { id: "turnover_z", label: "T/O Z", numeric: true },
  { id: "flags", label: "Flags", numeric: false },
];

// ---------------------------------------------------------------------------------------------
// Filter state helpers (pure)
// ---------------------------------------------------------------------------------------------

function isSet(value: string | undefined): boolean {
  return value !== undefined && value !== "";
}

/** The numeric drafts that show `filters`' values, empty where a filter is unset. */
function draftsFrom(filters: ScreenerFilters): Drafts {
  return {
    min_pe: filters.min_pe ?? "",
    max_pe: filters.max_pe ?? "",
    min_pb: filters.min_pb ?? "",
    max_pb: filters.max_pb ?? "",
    min_ev_ebitda: filters.min_ev_ebitda ?? "",
    max_ev_ebitda: filters.max_ev_ebitda ?? "",
    min_dividend_yield: filters.min_dividend_yield ?? "",
    min_volume_z: filters.min_volume_z ?? "",
  };
}

/**
 * `filters` with every numeric draft applied: the trimmed text is sent as typed (no numeric
 * conversion, the server validates it) and a blank draft removes its key. Returns `filters`
 * itself when nothing changes, so a repeated apply never re-renders.
 */
function applyDrafts(filters: ScreenerFilters, drafts: Drafts): ScreenerFilters {
  let next: ScreenerFilters | null = null;
  for (const { key } of NUMERIC_FILTERS) {
    const value = drafts[key].trim();
    const current = filters[key];
    if (value === "" ? current === undefined : current === value) {
      continue;
    }
    next = next ?? { ...filters };
    if (value === "") {
      delete next[key];
    } else {
      next[key] = value;
    }
  }
  return next ?? filters;
}

/** `filters` with a select's value applied; the empty "All" option removes the key. */
function withSelectFilter(
  filters: ScreenerFilters,
  key: "sector" | "watchlist_id",
  value: string,
): ScreenerFilters {
  if (value === "") {
    if (filters[key] === undefined) {
      return filters;
    }
    const next = { ...filters };
    delete next[key];
    return next;
  }
  return filters[key] === value ? filters : { ...filters, [key]: value };
}

/** `filters` with `anomalies_only` set when checked and removed otherwise. */
function withAnomaliesOnly(filters: ScreenerFilters, checked: boolean): ScreenerFilters {
  if (checked) {
    return filters.anomalies_only === true ? filters : { ...filters, anomalies_only: true };
  }
  if (filters.anomalies_only === undefined) {
    return filters;
  }
  const next = { ...filters };
  delete next.anomalies_only;
  return next;
}

/** `filters` sorted by `column`: the active column flips direction, a new one starts natural. */
function withSort(filters: ScreenerFilters, column: ScreenerSort): ScreenerFilters {
  const activeSort = filters.sort ?? DEFAULT_SORT;
  const activeOrder = filters.order ?? DEFAULT_ORDER;
  let order: SortOrder;
  if (activeSort === column) {
    order = activeOrder === "asc" ? "desc" : "asc";
  } else {
    order = column === "ticker" ? "asc" : "desc";
  }
  return { ...filters, sort: column, order };
}

/**
 * Whether a filter that narrows the rows is set, so that an empty result over a universe with
 * data reads as "no counters match" rather than as data not loaded yet.
 */
function hasActiveFilter(filters: ScreenerFilters): boolean {
  return (
    isSet(filters.sector) ||
    isSet(filters.watchlist_id) ||
    NUMERIC_FILTERS.some(({ key }) => isSet(filters[key])) ||
    filters.anomalies_only === true
  );
}

/**
 * The request query in a fixed key order, so equal filters always serialize to the same URL
 * whatever order the user set them in. `anomalies_only` is sent only when true.
 */
function screenerQuery(filters: ScreenerFilters): ScreenerFilters {
  return {
    sector: filters.sector,
    watchlist_id: filters.watchlist_id,
    min_pe: filters.min_pe,
    max_pe: filters.max_pe,
    min_pb: filters.min_pb,
    max_pb: filters.max_pb,
    min_ev_ebitda: filters.min_ev_ebitda,
    max_ev_ebitda: filters.max_ev_ebitda,
    min_dividend_yield: filters.min_dividend_yield,
    min_volume_z: filters.min_volume_z,
    anomalies_only: filters.anomalies_only === true ? true : undefined,
    anomaly_threshold: filters.anomaly_threshold,
    z_window: filters.z_window,
    as_of: filters.as_of,
    sort: filters.sort,
    order: filters.order,
  };
}

// ---------------------------------------------------------------------------------------------
// Display helpers (pure)
// ---------------------------------------------------------------------------------------------

/** Tooltip of a null counter metric: its own reason codes, else every row flag. */
function nullMetricTitle(flags: readonly string[], metric: NullableMetric): string {
  const prefixes = METRIC_FLAG_PREFIXES[metric];
  const relevant = flags.filter((flag) => prefixes.some((prefix) => flag.startsWith(prefix)));
  const shown = relevant.length > 0 ? relevant : flags;
  return shown.length > 0 ? shown.join(", ") : NO_REASON_TITLE;
}

/** Tooltip of a null sector aggregate: every flag of the sector. */
function aggregateTitle(flags: readonly string[]): string {
  return flags.length > 0 ? flags.join(", ") : NO_REASON_TITLE;
}

/** Sector names for the sector select: the universe's sectors, plus an applied one not listed. */
function sectorOptions(
  aggregates: readonly SectorAggregate[] | undefined,
  selected: string | undefined,
): string[] {
  const names = (aggregates ?? []).map((aggregate) => aggregate.sector);
  if (selected !== undefined && selected !== "" && !names.includes(selected)) {
    names.push(selected);
  }
  return names;
}

/** The visible label of a numeric filter. */
function filterLabel(key: NumericFilterKey): string {
  return NUMERIC_FILTERS.find((filter) => filter.key === key)?.label ?? key;
}

/**
 * A `min_exceeds_max` error on `field` in the visible labels, with both bounds echoed exactly as
 * they were sent, for example "Min P/E (10) must not exceed Max P/E (5).". `null` when `field` is
 * not the minimum of a bounded pair or either bound was not sent, so the server's text is kept.
 */
function minExceedsMaxMessage(field: string, sent: ScreenerFilters): string | null {
  const pair = BOUNDED_FILTERS.find(({ min }) => min === field);
  if (pair === undefined) {
    return null;
  }
  const low = sent[pair.min];
  const high = sent[pair.max];
  if (low === undefined || high === undefined) {
    return null;
  }
  return `${filterLabel(pair.min)} (${low}) must not exceed ${filterLabel(pair.max)} (${high}).`;
}

/** Whether a request field error reports the filtered watchlist as gone. */
function isMissingWatchlistEntry(field: string, code: string): boolean {
  return field === "watchlist_id" && code === "watchlist_not_found";
}

/** Whether the screener request failed because its `watchlist_id` names no watchlist. */
function isWatchlistNotFound(error: TerminalApiError | null): boolean {
  return (
    error !== null &&
    (error.code === "watchlist_not_found" ||
      error.fieldErrors.some(({ field, code }) => isMissingWatchlistEntry(field, code)))
  );
}

/**
 * The message beneath each request field the failed request names, first entry per field.
 * `min_exceeds_max` reads in the visible labels. A `watchlist_not_found` entry is left out: the
 * widget clears that filter and names the list in its own notice instead of echoing the
 * identifier. Every other code keeps the server's message.
 */
function fieldMessages(error: TerminalApiError, sent: ScreenerFilters): Record<string, string> {
  const messages: Record<string, string> = {};
  for (const { field, code, message } of error.fieldErrors) {
    if (
      Object.prototype.hasOwnProperty.call(messages, field) ||
      isMissingWatchlistEntry(field, code)
    ) {
      continue;
    }
    const labelled = code === "min_exceeds_max" ? minExceedsMaxMessage(field, sent) : null;
    messages[field] = labelled ?? message;
  }
  return messages;
}

/** Whether two ticker lists, each sorted ascending as the API returns them, are equal. */
function sameTickers(left: readonly string[], right: readonly string[]): boolean {
  return left.length === right.length && left.every((ticker, index) => ticker === right[index]);
}

/**
 * The content of a caller's watchlists as one string, the same for the same lists in any order:
 * each list's id, name and `updated_at`, which the server advances on an ORM update of the list
 * and on a ticker actually added or removed through the API. A database cascade (an equity's
 * ticker renamed or the equity deleted) changes `tickers` without advancing it, so such a change
 * alone does not alter the signature. Each entry is JSON-encoded, so no name can merge two
 * entries. `null` without lists.
 */
function watchlistsSignature(lists: readonly Watchlist[] | undefined): string | null {
  if (lists === undefined) {
    return null;
  }
  return lists
    .map(({ id, name, updated_at }) => JSON.stringify([id, name, updated_at]))
    .sort()
    .join("\n");
}

/** The notice shown once the filtered watchlist is found gone, naming it when it is known. */
function missingWatchlistNotice(name: string | undefined): string {
  return name === undefined
    ? "The selected watchlist no longer exists. Showing all counters."
    : `“${name}” was deleted. Showing all counters.`;
}

// Every class token below is written out in full, never assembled from fragments (strings are
// only joined from complete tokens), so Tailwind's source scan and a host's `@source` directive or
// `content` glob find each one. Only the default palette is used. Focus indicators are outlines,
// which forced-colors mode keeps. Labels, fields, Reset, Retry and the ticker buttons use the
// shared control classes of `shared/controlStyles`, so they match the other widgets' controls.
// 11.5rem keeps the longest select text ("Watchlists unavailable") unclipped beside its arrow.
const FILTER_GRID_CLASS = "grid grid-cols-[repeat(auto-fill,minmax(11.5rem,1fr))] gap-3";
const FIELD_CLASS = "flex min-w-0 flex-col gap-1";
/* BLITZY [A11Y]: filter inputs, selects, Reset and Retry are 26px tall and the table buttons at
   least 24px, the compact density of the card's other controls: at or above WCAG 2.2 AA's 24px
   target minimum, below the 44px guideline, so 16 columns stay readable. Flagged for designer
   review. */
// Selects and text inputs share one explicit height, so a filter row's bottom edges align.
const INPUT_CLASS = `${TEXT_FIELD_CLASS} w-full min-w-0`;
// The watchlist selector is the shared field too. It ends a clipped list name with an ellipsis,
// so it reads as truncated; the selector's title and the context line carry the full name.
const WATCHLIST_SELECT_CLASS = `${TEXT_FIELD_CLASS} w-full min-w-0 text-ellipsis`;
const FILTER_FOOTER_CLASS = "mt-3 flex flex-wrap items-center justify-between gap-3";
// The checkbox's label is its click target, so it tints on hover and deepens when pressed; the
// negative margin offsets its padding, keeping the text aligned with the filters above.
const CHECKBOX_LABEL_CLASS =
  "-mx-1.5 inline-flex cursor-pointer items-center gap-2 rounded px-1.5 py-0.5 text-sm text-slate-200 hover:bg-slate-800 hover:text-slate-50 active:bg-slate-700 motion-safe:transition-colors motion-safe:duration-150 motion-safe:ease-out";
const CHECKBOX_CLASS =
  "h-4 w-4 cursor-pointer accent-sky-500 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400";
const OTHER_ERRORS_CLASS = "mt-2";
// The watchlist selector's own failure notice, beneath it: the counters stay usable, so the list
// is retried here rather than through the card's error state. Its text matches field errors
// (rose-400 on the slate-900 card, about 6.6:1). Retry is the shared secondary button, like
// Reset; while a retry runs it stays focusable but aria-disabled, with slate-400 text (about
// 5.6:1), a wait cursor and no hover or press change to its background or border.
const WATCHLISTS_NOTICE_CLASS = "mt-1 flex flex-wrap items-center gap-x-2 gap-y-1";
const WATCHLISTS_NOTICE_TEXT_CLASS = "text-xs text-rose-400";
const WATCHLISTS_RETRY_CLASS = `${SECONDARY_BUTTON_CLASS} aria-[disabled=true]:cursor-wait aria-[disabled=true]:text-slate-400 aria-[disabled=true]:hover:border-slate-700 aria-[disabled=true]:hover:bg-slate-800 aria-[disabled=true]:active:border-slate-700 aria-[disabled=true]:active:bg-slate-800`;
// The notice that the filtered watchlist is gone and all counters are shown. It is always
// rendered, so its live region exists before the text arrives; while empty it is screen-reader
// only, which also takes it out of the field's flex gap. Amber-300 on the slate-900 card is about
// 12:1, and a long list name wraps anywhere rather than widen the field.
const WATCHLIST_NOTICE_CLASS = "mt-1 text-xs text-amber-300 [overflow-wrap:anywhere]";
const WATCHLIST_NOTICE_EMPTY_CLASS = "sr-only";

const BODY_CLASS = "flex flex-col gap-4 motion-safe:transition-opacity motion-safe:duration-200";
const BODY_DIMMED_CLASS =
  "flex flex-col gap-4 opacity-60 motion-safe:transition-opacity motion-safe:duration-200";
// The loaded body's height (counter table and sector summary), measured on the standalone page
// with the seeded data at each viewport width where it reflows, so the card keeps its height when
// the data arrives.
const SKELETON_HEIGHT_CLASS =
  "h-[44.25rem] min-[337px]:h-[43.25rem] min-[338px]:h-[42.25rem] min-[352px]:h-[41.25rem] min-[375px]:h-[40.25rem] min-[389px]:h-[39rem] min-[431px]:h-[38rem] min-[789px]:h-[37rem]";
const CONTEXT_CLASS = "flex flex-wrap gap-x-3 gap-y-1 text-xs text-slate-400";
const COUNT_CLASS = "font-medium text-slate-200";
// The applied watchlist's full name; a long one wraps anywhere instead of widening the card.
const CONTEXT_WATCHLIST_CLASS = "min-w-0 [overflow-wrap:anywhere]";
// `relative` makes each table's scroller the containing block of its absolutely positioned
// `sr-only` caption, which would otherwise escape the overflow clip and widen the host page.
const SCROLL_REGION_CLASS =
  "relative overflow-x-auto rounded focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400";
// The sector summary's scroller is also an inline-size container, so its sticky sector names can
// be capped against the scroller's visible width (`cqi`) rather than the table's (see
// SUMMARY_SECTOR_TEXT_CLASS). The arbitrary property `[container-type:inline-size]` makes it a
// container in Tailwind 3 and Tailwind 4 hosts alike; Tailwind 3 has no `@container` utility
// without a plugin.
const SUMMARY_SCROLL_REGION_CLASS =
  "[container-type:inline-size] relative overflow-x-auto rounded focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400";
// Each scroller sits in a positioned frame with an edge fade over its inline end, shown while
// columns lie beyond that edge (see useOverflowEnd), since overlay scrollbars give no cue. The
// fade blends the card surface (slate-900) in over 2rem and ignores the pointer, so the cells
// beneath stay clickable. It stays inside the scroller's box, clear of its offset focus outline,
// and is fully transparent once the end is reached or when the table fits, so nothing stays
// covered. `bg-gradient-to-*` is the gradient direction Tailwind 3 and Tailwind 4 both generate.
const SCROLL_FRAME_CLASS = "relative";
const SCROLL_FADE_CLASS =
  "pointer-events-none absolute inset-y-0 end-0 w-8 bg-gradient-to-l from-slate-900 to-transparent opacity-100 rtl:bg-gradient-to-r motion-safe:transition-opacity motion-safe:duration-150 motion-safe:ease-out";
const SCROLL_FADE_HIDDEN_CLASS =
  "pointer-events-none absolute inset-y-0 end-0 w-8 bg-gradient-to-l from-slate-900 to-transparent opacity-0 rtl:bg-gradient-to-r motion-safe:transition-opacity motion-safe:duration-150 motion-safe:ease-out";
const TABLE_CLASS = "min-w-full border-collapse text-xs";
const TBODY_CLASS = "divide-y divide-slate-800";
// The header divider is written as the arbitrary property `[border-block-end-width:1px]`
// because Tailwind 3 and Tailwind 4 hosts both generate it; the named block-end border width
// utility exists only in Tailwind 4.
const TH_CLASS =
  "whitespace-nowrap [border-block-end-width:1px] border-slate-700 px-2 py-1.5 text-start align-bottom text-xs font-medium text-slate-400";
// The first column (ticker or sector) stays in view while a wide table scrolls sideways; its
// opaque background matches the card surface so scrolled cells pass beneath it. Its inline-end
// edge is a 1px slate-700 `::after` line: the sticky cell is its containing block, so the line
// travels with the pinned column, which a collapsed cell border does not reliably do.
const TH_STICKY_CLASS =
  "sticky start-0 z-10 whitespace-nowrap [border-block-end-width:1px] border-slate-700 bg-slate-900 px-2 py-1.5 text-start align-bottom text-xs font-medium text-slate-400 after:pointer-events-none after:absolute after:inset-y-0 after:end-0 after:w-px after:bg-slate-700";
const TH_NUMERIC_CLASS =
  "whitespace-nowrap [border-block-end-width:1px] border-slate-700 px-2 py-1.5 text-end align-bottom text-xs font-medium text-slate-400";
// Sort buttons are text only, so their pressed state is the sky-200 text shade, 13.4:1 on the card
// surface and distinct from every resting and hover colour. The selected sort button, already
// slate-100, dims to slate-300 on hover.
const SORT_BUTTON_CLASS =
  "inline-flex min-h-6 cursor-pointer items-center gap-1 rounded text-xs font-medium text-slate-400 hover:text-slate-100 active:text-sky-200 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400 motion-safe:transition-colors motion-safe:duration-150 motion-safe:ease-out";
const SORT_BUTTON_ACTIVE_CLASS =
  "inline-flex min-h-6 cursor-pointer items-center gap-1 rounded text-xs font-semibold text-slate-100 hover:text-slate-300 active:text-sky-200 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400 motion-safe:transition-colors motion-safe:duration-150 motion-safe:ease-out";
// A plain header label gets the sort buttons' 24px centred line box, so in a bottom-aligned
// header row every label's text shares one baseline, sortable or not.
const HEADER_LABEL_CLASS = "inline-flex min-h-6 items-center";
const ROW_HEADER_CLASS =
  "sticky start-0 z-10 whitespace-nowrap bg-slate-900 px-2 py-1.5 text-start font-medium text-slate-200 after:pointer-events-none after:absolute after:inset-y-0 after:end-0 after:w-px after:bg-slate-700";
// A sector name wraps rather than widening its sticky row header, which would otherwise take the
// longest name's width and, on a narrow screen, crowd the values out of view. The cap keeps the
// header to 45% of the summary scroller's visible width, in container units because the table is
// wider than the scroller, leaving the rest for the columns scrolling beside it. It sits on an
// inner block because `max-w-*` on a table cell is not reliably honoured; the block overrides the
// header's `whitespace-nowrap`, which the ticker headers keep. The cap is 45cqi less the header's
// 1rem inline padding: the summary's cells have no side borders, and the `::after` divider takes
// no width. `w-max` sizes the block to its name up to the cap, so only a longer name wraps; an
// auto-width block would wrap every name at its longest word, because the table overflows even on
// wide screens and so gives every column its narrowest width. `break-words` breaks only a word
// too long for the cap, so ordinary words stay whole.
const SUMMARY_SECTOR_TEXT_CLASS =
  "block w-max max-w-[calc(45cqi_-_1rem)] whitespace-normal break-words";
// The selected counter's row is tinted slate-800, and its sticky ticker cell switches to the same
// opaque tint, so the row reads as one band while scrolled cells still pass beneath that cell. A
// 2px sky-400 bar at the cell's start edge (about 6.7:1 on the tint) marks the row beyond the
// tint alone. The bar is a pseudo-element placed inside the sticky cell, which positions it, so
// moving the selection shifts nothing. Every text colour in the row stays at least 5.1:1 on the
// tint (rose-400, the lowest), and the ticker button keeps its own hover and pressed states. The
// cell keeps the resting row header's weight, colour and inline-end divider.
const SELECTED_ROW_CLASS = "bg-slate-800";
const ROW_HEADER_SELECTED_CLASS =
  "sticky start-0 z-10 whitespace-nowrap bg-slate-800 px-2 py-1.5 text-start font-medium text-slate-200 before:absolute before:inset-y-0 before:start-0 before:w-0.5 before:bg-sky-400 before:content-[''] after:pointer-events-none after:absolute after:inset-y-0 after:end-0 after:w-px after:bg-slate-700";
// The ticker button is the shared 24px monospace ticker button that AnomalyFeed also uses, at the
// same size and weight; on the selected row its hover and pressed backgrounds step one shade
// past the row's slate-800 tint. Its -mx-1 only offsets its 4px inline padding, so the ticker
// text stays aligned with the column header.
const TICKER_BUTTON_CLASS = `${SHARED_TICKER_BUTTON_CLASS} -mx-1`;
const TICKER_BUTTON_SELECTED_CLASS = `${SHARED_TICKER_BUTTON_SELECTED_CLASS} -mx-1`;
const TEXT_CELL_CLASS = "px-2 py-1.5 text-start text-slate-200";
// A minimum width keeps company names to about two lines instead of one word per line.
const COMPANY_CELL_CLASS = "min-w-[12rem] px-2 py-1.5 text-start text-slate-200";
const NUMERIC_CELL_CLASS = "whitespace-nowrap px-2 py-1.5 text-end tabular-nums text-slate-100";
const NULL_CELL_CLASS = "cursor-help whitespace-nowrap px-2 py-1.5 text-end text-slate-400";
const POSITIVE_CELL_CLASS = "whitespace-nowrap px-2 py-1.5 text-end tabular-nums text-emerald-400";
const NEGATIVE_CELL_CLASS = "whitespace-nowrap px-2 py-1.5 text-end tabular-nums text-rose-400";
const Z_CELL_INNER_CLASS = "inline-flex items-center justify-end gap-1.5";
const VOLUME_BADGE_CLASS =
  "whitespace-nowrap rounded bg-amber-900/60 px-1.5 py-0.5 text-xs font-medium text-amber-200";
const TURNOVER_BADGE_CLASS =
  "whitespace-nowrap rounded bg-sky-900/60 px-1.5 py-0.5 text-xs font-medium text-sky-200";
const FLAGS_CELL_CLASS = "px-2 py-1.5 text-start text-xs text-slate-400";
const FLAG_LIST_CLASS = "flex min-w-[10rem] flex-wrap gap-1";
const FLAG_CLASS = "rounded bg-slate-800 px-1 py-0.5 font-mono text-xs text-slate-300";
// Shown in place of the counter table, outside its sideways scroll, with the frame's empty copy
// style, so it starts at the card's content edge however wide the table would be.
const NO_RESULT_NOTE_CLASS = "text-sm text-slate-400";

const SUMMARY_SECTION_CLASS = "flex flex-col gap-2";
const SUMMARY_NOTE_CLASS = "text-xs text-slate-400";
const ELIGIBLE_CLASS = "ms-1 text-xs text-slate-400";

/** Props of {@link MetricCell}. */
interface MetricCellProps {
  metric: NullableMetric;
  value: DecimalString | null;
  kind: DecimalKind;
  flags: readonly string[];
  /** Colours the value by sign (emerald up, rose down, neutral at zero) and prefixes `+`. */
  signed?: boolean;
}

/**
 * One nullable counter metric, formatted from its exact string. A null value shows the em dash
 * with its reason codes as the tooltip; the Flags column repeats them as visible text.
 */
function MetricCell({ metric, value, kind, flags, signed }: MetricCellProps): ReactElement {
  if (value === null) {
    return (
      <td data-column={metric} title={nullMetricTitle(flags, metric)} className={NULL_CELL_CLASS}>
        {EMPTY_VALUE}
      </td>
    );
  }
  let className = NUMERIC_CELL_CLASS;
  if (signed === true) {
    const sign = compareDecimal(value, "0");
    if (sign === 1) className = POSITIVE_CELL_CLASS;
    else if (sign === -1) className = NEGATIVE_CELL_CLASS;
  }
  return (
    <td data-column={metric} className={className}>
      {formatDecimal(value, kind, { signed: signed === true })}
    </td>
  );
}

/** Props of {@link ZScoreCell}. */
interface ZScoreCellProps {
  metric: "volume_z" | "turnover_z";
  value: DecimalString | null;
  isAnomaly: boolean;
  flags: readonly string[];
}

/**
 * A rolling Z-score with its spike badge; the badge's text, not its colour, carries meaning. The
 * badge precedes the value, so the value is always the cell's inline-end item and every row's
 * number stays aligned at the column's end whether or not it has a badge.
 */
function ZScoreCell({ metric, value, isAnomaly, flags }: ZScoreCellProps): ReactElement {
  const badge = isAnomaly ? (
    <span className={metric === "volume_z" ? VOLUME_BADGE_CLASS : TURNOVER_BADGE_CLASS}>
      {metric === "volume_z" ? "Vol spike" : "T/O spike"}
    </span>
  ) : null;
  if (value === null) {
    return (
      <td data-column={metric} title={nullMetricTitle(flags, metric)} className={NULL_CELL_CLASS}>
        <span className={Z_CELL_INNER_CLASS}>
          {badge}
          {EMPTY_VALUE}
        </span>
      </td>
    );
  }
  return (
    <td data-column={metric} className={NUMERIC_CELL_CLASS}>
      <span className={Z_CELL_INNER_CLASS}>
        {badge}
        {formatDecimal(value, "z")}
      </span>
    </td>
  );
}

/** The reason codes of a row or sector as visible text, so no tooltip is needed to read them. */
function FlagsCell({ flags }: { flags: readonly string[] }): ReactElement {
  return (
    <td data-column="flags" className={FLAGS_CELL_CLASS}>
      {flags.length === 0 ? (
        EMPTY_VALUE
      ) : (
        <ul className={FLAG_LIST_CLASS}>
          {flags.map((flag, index) => (
            <li key={`${flag}#${index}`} className={FLAG_CLASS}>
              {flag}
            </li>
          ))}
        </ul>
      )}
    </td>
  );
}

/** Props of {@link AggregateMultipleCell}. */
interface AggregateMultipleCellProps {
  column: "pe" | "pb" | "ev_ebitda" | "dividend_yield_pct";
  value: DecimalString | null;
  kind: DecimalKind;
  eligible: number;
  constituents: number;
  flags: readonly string[];
}

/**
 * A sector multiple followed by its matched eligible set against the constituents, as
 * "(eligible/constituents)", so partial coverage is visible beside the value.
 */
function AggregateMultipleCell({
  column,
  value,
  kind,
  eligible,
  constituents,
  flags,
}: AggregateMultipleCellProps): ReactElement {
  const isNull = value === null;
  return (
    <td
      data-column={column}
      title={isNull ? aggregateTitle(flags) : undefined}
      className={isNull ? NULL_CELL_CLASS : NUMERIC_CELL_CLASS}
    >
      <span data-part="value">{isNull ? EMPTY_VALUE : formatDecimal(value, kind)}</span>
      <span data-part="eligible" className={ELIGIBLE_CLASS}>
        ({formatInteger(eligible)}/{formatInteger(constituents)})
      </span>
    </td>
  );
}


/** Props of {@link CounterHeader}. */
interface CounterHeaderProps {
  column: CounterColumn;
  activeSort: ScreenerSort;
  activeOrder: SortOrder;
  onSort: (column: ScreenerSort) => void;
}

/**
 * A counter-table column header. A sortable column is a button that asks the server for that
 * order; only the active column carries `aria-sort`, as ARIA expects one sorted header at a time.
 */
function CounterHeader({ column, activeSort, activeOrder, onSort }: CounterHeaderProps): ReactElement {
  let thClass = column.numeric ? TH_NUMERIC_CLASS : TH_CLASS;
  if (column.sticky === true) thClass = TH_STICKY_CLASS;
  const sort = column.sort;
  if (sort === undefined) {
    return (
      <th scope="col" className={thClass}>
        <span className={HEADER_LABEL_CLASS}>{column.label}</span>
      </th>
    );
  }
  const isActive = sort === activeSort;
  let ariaSort: "ascending" | "descending" | undefined;
  if (isActive) ariaSort = activeOrder === "asc" ? "ascending" : "descending";
  let indicator = "\u2195";
  if (isActive) indicator = activeOrder === "asc" ? "\u25b2" : "\u25bc";
  return (
    <th scope="col" aria-sort={ariaSort} className={thClass}>
      <button
        type="button"
        data-testid={"screener-sort-" + sort}
        onClick={() => onSort(sort)}
        className={isActive ? SORT_BUTTON_ACTIVE_CLASS : SORT_BUTTON_CLASS}
      >
        {column.label}
        <span aria-hidden="true">{indicator}</span>
      </button>
    </th>
  );
}

/** Props of {@link CounterRow}. */
interface CounterRowProps {
  row: ScreenerRow;
  onSelectTicker?: (ticker: string) => void;
  /** Whether this row is the caller's `selectedTicker`. */
  selected: boolean;
}

/** One equity of the screener, every value formatted from the server's exact strings. */
function CounterRow({ row, onSelectTicker, selected }: CounterRowProps): ReactElement {
  return (
    <tr
      data-testid="screener-row"
      data-ticker={row.ticker}
      data-selected={selected ? "true" : "false"}
      className={selected ? SELECTED_ROW_CLASS : undefined}
    >
      <th
        scope="row"
        data-column="ticker"
        className={selected ? ROW_HEADER_SELECTED_CLASS : ROW_HEADER_CLASS}
      >
        {/* A button only when a handler exists: a control that does nothing would mislead
            keyboard and screen-reader users, so without one the ticker is plain text. Its name
            stays the bare ticker; aria-current tells assistive technology which one is chosen. */}
        {onSelectTicker ? (
          <button
            type="button"
            onClick={() => onSelectTicker(row.ticker)}
            aria-current={selected ? "true" : undefined}
            className={selected ? TICKER_BUTTON_SELECTED_CLASS : TICKER_BUTTON_CLASS}
          >
            {row.ticker}
          </button>
        ) : (
          row.ticker
        )}
      </th>
      <td data-column="company_name" className={COMPANY_CELL_CLASS}>
        {row.company_name}
      </td>
      <td data-column="sector" className={TEXT_CELL_CLASS}>
        {row.sector}
      </td>
      <td
        data-column="close_price"
        title={`${row.currency} per share, close of ${formatDate(row.trade_date)}`}
        className={NUMERIC_CELL_CLASS}
      >
        {formatDecimal(row.close_price, "per_share", { unit: false })}
      </td>
      <MetricCell metric="change_pct" value={row.change_pct} kind="pct" flags={row.flags} signed />
      <MetricCell metric="market_cap_kes" value={row.market_cap_kes} kind="kes" flags={row.flags} />
      <MetricCell metric="eps_kes" value={row.eps_kes} kind="per_share" flags={row.flags} />
      <MetricCell metric="pe" value={row.pe} kind="ratio" flags={row.flags} />
      <MetricCell metric="pb" value={row.pb} kind="ratio" flags={row.flags} />
      <MetricCell metric="ev_ebitda" value={row.ev_ebitda} kind="ratio" flags={row.flags} />
      <MetricCell
        metric="dividend_yield_pct"
        value={row.dividend_yield_pct}
        kind="pct"
        flags={row.flags}
      />
      <td data-column="volume" className={NUMERIC_CELL_CLASS}>
        {formatInteger(row.volume)}
      </td>
      {/* Turnover is in the row's listing currency, never converted, so the row's own code
          labels the exact digits; the "kes" kind only groups them here. */}
      <td data-column="turnover_kes" className={NUMERIC_CELL_CLASS}>
        {`${row.currency} ${formatDecimal(row.turnover_kes, "kes", { unit: false })}`}
      </td>
      <ZScoreCell
        metric="volume_z"
        value={row.volume_z}
        isAnomaly={row.is_volume_anomaly}
        flags={row.flags}
      />
      <ZScoreCell
        metric="turnover_z"
        value={row.turnover_z}
        isAnomaly={row.is_turnover_anomaly}
        flags={row.flags}
      />
      <FlagsCell flags={row.flags} />
    </tr>
  );
}

/** Column headers of the sector summary table. */
const SUMMARY_COLUMNS: readonly { label: string; numeric: boolean; sticky?: boolean }[] = [
  { label: "Sector", numeric: false, sticky: true },
  { label: "Constituents", numeric: true },
  { label: "Market cap", numeric: true },
  { label: "P/E", numeric: true },
  { label: "P/B", numeric: true },
  { label: "EV/EBITDA", numeric: true },
  { label: "Div. yield", numeric: true },
  { label: "Flags", numeric: false },
];

function summaryHeaderClass(column: { numeric: boolean; sticky?: boolean }): string {
  if (column.sticky === true) return TH_STICKY_CLASS;
  return column.numeric ? TH_NUMERIC_CLASS : TH_CLASS;
}

/** One sector's point-in-time aggregates, each multiple with its eligible count. */
function SectorAggregateRow({ aggregate }: { aggregate: SectorAggregate }): ReactElement {
  const constituents = aggregate.constituent_count;
  return (
    <tr data-testid="sector-aggregate-row" data-sector={aggregate.sector}>
      <th scope="row" data-column="sector" className={ROW_HEADER_CLASS}>
        <span className={SUMMARY_SECTOR_TEXT_CLASS}>{aggregate.sector}</span>
      </th>
      <td data-column="constituent_count" className={NUMERIC_CELL_CLASS}>
        {formatInteger(constituents)}
      </td>
      {aggregate.market_cap_kes === null ? (
        <td
          data-column="market_cap_kes"
          title={aggregateTitle(aggregate.flags)}
          className={NULL_CELL_CLASS}
        >
          {EMPTY_VALUE}
        </td>
      ) : (
        <td data-column="market_cap_kes" className={NUMERIC_CELL_CLASS}>
          {formatDecimal(aggregate.market_cap_kes, "kes")}
        </td>
      )}
      <AggregateMultipleCell
        column="pe"
        value={aggregate.pe}
        kind="ratio"
        eligible={aggregate.pe_eligible_count}
        constituents={constituents}
        flags={aggregate.flags}
      />
      <AggregateMultipleCell
        column="pb"
        value={aggregate.pb}
        kind="ratio"
        eligible={aggregate.pb_eligible_count}
        constituents={constituents}
        flags={aggregate.flags}
      />
      <AggregateMultipleCell
        column="ev_ebitda"
        value={aggregate.ev_ebitda}
        kind="ratio"
        eligible={aggregate.ev_ebitda_eligible_count}
        constituents={constituents}
        flags={aggregate.flags}
      />
      <AggregateMultipleCell
        column="dividend_yield_pct"
        value={aggregate.dividend_yield_pct}
        kind="pct"
        eligible={aggregate.dy_eligible_count}
        constituents={constituents}
        flags={aggregate.flags}
      />
      <FlagsCell flags={aggregate.flags} />
    </tr>
  );
}

/** A screener response with the filters it answered. */
interface AnsweredFilters {
  response: ScreenerResponse | undefined;
  filters: ScreenerFilters;
}

/** The caller's last `watchlists` array, with its {@link watchlistsSignature}. */
interface CallerWatchlists {
  signature: string | null;
  lists: Watchlist[] | undefined;
}

/** Whether the request failed on specific request fields, which are shown beside their inputs. */
function isFieldError(error: TerminalApiError | null): error is TerminalApiError {
  return error !== null && error.fieldErrors.length > 0;
}

/**
 * `useLayoutEffect` in the browser, so focus moves within the commit that removed its element,
 * before the browser paints or handles another event. During server rendering, where neither
 * effect runs and React 18 warns about layout effects, it is `useEffect`.
 */
const useBrowserLayoutEffect = typeof window === "undefined" ? useEffect : useLayoutEffect;

/** Content hidden past a scroller's inline end by at most this many pixels is rounding. */
const OVERFLOW_TOLERANCE_PX = 1;

/**
 * Whether a horizontal scroller has content hidden beyond its inline-end edge, which drives the
 * edge fade of {@link SCROLL_FADE_CLASS}. Returns a callback ref for the scroller, so a scroller
 * that mounts after the widget (the sector summary) is tracked too, and the flag.
 *
 * The flag starts false, so server and client markup match and no fade shows before layout. It is
 * set only from the scroller's `scroll` events and from a `ResizeObserver` on the scroller and its
 * table, whose first observation arrives once they are laid out and whose later ones follow a
 * resize or a table that grows or shrinks with its rows. `Math.abs(scrollLeft)` keeps the end
 * right in right-to-left layouts, where `scrollLeft` runs from 0 to negative values. Listeners are
 * removed, and the flag cleared, when the scroller unmounts. Without `ResizeObserver` (non-browser
 * test environments) only scrolling updates the flag.
 */
function useOverflowEnd(): [(node: HTMLElement | null) => void, boolean] {
  const [overflowEnd, setOverflowEnd] = useState(false);
  const detach = useRef<(() => void) | null>(null);
  const ref = useCallback((node: HTMLElement | null) => {
    detach.current?.();
    detach.current = null;
    if (node === null) {
      setOverflowEnd(false);
      return;
    }
    const update = (): void => {
      const hidden = node.scrollWidth - node.clientWidth - Math.abs(node.scrollLeft);
      setOverflowEnd(hidden > OVERFLOW_TOLERANCE_PX);
    };
    node.addEventListener("scroll", update, { passive: true });
    const observer = typeof ResizeObserver === "undefined" ? null : new ResizeObserver(update);
    if (observer !== null) {
      observer.observe(node);
      for (const child of Array.from(node.children)) {
        observer.observe(child);
      }
    }
    detach.current = () => {
      node.removeEventListener("scroll", update);
      observer?.disconnect();
    };
  }, []);
  return [ref, overflowEnd];
}


/**
 * Equity screener of `GET /market/screener`: filters on sector, watchlist, P/E, P/B, EV/EBITDA,
 * dividend yield, volume Z-score and anomalies; a server-sorted counter table with spike badges
 * and flag tooltips; and the sector summary from `sector_aggregates`.
 *
 * Selects and the checkbox apply at once. Numeric filters apply 400 ms after typing stops, or on
 * Enter or blur; their text is trimmed and then sent as typed, with no numeric conversion, so
 * values stay exact decimals. A result without rows shows the frame's empty state: "No counters
 * match these filters" when the filters it answered excluded every counter of a universe that has
 * data, and the default seeder copy when no data is loaded. Errors tied to a request field, such
 * as `min_exceeds_max` on `min_pe`, appear beneath that field while the last result stays on
 * screen (its rows dimmed, or its empty state); before any result has loaded, a note asking for
 * the errors to be corrected takes the table's place. Any other failure shows the frame's error
 * state with Retry. The filter panel is rendered in every state, so filters stay usable when
 * nothing matches. The watchlist selector lists the calling principal's watchlists from
 * `GET /watchlists` and refreshes them whenever it is focused. That request is independent of the
 * counters: if it fails before any list has loaded, the selector is disabled and a message with
 * its own Retry appears beneath it while the counters stay usable, and a list that loaded earlier
 * stays selectable after a later failure. The options and every list name shown come from that
 * request alone. Lists passed as `watchlists` only signal a change: an array with new content
 * fetches the watchlists again, and an applied list it drops from the previous array was deleted
 * by the caller, so the filter is cleared at once. Each newer payload is then compared with the
 * one before: an applied watchlist it no longer holds is cleared, and one whose tickers changed
 * has its counters fetched again while the previous rows stay dimmed. A screener request answered
 * with `watchlist_not_found` clears the filter as well and never shows its identifier. Every
 * clearing returns to all counters with a notice naming the list when known.
 */
export function EquityScreener({
  apiBaseUrl,
  getHeaders,
  className,
  initialFilters,
  onSelectTicker,
  selectedTicker,
  watchlists: callerWatchlists,
}: EquityScreenerProps): ReactElement {
  const baseId = useId();
  const [applied, setApplied] = useState<ScreenerFilters>(() => initialFilters ?? {});
  const [drafts, setDrafts] = useState<Drafts>(() => draftsFrom(initialFilters ?? {}));

  // Applies the drafts once typing pauses. The state update runs in the timer callback, never in
  // the effect body, and applyDrafts returns the same object when nothing changed.
  useEffect(() => {
    const timer = setTimeout(() => {
      setApplied((current) => applyDrafts(current, drafts));
    }, DRAFT_DEBOUNCE_MS);
    return () => {
      clearTimeout(timer);
    };
  }, [drafts]);

  const commitDrafts = useCallback(() => {
    setApplied((current) => applyDrafts(current, drafts));
  }, [drafts]);

  // The filters the shown response answered. A new query aborts the request it supersedes, so a
  // response always answers the filters current when it arrives, and they are recorded during
  // that render. Until a refetch settles, the previous response stays on screen under filters
  // that may have changed since, such as a cleared watchlist or a Reset, so its empty copy is
  // judged by the filters it answered: a "no match" never turns into the empty-database copy
  // meanwhile.
  const [answered, setAnswered] = useState<AnsweredFilters>(() => ({
    response: undefined,
    filters: applied,
  }));
  const screener = useTerminalQuery<ScreenerResponse>({
    path: "/market/screener",
    query: screenerQuery(applied),
    apiBaseUrl,
    getHeaders,
    // No rows is the empty state whatever the filters; the frame renders the filter panel in
    // every state, so the filters stay usable and the empty copy below names the cause.
    isEmpty: (response) => response.rows.length === 0,
  });
  if (screener.data !== answered.response) {
    setAnswered({ response: screener.data, filters: applied });
  }
  const watchlists = useTerminalQuery<Watchlist[]>({ path: "/watchlists", apiBaseUrl, getHeaders });
  // Set once a retry of the watchlist request is asked for, so the list's own Retry stays mounted
  // (as "Retrying…") while a retry of a list that never loaded is in flight.
  const [watchlistsRetryRequested, setWatchlistsRetryRequested] = useState(false);
  const watchlistSelectRef = useRef<HTMLSelectElement>(null);
  const watchlistsRetryButton = useRef<HTMLButtonElement | null>(null);
  // Whether the list's Retry held focus when it was removed, so focus moves to the selector.
  const restoreWatchlistFocus = useRef(false);
  // Set while focus is moved to the selector programmatically, so that focus does not refetch.
  const skipWatchlistFocusRefetch = useRef(false);
  // Why the watchlist filter was last cleared for the widget, until the user next picks a list or
  // presses Reset.
  const [watchlistNotice, setWatchlistNotice] = useState<string | null>(null);

  // The applied watchlist as this render leaves it: a check below that clears the filter sets it
  // to undefined, so a later check in the same render does not act on the cleared list again.
  let appliedWatchlistId = applied.watchlist_id;
  /** Clear the watchlist filter back to all counters, with a notice naming the list if known. */
  const clearAppliedWatchlist = (clearedId: string, name: string | undefined): void => {
    appliedWatchlistId = undefined;
    setApplied((current) =>
      current.watchlist_id === clearedId ? withSelectFilter(current, "watchlist_id", "") : current,
    );
    setWatchlistNotice(missingWatchlistNotice(name));
  };

  // The caller's lists, compared by content rather than identity, so a new array with the same
  // lists changes nothing. They may be stale (WatchlistManager never reloads lists created in
  // another tab), so they never become the options: new content only fetches the watchlists
  // again. The one conclusion drawn from them is a deletion the caller itself made, an applied
  // list in its previous array that the new one lacks, which is cleared before the next commit
  // so its rows go at once and no later request carries its id. The first array has nothing to
  // compare with. Both checks run during render, React's pattern for adjusting state when a prop
  // changes, and the stored signature stops them repeating, so no effect is needed.
  const callerSignature = watchlistsSignature(callerWatchlists);
  const [caller, setCaller] = useState<CallerWatchlists>(() => ({
    signature: callerSignature,
    lists: callerWatchlists,
  }));
  if (callerSignature !== caller.signature) {
    setCaller({ signature: callerSignature, lists: callerWatchlists });
    if (callerWatchlists !== undefined) {
      watchlists.refetch();
      const deletedId = appliedWatchlistId;
      const deleted =
        deletedId === undefined || deletedId === ""
          ? undefined
          : caller.lists?.find(({ id }) => id === deletedId);
      if (
        deletedId !== undefined &&
        deleted !== undefined &&
        !callerWatchlists.some(({ id }) => id === deletedId)
      ) {
        clearAppliedWatchlist(deletedId, deleted.name);
      }
    }
  }

  // Each fetched payload is compared with the one before it. An applied watchlist that has left
  // it, deleted here or in another tab, is cleared the same way; one whose tickers changed has
  // its counters fetched again, the previous rows staying dimmed until they arrive.
  const [seenWatchlists, setSeenWatchlists] = useState(watchlists.data);
  if (watchlists.data !== seenWatchlists) {
    setSeenWatchlists(watchlists.data);
    const appliedId = appliedWatchlistId;
    const before =
      appliedId === undefined || appliedId === ""
        ? undefined
        : seenWatchlists?.find(({ id }) => id === appliedId);
    const after = watchlists.data?.find(({ id }) => id === appliedId);
    if (
      appliedId !== undefined &&
      before !== undefined &&
      watchlists.data !== undefined &&
      after === undefined
    ) {
      clearAppliedWatchlist(appliedId, before.name);
    } else if (
      before !== undefined &&
      after !== undefined &&
      !sameTickers(before.tickers, after.tickers)
    ) {
      screener.refetch();
    }
  }

  const error = screener.error;
  // A filter naming a watchlist the server does not have, such as an initialFilters id or a list
  // deleted before the widget saw it go, is cleared the same way and never shown as the raw
  // message. A list still offered is stale, so the options are fetched again to drop it.
  const missingWatchlistId = appliedWatchlistId;
  if (
    isWatchlistNotFound(error) &&
    missingWatchlistId !== undefined &&
    missingWatchlistId !== ""
  ) {
    const missing = watchlists.data?.find(({ id }) => id === missingWatchlistId);
    clearAppliedWatchlist(missingWatchlistId, missing?.name);
    if (missing !== undefined && !watchlists.isFetching) {
      watchlists.refetch();
    }
  }
  const hasFieldErrors = isFieldError(error);
  const fieldErrors = hasFieldErrors ? fieldMessages(error, applied) : {};
  const otherFieldErrors = Object.entries(fieldErrors).filter(
    ([field]) => !INPUT_FIELDS.has(field),
  );
  const response = screener.data;
  // A field error keeps the last result on screen beside the inputs: its rows, dimmed, or its
  // empty state. Before any result has loaded, the ready body asks for the errors to be fixed.
  let state: WidgetState = screener.state;
  if (screener.state === "error" && hasFieldErrors) {
    state = response !== undefined && response.rows.length === 0 ? "empty" : "ready";
  }
  const rows = response?.rows ?? [];
  const aggregates = response?.sector_aggregates ?? [];
  // The empty copy names the cause of the empty result on screen. `sector_aggregates` covers the
  // whole universe whatever the filters, so sector data without rows under a narrowing filter
  // means the filters excluded every counter, while no sector data means nothing is loaded yet
  // (the default seeder copy). The filters are those the shown response answered, so clearing
  // them from a no-match result keeps the no-match copy until the new rows arrive.
  const shownFilters = response === answered.response ? answered.filters : applied;
  const emptyMessage =
    aggregates.length > 0 && hasActiveFilter(shownFilters) ? NO_MATCH_MESSAGE : undefined;
  const sectors = sectorOptions(response?.sector_aggregates, applied.sector);
  const activeSort = applied.sort ?? DEFAULT_SORT;
  const activeOrder = applied.order ?? DEFAULT_ORDER;
  // Rows that no longer match the filters (a refetch in flight, or a field error) are dimmed.
  const dimmed = response !== undefined && (screener.isFetching || hasFieldErrors);

  const watchlistItems = watchlists.data ?? [];
  const watchlistsUnavailable = watchlists.state === "error" && watchlists.data === undefined;
  // The list's message and Retry, shown until a list first loads: after a failure, and while a
  // requested retry is in flight. A list that loaded once is kept, so they never return after it.
  const showWatchlistsNotice =
    watchlists.data === undefined &&
    (watchlists.state === "error" || (watchlistsRetryRequested && watchlists.isFetching));
  const watchlistsRetrying = showWatchlistsNotice && watchlists.isFetching;

  // React detaches a ref before it removes the node, so the node still holds focus here when the
  // notice goes away because a list loaded.
  const setWatchlistsRetryButton = useCallback((node: HTMLButtonElement | null) => {
    const previous = watchlistsRetryButton.current;
    if (node === null && previous !== null) {
      restoreWatchlistFocus.current = previous.ownerDocument.activeElement === previous;
    }
    watchlistsRetryButton.current = node;
  }, []);

  // Focus that sat on the list's Retry moves to the selector the loaded list has enabled, in the
  // same commit, rather than staying on the document body. The move does not refetch the list
  // that just arrived.
  useBrowserLayoutEffect(() => {
    if (showWatchlistsNotice || !restoreWatchlistFocus.current) {
      return;
    }
    restoreWatchlistFocus.current = false;
    const select = watchlistSelectRef.current;
    if (select === null || select.disabled) {
      return;
    }
    const active = select.ownerDocument.activeElement;
    if (active !== null && active !== select.ownerDocument.body) {
      return;
    }
    skipWatchlistFocusRefetch.current = true;
    try {
      select.focus();
    } finally {
      skipWatchlistFocusRefetch.current = false;
    }
  }, [showWatchlistsNotice]);

  const selectedWatchlist = applied.watchlist_id;
  const showUnlistedWatchlist =
    selectedWatchlist !== undefined &&
    selectedWatchlist !== "" &&
    !watchlistItems.some((watchlist) => watchlist.id === selectedWatchlist);
  // The selector's narrow box clips a long list name, so the selected option's full text is its
  // title, and a known list name also appears in full on the context line above the counters.
  const selectedWatchlistName =
    selectedWatchlist === undefined || selectedWatchlist === ""
      ? undefined
      : watchlistItems.find((watchlist) => watchlist.id === selectedWatchlist)?.name;
  let watchlistTitle = "All counters";
  if (watchlistsUnavailable) watchlistTitle = "Watchlists unavailable";
  else if (selectedWatchlistName !== undefined) watchlistTitle = selectedWatchlistName;
  else if (showUnlistedWatchlist) watchlistTitle = "Selected watchlist";

  const sectorId = `${baseId}-sector`;
  const watchlistId = `${baseId}-watchlist_id`;
  const anomaliesId = `${baseId}-anomalies_only`;
  const summaryHeadingId = `${baseId}-sector-summary`;
  const countersCaptionId = `${baseId}-counters-caption`;
  const errorIdOf = (field: string): string => `${baseId}-${field}-error`;
  const describedByOf = (field: string): string | undefined =>
    fieldErrors[field] === undefined ? undefined : errorIdOf(field);
  const invalidOf = (field: string): true | undefined =>
    fieldErrors[field] === undefined ? undefined : true;
  const watchlistsNoticeId = `${baseId}-watchlists-notice`;
  const watchlistNoticeId = `${baseId}-watchlist-notice`;
  const watchlistDescribedBy =
    [
      describedByOf("watchlist_id"),
      showWatchlistsNotice ? watchlistsNoticeId : undefined,
      watchlistNotice !== null ? watchlistNoticeId : undefined,
    ]
      .filter((id): id is string => id !== undefined)
      .join(" ") || undefined;

  const handleDraftChange = (key: NumericFilterKey, value: string): void => {
    setDrafts((current) => ({ ...current, [key]: value }));
  };
  const handleDraftKeyDown = (event: KeyboardEvent<HTMLInputElement>): void => {
    if (event.key === "Enter") {
      commitDrafts();
    }
  };
  const handleReset = (): void => {
    const restored = initialFilters ?? {};
    setApplied(restored);
    setDrafts(draftsFrom(restored));
    setWatchlistNotice(null);
  };
  // A failure that reached the screener most likely failed the watchlists too, so Retry repeats
  // both; a watchlist list that loaded is left alone.
  const handleRetry = (): void => {
    screener.refetch();
    if (watchlists.state === "error") {
      setWatchlistsRetryRequested(true);
      watchlists.refetch();
    }
  };
  // The list's own Retry is aria-disabled, not disabled, while a retry runs, so it keeps focus;
  // a press then is ignored rather than restarting the request.
  const handleWatchlistsRetry = (): void => {
    if (watchlists.isFetching) {
      return;
    }
    setWatchlistsRetryRequested(true);
    watchlists.refetch();
  };
  // A request already in flight brings the current list, so focusing the selector then does not
  // abort and repeat it; nor does the focus moved to it after a retry has just loaded the list.
  const handleWatchlistFocus = (): void => {
    if (skipWatchlistFocusRefetch.current || watchlists.isFetching) {
      return;
    }
    watchlists.refetch();
  };
  const handleSort = (column: ScreenerSort): void => {
    setApplied((current) => withSort(current, column));
  };

  const controls = (
    <div role="search" aria-label="Screener filters">
      <div className={FILTER_GRID_CLASS}>
        <div className={FIELD_CLASS}>
          <label htmlFor={sectorId} className={FIELD_LABEL_CLASS}>
            Sector
          </label>
          <select
            id={sectorId}
            data-testid="screener-filter-sector"
            value={applied.sector ?? ""}
            onChange={(event) => {
              const value = event.target.value;
              setApplied((current) => withSelectFilter(current, "sector", value));
            }}
            aria-invalid={invalidOf("sector")}
            aria-describedby={describedByOf("sector")}
            className={INPUT_CLASS}
          >
            <option value="">All sectors</option>
            {sectors.map((name) => (
              <option key={name} value={name}>
                {name}
              </option>
            ))}
          </select>
          <FieldErrorText field="sector" message={fieldErrors.sector} id={errorIdOf("sector")} />
        </div>
        <div className={FIELD_CLASS}>
          <label htmlFor={watchlistId} className={FIELD_LABEL_CLASS}>
            Watchlist
          </label>
          <select
            ref={watchlistSelectRef}
            id={watchlistId}
            data-testid="screener-filter-watchlist"
            value={watchlistsUnavailable ? "" : (selectedWatchlist ?? "")}
            disabled={watchlistsUnavailable}
            onChange={(event) => {
              const value = event.target.value;
              setApplied((current) => withSelectFilter(current, "watchlist_id", value));
              setWatchlistNotice(null);
            }}
            // Watchlists can change in a sibling WatchlistManager, so the list is refreshed
            // whenever the selector is focused; the current options stay while it loads.
            onFocus={handleWatchlistFocus}
            aria-invalid={invalidOf("watchlist_id")}
            aria-describedby={watchlistDescribedBy}
            title={watchlistTitle}
            className={WATCHLIST_SELECT_CLASS}
          >
            {watchlistsUnavailable ? (
              <option value="">Watchlists unavailable</option>
            ) : (
              <>
                <option value="">All counters</option>
                {watchlistItems.map((watchlist) => (
                  <option key={watchlist.id} value={watchlist.id}>
                    {watchlist.name}
                  </option>
                ))}
                {showUnlistedWatchlist ? (
                  <option value={selectedWatchlist}>Selected watchlist</option>
                ) : null}
              </>
            )}
          </select>
          <FieldErrorText
            field="watchlist_id"
            message={fieldErrors.watchlist_id}
            id={errorIdOf("watchlist_id")}
          />
          <p
            id={watchlistNoticeId}
            data-testid="screener-watchlist-notice"
            role="status"
            aria-live="polite"
            className={
              watchlistNotice === null ? WATCHLIST_NOTICE_EMPTY_CLASS : WATCHLIST_NOTICE_CLASS
            }
          >
            {watchlistNotice}
          </p>
          {showWatchlistsNotice ? (
            <div className={WATCHLISTS_NOTICE_CLASS}>
              {/* The detail is removed while a retry runs, so a repeated failure is re-announced. */}
              <p
                id={watchlistsNoticeId}
                data-testid="screener-watchlists-error"
                role="alert"
                className={WATCHLISTS_NOTICE_TEXT_CLASS}
              >
                Watchlists could not be loaded
                {watchlists.error !== null ? <span>: {watchlists.error.message}</span> : null}
              </p>
              <button
                ref={setWatchlistsRetryButton}
                type="button"
                data-testid="screener-watchlists-retry"
                aria-label="Retry loading watchlists"
                aria-disabled={watchlistsRetrying ? true : undefined}
                onClick={handleWatchlistsRetry}
                className={WATCHLISTS_RETRY_CLASS}
              >
                {watchlistsRetrying ? "Retrying…" : "Retry"}
              </button>
            </div>
          ) : null}
        </div>
        {NUMERIC_FILTERS.map(({ key, label }) => {
          const inputId = `${baseId}-${key}`;
          return (
            <div key={key} className={FIELD_CLASS}>
              <label htmlFor={inputId} className={FIELD_LABEL_CLASS}>
                {label}
              </label>
              <input
                id={inputId}
                type="text"
                inputMode="decimal"
                autoComplete="off"
                spellCheck={false}
                data-testid={"screener-filter-" + key}
                value={drafts[key]}
                onChange={(event) => handleDraftChange(key, event.target.value)}
                onKeyDown={handleDraftKeyDown}
                onBlur={commitDrafts}
                aria-invalid={invalidOf(key)}
                aria-describedby={describedByOf(key)}
                className={INPUT_CLASS}
              />
              <FieldErrorText field={key} message={fieldErrors[key]} id={errorIdOf(key)} />
            </div>
          );
        })}
      </div>
      <div className={FILTER_FOOTER_CLASS}>
        <label htmlFor={anomaliesId} className={CHECKBOX_LABEL_CLASS}>
          <input
            id={anomaliesId}
            type="checkbox"
            data-testid="screener-filter-anomalies_only"
            checked={applied.anomalies_only === true}
            onChange={(event) => {
              const checked = event.target.checked;
              setApplied((current) => withAnomaliesOnly(current, checked));
            }}
            className={CHECKBOX_CLASS}
          />
          Anomalies only
        </label>
        <button
          type="button"
          data-testid="screener-reset"
          onClick={handleReset}
          className={SECONDARY_BUTTON_CLASS}
        >
          Reset
        </button>
      </div>
      {otherFieldErrors.length > 0 ? (
        // Errors on request fields without an input here, such as an initialFilters `as_of`.
        <div className={OTHER_ERRORS_CLASS}>
          {otherFieldErrors.map(([field, message]) => (
            <FieldErrorText key={field} field={field} message={`${field}: ${message}`} />
          ))}
        </div>
      ) : null}
    </div>
  );

  const [countersScrollRef, countersOverflowEnd] = useOverflowEnd();
  const [summaryScrollRef, summaryOverflowEnd] = useOverflowEnd();

  return (
    <WidgetFrame
      testId="terminal-equity-screener"
      title="Screener"
      state={state}
      error={hasFieldErrors ? null : error}
      onRetry={handleRetry}
      className={className}
      skeletonClassName={SKELETON_HEIGHT_CLASS}
      emptyMessage={emptyMessage}
      controls={controls}
    >
      <div className={dimmed ? BODY_DIMMED_CLASS : BODY_CLASS} aria-busy={screener.isFetching}>
        {response !== undefined ? (
          <p className={CONTEXT_CLASS}>
            <span className={COUNT_CLASS} aria-live="polite">
              {formatInteger(response.count)} {response.count === 1 ? "counter" : "counters"}
            </span>
            {selectedWatchlistName !== undefined ? (
              <span data-testid="screener-context-watchlist" className={CONTEXT_WATCHLIST_CLASS}>
                Watchlist: {selectedWatchlistName}
              </span>
            ) : null}
            <span>
              As of {formatDate(response.as_of)} · Z window {formatInteger(response.z_window)} ·
              threshold {formatDecimal(response.anomaly_threshold, "z")}
            </span>
          </p>
        ) : null}
        {response === undefined ? (
          // Ready without a result only after a field error on the first request: there are no
          // counters to show, so the note takes the table's place rather than a full-width cell.
          <p className={NO_RESULT_NOTE_CLASS}>Correct the filter errors above to load counters</p>
        ) : (
          <div className={SCROLL_FRAME_CLASS}>
            <div
              ref={countersScrollRef}
              role="region"
              aria-labelledby={countersCaptionId}
              tabIndex={0}
              data-overflow-end={countersOverflowEnd ? "true" : "false"}
              className={SCROLL_REGION_CLASS}
            >
              <table className={TABLE_CLASS}>
                <caption id={countersCaptionId} className="sr-only">
                  Screener counters
                </caption>
                <thead>
                  <tr>
                    {COUNTER_COLUMNS.map((column) => (
                      <CounterHeader
                        key={column.id}
                        column={column}
                        activeSort={activeSort}
                        activeOrder={activeOrder}
                        onSort={handleSort}
                      />
                    ))}
                  </tr>
                </thead>
                <tbody className={TBODY_CLASS}>
                  {rows.map((row) => (
                    <CounterRow
                      key={row.ticker}
                      row={row}
                      onSelectTicker={onSelectTicker}
                      selected={row.ticker === selectedTicker}
                    />
                  ))}
                </tbody>
              </table>
            </div>
            <div
              aria-hidden="true"
              className={countersOverflowEnd ? SCROLL_FADE_CLASS : SCROLL_FADE_HIDDEN_CLASS}
            />
          </div>
        )}
        {response !== undefined ? (
          <div className={SUMMARY_SECTION_CLASS}>
            <h3 id={summaryHeadingId} className={SECTION_HEADING_CLASS}>
              Sector summary
            </h3>
            <p className={SUMMARY_NOTE_CLASS}>
              Each multiple is followed by (eligible/constituents): the members it is computed
              over, as of the same date and independent of the filters.
            </p>
            {aggregates.length === 0 ? (
              <p className={SUMMARY_NOTE_CLASS}>No sector data for this date</p>
            ) : (
              <div className={SCROLL_FRAME_CLASS}>
                <div
                  ref={summaryScrollRef}
                  role="region"
                  aria-labelledby={summaryHeadingId}
                  tabIndex={0}
                  data-overflow-end={summaryOverflowEnd ? "true" : "false"}
                  className={SUMMARY_SCROLL_REGION_CLASS}
                >
                  <table className={TABLE_CLASS}>
                    {/* The region keeps the visible heading as its name; the caption names the
                        table itself with the same words. */}
                    <caption className="sr-only">Sector summary</caption>
                    <thead>
                      <tr>
                        {SUMMARY_COLUMNS.map((column) => (
                          <th
                            key={column.label}
                            scope="col"
                            className={summaryHeaderClass(column)}
                          >
                            <span className={HEADER_LABEL_CLASS}>{column.label}</span>
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className={TBODY_CLASS}>
                      {aggregates.map((aggregate) => (
                        <SectorAggregateRow key={aggregate.sector} aggregate={aggregate} />
                      ))}
                    </tbody>
                  </table>
                </div>
                <div
                  aria-hidden="true"
                  className={summaryOverflowEnd ? SCROLL_FADE_CLASS : SCROLL_FADE_HIDDEN_CLASS}
                />
              </div>
            )}
          </div>
        ) : null}
      </div>
    </WidgetFrame>
  );
}

