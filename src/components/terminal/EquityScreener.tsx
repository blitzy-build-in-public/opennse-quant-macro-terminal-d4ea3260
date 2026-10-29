"use client";

import { useCallback, useEffect, useId, useState } from "react";
import type { KeyboardEvent, ReactElement } from "react";

import { fieldErrorMap } from "./lib/apiClient";
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
import { FieldErrorText, WidgetFrame } from "./shared/WidgetFrame";
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

/** Typing pause after which the numeric drafts are applied; Enter and blur apply at once. */
const DRAFT_DEBOUNCE_MS = 400;

/** Server defaults of `sort` and `order`, used to mark the active column before any click. */
const DEFAULT_SORT: ScreenerSort = "market_cap_kes";
const DEFAULT_ORDER: SortOrder = "desc";

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
 * Flag-code prefixes that explain a null value of each metric (the reason codes of the valuation
 * and Z-score engines). A metric whose row carries none of them falls back to every row flag.
 */
const METRIC_FLAG_PREFIXES: Record<NullableMetric, readonly string[]> = {
  change_pct: [],
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

/** Whether a filter that narrows the rows is set; an empty result is then "no match". */
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

// Every class below is a complete static string, never assembled from fragments, so Tailwind's
// source scan and a host's `@source` directive or `content` glob find each one. Only the default
// palette is used. Focus indicators are outlines, which forced-colors mode keeps.
// 11.5rem keeps the longest select text ("Watchlists unavailable") unclipped beside its arrow.
const FILTER_GRID_CLASS = "grid grid-cols-[repeat(auto-fill,minmax(11.5rem,1fr))] gap-3";
const FIELD_CLASS = "flex min-w-0 flex-col gap-1";
const LABEL_CLASS = "text-xs font-medium text-slate-400";
/* BLITZY [A11Y]: filter inputs, selects and table buttons keep the compact density of the card's
   other controls (about 24–30px tall, at or above WCAG 2.2 AA's 24px target minimum, below the
   44px guideline), so 16 columns stay readable. Flagged for designer review. */
const INPUT_CLASS =
  "w-full min-w-0 rounded border border-slate-700 bg-slate-950 px-2 py-1 text-sm text-slate-100 placeholder:text-slate-600 aria-[invalid=true]:border-rose-500 focus-visible:outline-2 focus-visible:outline-offset-1 focus-visible:outline-sky-400 disabled:cursor-not-allowed disabled:text-slate-400";
const FILTER_FOOTER_CLASS = "mt-3 flex flex-wrap items-center justify-between gap-3";
const CHECKBOX_LABEL_CLASS = "inline-flex items-center gap-2 text-sm text-slate-200";
const CHECKBOX_CLASS =
  "h-4 w-4 accent-sky-500 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400";
const RESET_CLASS =
  "rounded border border-slate-700 bg-slate-800 px-3 py-1 text-xs font-medium text-slate-100 hover:bg-slate-700 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400 motion-safe:transition-colors motion-safe:duration-150 motion-safe:ease-out";
const OTHER_ERRORS_CLASS = "mt-2";

const BODY_CLASS = "flex flex-col gap-4 motion-safe:transition-opacity motion-safe:duration-200";
const BODY_DIMMED_CLASS =
  "flex flex-col gap-4 opacity-60 motion-safe:transition-opacity motion-safe:duration-200";
const CONTEXT_CLASS = "flex flex-wrap gap-x-3 gap-y-1 text-xs text-slate-400";
const COUNT_CLASS = "font-medium text-slate-200";
const SCROLL_REGION_CLASS =
  "overflow-x-auto rounded focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400";
const TABLE_CLASS = "min-w-full border-collapse text-sm";
const TBODY_CLASS = "divide-y divide-slate-800";
const TH_CLASS =
  "whitespace-nowrap border-be border-slate-700 px-2 py-1.5 text-start align-bottom text-xs font-medium text-slate-400";
// The first column (ticker or sector) stays in view while a wide table scrolls sideways; its
// opaque background matches the card surface so scrolled cells pass beneath it.
const TH_STICKY_CLASS =
  "sticky start-0 z-10 whitespace-nowrap border-be border-slate-700 bg-slate-900 px-2 py-1.5 text-start align-bottom text-xs font-medium text-slate-400";
const TH_NUMERIC_CLASS =
  "whitespace-nowrap border-be border-slate-700 px-2 py-1.5 text-end align-bottom text-xs font-medium text-slate-400";
const SORT_BUTTON_CLASS =
  "inline-flex min-h-6 items-center gap-1 rounded text-xs font-medium text-slate-400 hover:text-slate-100 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400 motion-safe:transition-colors motion-safe:duration-150 motion-safe:ease-out";
const SORT_BUTTON_ACTIVE_CLASS =
  "inline-flex min-h-6 items-center gap-1 rounded text-xs font-semibold text-slate-100 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400";
const ROW_HEADER_CLASS =
  "sticky start-0 z-10 whitespace-nowrap bg-slate-900 px-2 py-1.5 text-start font-semibold text-slate-100";
const TICKER_BUTTON_CLASS =
  "min-h-6 rounded font-semibold text-sky-300 underline-offset-2 hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400";
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
const MESSAGE_CELL_CLASS = "px-2 py-4 text-center text-sm text-slate-400";

const SUMMARY_SECTION_CLASS = "flex flex-col gap-2";
const SUMMARY_HEADING_CLASS = "text-sm font-semibold text-slate-200";
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

/** A rolling Z-score with its spike badge; the badge's text, not its colour, carries meaning. */
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
          {EMPTY_VALUE}
          {badge}
        </span>
      </td>
    );
  }
  return (
    <td data-column={metric} className={NUMERIC_CELL_CLASS}>
      <span className={Z_CELL_INNER_CLASS}>
        {formatDecimal(value, "z")}
        {badge}
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
        {column.label}
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
}

/** One equity of the screener, every value formatted from the server's exact strings. */
function CounterRow({ row, onSelectTicker }: CounterRowProps): ReactElement {
  return (
    <tr data-testid="screener-row" data-ticker={row.ticker}>
      <th scope="row" data-column="ticker" className={ROW_HEADER_CLASS}>
        {/* A button only when a handler exists: a control that does nothing would mislead
            keyboard and screen-reader users, so without one the ticker is plain text. */}
        {onSelectTicker ? (
          <button
            type="button"
            onClick={() => onSelectTicker(row.ticker)}
            className={TICKER_BUTTON_CLASS}
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
      <td data-column="turnover_kes" className={NUMERIC_CELL_CLASS}>
        {formatDecimal(row.turnover_kes, "kes")}
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
        {aggregate.sector}
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

/** Whether the request failed on specific request fields, which are shown beside their inputs. */
function isFieldError(error: TerminalApiError | null): error is TerminalApiError {
  return error !== null && error.fieldErrors.length > 0;
}


/**
 * Equity screener of `GET /market/screener`: filters on sector, watchlist, P/E, P/B, EV/EBITDA,
 * dividend yield, volume Z-score and anomalies; a server-sorted counter table with spike badges
 * and flag tooltips; and the sector summary from `sector_aggregates`.
 *
 * Selects and the checkbox apply at once. Numeric filters apply 400 ms after typing stops, or on
 * Enter or blur; their text is sent exactly as typed, so values stay exact decimals. Errors tied
 * to a request field, such as `min_exceeds_max` on `min_pe`, appear beneath that field while the
 * last result stays on screen; any other failure shows the frame's error state with Retry. The
 * filter panel is rendered in every state. The watchlist selector lists the calling principal's
 * watchlists from `GET /watchlists`; its failure only disables the selector.
 */
export function EquityScreener({
  apiBaseUrl,
  getHeaders,
  className,
  initialFilters,
  onSelectTicker,
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

  const screener = useTerminalQuery<ScreenerResponse>({
    path: "/market/screener",
    query: screenerQuery(applied),
    apiBaseUrl,
    getHeaders,
    // With a narrowing filter set, no rows is a "no match" inside the table, so the filters stay.
    isEmpty: (response) => response.rows.length === 0 && !hasActiveFilter(applied),
  });
  const watchlists = useTerminalQuery<Watchlist[]>({ path: "/watchlists", apiBaseUrl, getHeaders });

  const error = screener.error;
  const hasFieldErrors = isFieldError(error);
  const fieldErrors = hasFieldErrors ? fieldErrorMap(error) : {};
  const otherFieldErrors = Object.entries(fieldErrors).filter(
    ([field]) => !INPUT_FIELDS.has(field),
  );
  const state: WidgetState =
    screener.state === "error" && hasFieldErrors ? "ready" : screener.state;
  const response = screener.data;
  const rows = response?.rows ?? [];
  const aggregates = response?.sector_aggregates ?? [];
  const sectors = sectorOptions(response?.sector_aggregates, applied.sector);
  const activeSort = applied.sort ?? DEFAULT_SORT;
  const activeOrder = applied.order ?? DEFAULT_ORDER;
  // Rows that no longer match the filters (a refetch in flight, or a field error) are dimmed.
  const dimmed = response !== undefined && (screener.isFetching || hasFieldErrors);

  const watchlistItems = watchlists.data ?? [];
  const watchlistsUnavailable = watchlists.state === "error" && watchlists.data === undefined;
  const selectedWatchlist = applied.watchlist_id;
  const showUnlistedWatchlist =
    selectedWatchlist !== undefined &&
    selectedWatchlist !== "" &&
    !watchlistItems.some((watchlist) => watchlist.id === selectedWatchlist);

  const sectorId = `${baseId}-sector`;
  const watchlistId = `${baseId}-watchlist_id`;
  const anomaliesId = `${baseId}-anomalies_only`;
  const summaryHeadingId = `${baseId}-sector-summary`;
  const errorIdOf = (field: string): string => `${baseId}-${field}-error`;
  const describedByOf = (field: string): string | undefined =>
    fieldErrors[field] === undefined ? undefined : errorIdOf(field);
  const invalidOf = (field: string): true | undefined =>
    fieldErrors[field] === undefined ? undefined : true;

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
  };
  // A failure that reached the screener most likely failed the watchlists too, so Retry repeats
  // both; a watchlist list that loaded is left alone.
  const handleRetry = (): void => {
    screener.refetch();
    if (watchlists.state === "error") {
      watchlists.refetch();
    }
  };
  const handleSort = (column: ScreenerSort): void => {
    setApplied((current) => withSort(current, column));
  };

  const controls = (
    <div role="search" aria-label="Screener filters">
      <div className={FILTER_GRID_CLASS}>
        <div className={FIELD_CLASS}>
          <label htmlFor={sectorId} className={LABEL_CLASS}>
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
          <label htmlFor={watchlistId} className={LABEL_CLASS}>
            Watchlist
          </label>
          <select
            id={watchlistId}
            data-testid="screener-filter-watchlist"
            value={watchlistsUnavailable ? "" : (selectedWatchlist ?? "")}
            disabled={watchlistsUnavailable}
            onChange={(event) => {
              const value = event.target.value;
              setApplied((current) => withSelectFilter(current, "watchlist_id", value));
            }}
            // Watchlists can change in a sibling WatchlistManager, so the list is refreshed
            // whenever the selector is focused; the current options stay while it loads.
            onFocus={watchlists.refetch}
            aria-invalid={invalidOf("watchlist_id")}
            aria-describedby={describedByOf("watchlist_id")}
            className={INPUT_CLASS}
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
        </div>
        {NUMERIC_FILTERS.map(({ key, label }) => {
          const inputId = `${baseId}-${key}`;
          return (
            <div key={key} className={FIELD_CLASS}>
              <label htmlFor={inputId} className={LABEL_CLASS}>
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
          className={RESET_CLASS}
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

  let tableBody: ReactElement | ReactElement[];
  if (response === undefined) {
    tableBody = (
      <tr>
        <td colSpan={COUNTER_COLUMNS.length} className={MESSAGE_CELL_CLASS}>
          Correct the filter errors above to load counters
        </td>
      </tr>
    );
  } else if (rows.length === 0) {
    tableBody = (
      <tr data-testid="screener-no-match">
        <td colSpan={COUNTER_COLUMNS.length} className={MESSAGE_CELL_CLASS}>
          No counters match these filters
        </td>
      </tr>
    );
  } else {
    tableBody = rows.map((row) => (
      <CounterRow key={row.ticker} row={row} onSelectTicker={onSelectTicker} />
    ));
  }

  return (
    <WidgetFrame
      testId="terminal-equity-screener"
      title="Screener"
      state={state}
      error={hasFieldErrors ? null : error}
      onRetry={handleRetry}
      className={className}
      skeletonClassName="h-96"
      controls={controls}
    >
      <div className={dimmed ? BODY_DIMMED_CLASS : BODY_CLASS} aria-busy={screener.isFetching}>
        {response !== undefined ? (
          <p className={CONTEXT_CLASS}>
            <span className={COUNT_CLASS} aria-live="polite">
              {formatInteger(response.count)} {response.count === 1 ? "counter" : "counters"}
            </span>
            <span>
              As of {formatDate(response.as_of)} · Z window {formatInteger(response.z_window)} ·
              threshold {formatDecimal(response.anomaly_threshold, "z")}
            </span>
          </p>
        ) : null}
        <div
          role="region"
          aria-label="Screener counters"
          tabIndex={0}
          className={SCROLL_REGION_CLASS}
        >
          <table className={TABLE_CLASS}>
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
            <tbody className={TBODY_CLASS}>{tableBody}</tbody>
          </table>
        </div>
        {response !== undefined ? (
          <div className={SUMMARY_SECTION_CLASS}>
            <h3 id={summaryHeadingId} className={SUMMARY_HEADING_CLASS}>
              Sector summary
            </h3>
            <p className={SUMMARY_NOTE_CLASS}>
              Each multiple is followed by (eligible/constituents): the members it is computed
              over, as of the same date and independent of the filters.
            </p>
            {aggregates.length === 0 ? (
              <p className={SUMMARY_NOTE_CLASS}>No sector data for this date</p>
            ) : (
              <div
                role="region"
                aria-labelledby={summaryHeadingId}
                tabIndex={0}
                className={SCROLL_REGION_CLASS}
              >
                <table className={TABLE_CLASS}>
                  <thead>
                    <tr>
                      {SUMMARY_COLUMNS.map((column) => (
                        <th
                          key={column.label}
                          scope="col"
                          className={summaryHeaderClass(column)}
                        >
                          {column.label}
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
            )}
          </div>
        ) : null}
      </div>
    </WidgetFrame>
  );
}

