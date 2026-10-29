"use client";

import { useEffect, useId, useMemo, useRef, useState } from "react";
import type { ChangeEvent, FormEvent, MouseEvent, ReactElement } from "react";
import {
  CartesianGrid,
  Line,
  LineChart,
  ReferenceDot,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { AXIS_TICK, CHART_COLORS, PROJECTION_DASH, SERIES_PALETTE } from "./lib/chartTheme";
import { EMPTY_VALUE, formatDate, formatDecimal, formatInteger, toChartNumber } from "./lib/format";
import { useTerminalQuery } from "./lib/useTerminalQuery";
import { WidgetFrame } from "./shared/WidgetFrame";
import type {
  BacktestResponse,
  BacktestStatus,
  BacktestWindow,
  CycleMarker,
  CyclePoint,
  CycleProjectionPoint,
  CycleResult,
  DecimalString,
  TerminalWidgetProps,
  WidgetState,
} from "./types";

/** Props of {@link CycleBacktester}. */
export interface CycleBacktesterProps extends TerminalWidgetProps {
  /**
   * Event dates T to compare, as ISO `YYYY-MM-DD` strings, at most 8. When omitted or empty, the
   * server's defaults apply: every `terminal_cycles` row (the seeded election dates 2013-03-04,
   * 2017-08-08, 2022-08-09 and 2027-08-10). The user can then remove and add dates.
   */
  defaultEventDates?: string[];
  /**
   * Initial window preset: `"180d"` for T−180 to T+180 calendar days (sends
   * `pre_days=180&post_days=180`), or `"12m"` for T−12 to T+12 calendar months (sends
   * `pre_months=12&post_months=12`). Default `"180d"`.
   */
  defaultWindow?: "180d" | "12m";
  /**
   * Cohort candidate tickers, at most 100. Each cycle's cohort is the candidates with a close at
   * its anchor. When omitted or empty, every equity is a candidate.
   */
  tickers?: string[];
}

type WindowPreset = "180d" | "12m";

/** The {@link CyclePoint} field plotted on the main chart. */
type Metric = "price_index" | "pe_index" | "turnover_index" | "cumulative_foreign_net_kes";

/** The metrics a {@link CycleProjectionPoint} carries; projections have no foreign flow. */
type IndexMetric = Exclude<Metric, "cumulative_foreign_net_kes">;

const MARKER_KINDS = ["valuation_floor", "volume_exhaustion"] as const;
type MarkerKind = (typeof MARKER_KINDS)[number];

/** The overlay series of the macro chart, each a {@link CyclePoint} percentage field. */
type OverlayField = "inflation_yoy_pct" | "tbill_91d_pct" | "tbill_364d_pct";

/** One observed session of a cycle on the main chart. `y` is a coordinate only. */
interface ObservedRow {
  kind: "observed";
  x: number;
  y: number | null;
  point: CyclePoint;
}

/** One calendar offset of a cycle's projection on the main chart. `y` is a coordinate only. */
interface ProjectionRow {
  kind: "projection";
  x: number;
  y: number | null;
  point: CycleProjectionPoint;
}

/** Everything drawn for one cycle on the main chart, in the cycle's palette colour. */
interface ChartSeries {
  key: string;
  cycle: CycleResult;
  color: string;
  observed: ObservedRow[];
  /** Projection rows for the current metric; null when no projection line is drawn. */
  projection: ProjectionRow[] | null;
  /** "Projection (mean of …)"; null when no projection line is drawn. */
  projectionName: string | null;
}

/** A valuation-floor or volume-exhaustion marker placed on its cycle's line. */
interface MarkerSpec {
  key: string;
  kind: MarkerKind;
  cycle: CycleResult;
  color: string;
  marker: CycleMarker;
  /** The marked session's value of the plotted metric, as a coordinate. */
  y: number;
}

interface ChartModel {
  series: ChartSeries[];
  markers: MarkerSpec[];
  /**
   * Offset range of every drawn row, widened to reach the event (0) so its reference line is
   * always on the axis; shared by both charts; null when nothing is drawn.
   */
  xDomain: [number, number] | null;
  /** Ticks of that offset axis; empty when nothing is drawn. */
  xTicks: number[];
}

/** One session of the overlay chart; the numeric fields are coordinates only. */
interface OverlayRow {
  x: number;
  inflation_yoy_pct: number | null;
  tbill_91d_pct: number | null;
  tbill_364d_pct: number | null;
  point: CyclePoint;
}

/** The pixel centre Recharts passes to a `ReferenceDot` shape. */
interface MarkerShapeProps {
  cx?: number;
  cy?: number;
}

/** Where keyboard focus goes once a removed chip's focused Remove button has unmounted. */
type ChipFocusTarget = { kind: "remove"; date: string } | { kind: "date-input" };

const WINDOW_DAYS = 180;
const WINDOW_MONTHS = 12;
/** The endpoint accepts at most 8 `event_dates`. */
const MAX_EVENT_DATES = 8;
const ISO_DATE_PATTERN = /^(\d{4})-(\d{2})-(\d{2})$/;
/** A complete date input value. Past year 9999 the input reports a five- or six-digit year. */
const DATE_VALUE_PATTERN = /^(\d{4,})-\d{2}-\d{2}$/;
/**
 * The event dates the API accepts under each window preset. Its calendar runs from 0001-01-01 to
 * 9999-12-31, and it rejects an event whose window would cross either end
 * (`window_out_of_range`): ±180 days needs 180 days on each side, ±12 months a whole year.
 */
const EVENT_DATE_BOUNDS: Readonly<Record<WindowPreset, { min: string; max: string }>> = {
  "180d": { min: "0001-06-30", max: "9999-07-04" },
  "12m": { min: "0002-01-01", max: "9998-12-31" },
};
const MARKER_RADIUS = 5;

const WINDOW_OPTIONS: ReadonlyArray<{ value: WindowPreset; label: string; testId: string }> = [
  { value: "180d", label: "±180 days", testId: "cycle-window-180d" },
  { value: "12m", label: "±12 months", testId: "cycle-window-12m" },
];

const METRIC_OPTIONS: ReadonlyArray<{
  value: Metric;
  label: string;
  testId: string;
  axisLabel: string;
}> = [
  {
    value: "price_index",
    label: "Price index",
    testId: "cycle-metric-price_index",
    axisLabel: "Price index (anchor = 100)",
  },
  {
    value: "pe_index",
    label: "P/E index",
    testId: "cycle-metric-pe_index",
    axisLabel: "P/E index (anchor = 100)",
  },
  {
    value: "turnover_index",
    label: "Turnover index",
    testId: "cycle-metric-turnover_index",
    axisLabel: "Turnover index (anchor = 100)",
  },
  {
    value: "cumulative_foreign_net_kes",
    label: "Cumulative foreign flow",
    testId: "cycle-metric-cumulative_foreign_net_kes",
    axisLabel: "Cumulative foreign net flow (KES)",
  },
];

const MARKER_META: Record<MarkerKind, { label: string; testId: string; fill: string }> = {
  valuation_floor: {
    label: "Valuation floor",
    testId: "marker-valuation-floor",
    fill: CHART_COLORS.warning,
  },
  volume_exhaustion: {
    label: "Volume exhaustion",
    testId: "marker-volume-exhaustion",
    fill: CHART_COLORS.accent,
  },
};

const OVERLAY_SERIES: ReadonlyArray<{ field: OverlayField; label: string; color: string }> = [
  { field: "inflation_yoy_pct", label: "12-month inflation", color: CHART_COLORS.negative },
  { field: "tbill_91d_pct", label: "91-day T-bill", color: CHART_COLORS.accent },
  { field: "tbill_364d_pct", label: "364-day T-bill", color: CHART_COLORS.positive },
];

const STATUS_BADGES: Record<BacktestStatus, { text: string; className: string }> = {
  complete: {
    text: "complete",
    className:
      "inline-flex whitespace-nowrap rounded border border-emerald-800 bg-emerald-950 px-1.5 py-0.5 text-xs font-medium text-emerald-300",
  },
  partial: {
    text: "partial",
    className:
      "inline-flex whitespace-nowrap rounded border border-amber-800 bg-amber-950 px-1.5 py-0.5 text-xs font-medium text-amber-300",
  },
  awaiting_data: {
    text: "awaiting data",
    className:
      "inline-flex whitespace-nowrap rounded border border-sky-800 bg-sky-950 px-1.5 py-0.5 text-xs font-medium text-sky-300",
  },
  no_constituents: {
    text: "no constituents",
    className:
      "inline-flex whitespace-nowrap rounded border border-rose-800 bg-rose-950 px-1.5 py-0.5 text-xs font-medium text-rose-300",
  },
};
// Shown only if the server sends a status this build does not know.
const UNKNOWN_STATUS_CLASS =
  "inline-flex whitespace-nowrap rounded border border-slate-700 bg-slate-800 px-1.5 py-0.5 text-xs font-medium text-slate-300";

// Every class below is a complete static string, so Tailwind's source scan and a host's `@source`
// directive or `content` glob find each one. Margins and paddings of lists, paragraphs and
// figures are reset explicitly, so the layout does not depend on a host's CSS reset.
/* BLITZY [A11Y]: the toggle, chip and Add controls keep the card's compact text-control size
   (24-26px tall, at or above WCAG 2.2 AA's 24px target minimum, below the 44px guideline), as
   WidgetFrame's Retry button does. Flagged for designer review. */
const CONTROLS_CLASS = "flex flex-col gap-3 text-xs text-slate-300";
const TOOLBAR_CLASS = "flex flex-wrap items-center gap-x-6 gap-y-2";
const GROUP_CLASS = "flex flex-wrap items-center gap-1";
const GROUP_LABEL_CLASS = "me-1 text-slate-400";
const TOGGLE_CLASS =
  "m-0 cursor-pointer rounded border border-slate-700 bg-slate-900 px-2.5 py-1 text-xs font-medium text-slate-300 hover:bg-slate-800 hover:text-slate-100 active:bg-slate-700 active:text-slate-100 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400 motion-safe:transition-colors motion-safe:duration-150 motion-safe:ease-out";
const TOGGLE_ACTIVE_CLASS =
  "m-0 cursor-pointer rounded border border-sky-700 bg-sky-900 px-2.5 py-1 text-xs font-medium text-sky-100 hover:bg-sky-800 active:bg-sky-700 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400 motion-safe:transition-colors motion-safe:duration-150 motion-safe:ease-out";
const CHIP_LIST_CLASS = "m-0 flex list-none flex-wrap items-center gap-2 p-0";
const CHIP_CLASS =
  "inline-flex items-center gap-1 rounded-full border border-slate-700 bg-slate-800 py-0.5 ps-2.5 pe-0.5 text-xs text-slate-200";
const CHIP_DATE_CLASS = "text-slate-400";
const CHIP_REMOVE_CLASS =
  "m-0 inline-flex h-6 w-6 cursor-pointer items-center justify-center rounded-full border-0 bg-transparent p-0 text-slate-400 hover:bg-slate-700 hover:text-slate-100 active:bg-slate-600 active:text-slate-100 disabled:pointer-events-none disabled:opacity-40 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400 motion-safe:transition-colors motion-safe:duration-150 motion-safe:ease-out";
const ICON_CLASS = "h-3 w-3";
const MUTED_TEXT_CLASS = "m-0 text-xs text-slate-400";
const ADD_FORM_CLASS = "flex flex-col gap-1";
const ADD_ROW_CLASS = "flex flex-wrap items-center gap-2";
const INPUT_CLASS =
  "m-0 rounded border border-slate-700 bg-slate-950 px-2 py-1 text-xs text-slate-100 [color-scheme:dark] hover:border-slate-500 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400 motion-safe:transition-colors motion-safe:duration-150 motion-safe:ease-out";
const CHIP_ERROR_CLASS = "m-0 text-xs text-rose-400";
// While a new window or event set is fetched, the previous result stays mounted and is dimmed.
const BODY_CLASS = "flex flex-col gap-5";
const BODY_FETCHING_CLASS =
  "flex flex-col gap-5 opacity-60 motion-safe:transition-opacity motion-safe:duration-200 motion-safe:ease-out";
const SECTION_CLASS = "m-0 flex min-w-0 flex-col gap-2";
const SECTION_HEADER_CLASS = "flex flex-wrap items-baseline justify-between gap-2";
const SECTION_TITLE_CLASS = "m-0 text-xs font-semibold uppercase tracking-wide text-slate-400";
// Recharts 3 makes each chart's `<svg class="recharts-surface">` a tabindex 0 application, so it
// gets the same keyboard focus ring as the other controls. Its `<g tabindex="-1">` layers are
// never keyboard-reachable but a mouse click can focus them, so neither they nor a mouse-focused
// surface draws an outline.
const MAIN_CHART_CLASS =
  "h-80 w-full [&_.recharts-surface:focus-visible]:outline-2 [&_.recharts-surface:focus-visible]:outline-offset-2 [&_.recharts-surface:focus-visible]:outline-sky-400 [&_g[tabindex='-1']]:outline-none [&_*:focus:not(:focus-visible)]:outline-none";
const OVERLAY_CHART_CLASS =
  "h-48 w-full [&_.recharts-surface:focus-visible]:outline-2 [&_.recharts-surface:focus-visible]:outline-offset-2 [&_.recharts-surface:focus-visible]:outline-sky-400 [&_g[tabindex='-1']]:outline-none [&_*:focus:not(:focus-visible)]:outline-none";
const LEGEND_CLASS = "m-0 flex list-none flex-wrap gap-x-4 gap-y-1 p-0 text-xs text-slate-300";
const LEGEND_ITEM_CLASS = "inline-flex items-center gap-1.5";
const SWATCH_CLASS = "h-2 w-5 shrink-0";
const MARKER_SWATCH_CLASS = "h-3 w-3 shrink-0";
const SELECT_CLASS =
  "m-0 rounded border border-slate-700 bg-slate-950 px-2 py-1 text-xs text-slate-100 [color-scheme:dark] hover:border-slate-500 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400 motion-safe:transition-colors motion-safe:duration-150 motion-safe:ease-out";
const SELECT_LABEL_CLASS = "inline-flex items-center gap-2 text-xs text-slate-400";
// The wrapper scrolls horizontally on narrow cards, so it is focusable for keyboard scrolling.
const TABLE_WRAPPER_CLASS =
  "min-w-0 overflow-x-auto rounded focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400";
const TABLE_CLASS = "w-full border-collapse text-xs";
// Headers and the window cell may wrap between words, so the table fits narrower cards.
const TH_CLASS = "px-2 py-1.5 text-start align-bottom font-medium text-slate-400";
const TH_NUMERIC_CLASS = "px-2 py-1.5 text-end align-bottom font-medium text-slate-400";
const TH_ROW_CLASS = "whitespace-nowrap px-2 py-1.5 text-start align-top font-normal";
const TR_CLASS = "border-t border-slate-800";
const TD_CLASS = "whitespace-nowrap px-2 py-1.5 align-top text-slate-200";
const TD_WRAP_CLASS = "px-2 py-1.5 align-top text-slate-200";
const NOWRAP_CLASS = "whitespace-nowrap";
const TD_NUMERIC_CLASS = "whitespace-nowrap px-2 py-1.5 text-end align-top tabular-nums text-slate-200";
// Flag codes wrap between codes; a code breaks only when it alone is wider than the column.
const TD_FLAGS_CLASS = "min-w-[11rem] break-words px-2 py-1.5 align-top text-slate-500";
const CYCLE_LABEL_CLASS = "block font-medium text-slate-100";
const CYCLE_DATE_CLASS = "block text-slate-400";
const TOOLTIP_CLASS =
  "flex min-w-[12rem] flex-col gap-1 rounded border border-slate-700 bg-slate-900 px-2.5 py-2 text-xs text-slate-300 shadow-lg";
const TOOLTIP_TITLE_CLASS = "m-0 font-medium text-slate-100";
const TOOLTIP_LIST_CLASS = "m-0 flex list-none flex-col gap-1 p-0";
const TOOLTIP_ROW_CLASS = "flex items-center justify-between gap-3";
const TOOLTIP_NAME_CLASS = "inline-flex items-center gap-1.5";
const TOOLTIP_META_CLASS = "text-slate-400";
const TOOLTIP_VALUE_CLASS = "tabular-nums font-medium text-slate-100";

const CHART_MARGIN = { top: 12, right: 16, bottom: 4, left: 4 };
const MAIN_CHART_DESCRIPTION =
  "Solid lines are observed cycles and dashed lines are projections; circles mark valuation " +
  "floors and diamonds mark volume exhaustion.";
const AXIS_LABEL_STYLE = { fill: CHART_COLORS.axis, fontSize: 11 };

// ---------------------------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------------------------

function isIndexMetric(metric: Metric): metric is IndexMetric {
  return metric !== "cumulative_foreign_net_kes";
}

/** The palette colour of the cycle at `index`, by its position in the response. */
function seriesColor(index: number): string {
  return SERIES_PALETTE[index % SERIES_PALETTE.length] ?? CHART_COLORS.accent;
}

/** Whether `iso` is `YYYY-MM-DD` naming a real Gregorian calendar day, checked on its digits. */
function isCalendarDate(iso: string): boolean {
  const match = ISO_DATE_PATTERN.exec(iso);
  if (match === null) return false;
  const year = parseInt(match[1] ?? "", 10);
  const month = parseInt(match[2] ?? "", 10);
  const day = parseInt(match[3] ?? "", 10);
  if (month < 1 || month > 12 || day < 1) return false;
  const isLeapYear = (year % 4 === 0 && year % 100 !== 0) || year % 400 === 0;
  const daysInMonth = [31, isLeapYear ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  return day <= (daysInMonth[month - 1] ?? 0);
}

/**
 * Whether `value` is a complete date input value outside `bounds`. A year past 9999 has five or
 * more digits; four-digit ISO dates order correctly as strings.
 */
function isOutsideEventDateRange(value: string, bounds: { min: string; max: string }): boolean {
  const yearText = DATE_VALUE_PATTERN.exec(value)?.[1];
  if (yearText === undefined) return false;
  return yearText.length > 4 || value < bounds.min || value > bounds.max;
}

/**
 * The event dates `preset` accepts, worded for a validation message, such as
 * `between 30 Jun 0001 and 4 Jul 9999 for the ±180 days window`.
 */
function eventDateRangePhrase(preset: WindowPreset): string {
  const bounds = EVENT_DATE_BOUNDS[preset];
  const windowLabel = WINDOW_OPTIONS.find((option) => option.value === preset)?.label ?? preset;
  return (
    `between ${formatDate(bounds.min)} and ${formatDate(bounds.max)} ` +
    `for the ${windowLabel} window`
  );
}

/** An event-relative offset in days with an explicit sign: `+12`, `−5`, `0`. */
function formatOffset(offset: number): string {
  return offset > 0 ? "+" + formatInteger(offset) : formatInteger(offset);
}

/** `XAxis` tick of the offset axis. */
function formatOffsetTick(value: unknown): string {
  return typeof value === "number" ? formatOffset(value) : "";
}

const AXIS_ABBREVIATIONS: ReadonlyArray<{ divisor: number; suffix: string }> = [
  { divisor: 1e12, suffix: "T" },
  { divisor: 1e9, suffix: "B" },
  { divisor: 1e6, suffix: "M" },
  { divisor: 1e3, suffix: "K" },
];

/**
 * `YAxis` tick of the cumulative-flow axis, abbreviated (`−1.5B`, `350M`). A tick is a generated
 * axis coordinate with no server string behind it, so it is rounded to one decimal here and then
 * grouped and signed by `formatDecimal`; displayed flow values themselves come from the API.
 */
function formatKesAxisTick(value: unknown): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "";
  const magnitude = Math.abs(value);
  const unit = AXIS_ABBREVIATIONS.find((candidate) => magnitude >= candidate.divisor);
  const scaled = unit === undefined ? value : value / unit.divisor;
  const text = scaled.toFixed(unit === undefined ? 0 : 1).replace(/\.0$/, "");
  return formatDecimal(text, "kes", { unit: false }) + (unit?.suffix ?? "");
}

const OFFSET_TICK_STEPS = [5, 10, 15, 30, 60, 90, 180, 365] as const;
const LARGEST_OFFSET_TICK_STEP = 365;
const MAX_OFFSET_TICKS = 9;

/** The `[min, max]` of the rows' offsets, or null when there are no rows. */
function offsetRange(rows: Iterable<{ x: number }>): [number, number] | null {
  let min = Number.POSITIVE_INFINITY;
  let max = Number.NEGATIVE_INFINITY;
  for (const row of rows) {
    if (row.x < min) min = row.x;
    if (row.x > max) max = row.x;
  }
  return Number.isFinite(min) && Number.isFinite(max) ? [min, max] : null;
}

/**
 * Ticks of an offset axis over `range`: multiples of the smallest round step that gives at most
 * 9 ticks. Ticks are anchored at the event, so 0 is a tick whenever it is in range, even when a
 * window's first session falls a day or two after its start.
 */
function offsetTicks(range: [number, number] | null): number[] {
  if (range === null) return [];
  const [min, max] = range;
  const step =
    OFFSET_TICK_STEPS.find(
      (candidate) =>
        Math.floor(max / candidate) - Math.ceil(min / candidate) + 1 <= MAX_OFFSET_TICKS,
    ) ?? LARGEST_OFFSET_TICK_STEP;
  const ticks: number[] = [];
  for (let k = Math.ceil(min / step); k <= Math.floor(max / step); k += 1) {
    // `+ 0` turns -0 into 0.
    ticks.push(k * step + 0);
  }
  return ticks.length > 0 ? ticks : [min];
}

/** Fraction digits of a generated percentage or index tick: the server's precision for both. */
const PCT_INDEX_TICK_PLACES = 2;

/**
 * A generated `YAxis` coordinate as a plain decimal string for `formatDecimal`, or null when the
 * tick is not a finite number. It is rounded to {@link PCT_INDEX_TICK_PLACES} places with trailing
 * fraction zeros and a bare `.` removed, and never uses exponent notation: `15.5`, `16`, `0.3` for
 * `0.30000000000000004`, `0` for `1e-7`, `-2.5`.
 */
function axisTickDecimal(value: unknown): string | null {
  if (typeof value !== "number" || !Number.isFinite(value)) return null;
  // `toFixed` switches to exponent notation from 1e21; a float that large is integral, so BigInt
  // prints all of its digits instead.
  const fixed =
    Math.abs(value) < 1e21 ? value.toFixed(PCT_INDEX_TICK_PLACES) : BigInt(value).toString();
  return fixed.replace(/(\.\d*?)0+$/, "$1").replace(/\.$/, "");
}

/**
 * `YAxis` tick of the overlay chart, in percentage points (`15.5%`, `−2.5%`). A tick is a
 * generated axis coordinate with no server string behind it, so it is rounded to the server's
 * 2 dp here and then grouped and signed by `formatDecimal`; displayed rates themselves come from
 * the API.
 */
function formatPctAxisTick(value: unknown): string {
  const text = axisTickDecimal(value);
  return text === null ? "" : formatDecimal(text, "pct");
}

/**
 * `YAxis` tick of the main chart's base-100 index metrics (`97.5`, `1,050`). A tick is a
 * generated axis coordinate with no server string behind it, so it is rounded to the server's
 * 2 dp here and then grouped and signed by `formatDecimal`; displayed index levels themselves
 * come from the API.
 */
function formatIndexAxisTick(value: unknown): string {
  const text = axisTickDecimal(value);
  return text === null ? "" : formatDecimal(text, "index");
}

/**
 * The row at or before offset `x`: the latest row with `row.x <= x`, or undefined when `x` lies
 * outside the rows' range. Each line keeps its own sessions, so a hovered offset is paired with
 * that line's own last session rather than with an unrelated row at the same array index.
 */
function rowAtOrBefore<R extends { x: number }>(rows: readonly R[], x: number): R | undefined {
  let found: R | undefined;
  let maxX = Number.NEGATIVE_INFINITY;
  for (const row of rows) {
    if (row.x > maxX) maxX = row.x;
    if (row.x <= x && (found === undefined || row.x > found.x)) found = row;
  }
  return x > maxX ? undefined : found;
}

/** The hovered offset Recharts reports as the tooltip `label`, as a number. */
function tooltipOffset(label: string | number | undefined): number | null {
  if (typeof label === "number") return Number.isFinite(label) ? label : null;
  if (typeof label === "string" && /^[+-]?\d+$/.test(label)) return parseInt(label, 10);
  return null;
}

function projectionName(basis: readonly string[]): string {
  return basis.length > 0 ? "Projection (mean of " + basis.join(", ") + ")" : "Projection";
}

/** The `<title>` of a marker, for example "Valuation floor (pe_ratio 8.12) on 15 Mar 2013". */
function markerTitle(kind: MarkerKind, marker: CycleMarker): string {
  const valueKind = marker.metric === "pe_ratio" ? "ratio" : "index";
  const value = formatDecimal(marker.value, valueKind, { unit: false });
  const session = formatDate(marker.trade_date);
  return `${MARKER_META[kind].label} (${marker.metric} ${value}) on ${session}`;
}

/** The resolved window, as echoed by the server. */
function describeWindow(resolved: BacktestWindow): string {
  if (resolved.pre_days !== null && resolved.post_days !== null) {
    const pre = formatInteger(resolved.pre_days);
    return `T\u2212${pre} to T+${formatInteger(resolved.post_days)} calendar days`;
  }
  if (resolved.pre_months !== null && resolved.post_months !== null) {
    const pre = formatInteger(resolved.pre_months);
    return `T\u2212${pre} to T+${formatInteger(resolved.post_months)} calendar months`;
  }
  return "";
}

/**
 * Text of the polite status region: "Recalculating…" while a new window or event set is fetched
 * behind the dimmed previous result, then the number of cycles and the window now shown. It is
 * empty while loading, empty or failed, where the frame shows its own state, so it never repeats
 * a stale result.
 */
function describeUpdateStatus(
  state: WidgetState,
  data: BacktestResponse | undefined,
  isFetching: boolean,
): string {
  if (state !== "ready" || data === undefined) return "";
  if (isFetching) return "Recalculating\u2026";
  const count = data.cycles.length;
  const shown = `Showing ${formatInteger(count)} ${count === 1 ? "cycle" : "cycles"}`;
  const windowText = describeWindow(data.window);
  return windowText === "" ? `${shown}.` : `${shown}, ${windowText}.`;
}

/**
 * The lines and markers of the main chart for `metric`. Coordinates come from `toChartNumber`;
 * every row keeps its raw point, whose decimal strings the tooltip formats for display.
 */
function buildChartModel(cycles: readonly CycleResult[], metric: Metric): ChartModel {
  const series: ChartSeries[] = [];
  const markers: MarkerSpec[] = [];
  cycles.forEach((cycle, index) => {
    const key = `${index}-${cycle.event_date}`;
    const color = seriesColor(index);
    const observed = cycle.points.map(
      (point): ObservedRow => ({
        kind: "observed",
        x: point.offset_days,
        y: toChartNumber(point[metric]),
        point,
      }),
    );

    let projection: ProjectionRow[] | null = null;
    let name: string | null = null;
    // Projections carry only the three index levels, so none is drawn for cumulative flow.
    if (cycle.projection !== null && cycle.projection.points.length > 0 && isIndexMetric(metric)) {
      projection = cycle.projection.points.map(
        (point): ProjectionRow => ({
          kind: "projection",
          x: point.offset_days,
          y: toChartNumber(point[metric]),
          point,
        }),
      );
      name = projectionName(cycle.projection.basis);
    }
    series.push({ key, cycle, color, observed, projection, projectionName: name });

    if (cycle.markers === null) return;
    for (const kind of MARKER_KINDS) {
      const marker = cycle.markers[kind];
      if (marker === null) continue;
      const point = cycle.points.find((candidate) => candidate.offset_days === marker.offset_days);
      const y = point === undefined ? null : toChartNumber(point[metric]);
      if (y !== null) {
        markers.push({ key: `${key}-${kind}`, kind, cycle, color, marker, y });
      }
    }
  });
  const drawnRange = offsetRange(
    series.flatMap((item) => [...item.observed, ...(item.projection ?? [])]),
  );
  // Recharts discards a reference line outside the axis domain, so the domain always reaches 0:
  // a partial cycle observed only before the event, or a cycle observed only after it, still
  // shows the event line and a 0 tick.
  const xDomain: [number, number] | null =
    drawnRange === null ? null : [Math.min(drawnRange[0], 0), Math.max(drawnRange[1], 0)];
  return { series, markers, xDomain, xTicks: offsetTicks(xDomain) };
}

/** The display value of a main-chart row for `metric`, from its raw decimal string. */
function formatRowValue(row: ObservedRow | ProjectionRow, metric: Metric): string {
  let raw: DecimalString | null = null;
  if (row.kind === "observed") raw = row.point[metric];
  else if (isIndexMetric(metric)) raw = row.point[metric];
  return formatDecimal(raw, metric === "cumulative_foreign_net_kes" ? "kes" : "index");
}


// ---------------------------------------------------------------------------------------------
// Presentational pieces
// ---------------------------------------------------------------------------------------------

/** A decorative line swatch matching a chart stroke; dashed for projections. */
function LineSwatch({ color, dashed = false }: { color: string; dashed?: boolean }): ReactElement {
  return (
    <svg viewBox="0 0 20 8" aria-hidden="true" focusable="false" className={SWATCH_CLASS}>
      <line
        x1="0"
        y1="4"
        x2="20"
        y2="4"
        stroke={color}
        strokeWidth="2"
        strokeDasharray={dashed ? PROJECTION_DASH : undefined}
      />
    </svg>
  );
}

/**
 * A marker glyph centred on (`cx`, `cy`): an amber circle for the valuation floor and a sky
 * diamond for volume exhaustion, so the two kinds differ in shape as well as colour.
 */
function MarkerGlyph({
  kind,
  cx,
  cy,
  stroke,
}: {
  kind: MarkerKind;
  cx: number;
  cy: number;
  stroke: string;
}): ReactElement {
  const fill = MARKER_META[kind].fill;
  if (kind === "valuation_floor") {
    return <circle cx={cx} cy={cy} r={MARKER_RADIUS} fill={fill} stroke={stroke} strokeWidth={2} />;
  }
  const r = MARKER_RADIUS + 1;
  return (
    <polygon
      points={`${cx},${cy - r} ${cx + r},${cy} ${cx},${cy + r} ${cx - r},${cy}`}
      fill={fill}
      stroke={stroke}
      strokeWidth={2}
    />
  );
}

/** A decorative legend swatch of a marker kind. */
function MarkerSwatch({ kind }: { kind: MarkerKind }): ReactElement {
  return (
    <svg viewBox="0 0 16 16" aria-hidden="true" focusable="false" className={MARKER_SWATCH_CLASS}>
      <MarkerGlyph kind={kind} cx={8} cy={8} stroke={CHART_COLORS.text} />
    </svg>
  );
}

/**
 * The `ReferenceDot` shape of a marker: its glyph outlined in the cycle's colour, with a `<title>`
 * naming the kind, the selecting metric and value, and the session.
 */
function markerShape(spec: MarkerSpec): (props: MarkerShapeProps) => ReactElement<SVGElement> {
  const meta = MARKER_META[spec.kind];
  const title = markerTitle(spec.kind, spec.marker);
  return function drawMarker({ cx, cy }: MarkerShapeProps): ReactElement<SVGElement> {
    if (cx === undefined || cy === undefined || !Number.isFinite(cx) || !Number.isFinite(cy)) {
      return <g />;
    }
    return (
      <g data-testid={meta.testId} data-event-date={spec.cycle.event_date}>
        <title>{title}</title>
        <MarkerGlyph kind={spec.kind} cx={cx} cy={cy} stroke={spec.color} />
      </g>
    );
  };
}

interface CycleChartTooltipProps {
  active?: boolean;
  label?: string | number;
  series: readonly ChartSeries[];
  metric: Metric;
}

/**
 * Main-chart tooltip: for the hovered offset, each cycle's own session at or before it and each
 * drawn projection's value there. Rows are resolved from the series rather than from Recharts'
 * payload, which pairs lines that have their own `data` by array index when a line has no row at
 * exactly the hovered offset, and would show an unrelated session.
 */
function CycleChartTooltip({
  active,
  label,
  series,
  metric,
}: CycleChartTooltipProps): ReactElement | null {
  const x = tooltipOffset(label);
  if (active !== true || x === null) return null;

  const entries: Array<{
    key: string;
    name: string;
    color: string;
    row: ObservedRow | ProjectionRow;
  }> = [];
  for (const item of series) {
    const observed = rowAtOrBefore(item.observed, x);
    if (observed !== undefined) {
      entries.push({
        key: item.key + "-observed",
        name: item.cycle.label,
        color: item.color,
        row: observed,
      });
    }
    if (item.projection !== null) {
      const projected = rowAtOrBefore(item.projection, x);
      if (projected !== undefined) {
        entries.push({
          key: item.key + "-projection",
          name: "Projection \u00b7 " + item.cycle.label,
          color: item.color,
          row: projected,
        });
      }
    }
  }
  if (entries.length === 0) return null;

  return (
    <div className={TOOLTIP_CLASS}>
      <p className={TOOLTIP_TITLE_CLASS}>Day {formatOffset(x)} from event</p>
      <ul className={TOOLTIP_LIST_CLASS}>
        {entries.map((entry) => (
          <li key={entry.key} className={TOOLTIP_ROW_CLASS}>
            <span className={TOOLTIP_NAME_CLASS}>
              <LineSwatch color={entry.color} dashed={entry.row.kind === "projection"} />
              <span>{entry.name}</span>
              <span className={TOOLTIP_META_CLASS}>
                {entry.row.kind === "observed"
                  ? formatDate(entry.row.point.trade_date) + " \u00b7 "
                  : ""}
                {formatOffset(entry.row.x) + " d"}
              </span>
            </span>
            <span className={TOOLTIP_VALUE_CLASS}>{formatRowValue(entry.row, metric)}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}

interface OverlayTooltipProps {
  active?: boolean;
  label?: string | number;
  rows: readonly OverlayRow[];
}

/** Overlay-chart tooltip: the session at or before the hovered offset, in percentage points. */
function OverlayTooltip({ active, label, rows }: OverlayTooltipProps): ReactElement | null {
  const x = tooltipOffset(label);
  if (active !== true || x === null) return null;
  const row = rowAtOrBefore(rows, x);
  if (row === undefined) return null;
  return (
    <div className={TOOLTIP_CLASS}>
      <p className={TOOLTIP_TITLE_CLASS}>
        {formatDate(row.point.trade_date)}{" "}
        <span className={TOOLTIP_META_CLASS}>({formatOffset(row.x)} d)</span>
      </p>
      <ul className={TOOLTIP_LIST_CLASS}>
        {OVERLAY_SERIES.map((overlay) => (
          <li key={overlay.field} className={TOOLTIP_ROW_CLASS}>
            <span className={TOOLTIP_NAME_CLASS}>
              <LineSwatch color={overlay.color} />
              <span>{overlay.label}</span>
            </span>
            <span className={TOOLTIP_VALUE_CLASS}>
              {formatDecimal(row.point[overlay.field], "pct")}
            </span>
          </li>
        ))}
      </ul>
    </div>
  );
}

/** The cycle's status badge; an awaiting cycle also says when its window opens. */
function StatusBadge({ cycle }: { cycle: CycleResult }): ReactElement {
  const badge: { text: string; className: string } | undefined = STATUS_BADGES[cycle.status];
  const days = cycle.days_until_window_start;
  const opensIn =
    cycle.status === "awaiting_data" && days !== null
      ? ` \u00b7 opens in ${formatInteger(days)} ${days === 1 ? "day" : "days"}`
      : "";
  return (
    <span
      data-testid={"cycle-status-" + cycle.event_date}
      className={badge?.className ?? UNKNOWN_STATUS_CLASS}
    >
      {(badge?.text ?? String(cycle.status)) + opensIn}
    </span>
  );
}

/** A summary figure of a cycle in signed percentage points, or "—" when undefined. */
function formatSummary(
  cycle: CycleResult,
  field: keyof NonNullable<CycleResult["summary"]>,
): string {
  return formatDecimal(cycle.summary?.[field] ?? null, "pct", { signed: true });
}

// ---------------------------------------------------------------------------------------------
// Widget
// ---------------------------------------------------------------------------------------------

/**
 * Event and election cycle backtester over `GET /cycles/backtest`.
 *
 * Controls (rendered in every state, so an error such as `no_event_dates` can be recovered from
 * by editing the events): a "±180 days" or "±12 months" window preset, removable event chips with
 * a custom-date input (at most 8 events), and a metric toggle across price index, P/E index,
 * turnover index and cumulative foreign flow.
 *
 * Views: one solid line per cycle against calendar days from the event, a dashed "Projection
 * (mean of …)" line for each partial or awaiting cycle, a reference line at the event, and the
 * valuation-floor and volume-exhaustion markers; an inflation and 91/364-day T-bill overlay for a
 * selected cycle; and a summary table with status badges, returns, drawdown, P/E compression,
 * cohort sizes and flag codes.
 *
 * Every displayed number is the API's decimal string formatted by `formatDecimal`; only chart
 * coordinates pass through `toChartNumber`.
 */
export function CycleBacktester({
  apiBaseUrl,
  getHeaders,
  className,
  defaultEventDates,
  defaultWindow,
  tickers,
}: CycleBacktesterProps): ReactElement {
  // Props seed the state once; later prop changes never overwrite the user's choices.
  const [windowPreset, setWindowPreset] = useState<WindowPreset>(() =>
    defaultWindow === "12m" ? "12m" : "180d",
  );
  // null means "the server's default events"; an empty prop list means the same. A repeated date
  // is kept once, at its first position, so every chip is keyed by its date.
  const [eventDates, setEventDates] = useState<string[] | null>(() =>
    defaultEventDates !== undefined && defaultEventDates.length > 0
      ? Array.from(new Set(defaultEventDates))
      : null,
  );
  const [metric, setMetric] = useState<Metric>("price_index");
  const [overlayEventDate, setOverlayEventDate] = useState<string | null>(null);
  const [customDate, setCustomDate] = useState("");
  const [chipError, setChipError] = useState<string | null>(null);
  const removeButtonRefs = useRef(new Map<string, HTMLButtonElement>());
  const dateInputRef = useRef<HTMLInputElement>(null);
  const pendingChipFocus = useRef<ChipFocusTarget | null>(null);

  const chipsLabelId = useId();
  const dateInputId = useId();
  const chipErrorId = useId();
  const mainTitleId = useId();
  const overlayTitleId = useId();
  const overlaySelectId = useId();
  const summaryTitleId = useId();

  // Exactly one window form is ever sent: the server rejects days and months together with
  // `conflicting_window_units`.
  const query = useMemo(() => {
    const windowQuery =
      windowPreset === "12m"
        ? { pre_months: WINDOW_MONTHS, post_months: WINDOW_MONTHS }
        : { pre_days: WINDOW_DAYS, post_days: WINDOW_DAYS };
    return {
      event_dates: eventDates ?? undefined,
      ...windowQuery,
      tickers: tickers !== undefined && tickers.length > 0 ? tickers : undefined,
    };
  }, [eventDates, windowPreset, tickers]);

  const { state, data, error, isFetching, refetch } = useTerminalQuery<BacktestResponse>({
    path: "/cycles/backtest",
    query,
    apiBaseUrl,
    getHeaders,
    isEmpty: (response) => response.cycles.length === 0,
  });

  const cycles = data?.cycles;
  const chips = eventDates ?? cycles?.map((cycle) => cycle.event_date) ?? [];
  const chartModel = useMemo(() => buildChartModel(cycles ?? [], metric), [cycles, metric]);

  // The overlay shows the chosen cycle while it still has points, otherwise the first that does.
  const overlay = useMemo(() => {
    const candidates = (cycles ?? []).filter((cycle) => cycle.points.length > 0);
    const selected =
      candidates.find((cycle) => cycle.event_date === overlayEventDate) ?? candidates[0];
    const rows = (selected?.points ?? []).map(
      (point): OverlayRow => ({
        x: point.offset_days,
        inflation_yoy_pct: toChartNumber(point.inflation_yoy_pct),
        tbill_91d_pct: toChartNumber(point.tbill_91d_pct),
        tbill_364d_pct: toChartNumber(point.tbill_364d_pct),
        point,
      }),
    );
    return { candidates, selected, rows };
  }, [cycles, overlayEventDate]);
  const overlayCandidates = overlay.candidates;
  const overlayCycle = overlay.selected;
  const overlayRows = overlay.rows;

  // Runs once the removed chip and its focused Remove button have unmounted.
  useEffect(() => {
    const target = pendingChipFocus.current;
    if (target === null) return;
    pendingChipFocus.current = null;
    if (target.kind === "remove") removeButtonRefs.current.get(target.date)?.focus();
    else dateInputRef.current?.focus();
  }, [eventDates]);

  function chipLabel(date: string): string | null {
    return cycles?.find((cycle) => cycle.event_date === date)?.label ?? null;
  }

  function handleRemoveChip(event: MouseEvent<HTMLButtonElement>, date: string): void {
    const position = chips.indexOf(date);
    // The last chip stays, so an explicit empty list is never sent.
    if (chips.length <= 1 || position < 0) return;
    const remaining = chips.filter((chip) => chip !== date);
    const button = event.currentTarget;
    if (button.ownerDocument.activeElement === button) {
      // Focus would fall to the page with the removed button. It moves to the Remove button now
      // in the same position, else the previous one; a lone chip's Remove is disabled, so focus
      // then moves to the date input.
      const neighbour =
        remaining.length > 1 ? (remaining[position] ?? remaining[position - 1]) : undefined;
      pendingChipFocus.current =
        neighbour !== undefined ? { kind: "remove", date: neighbour } : { kind: "date-input" };
    }
    setEventDates(remaining);
    setChipError(null);
  }

  function handleCustomDateChange(event: ChangeEvent<HTMLInputElement>): void {
    setCustomDate(event.target.value);
    setChipError(null);
  }

  const eventDateBounds = EVENT_DATE_BOUNDS[windowPreset];

  function handleAddDate(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault();
    const candidate = customDate.trim();
    // The form is `noValidate`, so the input's min and max bounds are checked here.
    if (isOutsideEventDateRange(candidate, eventDateBounds)) {
      setChipError(`Enter a date ${eventDateRangePhrase(windowPreset)}.`);
      return;
    }
    if (!isCalendarDate(candidate)) {
      // An unfinished entry and a year the input rejects against `max` both leave the value empty
      // with `badInput` set; Enter can submit before the input normalizes the year on blur, so
      // the message names both requirements. An untouched input has no bad input.
      setChipError(
        candidate === "" && dateInputRef.current?.validity.badInput === true
          ? `Enter a complete date ${eventDateRangePhrase(windowPreset)}.`
          : "Enter a complete date.",
      );
      return;
    }
    if (chips.includes(candidate)) {
      setChipError(`${formatDate(candidate)} is already selected.`);
      return;
    }
    if (chips.length >= MAX_EVENT_DATES) {
      setChipError(`At most ${MAX_EVENT_DATES} event dates can be compared. Remove one first.`);
      return;
    }
    // ISO dates order correctly as strings.
    setEventDates([...chips, candidate].sort());
    setCustomDate("");
    setChipError(null);
  }

  const controls = (
    <div className={CONTROLS_CLASS}>
      <div className={TOOLBAR_CLASS}>
        <div role="group" aria-label="Window around each event" className={GROUP_CLASS}>
          <span aria-hidden="true" className={GROUP_LABEL_CLASS}>
            Window
          </span>
          {WINDOW_OPTIONS.map((option) => (
            <button
              key={option.value}
              type="button"
              data-testid={option.testId}
              aria-pressed={windowPreset === option.value}
              onClick={() => setWindowPreset(option.value)}
              className={windowPreset === option.value ? TOGGLE_ACTIVE_CLASS : TOGGLE_CLASS}
            >
              {option.label}
            </button>
          ))}
        </div>
        <div role="group" aria-label="Chart metric" className={GROUP_CLASS}>
          <span aria-hidden="true" className={GROUP_LABEL_CLASS}>
            Metric
          </span>
          {METRIC_OPTIONS.map((option) => (
            <button
              key={option.value}
              type="button"
              data-testid={option.testId}
              aria-pressed={metric === option.value}
              onClick={() => setMetric(option.value)}
              className={metric === option.value ? TOGGLE_ACTIVE_CLASS : TOGGLE_CLASS}
            >
              {option.label}
            </button>
          ))}
        </div>
      </div>

      <div className={GROUP_CLASS}>
        <span id={chipsLabelId} className={GROUP_LABEL_CLASS}>
          Events
        </span>
        {chips.length === 0 ? (
          <span className={MUTED_TEXT_CLASS}>Stored election cycles</span>
        ) : (
          <ul aria-labelledby={chipsLabelId} className={CHIP_LIST_CLASS}>
            {chips.map((date) => {
              const label = chipLabel(date);
              return (
                <li key={date}>
                  <span
                    data-testid="cycle-event-chip"
                    data-event-date={date}
                    className={CHIP_CLASS}
                  >
                    <span>{label ?? `Event ${date}`}</span>
                    {label !== null ? (
                      <span className={CHIP_DATE_CLASS}> {formatDate(date)}</span>
                    ) : null}
                    <button
                      type="button"
                      aria-label={`Remove ${date}`}
                      disabled={chips.length <= 1}
                      onClick={(event) => handleRemoveChip(event, date)}
                      ref={(element) => {
                        if (element === null) removeButtonRefs.current.delete(date);
                        else removeButtonRefs.current.set(date, element);
                      }}
                      className={CHIP_REMOVE_CLASS}
                    >
                      <svg
                        viewBox="0 0 12 12"
                        aria-hidden="true"
                        focusable="false"
                        fill="none"
                        stroke="currentColor"
                        strokeWidth="1.5"
                        strokeLinecap="round"
                        className={ICON_CLASS}
                      >
                        <path d="M3 3l6 6M9 3l-6 6" />
                      </svg>
                    </button>
                  </span>
                </li>
              );
            })}
          </ul>
        )}
      </div>

      <form onSubmit={handleAddDate} noValidate className={ADD_FORM_CLASS}>
        <div className={ADD_ROW_CLASS}>
          <label htmlFor={dateInputId} className={GROUP_LABEL_CLASS}>
            Add event date
          </label>
          <input
            id={dateInputId}
            ref={dateInputRef}
            type="date"
            min={eventDateBounds.min}
            max={eventDateBounds.max}
            value={customDate}
            onChange={handleCustomDateChange}
            aria-invalid={chipError !== null}
            aria-describedby={chipError !== null ? chipErrorId : undefined}
            data-testid="cycle-custom-date"
            className={INPUT_CLASS}
          />
          <button type="submit" data-testid="cycle-add-date" className={TOGGLE_CLASS}>
            Add
          </button>
          <span className={MUTED_TEXT_CLASS}>Up to {MAX_EVENT_DATES} events</span>
        </div>
        {chipError !== null ? (
          <p
            id={chipErrorId}
            role="alert"
            data-testid="cycle-chip-error"
            className={CHIP_ERROR_CLASS}
          >
            {chipError}
          </p>
        ) : null}
      </form>
      {/* Mounted in every frame state so screen readers register it before it first changes,
          and outside the aria-busy result body, which defers announcing its descendants. A
          metric change sends no request, so it leaves this text unchanged. */}
      <p
        role="status"
        aria-live="polite"
        aria-atomic="true"
        data-testid="cycle-update-status"
        className="sr-only"
      >
        {describeUpdateStatus(state, data, isFetching)}
      </p>
    </div>
  );

  const metricOption = METRIC_OPTIONS.find((option) => option.value === metric);
  const drawnProjections = chartModel.series.filter((item) => item.projection !== null);
  const hasPlot =
    chartModel.series.some((item) => item.observed.length > 0) || drawnProjections.length > 0;
  const hasAnyProjection = (cycles ?? []).some((cycle) => cycle.projection !== null);

  const body = (
    <div className={isFetching ? BODY_FETCHING_CLASS : BODY_CLASS} aria-busy={isFetching}>
      <section aria-labelledby={mainTitleId} className={SECTION_CLASS}>
        <div className={SECTION_HEADER_CLASS}>
          <h3 id={mainTitleId} className={SECTION_TITLE_CLASS}>
            {metricOption?.label ?? ""} by days from event
          </h3>
          {data !== undefined ? (
            <p className={MUTED_TEXT_CLASS}>{describeWindow(data.window)}</p>
          ) : null}
        </div>
        {hasPlot ? (
          <div data-testid="cycle-main-chart" className={MAIN_CHART_CLASS}>
            <ResponsiveContainer width="100%" height="100%">
              <LineChart
                margin={CHART_MARGIN}
                title={(metricOption?.label ?? "") + " of each cycle by days from the event"}
                desc={MAIN_CHART_DESCRIPTION}
              >
                <CartesianGrid stroke={CHART_COLORS.grid} strokeDasharray="3 3" />
                <XAxis
                  type="number"
                  dataKey="x"
                  domain={chartModel.xDomain ?? ["dataMin", "dataMax"]}
                  allowDecimals={false}
                  ticks={chartModel.xTicks}
                  tick={AXIS_TICK}
                  stroke={CHART_COLORS.axis}
                  tickFormatter={formatOffsetTick}
                  height={40}
                  label={{
                    value: "Days from event",
                    position: "insideBottom",
                    ...AXIS_LABEL_STYLE,
                  }}
                />
                <YAxis
                  type="number"
                  domain={["auto", "auto"]}
                  tick={AXIS_TICK}
                  stroke={CHART_COLORS.axis}
                  width={76}
                  tickFormatter={
                    metric === "cumulative_foreign_net_kes"
                      ? formatKesAxisTick
                      : formatIndexAxisTick
                  }
                  label={{
                    value: metricOption?.axisLabel ?? "",
                    angle: -90,
                    position: "insideLeft",
                    textAnchor: "middle",
                    ...AXIS_LABEL_STYLE,
                  }}
                />
                <Tooltip
                  isAnimationActive={false}
                  cursor={{ stroke: CHART_COLORS.muted }}
                  content={(props) => (
                    <CycleChartTooltip
                      active={props.active}
                      label={props.label}
                      series={chartModel.series}
                      metric={metric}
                    />
                  )}
                />
                {/* The label sits left of the line, unless 0 is the domain's left edge (every
                    drawn session after the event), where it goes right of it, off the Y axis. */}
                <ReferenceLine
                  x={0}
                  stroke={CHART_COLORS.axis}
                  label={{
                    value: "Event",
                    position: chartModel.xDomain?.[0] === 0 ? "insideTopLeft" : "insideTopRight",
                    ...AXIS_LABEL_STYLE,
                  }}
                />
                {/* activeDot is off: with per-line data Recharts can place it on another line's
                    index, while the tooltip resolves each line's own session. */}
                {chartModel.series.map((item) =>
                  item.observed.length > 0 ? (
                    <Line
                      key={item.key + "-observed"}
                      data={item.observed}
                      dataKey="y"
                      name={item.cycle.label}
                      type="linear"
                      stroke={item.color}
                      strokeWidth={2}
                      dot={false}
                      activeDot={false}
                      connectNulls
                      isAnimationActive={false}
                    />
                  ) : null,
                )}
                {drawnProjections.map((item) => (
                  <Line
                    key={item.key + "-projection"}
                    data={item.projection ?? []}
                    dataKey="y"
                    name={item.projectionName ?? "Projection"}
                    type="linear"
                    stroke={item.color}
                    strokeWidth={1.5}
                    strokeDasharray={PROJECTION_DASH}
                    dot={false}
                    activeDot={false}
                    connectNulls
                    isAnimationActive={false}
                  />
                ))}
                {/* Recharts 3 has no `isFront`: a ReferenceDot's default z-index (600) already
                    draws it above every Line (400). */}
                {chartModel.markers.map((spec) => (
                  <ReferenceDot
                    key={spec.key}
                    x={spec.marker.offset_days}
                    y={spec.y}
                    r={MARKER_RADIUS}
                    ifOverflow="extendDomain"
                    shape={markerShape(spec)}
                  />
                ))}
              </LineChart>
            </ResponsiveContainer>
          </div>
        ) : (
          <p className={MUTED_TEXT_CLASS}>No observed or projected sessions in this window yet.</p>
        )}
        <ul aria-label="Cycles" className={LEGEND_CLASS}>
          {chartModel.series.map((item) => (
            <li
              key={item.key}
              data-testid="cycle-legend-item"
              data-event-date={item.cycle.event_date}
              className={LEGEND_ITEM_CLASS}
            >
              <LineSwatch color={item.color} />
              <span>{item.cycle.label}</span>
              {item.observed.length === 0 ? (
                <span className={TOOLTIP_META_CLASS}> (no observed sessions)</span>
              ) : null}
            </li>
          ))}
          {drawnProjections.map((item) => (
            <li
              key={item.key + "-projection"}
              data-testid="cycle-projection-legend"
              data-event-date={item.cycle.event_date}
              className={LEGEND_ITEM_CLASS}
            >
              <LineSwatch color={item.color} dashed />
              <span>
                {item.projectionName}{" "}
                <span className={TOOLTIP_META_CLASS}>{"\u00b7 " + item.cycle.label}</span>
              </span>
            </li>
          ))}
        </ul>
        {chartModel.markers.length > 0 ? (
          <ul aria-label="Markers" className={LEGEND_CLASS}>
            <li className={LEGEND_ITEM_CLASS}>
              <MarkerSwatch kind="valuation_floor" />
              <span>
                {MARKER_META.valuation_floor.label}{" "}
                <span className={TOOLTIP_META_CLASS}>(lowest P/E, else lowest price index)</span>
              </span>
            </li>
            <li className={LEGEND_ITEM_CLASS}>
              <MarkerSwatch kind="volume_exhaustion" />
              <span>
                {MARKER_META.volume_exhaustion.label}{" "}
                <span className={TOOLTIP_META_CLASS}>(lowest turnover index)</span>
              </span>
            </li>
          </ul>
        ) : null}
        {hasAnyProjection ? (
          <p className={MUTED_TEXT_CLASS}>
            Projections are descriptive averages of completed cycles, not forecasts
            {isIndexMetric(metric) ? "." : "; none is drawn for cumulative foreign flow."}
          </p>
        ) : null}
      </section>

      <section aria-labelledby={overlayTitleId} className={SECTION_CLASS}>
        <div className={SECTION_HEADER_CLASS}>
          <h3 id={overlayTitleId} className={SECTION_TITLE_CLASS}>
            Inflation and T-bill overlay
          </h3>
          {overlayCycle !== undefined ? (
            <span className={SELECT_LABEL_CLASS}>
              <label htmlFor={overlaySelectId}>Overlay cycle</label>
              <select
                id={overlaySelectId}
                data-testid="cycle-overlay-select"
                value={overlayCycle.event_date}
                onChange={(event) => setOverlayEventDate(event.target.value)}
                className={SELECT_CLASS}
              >
                {overlayCandidates.map((cycle, index) => (
                  <option key={`${index}-${cycle.event_date}`} value={cycle.event_date}>
                    {cycle.label}
                  </option>
                ))}
              </select>
            </span>
          ) : null}
        </div>
        {overlayCycle !== undefined ? (
          <>
            <div data-testid="cycle-overlay-chart" className={OVERLAY_CHART_CLASS}>
              <ResponsiveContainer width="100%" height="100%">
                <LineChart
                  data={overlayRows}
                  margin={CHART_MARGIN}
                  title={
                    "12-month inflation and 91- and 364-day T-bill yields around " +
                    overlayCycle.label
                  }
                  desc="Percentage points as of each session, on the main chart's day axis."
                >
                  <CartesianGrid stroke={CHART_COLORS.grid} strokeDasharray="3 3" />
                  {/* The overlay shares the main chart's offset domain and ticks (and its Y
                      axis width), so both charts place a given day at the same position. */}
                  <XAxis
                    type="number"
                    dataKey="x"
                    domain={chartModel.xDomain ?? ["dataMin", "dataMax"]}
                    allowDecimals={false}
                    ticks={chartModel.xTicks}
                    tick={AXIS_TICK}
                    stroke={CHART_COLORS.axis}
                    tickFormatter={formatOffsetTick}
                  />
                  <YAxis
                    type="number"
                    domain={["auto", "auto"]}
                    tick={AXIS_TICK}
                    stroke={CHART_COLORS.axis}
                    width={76}
                    tickFormatter={formatPctAxisTick}
                  />
                  <Tooltip
                    isAnimationActive={false}
                    cursor={{ stroke: CHART_COLORS.muted }}
                    content={(props) => (
                      <OverlayTooltip
                        active={props.active}
                        label={props.label}
                        rows={overlayRows}
                      />
                    )}
                  />
                  <ReferenceLine x={0} stroke={CHART_COLORS.axis} />
                  {OVERLAY_SERIES.map((overlay) => (
                    <Line
                      key={overlay.field}
                      dataKey={overlay.field}
                      name={overlay.label}
                      type="linear"
                      stroke={overlay.color}
                      strokeWidth={1.5}
                      dot={false}
                      connectNulls
                      isAnimationActive={false}
                    />
                  ))}
                </LineChart>
              </ResponsiveContainer>
            </div>
            <ul aria-label="Overlay series" className={LEGEND_CLASS}>
              {OVERLAY_SERIES.map((overlay) => (
                <li key={overlay.field} className={LEGEND_ITEM_CLASS}>
                  <LineSwatch color={overlay.color} />
                  <span>{overlay.label}</span>
                </li>
              ))}
            </ul>
          </>
        ) : (
          <p className={MUTED_TEXT_CLASS}>No cycle has observed sessions in this window yet.</p>
        )}
      </section>

      <section aria-labelledby={summaryTitleId} className={SECTION_CLASS}>
        <h3 id={summaryTitleId} className={SECTION_TITLE_CLASS}>
          Cycle summary
        </h3>
        <div
          role="region"
          aria-label="Cycle summary table"
          tabIndex={0}
          className={TABLE_WRAPPER_CLASS}
        >
          <table data-testid="cycle-summary-table" className={TABLE_CLASS}>
            <caption className="sr-only">
              Returns, drawdown, P/E compression and cohort of each cycle
            </caption>
            <thead>
              <tr>
                <th scope="col" className={TH_CLASS}>
                  Cycle
                </th>
                <th scope="col" className={TH_CLASS}>
                  Window
                </th>
                <th scope="col" className={TH_CLASS}>
                  Status
                </th>
                <th scope="col" className={TH_CLASS}>
                  Anchor
                </th>
                <th scope="col" className={TH_NUMERIC_CLASS}>
                  Pre-event return
                </th>
                <th scope="col" className={TH_NUMERIC_CLASS}>
                  Post-event return
                </th>
                <th scope="col" className={TH_NUMERIC_CLASS}>
                  Max drawdown
                </th>
                <th scope="col" className={TH_NUMERIC_CLASS}>
                  P/E compression
                </th>
                <th scope="col" className={TH_NUMERIC_CLASS}>
                  Constituents
                </th>
                <th scope="col" className={TH_NUMERIC_CLASS}>
                  Excluded
                </th>
                <th scope="col" className={TH_CLASS}>
                  Flags
                </th>
              </tr>
            </thead>
            <tbody>
              {(cycles ?? []).map((cycle, index) => (
                <tr
                  key={`${index}-${cycle.event_date}`}
                  data-testid="cycle-summary-row"
                  data-event-date={cycle.event_date}
                  className={TR_CLASS}
                >
                  <th scope="row" className={TH_ROW_CLASS}>
                    <span className={CYCLE_LABEL_CLASS}>{cycle.label}</span>{" "}
                    <span className={CYCLE_DATE_CLASS}>{formatDate(cycle.event_date)}</span>
                  </th>
                  <td className={TD_WRAP_CLASS}>
                    <span className={NOWRAP_CLASS}>
                      {formatDate(cycle.window_start) + " \u2013"}
                    </span>{" "}
                    <span className={NOWRAP_CLASS}>{formatDate(cycle.window_end)}</span>
                  </td>
                  <td className={TD_CLASS}>
                    <StatusBadge cycle={cycle} />
                  </td>
                  <td className={TD_CLASS}>{formatDate(cycle.anchor_date)}</td>
                  <td className={TD_NUMERIC_CLASS}>
                    {formatSummary(cycle, "pre_event_return_pct")}
                  </td>
                  <td className={TD_NUMERIC_CLASS}>
                    {formatSummary(cycle, "post_event_return_pct")}
                  </td>
                  <td className={TD_NUMERIC_CLASS}>
                    {formatSummary(cycle, "max_drawdown_pct")}
                  </td>
                  <td className={TD_NUMERIC_CLASS}>
                    {formatSummary(cycle, "pe_compression_pct")}
                  </td>
                  <td className={TD_NUMERIC_CLASS}>
                    <span title={cycle.constituents.join(", ") || undefined}>
                      {formatInteger(cycle.constituents.length)}
                    </span>
                  </td>
                  <td className={TD_NUMERIC_CLASS}>
                    <span
                      data-testid="cycle-excluded-count"
                      title={cycle.excluded_constituents.join(", ") || undefined}
                    >
                      {formatInteger(cycle.excluded_constituents.length)}
                    </span>
                    {cycle.excluded_constituents.length > 0 ? (
                      <span className="sr-only">
                        {": " + cycle.excluded_constituents.join(", ")}
                      </span>
                    ) : null}
                  </td>
                  <td className={TD_FLAGS_CLASS}>
                    {cycle.flags.length > 0 ? cycle.flags.join(", ") : EMPTY_VALUE}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <p data-testid="cycle-data-through" className={MUTED_TEXT_CLASS}>
          Data available through {formatDate(data?.data_available_through)}
        </p>
      </section>
    </div>
  );

  return (
    <WidgetFrame
      testId="terminal-cycle-backtester"
      title="Election & cycle backtester"
      state={state}
      error={error}
      onRetry={refetch}
      className={className}
      skeletonClassName="h-96"
      controls={controls}
    >
      {body}
    </WidgetFrame>
  );
}

