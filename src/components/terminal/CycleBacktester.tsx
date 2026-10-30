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
import { SECTION_HEADING_CLASS } from "./shared/headingAndTileClasses";
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
   * 2017-08-08, 2022-08-09 and 2027-08-10). The user can then remove and add dates. The chips,
   * like the charts and the table, list the dates in ascending order, whatever the order given.
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
// `relative` makes it the containing block of the absolutely positioned `sr-only` text inside the
// table (the caption and each cohort count's name); without it that text escapes the overflow
// clip and widens the host page beyond a narrow viewport.
const TABLE_WRAPPER_CLASS =
  "relative min-w-0 overflow-x-auto rounded focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400";
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
// A cohort count with tickers is a disclosure that lists them in place, so they are reachable by
// keyboard and touch, not only from its hover tooltip. Closed, it is an end-aligned count with a
// dotted underline and a chevron; open, the list wraps beneath it within a bounded width.
const TICKER_DISCLOSURE_CLASS = "group/tickers m-0 p-0";
const TICKER_SUMMARY_CLASS =
  "m-0 inline-flex cursor-pointer list-none items-center gap-0.5 rounded p-0 text-slate-200 underline decoration-slate-500 decoration-dotted underline-offset-2 hover:text-slate-100 hover:decoration-slate-300 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400 [&::-webkit-details-marker]:hidden motion-safe:transition-colors motion-safe:duration-150 motion-safe:ease-out";
const TICKER_ICON_CLASS =
  "h-3 w-3 shrink-0 text-slate-400 group-open/tickers:rotate-180 motion-safe:transition-transform motion-safe:duration-150 motion-safe:ease-out";
const TICKER_LIST_CLASS =
  "mb-0 me-0 ms-auto mt-1 w-max max-w-64 whitespace-normal break-words p-0 text-start text-slate-400";
const TOOLTIP_CLASS =
  "flex min-w-[12rem] flex-col gap-1 rounded border border-slate-700 bg-slate-900 px-2.5 py-2 text-xs text-slate-300 shadow-lg";
const TOOLTIP_TITLE_CLASS = "m-0 font-medium text-slate-100";
const TOOLTIP_LIST_CLASS = "m-0 flex list-none flex-col gap-1 p-0";
const TOOLTIP_ROW_CLASS = "flex items-center justify-between gap-3";
const TOOLTIP_NAME_CLASS = "inline-flex items-center gap-1.5";
const TOOLTIP_META_CLASS = "text-slate-400";
const TOOLTIP_VALUE_CLASS = "tabular-nums font-medium text-slate-100";

const CHART_MARGIN = { top: 12, right: 16, bottom: 4, left: 4 };
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

/**
 * Why switching to `preset` was refused: the selected event `dates` fall outside its accepted
 * range, so its request would fail with `window_out_of_range`. For example `4 Jul 9999 is outside
 * the ±12 months window, which accepts event dates between 1 Jan 0002 and 31 Dec 9998. Remove it
 * to use that window.`
 */
function describeWindowBlock(preset: WindowPreset, dates: readonly string[]): string {
  const bounds = EVENT_DATE_BOUNDS[preset];
  const windowLabel = WINDOW_OPTIONS.find((option) => option.value === preset)?.label ?? preset;
  const list = dates.reduce(
    (text, date, index) =>
      index === 0
        ? formatDate(date)
        : text + (index === dates.length - 1 ? " and " : ", ") + formatDate(date),
    "",
  );
  const several = dates.length > 1;
  return (
    `${list} ${several ? "are" : "is"} outside the ${windowLabel} window, which accepts event ` +
    `dates between ${formatDate(bounds.min)} and ${formatDate(bounds.max)}. ` +
    `Remove ${several ? "them" : "it"} to use that window.`
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

/** The label the server gives an event date with no stored cycle: `Event YYYY-MM-DD`. */
const GENERIC_EVENT_LABEL_PATTERN = /^Event (\d{4}-\d{2}-\d{2})$/;

/** The label the server gives `date` when no stored cycle names it. */
function genericEventLabel(date: string): string {
  return `Event ${date}`;
}

/** The ISO date a label of the server's generic `Event YYYY-MM-DD` form names, else null. */
function genericLabelDate(label: string): string | null {
  return GENERIC_EVENT_LABEL_PATTERN.exec(label)?.[1] ?? null;
}

/**
 * A cycle label as displayed. The server's generic `Event 2024-02-29` names its date in the short
 * format every other date uses, `Event 29 Feb 2024`; a stored label such as `2013 Cycle` is
 * returned unchanged.
 */
function displayLabel(label: string): string {
  const date = genericLabelDate(label);
  return date === null ? label : `Event ${formatDate(date)}`;
}

/** Whether `label` already names `date`, so the date is not shown a second time beside it. */
function labelNamesDate(label: string, date: string): boolean {
  return genericLabelDate(label) === date;
}

/**
 * `known` with the label of each of `cycles` recorded under its event date; `known` itself when
 * that adds or changes nothing.
 */
function withCycleLabels(
  known: Readonly<Record<string, string>>,
  cycles: readonly CycleResult[] | undefined,
): Readonly<Record<string, string>> {
  let merged: Record<string, string> | null = null;
  for (const cycle of cycles ?? []) {
    if (known[cycle.event_date] === cycle.label) continue;
    merged ??= { ...known };
    merged[cycle.event_date] = cycle.label;
  }
  return merged ?? known;
}

function projectionName(basis: readonly string[]): string {
  return basis.length > 0
    ? "Projection (mean of " + basis.map(displayLabel).join(", ") + ")"
    : "Projection";
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

/**
 * Whether a main-chart line draws a visible segment: at least two of its rows have a coordinate.
 * Every main-chart `Line` has `dot` and `activeDot` off and `connectNulls` on, so Recharts joins
 * the rows that have a coordinate and draws no dot, and a lone coordinate draws nothing.
 */
function drawsSegment(rows: readonly (ObservedRow | ProjectionRow)[] | null): boolean {
  return rows !== null && rows.filter((row) => row.y !== null).length >= 2;
}

/**
 * The `<desc>` of the main chart, naming only what `model` draws: solid observed lines, dashed
 * projection lines, circles for valuation floors and diamonds for volume exhaustion, in the
 * singular where exactly one of a kind is drawn. A line with fewer than two coordinates for the
 * plotted metric is not counted, since it draws nothing; markers are counted on their own, since
 * a `ReferenceDot` draws whether or not its cycle's line does.
 */
function describeMainChart(model: ChartModel): string {
  const observed = model.series.filter((item) => drawsSegment(item.observed)).length;
  const projected = model.series.filter((item) => drawsSegment(item.projection)).length;
  const floors = model.markers.filter((spec) => spec.kind === "valuation_floor").length;
  const exhaustions = model.markers.filter((spec) => spec.kind === "volume_exhaustion").length;

  const lines: string[] = [];
  if (observed === 1) lines.push("the solid line is the observed cycle");
  else if (observed > 1) lines.push("solid lines are observed cycles");
  if (projected === 1) lines.push("the dashed line is a projection");
  else if (projected > 1) lines.push("dashed lines are projections");

  const marks: string[] = [];
  if (floors === 1) marks.push("the circle marks the valuation floor");
  else if (floors > 1) marks.push("circles mark valuation floors");
  if (exhaustions === 1) marks.push("the diamond marks volume exhaustion");
  else if (exhaustions > 1) marks.push("diamonds mark volume exhaustion");

  const clauses = [lines, marks]
    .filter((group) => group.length > 0)
    .map((group) => group.join(" and "));
  if (clauses.length === 0) {
    return "No line or marker is drawn for this metric in this window.";
  }
  const sentence = clauses.join("; ");
  return sentence.charAt(0).toUpperCase() + sentence.slice(1) + ".";
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
        name: displayLabel(item.cycle.label),
        color: item.color,
        row: observed,
      });
    }
    if (item.projection !== null) {
      const projected = rowAtOrBefore(item.projection, x);
      if (projected !== undefined) {
        entries.push({
          key: item.key + "-projection",
          name: "Projection \u00b7 " + displayLabel(item.cycle.label),
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

/**
 * Why a cycle has no observed session to draw, so each such cycle carries an inline explanation:
 * - `future`: its window opens after the latest data (`days_until_window_start` is set);
 * - `outside_coverage`: its window ends before `data_available_through`, so data exists after
 *   it, yet none lies inside it. The response gives only the latest date of the data, never its
 *   first, so a window before the data's first session cannot be told from one in a gap between
 *   sessions, and neither is claimed;
 * - `empty_cohort`: its window has sessions, but no candidate qualified for the cohort;
 * - `no_data`: no candidate has any price yet;
 * - `unexplained`: the response states no reason.
 */
type NoSessionCause =
  | "future"
  | "outside_coverage"
  | "empty_cohort"
  | "no_data"
  | "unexplained";

/** The order in which {@link noSessionsMessage} names causes, those that end in "yet" last. */
const NO_SESSION_CAUSE_ORDER: readonly NoSessionCause[] = [
  "outside_coverage",
  "empty_cohort",
  "unexplained",
  "future",
  "no_data",
];

/** A base close must fall on the anchor or within this many market sessions before it. */
const BASE_CLOSE_MAX_SESSIONS = 10;

/**
 * Why `cycle` has no observed session, or null when it has some. It reads only the cycle's own
 * status, `days_until_window_start` and `window_end`, and the response's `data_available_through`,
 * so a cycle's cause never depends on which other cycles are selected. An `awaiting_data` cycle
 * without `days_until_window_start` whose window ends before `data_available_through` (ISO dates
 * order correctly as strings) is `outside_coverage`: data exists after the window and none inside
 * it. The response carries no coverage start, so a window before the data's first session and one
 * in a gap between sessions are not told apart.
 */
function noSessionCause(
  cycle: CycleResult,
  response: BacktestResponse | undefined,
): NoSessionCause | null {
  if (cycle.points.length > 0) return null;
  if (cycle.status === "no_constituents") return "empty_cohort";
  if (cycle.status !== "awaiting_data") return "unexplained";
  if (cycle.days_until_window_start !== null) return "future";
  const dataThrough = response?.data_available_through ?? null;
  if (dataThrough === null) return "no_data";
  return cycle.window_end < dataThrough ? "outside_coverage" : "unexplained";
}

/**
 * The legend note of a cycle without observed sessions: "no observed sessions" while its data is
 * still to come, otherwise why it has none. An empty cohort names the base-close rule and, with
 * `currency_mismatch`, the excluded non-KES listings.
 */
function noSessionNote(cycle: CycleResult, cause: NoSessionCause | null): string {
  if (cause === "outside_coverage") return "outside data coverage";
  if (cause !== "empty_cohort") return "no observed sessions";
  const baseClose = `closed on or within ${BASE_CLOSE_MAX_SESSIONS} sessions before the anchor`;
  return cycle.flags.includes("currency_mismatch")
    ? `no constituents: non-KES listings are excluded and no other candidate ${baseClose}`
    : `no constituents: no candidate ${baseClose}`;
}

/**
 * One clause of {@link noSessionsMessage} for the `count` of `total` cycles sharing `cause`, such
 * as "every window is outside data coverage" or "1 cycle has no qualifying constituents".
 */
function describeNoSessionCause(cause: NoSessionCause, count: number, total: number): string {
  if (cause === "no_data") return "no prices are loaded yet";
  const noun = cause === "empty_cohort" || cause === "unexplained" ? "cycle" : "window";
  const all = count === total;
  const plural = !all && count !== 1;
  let subject = `${formatInteger(count)} ${plural ? noun + "s" : noun}`;
  if (all) subject = total === 1 ? `the ${noun}` : `every ${noun}`;
  const has = plural ? "have" : "has";
  const is = plural ? "are" : "is";
  if (cause === "outside_coverage") return `${subject} ${is} outside data coverage`;
  if (cause === "empty_cohort") return `${subject} ${has} no qualifying constituents`;
  if (cause === "future") return `${subject} ${has} no data yet`;
  return `${subject} ${has} no observed sessions`;
}

/**
 * The sentence shown in place of a chart with nothing to draw: `lead`, then why the cycles have no
 * observed session, for example "No cycle has observed sessions: 1 window is outside data coverage
 * and 1 window has no data yet." It says "yet" only for data still to come.
 */
function noSessionsMessage(lead: string, response: BacktestResponse | undefined): string {
  const cycles = response?.cycles ?? [];
  const counts = new Map<NoSessionCause, number>();
  for (const cycle of cycles) {
    const cause = noSessionCause(cycle, response);
    if (cause !== null) counts.set(cause, (counts.get(cause) ?? 0) + 1);
  }
  const clauses: string[] = [];
  for (const cause of NO_SESSION_CAUSE_ORDER) {
    const count = counts.get(cause);
    if (count !== undefined) clauses.push(describeNoSessionCause(cause, count, cycles.length));
  }
  if (clauses.length === 0) return `${lead}.`;
  const head = clauses.slice(0, -1);
  const last = clauses.slice(-1).join("");
  return `${lead}: ${head.length > 0 ? `${head.join(", ")} and ${last}` : last}.`;
}

/**
 * The cycle's status badge. An awaiting cycle also says when its window opens, or, by its
 * {@link NoSessionCause}, that its window is outside the data's coverage.
 */
function StatusBadge({
  cycle,
  cause,
}: {
  cycle: CycleResult;
  cause: NoSessionCause | null;
}): ReactElement {
  const badge: { text: string; className: string } | undefined = STATUS_BADGES[cycle.status];
  const days = cycle.days_until_window_start;
  let detail = "";
  if (cycle.status === "awaiting_data") {
    if (days !== null) {
      detail = ` \u00b7 opens in ${formatInteger(days)} ${days === 1 ? "day" : "days"}`;
    } else if (cause === "outside_coverage") {
      detail = " \u00b7 outside data coverage";
    }
  }
  return (
    <span
      data-testid={"cycle-status-" + cycle.event_date}
      className={badge?.className ?? UNKNOWN_STATUS_CLASS}
    >
      {(badge?.text ?? String(cycle.status)) + detail}
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

/**
 * The size of a cycle's cohort list. With tickers it is a native disclosure: the count, which
 * also carries the list as its tooltip, opens the comma-separated tickers in place. Its
 * accessible name gives the count, what is counted and the action, such as "3 constituents, show
 * tickers". An empty list is the bare count.
 */
function TickerCount({
  tickers,
  noun,
  testId,
}: {
  tickers: readonly string[];
  /** What is counted, singular and plural, completing the count's accessible name. */
  noun: { one: string; other: string };
  /** `data-testid` of the element whose text is the count. */
  testId?: string;
}): ReactElement {
  const count = formatInteger(tickers.length);
  if (tickers.length === 0) return <span data-testid={testId}>{count}</span>;
  const list = tickers.join(", ");
  return (
    <details className={TICKER_DISCLOSURE_CLASS}>
      <summary title={list} className={TICKER_SUMMARY_CLASS}>
        <span data-testid={testId}>{count}</span>
        <span className="sr-only">
          {` ${tickers.length === 1 ? noun.one : noun.other}, show tickers`}
        </span>
        <svg
          viewBox="0 0 12 12"
          aria-hidden="true"
          focusable="false"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.5"
          strokeLinecap="round"
          strokeLinejoin="round"
          className={TICKER_ICON_CLASS}
        >
          <path d="M3 4.5l3 3 3-3" />
        </svg>
      </summary>
      <p className={TICKER_LIST_CLASS}>{list}</p>
    </details>
  );
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
  // A preset switch refused because these selected dates fall outside its range; the applied
  // preset is kept, so no request is sent. It lasts until the next switch attempt or until its
  // dates are removed.
  const [windowBlock, setWindowBlock] = useState<{
    preset: WindowPreset;
    dates: string[];
  } | null>(null);
  // null means "the server's default events"; an empty prop list means the same. A repeated date
  // is kept once, so every chip is keyed by its date, and the list is sorted (ISO dates order
  // correctly as strings) so the chips follow the server's ascending cycle order, as added
  // dates do.
  const [eventDates, setEventDates] = useState<string[] | null>(() =>
    defaultEventDates !== undefined && defaultEventDates.length > 0
      ? Array.from(new Set(defaultEventDates)).sort()
      : null,
  );
  const [metric, setMetric] = useState<Metric>("price_index");
  const [overlayEventDate, setOverlayEventDate] = useState<string | null>(null);
  const [customDate, setCustomDate] = useState("");
  const [chipError, setChipError] = useState<string | null>(null);
  // The label of every event date a response has named, by date, recorded whenever the events
  // change, so a re-added stored date keeps its label while the new result loads.
  const [knownLabels, setKnownLabels] = useState<Readonly<Record<string, string>>>({});
  const removeButtonRefs = useRef(new Map<string, HTMLButtonElement>());
  const dateInputRef = useRef<HTMLInputElement>(null);
  const pendingChipFocus = useRef<ChipFocusTarget | null>(null);

  const chipsLabelId = useId();
  const dateInputId = useId();
  const chipErrorId = useId();
  const windowErrorId = useId();
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

  /**
   * The label of `date`'s chip: the shown result's, else the last one a response gave it, else the
   * server's generic label, which every date without a stored cycle eventually receives.
   */
  function chipLabel(date: string): string {
    return (
      cycles?.find((cycle) => cycle.event_date === date)?.label ??
      knownLabels[date] ??
      genericEventLabel(date)
    );
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
    setKnownLabels(withCycleLabels(knownLabels, cycles));
    setChipError(null);
    // A removed date no longer blocks the refused switch, so re-adding it never revives the
    // refusal; the refusal ends with its last date.
    setWindowBlock((block) => {
      if (block === null || !block.dates.includes(date)) return block;
      const dates = block.dates.filter((blocked) => blocked !== date);
      return dates.length > 0 ? { preset: block.preset, dates } : null;
    });
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
    setKnownLabels(withCycleLabels(knownLabels, cycles));
    setCustomDate("");
    setChipError(null);
  }

  // A switch is refused while a selected date lies outside the new preset's range, since the
  // server would reject the request with `window_out_of_range`: the applied preset stays and no
  // request is sent.
  function handleWindowChange(preset: WindowPreset): void {
    const outside = chips.filter((date) =>
      isOutsideEventDateRange(date, EVENT_DATE_BOUNDS[preset]),
    );
    if (outside.length > 0) {
      setWindowBlock({ preset, dates: outside });
      return;
    }
    setWindowBlock(null);
    if (preset !== windowPreset) {
      setWindowPreset(preset);
      // An add-date message names the previous window's range.
      setChipError(null);
    }
  }

  // The refusal names only the dates still selected, so it clears once they are removed.
  const blockedDates = windowBlock?.dates.filter((date) => chips.includes(date)) ?? [];
  const blockedPreset = windowBlock !== null && blockedDates.length > 0 ? windowBlock.preset : null;

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
              aria-describedby={blockedPreset === option.value ? windowErrorId : undefined}
              onClick={() => handleWindowChange(option.value)}
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
      {blockedPreset !== null ? (
        <p
          id={windowErrorId}
          role="alert"
          data-testid="cycle-window-error"
          className={CHIP_ERROR_CLASS}
        >
          {describeWindowBlock(blockedPreset, blockedDates)}
        </p>
      ) : null}

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
              const shownLabel = displayLabel(label);
              // A generic label already names the date; a stored one is followed by it.
              const shownDate = labelNamesDate(label, date) ? null : formatDate(date);
              return (
                <li key={date}>
                  <span
                    data-testid="cycle-event-chip"
                    data-event-date={date}
                    className={CHIP_CLASS}
                  >
                    <span>{shownLabel}</span>
                    {shownDate !== null ? (
                      <span className={CHIP_DATE_CLASS}> {shownDate}</span>
                    ) : null}
                    <button
                      type="button"
                      aria-label={
                        shownDate === null
                          ? `Remove ${shownLabel}`
                          : `Remove ${shownLabel} (${shownDate})`
                      }
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
  const mainChartDescription = describeMainChart(chartModel);
  const hasAnyProjection = (cycles ?? []).some((cycle) => cycle.projection !== null);

  const body = (
    <div className={isFetching ? BODY_FETCHING_CLASS : BODY_CLASS} aria-busy={isFetching}>
      <section aria-labelledby={mainTitleId} className={SECTION_CLASS}>
        <div className={SECTION_HEADER_CLASS}>
          <h3 id={mainTitleId} className={SECTION_HEADING_CLASS}>
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
                desc={mainChartDescription}
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
                      name={displayLabel(item.cycle.label)}
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
          <p className={MUTED_TEXT_CLASS}>
            {noSessionsMessage("No observed or projected sessions", data)}
          </p>
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
              <span>{displayLabel(item.cycle.label)}</span>
              {item.observed.length === 0 ? (
                <span className={TOOLTIP_META_CLASS}>
                  {` (${noSessionNote(item.cycle, noSessionCause(item.cycle, data))})`}
                </span>
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
                <span className={TOOLTIP_META_CLASS}>
                  {"\u00b7 " + displayLabel(item.cycle.label)}
                </span>
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
          <h3 id={overlayTitleId} className={SECTION_HEADING_CLASS}>
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
                    {displayLabel(cycle.label)}
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
                    displayLabel(overlayCycle.label)
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
          <p className={MUTED_TEXT_CLASS}>
            {noSessionsMessage("No cycle has observed sessions", data)}
          </p>
        )}
      </section>

      <section aria-labelledby={summaryTitleId} className={SECTION_CLASS}>
        <h3 id={summaryTitleId} className={SECTION_HEADING_CLASS}>
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
                    <span className={CYCLE_LABEL_CLASS}>{displayLabel(cycle.label)}</span>
                    {labelNamesDate(cycle.label, cycle.event_date) ? null : (
                      <>
                        {" "}
                        <span className={CYCLE_DATE_CLASS}>{formatDate(cycle.event_date)}</span>
                      </>
                    )}
                  </th>
                  <td className={TD_WRAP_CLASS}>
                    <span className={NOWRAP_CLASS}>
                      {formatDate(cycle.window_start) + " \u2013"}
                    </span>{" "}
                    <span className={NOWRAP_CLASS}>{formatDate(cycle.window_end)}</span>
                  </td>
                  <td className={TD_CLASS}>
                    <StatusBadge cycle={cycle} cause={noSessionCause(cycle, data)} />
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
                    <TickerCount
                      tickers={cycle.constituents}
                      noun={{ one: "constituent", other: "constituents" }}
                    />
                  </td>
                  <td className={TD_NUMERIC_CLASS}>
                    <TickerCount
                      tickers={cycle.excluded_constituents}
                      noun={{ one: "excluded", other: "excluded" }}
                      testId="cycle-excluded-count"
                    />
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

