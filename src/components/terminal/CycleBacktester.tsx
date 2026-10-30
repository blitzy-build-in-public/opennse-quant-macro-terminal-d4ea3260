"use client";

import { useEffect, useId, useMemo, useRef, useState } from "react";
import type { ChangeEvent, FormEvent, MouseEvent, ReactElement } from "react";
import {
  CartesianGrid,
  DefaultZIndexes,
  Line,
  LineChart,
  ReferenceDot,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
  ZIndexLayer,
  usePlotArea,
  useXAxisScale,
  useYAxisScale,
} from "recharts";

import {
  AXIS_TICK,
  CHART_COLORS,
  NEUTRAL_SERIES_PALETTE,
  PROJECTION_DASH,
  SERIES_PALETTE,
} from "./lib/chartTheme";
import { EMPTY_VALUE, formatDate, formatDecimal, formatInteger, toChartNumber } from "./lib/format";
import type { DecimalKind } from "./lib/format";
import { useTerminalQuery } from "./lib/useTerminalQuery";
import {
  CHIP_CLASS,
  CHIP_REMOVE_BUTTON_CLASS,
  SECONDARY_BUTTON_CLASS,
  TEXT_FIELD_CLASS,
  TOGGLE_BUTTON_CLASS,
  TOGGLE_BUTTON_PRESSED_CLASS,
} from "./shared/controlStyles";
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
  MarkerMetric,
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
  /** `strokeWidth` of the observed line, from {@link seriesStrokeWidth}. */
  strokeWidth: number;
  /** `strokeWidth` of the dashed projection line, half a pixel thinner than the observed one. */
  projectionStrokeWidth: number;
  observed: ObservedRow[];
  /** The {@link GridRow} field of the observed line, `o<index>`. */
  observedField: string;
  /** The {@link GridRow} field of the projection line, `p<index>`. */
  projectionField: string;
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
   * Offset range of every cycle's requested window (its `window_start` and `window_end` in
   * calendar days from its event) and of every drawn row, widened to reach the event (0) so its
   * reference line is always on the axis; shared by both charts; null when there are no cycles.
   */
  xDomain: [number, number] | null;
  /** The value axis: automatic, or the metric's base band when no row has a coordinate. */
  yAxis: ValueAxisScale;
}

/** `YAxis` `domain`, `ticks` and `allowDataOverflow`; `ticks` undefined lets Recharts choose. */
interface ValueAxisScale {
  domain: [number, number] | ["auto", "auto"];
  ticks: number[] | undefined;
  allowDataOverflow: boolean;
}

/**
 * One integer day of the shared offset axis: a chart-level `data` row of both charts. Each line
 * reads its own field, which is null on days without a row of its series. Recharts snaps the
 * tooltip to the nearest row of the chart's data, so rows on every day make both charts snap a
 * given x to the same day. The fields are coordinates only.
 */
interface GridRow {
  x: number;
  [field: string]: number | null;
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

/** A marker's true pixel centre on the main chart: its session's day and plotted value. */
interface MarkerAnchor {
  key: string;
  x: number;
  y: number;
}

/** Where a marker's glyph is drawn: its true pixel centre and the glyph's vertical shift. */
interface MarkerGlyphPlacement extends MarkerAnchor {
  /** Pixels from the true `y` to the glyph's centre; 0 leaves the glyph on its session. */
  dy: number;
}

/** The top and bottom pixel rows of the main chart's plot area. */
interface PlotRows {
  top: number;
  bottom: number;
}

/** Where keyboard focus goes once a removed chip's focused Remove button has unmounted. */
type ChipFocusTarget = { kind: "remove"; date: string } | { kind: "date-input" };

const WINDOW_DAYS = 180;
const WINDOW_MONTHS = 12;
/** The endpoint accepts at most 8 `event_dates`. */
const MAX_EVENT_DATES = 8;
/**
 * Code of the API's 422 when the request names no event and no cycle is stored: there is nothing
 * to backtest yet, so the widget shows its empty state rather than an error.
 */
const NO_EVENT_DATES_CODE = "no_event_dates";
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
/**
 * Half the side of the square a marker glyph covers, stroke included: the diamond's vertices sit
 * `MARKER_RADIUS + 1` from its centre and their 2px mitred stroke reaches √2 px beyond them.
 */
const MARKER_GLYPH_HALF_EXTENT = MARKER_RADIUS + 2.5;
/** Clear space kept between two marker glyphs. */
const MARKER_GLYPH_GAP = 2;
/** The vertical step between a glyph's candidate positions: its covered side plus the gap. */
const MARKER_LAYOUT_STEP = 2 * MARKER_GLYPH_HALF_EXTENT + MARKER_GLYPH_GAP;
/** How many steps up and down a glyph may move to clear the glyphs placed before it. */
const MARKER_LAYOUT_MAX_STEPS = 8;
/** Radius of the dot that keeps a moved glyph's true session marked on its line. */
const MARKER_ANCHOR_RADIUS = 2;
/**
 * Inner padding in px at both ends of each chart's offset and value axes, so a marker on a
 * domain edge is drawn whole inside the plot: a glyph covers {@link MARKER_GLYPH_HALF_EXTENT} px
 * on each side of its centre. The value axis padding also grows with the chart text, so its end
 * ticks keep clear of the offset ticks and the top.
 */
const AXIS_EDGE_PADDING = Math.ceil(MARKER_GLYPH_HALF_EXTENT);
// Both charts pad the offset axis identically, so a given day sits at the same x in each.
const X_AXIS_PADDING = { left: AXIS_EDGE_PADDING, right: AXIS_EDGE_PADDING };

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

// Both kinds share the neutral slate-100 fill and differ by shape; each plotted marker is outlined
// in its cycle's colour, so no marker fill repeats a cycle line's colour.
const MARKER_META: Record<MarkerKind, { label: string; testId: string; fill: string }> = {
  valuation_floor: {
    label: "Valuation floor",
    testId: "marker-valuation-floor",
    fill: CHART_COLORS.markerFill,
  },
  volume_exhaustion: {
    label: "Volume exhaustion",
    testId: "marker-volume-exhaustion",
    fill: CHART_COLORS.markerFill,
  },
};

/** The user-facing label and `formatDecimal` kind of each metric that can select a marker. */
const MARKER_METRIC_META: Record<MarkerMetric, { label: string; kind: DecimalKind }> = {
  pe_ratio: { label: "P/E", kind: "ratio" },
  price_index: { label: "Price index", kind: "index" },
  turnover_index: { label: "Turnover index", kind: "index" },
};

/** The `stroke-linecap` of a drawn line: `round` turns a short dash pattern into dots. */
type StrokeLinecap = "butt" | "round";

/** The `strokeWidth` of every overlay line. */
const OVERLAY_STROKE_WIDTH = 2;

// The overlay lines take the neutral slate shades, never a cycle colour, and since those differ
// only in lightness each line also has its own pattern: solid, dashed, dotted. None uses
// PROJECTION_DASH, which marks projections on the main chart.
const OVERLAY_SERIES: ReadonlyArray<{
  field: OverlayField;
  label: string;
  color: string;
  /** `strokeDasharray`; undefined for a solid line. */
  dash: string | undefined;
  linecap: StrokeLinecap;
}> = [
  {
    field: "inflation_yoy_pct",
    label: "12-month inflation",
    color: NEUTRAL_SERIES_PALETTE[0],
    dash: undefined,
    linecap: "butt",
  },
  {
    field: "tbill_91d_pct",
    label: "91-day T-bill",
    color: NEUTRAL_SERIES_PALETTE[1],
    dash: "5 3",
    linecap: "butt",
  },
  {
    field: "tbill_364d_pct",
    label: "364-day T-bill",
    color: NEUTRAL_SERIES_PALETTE[2],
    dash: "1 3",
    linecap: "round",
  },
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

// Every class token below is written out in full (strings are only joined from complete tokens),
// so Tailwind's source scan and a host's `@source` directive or `content` glob find each one.
// Margins and paddings of lists, paragraphs and figures are reset explicitly, so the layout does
// not depend on a host's CSS reset. The toggles, chips, Add button and fields use the shared
// control classes of `shared/controlStyles`, so they match the other widgets' controls.
/* BLITZY [A11Y]: the toggle, chip, Add and field controls are 26px tall (the chip remove buttons
   24px square), at or above WCAG 2.2 AA's 24px target minimum but below the 44px guideline, as
   WidgetFrame's Retry button is. Flagged for designer review. */
const CONTROLS_CLASS = "flex flex-col gap-3 text-xs text-slate-300";
const TOOLBAR_CLASS = "flex flex-wrap items-center gap-x-6 gap-y-2";
const GROUP_CLASS = "flex flex-wrap items-center gap-1";
// The stored cycles' chips arrive with the first response, so until then the Events row reserves
// their height, measured on the standalone page with the seeded cycles at each viewport width
// where the chips wrap: wrapped beneath the label below 852px, beside it in one row from there.
// The controls, and with them the card, therefore keep their height from loading to ready.
const EVENTS_GROUP_LOADING_CLASS =
  "flex min-h-[9.25rem] flex-wrap content-start items-center gap-1 min-[405px]:min-h-[7.125rem] min-[462px]:min-h-[5rem] min-[808px]:min-h-[2.875rem] min-[852px]:min-h-[1.625rem] min-[852px]:content-center";
const GROUP_LABEL_CLASS = "me-1 text-slate-400";
const CHIP_LIST_CLASS = "m-0 flex list-none flex-wrap items-center gap-2 p-0";
const CHIP_DATE_CLASS = "text-slate-400";
const ICON_CLASS = "h-3 w-3";
const MUTED_TEXT_CLASS = "m-0 text-xs text-slate-400";
const ADD_FORM_CLASS = "flex flex-col gap-1";
const ADD_ROW_CLASS = "flex flex-wrap items-center gap-2";
// The shared field turns its border rose while `aria-invalid` is set. No CSS pseudo-class matches
// an empty date input, so `data-empty` dims the empty value (the mm/dd/yyyy placeholder) to
// slate-400. Chromium's UA stylesheet draws its own focus box on the calendar-picker stop, and
// while that stop is focused neither the host input (:focus, :focus-visible) nor the
// pseudo-element itself matches a focus pseudo-class, so the pseudo-element is given the sky-400
// ring from the host's state: focus within the input (:focus-within) but not on its month, day
// or year fields (which match :focus) is on the picker stop. The ring's solid style is the
// arbitrary property `[outline-style:solid]` because Tailwind 3 has no `outline-solid` and its
// `outline-2` sets only the width.
const DATE_INPUT_CLASS = `${TEXT_FIELD_CLASS} data-[empty=true]:text-slate-400 [&::-webkit-calendar-picker-indicator]:cursor-pointer [&::-webkit-calendar-picker-indicator]:rounded-sm [&:focus-within:not(:focus)::-webkit-calendar-picker-indicator]:outline-2 [&:focus-within:not(:focus)::-webkit-calendar-picker-indicator]:outline-offset-0 [&:focus-within:not(:focus)::-webkit-calendar-picker-indicator]:[outline-style:solid] [&:focus-within:not(:focus)::-webkit-calendar-picker-indicator]:outline-sky-400`;
const CHIP_ERROR_CLASS = "m-0 text-xs text-rose-400";
// While a new window or event set is fetched, the previous result stays mounted and is dimmed.
const BODY_CLASS = "flex flex-col gap-5";
const BODY_FETCHING_CLASS =
  "flex flex-col gap-5 opacity-60 motion-safe:transition-opacity motion-safe:duration-200 motion-safe:ease-out";
// The loaded body's height, measured on the standalone page with the seeded cycles at each
// viewport width where it reflows, so the card keeps its height when the data arrives.
const SKELETON_HEIGHT_CLASS =
  "h-[74.625rem] min-[329px]:h-[73.625rem] min-[353px]:h-[72.375rem] min-[358px]:h-[71.375rem] min-[407px]:h-[70.125rem] min-[443px]:h-[68.625rem] min-[449px]:h-[67.625rem] min-[475px]:h-[66.125rem] min-[511px]:h-[65.125rem] min-[556px]:h-[63.875rem] min-[632px]:h-[62.625rem] min-[863px]:h-[61.375rem] min-[1093px]:h-[60.125rem]";
const SECTION_CLASS = "m-0 flex min-w-0 flex-col gap-2";
const SECTION_HEADER_CLASS = "flex flex-wrap items-baseline justify-between gap-2";
// Recharts 3 makes each chart's `<svg class="recharts-surface">` a tabindex 0 application, so it
// gets the same keyboard focus ring as the other controls. Its `<g tabindex="-1">` layers are
// never keyboard-reachable but a mouse click can focus them, so neither they nor a mouse-focused
// surface draws an outline.
// Forced-colours mode does not repaint the `fill` and `stroke` attributes Recharts writes, so they
// would keep their slate and palette colours on the system `Canvas`. Under `forced-colors: active`
// every SVG `<text>` (tick values, axis labels, the "Event" label) is filled with `CanvasText`;
// axis, tick, reference and tooltip-cursor lines are stroked with `CanvasText` and grid lines with
// `GrayText`. Series curves, the marker groups (`g[data-testid^='marker-']`, each a title and its
// glyph), the moved markers' leaders and anchor dots (one `g[data-marker-connectors]` layer beneath
// every marker group) and the overlay's hover dots keep their colours, which identify each series,
// and are cased in `CanvasText` by four 1px `drop-shadow()`s, so every coloured shape is edged by
// the system text colour, the one colour guaranteed to contrast with `Canvas`. Each rule sits
// inside `forced-colors:`, so normal rendering is unchanged.
const MAIN_CHART_CLASS =
  "h-80 w-full [&_.recharts-surface:focus-visible]:outline-2 [&_.recharts-surface:focus-visible]:outline-offset-2 [&_.recharts-surface:focus-visible]:outline-sky-400 [&_g[tabindex='-1']]:outline-none [&_*:focus:not(:focus-visible)]:outline-none forced-colors:[&_text]:fill-[CanvasText] forced-colors:[&_.recharts-cartesian-axis-line]:stroke-[color:CanvasText] forced-colors:[&_.recharts-cartesian-axis-tick-line]:stroke-[color:CanvasText] forced-colors:[&_.recharts-reference-line-line]:stroke-[color:CanvasText] forced-colors:[&_.recharts-tooltip-cursor]:stroke-[color:CanvasText] forced-colors:[&_.recharts-cartesian-grid_line]:stroke-[color:GrayText] forced-colors:[&_.recharts-line-curve]:[filter:drop-shadow(1px_0_0_CanvasText)_drop-shadow(-1px_0_0_CanvasText)_drop-shadow(0_1px_0_CanvasText)_drop-shadow(0_-1px_0_CanvasText)] forced-colors:[&_g[data-testid^='marker-']]:[filter:drop-shadow(1px_0_0_CanvasText)_drop-shadow(-1px_0_0_CanvasText)_drop-shadow(0_1px_0_CanvasText)_drop-shadow(0_-1px_0_CanvasText)] forced-colors:[&_g[data-marker-connectors]]:[filter:drop-shadow(1px_0_0_CanvasText)_drop-shadow(-1px_0_0_CanvasText)_drop-shadow(0_1px_0_CanvasText)_drop-shadow(0_-1px_0_CanvasText)]";
const OVERLAY_CHART_CLASS =
  "h-48 w-full [&_.recharts-surface:focus-visible]:outline-2 [&_.recharts-surface:focus-visible]:outline-offset-2 [&_.recharts-surface:focus-visible]:outline-sky-400 [&_g[tabindex='-1']]:outline-none [&_*:focus:not(:focus-visible)]:outline-none forced-colors:[&_text]:fill-[CanvasText] forced-colors:[&_.recharts-cartesian-axis-line]:stroke-[color:CanvasText] forced-colors:[&_.recharts-cartesian-axis-tick-line]:stroke-[color:CanvasText] forced-colors:[&_.recharts-reference-line-line]:stroke-[color:CanvasText] forced-colors:[&_.recharts-tooltip-cursor]:stroke-[color:CanvasText] forced-colors:[&_.recharts-cartesian-grid_line]:stroke-[color:GrayText] forced-colors:[&_.recharts-line-curve]:[filter:drop-shadow(1px_0_0_CanvasText)_drop-shadow(-1px_0_0_CanvasText)_drop-shadow(0_1px_0_CanvasText)_drop-shadow(0_-1px_0_CanvasText)] forced-colors:[&_.recharts-active-dot]:[filter:drop-shadow(1px_0_0_CanvasText)_drop-shadow(-1px_0_0_CanvasText)_drop-shadow(0_1px_0_CanvasText)_drop-shadow(0_-1px_0_CanvasText)]";
// On a body narrower than the chart's minimum width, each chart wrapper scrolls horizontally
// and, like the summary table's wrapper, is focusable for keyboard scrolling. Its overflow clips
// whatever is drawn outside it, so the surface's focus ring is drawn inset there. The
// forced-colours rules are those of the non-scrolling classes.
const MAIN_CHART_SCROLL_CLASS =
  "h-80 w-full overflow-x-auto rounded focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400 [&_.recharts-surface:focus-visible]:outline-2 [&_.recharts-surface:focus-visible]:-outline-offset-2 [&_.recharts-surface:focus-visible]:outline-sky-400 [&_g[tabindex='-1']]:outline-none [&_*:focus:not(:focus-visible)]:outline-none forced-colors:[&_text]:fill-[CanvasText] forced-colors:[&_.recharts-cartesian-axis-line]:stroke-[color:CanvasText] forced-colors:[&_.recharts-cartesian-axis-tick-line]:stroke-[color:CanvasText] forced-colors:[&_.recharts-reference-line-line]:stroke-[color:CanvasText] forced-colors:[&_.recharts-tooltip-cursor]:stroke-[color:CanvasText] forced-colors:[&_.recharts-cartesian-grid_line]:stroke-[color:GrayText] forced-colors:[&_.recharts-line-curve]:[filter:drop-shadow(1px_0_0_CanvasText)_drop-shadow(-1px_0_0_CanvasText)_drop-shadow(0_1px_0_CanvasText)_drop-shadow(0_-1px_0_CanvasText)] forced-colors:[&_g[data-testid^='marker-']]:[filter:drop-shadow(1px_0_0_CanvasText)_drop-shadow(-1px_0_0_CanvasText)_drop-shadow(0_1px_0_CanvasText)_drop-shadow(0_-1px_0_CanvasText)] forced-colors:[&_g[data-marker-connectors]]:[filter:drop-shadow(1px_0_0_CanvasText)_drop-shadow(-1px_0_0_CanvasText)_drop-shadow(0_1px_0_CanvasText)_drop-shadow(0_-1px_0_CanvasText)]";
const OVERLAY_CHART_SCROLL_CLASS =
  "h-48 w-full overflow-x-auto rounded focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400 [&_.recharts-surface:focus-visible]:outline-2 [&_.recharts-surface:focus-visible]:-outline-offset-2 [&_.recharts-surface:focus-visible]:outline-sky-400 [&_g[tabindex='-1']]:outline-none [&_*:focus:not(:focus-visible)]:outline-none forced-colors:[&_text]:fill-[CanvasText] forced-colors:[&_.recharts-cartesian-axis-line]:stroke-[color:CanvasText] forced-colors:[&_.recharts-cartesian-axis-tick-line]:stroke-[color:CanvasText] forced-colors:[&_.recharts-reference-line-line]:stroke-[color:CanvasText] forced-colors:[&_.recharts-tooltip-cursor]:stroke-[color:CanvasText] forced-colors:[&_.recharts-cartesian-grid_line]:stroke-[color:GrayText] forced-colors:[&_.recharts-line-curve]:[filter:drop-shadow(1px_0_0_CanvasText)_drop-shadow(-1px_0_0_CanvasText)_drop-shadow(0_1px_0_CanvasText)_drop-shadow(0_-1px_0_CanvasText)] forced-colors:[&_.recharts-active-dot]:[filter:drop-shadow(1px_0_0_CanvasText)_drop-shadow(-1px_0_0_CanvasText)_drop-shadow(0_1px_0_CanvasText)_drop-shadow(0_-1px_0_CanvasText)]";
const LEGEND_CLASS = "m-0 flex list-none flex-wrap gap-x-4 gap-y-1 p-0 text-xs text-slate-300";
const LEGEND_ITEM_CLASS = "inline-flex items-center gap-1.5";
// Line and marker swatches get the plotted shapes' forced-colours `CanvasText` casing.
const SWATCH_CLASS =
  "h-2 w-5 shrink-0 forced-colors:[filter:drop-shadow(1px_0_0_CanvasText)_drop-shadow(-1px_0_0_CanvasText)_drop-shadow(0_1px_0_CanvasText)_drop-shadow(0_-1px_0_CanvasText)]";
const MARKER_SWATCH_CLASS =
  "h-3 w-3 shrink-0 forced-colors:[filter:drop-shadow(1px_0_0_CanvasText)_drop-shadow(-1px_0_0_CanvasText)_drop-shadow(0_1px_0_CanvasText)_drop-shadow(0_-1px_0_CanvasText)]";
// The overlay select is the shared text field. It drops below its label, and narrows to the card,
// when the two do not fit one line (a narrow card with a large text setting).
const SELECT_CLASS = `${TEXT_FIELD_CLASS} m-0 max-w-full`;
const SELECT_LABEL_CLASS =
  "inline-flex min-w-0 flex-wrap items-center gap-2 text-xs text-slate-400";
// The wrapper scrolls horizontally on narrow cards, so it is focusable for keyboard scrolling.
// `relative` makes it the containing block of the absolutely positioned `sr-only` text inside the
// table (the caption and each cohort count's name); without it that text escapes the overflow
// clip and widens the host page beyond a narrow viewport.
const TABLE_WRAPPER_CLASS =
  "relative min-w-0 overflow-x-auto rounded focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400";
const TABLE_CLASS = "w-full border-collapse text-xs";
// Column headers stay on one line, so a long off-screen header never deepens the header row;
// the wrapper scrolls instead. The window cell may wrap between its two dates.
const TH_CLASS = "whitespace-nowrap px-2 py-1.5 text-start align-bottom font-medium text-slate-400";
const TH_NUMERIC_CLASS =
  "whitespace-nowrap px-2 py-1.5 text-end align-bottom font-medium text-slate-400";
const TH_ROW_CLASS =
  "whitespace-nowrap px-2 py-1.5 text-start align-top font-medium text-slate-200";
const TR_CLASS = "border-t border-slate-800";
const TD_CLASS = "whitespace-nowrap px-2 py-1.5 align-top text-slate-200";
const TD_WRAP_CLASS = "px-2 py-1.5 align-top text-slate-200";
const NOWRAP_CLASS = "whitespace-nowrap";
const TD_NUMERIC_CLASS = "whitespace-nowrap px-2 py-1.5 text-end align-top tabular-nums text-slate-200";
// Flag codes wrap between codes; a code breaks only when it alone is wider than the column.
// The 11rem minimum applies only to a cell that holds codes, so a column of "—" cells stays
// as narrow as its header and the table still fits a wide card.
const TD_FLAGS_CLASS = "min-w-[11rem] break-words px-2 py-1.5 align-top text-slate-400";
const TD_FLAGS_EMPTY_CLASS = "px-2 py-1.5 align-top text-slate-400";
// The cycle label takes the row header's weight and colour; the event date beneath it is the
// secondary line.
const CYCLE_LABEL_CLASS = "block";
const CYCLE_DATE_CLASS = "block font-normal text-slate-400";
// The Cycle cell leads with the cycle's line swatch, centred on the label's 1rem text-xs line, so
// a row is identifiable by the same colour as its line on the main chart.
const CYCLE_CELL_CLASS = "flex items-start gap-1.5";
const CYCLE_SWATCH_SLOT_CLASS = "flex h-4 shrink-0 items-center";
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
// A marker row sits beneath its cycle's row: its glyph is centred in a slot as wide as the line
// swatch above, so its text starts where the cycle's label does.
const TOOLTIP_MARKER_ROW_CLASS = "flex items-center gap-1.5";
const TOOLTIP_MARKER_SLOT_CLASS = "flex w-5 shrink-0 justify-center";
// The marker under the pointer leads the tooltip, set off from the day's rows by a rule.
const TOOLTIP_HOVERED_MARKER_CLASS =
  "m-0 flex items-center gap-1.5 border-b border-slate-700 pb-1.5 font-medium text-slate-100";

const CHART_MARGIN = { top: 12, right: 16, bottom: 4, left: 4 };
/**
 * px of 1rem at the browser's default text size, the size the chart's px values below are set
 * for; it is also assumed until the root font size is first read.
 */
const DEFAULT_REM_PX = 16;
/** Offset axis height in px at {@link DEFAULT_REM_PX}: ticks plus the "Days from event" title. */
const X_AXIS_HEIGHT = 40;
/** Value axis width in px at {@link DEFAULT_REM_PX}: ticks plus the rotated axis title. */
const Y_AXIS_WIDTH = 76;
/** Recharts' default `Label` offset: the value axis title's px inset at {@link DEFAULT_REM_PX}. */
const AXIS_TITLE_OFFSET = 5;
/**
 * Value axis width in px at {@link DEFAULT_REM_PX} on a body narrower than
 * {@link COMPACT_BODY_REM}: the least that keeps the rotated title clear of five-character ticks
 * such as `1,050`, `−1.5B`, `15.5%` and `−195M`.
 */
const Y_AXIS_COMPACT_WIDTH = 56;
/** Body width in rem below which both charts use {@link Y_AXIS_COMPACT_WIDTH}. */
const COMPACT_BODY_REM = 30;
/** Plot width that each offset tick needs, in multiples of the chart's scaled 1rem. */
const OFFSET_TICK_SPACING_REM = 2;
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

/** How many leading {@link SERIES_PALETTE} entries are its 400 shades, drawn at the base width. */
const BASE_TONE_SERIES = 4;
const BASE_SERIES_STROKE_WIDTH = 2;
const DARK_TONE_SERIES_STROKE_WIDTH = 3;
/** How much thinner a projection line is than its cycle's observed line. */
const PROJECTION_STROKE_INSET = 0.5;

/**
 * The observed-line `strokeWidth` of the cycle at `index`, by its position in the response: 2px
 * for the cycles in the palette's 400 shades, 3px for the fifth to eighth cycles in its 600
 * shades, so those differ from the first four in weight as well as tone while every observed
 * line stays solid.
 */
function seriesStrokeWidth(index: number): number {
  return index % SERIES_PALETTE.length < BASE_TONE_SERIES
    ? BASE_SERIES_STROKE_WIDTH
    : DARK_TONE_SERIES_STROKE_WIDTH;
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
 * Days from 1970-01-01 to the ISO date `iso` in the proleptic Gregorian calendar, or null when
 * `iso` is not a real `YYYY-MM-DD` day. Integer days-from-civil arithmetic is exact across the
 * API's whole 0001–9999 calendar, where `Date.UTC` would read years 0–99 as 1900–1999.
 */
function isoDayNumber(iso: string): number | null {
  const match = ISO_DATE_PATTERN.exec(iso);
  if (match === null || !isCalendarDate(iso)) return null;
  const year = parseInt(match[1] ?? "", 10);
  const month = parseInt(match[2] ?? "", 10);
  const day = parseInt(match[3] ?? "", 10);
  // Years are counted from 1 March, so a leap day is the last day of its year.
  const marchYear = month <= 2 ? year - 1 : year;
  const era = Math.floor(marchYear / 400);
  const yearOfEra = marchYear - era * 400;
  const dayOfYear = Math.floor((153 * ((month + 9) % 12) + 2) / 5) + day - 1;
  const dayOfEra =
    yearOfEra * 365 + Math.floor(yearOfEra / 4) - Math.floor(yearOfEra / 100) + dayOfYear;
  return era * 146097 + dayOfEra - 719468;
}

/** Calendar days from `fromIso` to `toIso`, negative when `toIso` is earlier, or null. */
function calendarDaysBetween(fromIso: string, toIso: string): number | null {
  const from = isoDayNumber(fromIso);
  const to = isoDayNumber(toIso);
  return from === null || to === null ? null : to - from;
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
/** Bounds of the offset tick budget, which otherwise follows the plot width. */
const MIN_OFFSET_TICKS = 3;
const MAX_OFFSET_TICKS = 9;

const AUTO_VALUE_AXIS: ValueAxisScale = {
  domain: ["auto", "auto"],
  ticks: undefined,
  allowDataOverflow: false,
};

/**
 * The value axis of a chart whose rows have no coordinate at all: Recharts finds no domain in
 * them and drops the Y ticks and the event reference line, so the axis is fixed to a band around
 * `base` with `base` as its only tick. Recharts applies a fixed domain to an axis without data
 * only with `allowDataOverflow`. The rows themselves stay null; nothing is drawn there.
 */
function emptyValueAxis(base: number): ValueAxisScale {
  return { domain: [base - 1, base + 1], ticks: [base], allowDataOverflow: true };
}

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
 * `maxTicks` ticks. Ticks are anchored at the event, so 0 is a tick whenever it is in range, even
 * when a window's first session falls a day or two after its start.
 */
function offsetTicks(range: [number, number] | null, maxTicks: number): number[] {
  if (range === null) return [];
  const [min, max] = range;
  const step =
    OFFSET_TICK_STEPS.find(
      (candidate) => Math.floor(max / candidate) - Math.ceil(min / candidate) + 1 <= maxTicks,
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
 * outside the rows' range. A cycle has no session on most days, so a hovered day is paired with
 * each line's own last session on or before it.
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

/**
 * The selecting metric of a marker, in the user's terms (`P/E 10.55×`, `Turnover index 35.92`):
 * its label and its value formatted by `formatDecimal` with that metric's unit. A metric this
 * build does not know is shown by its API name as an index level.
 */
function markerMetricText(marker: CycleMarker): string {
  const meta: { label: string; kind: DecimalKind } | undefined = MARKER_METRIC_META[marker.metric];
  const label = meta?.label ?? String(marker.metric);
  return `${label} ${formatDecimal(marker.value, meta?.kind ?? "index")}`;
}

/**
 * The `<title>` of a plotted marker, naming its cycle as displayed, kind, selecting metric and
 * value, and session, for example "2013 Cycle: Valuation floor, P/E 10.55×, 28 Feb 2013".
 */
function markerTitle(spec: MarkerSpec): string {
  const kindLabel = MARKER_META[spec.kind].label;
  const session = formatDate(spec.marker.trade_date);
  const cycleLabel = displayLabel(spec.cycle.label);
  return `${cycleLabel}: ${kindLabel}, ${markerMetricText(spec.marker)}, ${session}`;
}

/**
 * The area two marker glyphs centred at `a` and `b` share, each glyph counted as a square of side
 * {@link MARKER_LAYOUT_STEP} (its covered side plus the gap); 0 when they are clear of each other.
 */
function glyphOverlapArea(a: { x: number; y: number }, b: { x: number; y: number }): number {
  const width = MARKER_LAYOUT_STEP - Math.abs(a.x - b.x);
  const height = MARKER_LAYOUT_STEP - Math.abs(a.y - b.y);
  return width > 0 && height > 0 ? width * height : 0;
}

/**
 * The vertical pixel offset of each marker glyph, by marker key. A deterministic, bounded search
 * separates the glyphs wherever it finds room, so each separated glyph's title and tooltip can be
 * reached; its minimum-overlap fallback lets a glyph overlap those placed before it only when every
 * candidate within the bound does, and then by the least total area. {@link useMarkerGlyphLayout}
 * runs it inside the chart for both the glyphs and their leaders.
 *
 * Glyphs are placed one at a time in `anchors` order, so the same markers always get the same
 * layout. A glyph keeps its true x, the marked session's day, and moves only vertically. Its
 * candidate offsets are 0, −S, +S, −2S, +2S, … up to {@link MARKER_LAYOUT_MAX_STEPS} steps each
 * way, where S is {@link MARKER_LAYOUT_STEP}; the first whose glyph overlaps none placed before it
 * is taken. A moved glyph must also stay inside `rows`; the true position is always a candidate,
 * because the axis domain already extends to every marker. When every candidate overlaps, the one
 * with the least total overlap area is taken, the earliest on a tie.
 *
 * @param anchors Each marker's key and true pixel centre, in placement order.
 * @param rows The plot area's top and bottom pixel rows, or null when unknown.
 * @returns Pixels to add to each marker's true y; 0 leaves the glyph on its session.
 */
function layoutMarkerGlyphs(
  anchors: readonly MarkerAnchor[],
  rows: PlotRows | null,
): Map<string, number> {
  const offsets = new Map<string, number>();
  const placed: Array<{ x: number; y: number }> = [];
  for (const anchor of anchors) {
    let chosen: number | null = null;
    let fallback = 0;
    let fallbackOverlap = Number.POSITIVE_INFINITY;
    for (let step = 0; step <= 2 * MARKER_LAYOUT_MAX_STEPS && chosen === null; step += 1) {
      const magnitude = Math.ceil(step / 2) * MARKER_LAYOUT_STEP;
      const dy = step % 2 === 1 ? -magnitude : magnitude;
      const centre = { x: anchor.x, y: anchor.y + dy };
      const outsidePlot =
        dy !== 0 &&
        rows !== null &&
        (centre.y - MARKER_GLYPH_HALF_EXTENT < rows.top ||
          centre.y + MARKER_GLYPH_HALF_EXTENT > rows.bottom);
      if (outsidePlot) continue;
      const overlap = placed.reduce((total, other) => total + glyphOverlapArea(centre, other), 0);
      if (overlap === 0) chosen = dy;
      else if (overlap < fallbackOverlap) {
        fallback = dy;
        fallbackOverlap = overlap;
      }
    }
    const dy = chosen ?? fallback;
    placed.push({ x: anchor.x, y: anchor.y + dy });
    offsets.set(anchor.key, dy);
  }
  return offsets;
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
    const strokeWidth = seriesStrokeWidth(index);
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
    series.push({
      key,
      cycle,
      color,
      strokeWidth,
      projectionStrokeWidth: strokeWidth - PROJECTION_STROKE_INSET,
      observed,
      observedField: `o${index}`,
      projectionField: `p${index}`,
      projection,
      projectionName: name,
    });

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
  // The axis spans each cycle's requested window, not only what the metric draws, so it matches
  // the window caption for every metric: no projection is drawn for cumulative flow, and a
  // cycle's sessions need not reach either end of its window.
  const windowEdges: Array<{ x: number }> = [];
  for (const cycle of cycles) {
    for (const edge of [cycle.window_start, cycle.window_end]) {
      const x = calendarDaysBetween(cycle.event_date, edge);
      if (x !== null) windowEdges.push({ x });
    }
  }
  const range = offsetRange([
    ...windowEdges,
    ...series.flatMap((item) => [...item.observed, ...(item.projection ?? [])]),
  ]);
  // Recharts discards a reference line outside the axis domain, so the domain always reaches 0:
  // a partial cycle observed only before the event, or a cycle observed only after it, still
  // shows the event line and a 0 tick.
  const xDomain: [number, number] | null =
    range === null ? null : [Math.min(range[0], 0), Math.max(range[1], 0)];
  // A marker's coordinate is a row's coordinate, so markers need no separate check. The empty
  // axis is centred on what the metric starts from: 100 for an index ("anchor = 100"), 0 for
  // cumulative flow.
  const hasCoordinate = series.some((item) =>
    [...item.observed, ...(item.projection ?? [])].some((row) => row.y !== null),
  );
  const yAxis = hasCoordinate ? AUTO_VALUE_AXIS : emptyValueAxis(isIndexMetric(metric) ? 100 : 0);
  return { series, markers, xDomain, yAxis };
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
 * singular where exactly one of a kind is drawn, and that each marker is outlined in its cycle's
 * colour. A line with fewer than two coordinates for the plotted metric is not counted, since it
 * draws nothing; markers are counted on their own, since a `ReferenceDot` draws whether or not
 * its cycle's line does.
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
  // Marker fills are shared, so the outline is what ties a marker to its cycle.
  const outline =
    floors + exhaustions === 1
      ? ", outlined in its cycle's colour"
      : ", each marker outlined in its cycle's colour";

  const clauses = [lines, marks]
    .filter((group) => group.length > 0)
    .map((group) => group.join(" and ") + (group === marks ? outline : ""));
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

/**
 * The {@link GridRow}s of `domain`, one per integer day, with each column's coordinate on the
 * days of its own rows and null on every other day; a missing value is never filled in.
 */
function buildGridRows(
  domain: [number, number] | null,
  columns: ReadonlyArray<{ field: string; rows: ReadonlyArray<{ x: number; y: number | null }> }>,
): GridRow[] {
  if (domain === null) return [];
  const [min, max] = domain;
  const grid: GridRow[] = [];
  for (let x = min; x <= max; x += 1) {
    const row: GridRow = { x };
    for (const column of columns) row[column.field] = null;
    grid.push(row);
  }
  for (const column of columns) {
    for (const source of column.rows) {
      const row = grid[source.x - min];
      if (row !== undefined) row[column.field] = source.y;
    }
  }
  return grid;
}

/** The computed root font size in px (the size of 1rem), or {@link DEFAULT_REM_PX} if unusable. */
function rootFontSizePx(): number {
  const size = parseFloat(getComputedStyle(document.documentElement).fontSize);
  return Number.isFinite(size) && size > 0 ? size : DEFAULT_REM_PX;
}

/** The root font size and an element's content width, both in CSS px. */
interface ChartMetrics {
  rem: number;
  /** null until the element is first measured. */
  width: number | null;
}

/**
 * The root font size, which follows the user's text-size setting, and the content width of
 * `element`, read when the element mounts and on every resize it reports. The widget body is
 * sized in rem, so a root font-size change resizes it as well. The initial value is
 * {@link DEFAULT_REM_PX} with no width, so the server and the first client render agree.
 */
function useChartMetrics(element: HTMLElement | null): ChartMetrics {
  const [metrics, setMetrics] = useState<ChartMetrics>({ rem: DEFAULT_REM_PX, width: null });
  useEffect(() => {
    if (element === null) return undefined;
    const measure = (): void => {
      const rem = rootFontSizePx();
      const width = element.clientWidth;
      setMetrics((previous) =>
        previous.rem === rem && previous.width === width ? previous : { rem, width },
      );
    };
    measure();
    if (typeof ResizeObserver === "undefined") return undefined;
    const observer = new ResizeObserver(measure);
    observer.observe(element);
    return () => observer.disconnect();
  }, [element]);
  return metrics;
}


// ---------------------------------------------------------------------------------------------
// Presentational pieces
// ---------------------------------------------------------------------------------------------

/** The stroke of the chart line a {@link LineSwatch} stands for. */
interface LineSwatchProps {
  /** `stroke` colour. */
  color: string;
  /** `strokeWidth` in pixels. Default 2. */
  width?: number;
  /** `strokeDasharray`, such as `PROJECTION_DASH` for a projection; omitted for a solid line. */
  dash?: string;
  /** `strokeLinecap`: `round` for a dotted pattern. Default `butt`. */
  linecap?: StrokeLinecap;
}

/** A decorative line swatch drawn with the same stroke, width and pattern as its chart line. */
function LineSwatch({ color, width = 2, dash, linecap = "butt" }: LineSwatchProps): ReactElement {
  return (
    <svg viewBox="0 0 20 8" aria-hidden="true" focusable="false" className={SWATCH_CLASS}>
      <line
        x1="0"
        y1="4"
        x2="20"
        y2="4"
        stroke={color}
        strokeWidth={width}
        strokeDasharray={dash}
        strokeLinecap={linecap}
      />
    </svg>
  );
}

/**
 * A marker glyph centred on (`cx`, `cy`), outlined in `stroke`: a circle for the valuation floor
 * and a diamond for volume exhaustion. Both kinds share the neutral slate-100 fill, so they differ
 * by shape, and a plotted marker's outline carries its cycle's colour without its fill ever
 * matching a cycle line.
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

/**
 * A decorative legend swatch of a marker kind: its glyph outlined in its own fill, since the
 * outline of a plotted marker varies with its cycle and only the shape and fill are shared.
 */
function MarkerSwatch({ kind }: { kind: MarkerKind }): ReactElement {
  return (
    <svg viewBox="0 0 16 16" aria-hidden="true" focusable="false" className={MARKER_SWATCH_CLASS}>
      <MarkerGlyph kind={kind} cx={8} cy={8} stroke={MARKER_META[kind].fill} />
    </svg>
  );
}

/**
 * The pixel layout of the main chart's markers, by marker key: each marker's true pixel centre,
 * from the chart's axis scales, and the vertical shift {@link layoutMarkerGlyphs} gives its glyph
 * inside the plot area. A marker whose day or value has no finite pixel is left out. Both
 * {@link PlacedMarker} and {@link MarkerConnectors} read it, so a glyph and its leader always
 * agree. It must run inside the chart.
 *
 * @param markers Every marker of the main chart, in placement order.
 * @returns Each marker's placement by key, or null while the chart has no axis scales.
 */
function useMarkerGlyphLayout(
  markers: readonly MarkerSpec[],
): ReadonlyMap<string, MarkerGlyphPlacement> | null {
  const xScale = useXAxisScale();
  const yScale = useYAxisScale();
  const plotArea = usePlotArea();
  return useMemo(() => {
    if (xScale === undefined || yScale === undefined) return null;
    const anchors: MarkerAnchor[] = [];
    for (const item of markers) {
      const x = xScale(item.marker.offset_days);
      const y = yScale(item.y);
      if (x !== undefined && y !== undefined && Number.isFinite(x) && Number.isFinite(y)) {
        anchors.push({ key: item.key, x, y });
      }
    }
    const rows =
      plotArea === undefined ? null : { top: plotArea.y, bottom: plotArea.y + plotArea.height };
    const offsets = layoutMarkerGlyphs(anchors, rows);
    return new Map(
      anchors.map((anchor): [string, MarkerGlyphPlacement] => [
        anchor.key,
        { ...anchor, dy: offsets.get(anchor.key) ?? 0 },
      ]),
    );
  }, [xScale, yScale, plotArea, markers]);
}

/** Props of {@link PlacedMarker}. */
interface PlacedMarkerProps {
  /** The marker drawn. */
  spec: MarkerSpec;
  /** Every marker of the main chart, in placement order, this one included. */
  markers: readonly MarkerSpec[];
  /** The marked session's true pixel centre, as `ReferenceDot` computed it. */
  cx: number;
  cy: number;
  /** Called with the marker's key when the pointer enters its glyph, and with null on leaving. */
  onHover: (key: string | null) => void;
}

/**
 * One marker of the main chart, rendered by its `ReferenceDot` inside the chart: its glyph at the
 * marked session's day, moved vertically by the offset {@link useMarkerGlyphLayout} gives it,
 * clear of the other glyphs where the bounded search finds room and least-overlapping otherwise.
 * Without axis scales the glyph stays at its true point.
 *
 * The marker's `<g data-testid data-event-date>` holds only the `<title>` that names it, as its
 * first child, and the glyph. A moved glyph's leader and anchor dot are drawn by
 * {@link MarkerConnectors} in a layer beneath every marker group, so no leader or dot ever paints
 * over a glyph. The group reports the pointer entering and leaving its glyph through `onHover`, so
 * the chart tooltip can name the marker under the pointer whichever day the pointer's x selects.
 */
function PlacedMarker({ spec, markers, cx, cy, onHover }: PlacedMarkerProps): ReactElement {
  const dy = useMarkerGlyphLayout(markers)?.get(spec.key)?.dy ?? 0;
  const meta = MARKER_META[spec.kind];
  return (
    <g
      data-testid={meta.testId}
      data-event-date={spec.cycle.event_date}
      onPointerEnter={() => onHover(spec.key)}
      onPointerLeave={() => onHover(null)}
    >
      <title>{markerTitle(spec)}</title>
      <MarkerGlyph kind={spec.kind} cx={cx} cy={cy + dy} stroke={spec.color} />
    </g>
  );
}

/** Props of {@link MarkerConnectors}. */
interface MarkerConnectorsProps {
  /** Every marker of the main chart, in placement order, as its `ReferenceDot`s receive them. */
  markers: readonly MarkerSpec[];
}

/**
 * The connectors of the main chart's moved markers. For each glyph {@link useMarkerGlyphLayout}
 * moves off its session, a 1px leader in the cycle's colour runs from the true session to the
 * glyph's centre, and a small dot of that colour marks the session on its line. Every leader is
 * drawn before every dot, so no leader crosses another marker's dot.
 *
 * They share one decorative `<g data-marker-connectors>` in a z-index layer just below the
 * `ReferenceDot`s (and above every line and axis), so each leader and dot lies beneath every
 * marker glyph, and a glyph covers the end of its own leader whatever its shape. The group
 * ignores the pointer, so it never takes a marker's hover. Nothing is drawn while no glyph is
 * moved.
 */
function MarkerConnectors({ markers }: MarkerConnectorsProps): ReactElement | null {
  const layout = useMarkerGlyphLayout(markers);
  const moved: Array<{ spec: MarkerSpec; placement: MarkerGlyphPlacement }> = [];
  for (const spec of markers) {
    const placement = layout?.get(spec.key);
    if (placement !== undefined && placement.dy !== 0) moved.push({ spec, placement });
  }
  if (moved.length === 0) return null;
  return (
    <ZIndexLayer zIndex={DefaultZIndexes.scatter - 1}>
      <g data-marker-connectors="" aria-hidden="true" pointerEvents="none">
        {moved.map(({ spec, placement }) => (
          <line
            key={spec.key + "-leader"}
            data-event-date={spec.cycle.event_date}
            data-marker-kind={spec.kind}
            x1={placement.x}
            y1={placement.y}
            x2={placement.x}
            y2={placement.y + placement.dy}
            stroke={spec.color}
            strokeWidth={1}
          />
        ))}
        {moved.map(({ spec, placement }) => (
          <circle
            key={spec.key + "-anchor"}
            data-event-date={spec.cycle.event_date}
            data-marker-kind={spec.kind}
            cx={placement.x}
            cy={placement.y}
            r={MARKER_ANCHOR_RADIUS}
            fill={spec.color}
          />
        ))}
      </g>
    </ZIndexLayer>
  );
}

/**
 * The `ReferenceDot` shape of a marker: a {@link PlacedMarker}, its glyph outlined in the cycle's
 * colour and placed by the bounded layout of every marker in `markers`, with a `<title>` naming
 * the cycle, the kind, the selecting metric and value, and the session. `onHover` receives the
 * marker's key when the pointer enters its glyph and null when it leaves.
 */
function markerShape(
  spec: MarkerSpec,
  markers: readonly MarkerSpec[],
  onHover: (key: string | null) => void,
): (props: MarkerShapeProps) => ReactElement<SVGElement> {
  return function drawMarker({ cx, cy }: MarkerShapeProps): ReactElement<SVGElement> {
    if (cx === undefined || cy === undefined || !Number.isFinite(cx) || !Number.isFinite(cy)) {
      return <g />;
    }
    return <PlacedMarker spec={spec} markers={markers} cx={cx} cy={cy} onHover={onHover} />;
  };
}

interface CycleChartTooltipProps {
  active?: boolean;
  label?: string | number;
  series: readonly ChartSeries[];
  metric: Metric;
  /** The marker whose glyph is under the pointer, or null when the pointer is on none. */
  hoveredMarker: MarkerSpec | null;
}

/** A main-chart tooltip row: a cycle's or projection's value, or a marker on the session above. */
type CycleTooltipEntry =
  | {
      type: "line";
      key: string;
      /** The event date of the row's cycle. */
      eventDate: string;
      name: string;
      color: string;
      width: number;
      row: ObservedRow | ProjectionRow;
    }
  | {
      type: "marker";
      key: string;
      kind: MarkerKind;
      eventDate: string;
      color: string;
      text: string;
    };

/**
 * Main-chart tooltip: for the hovered day, each cycle's own session at or before it and each
 * drawn projection's value at or before it. Rows are resolved from the series rather than from
 * Recharts' payload, which is null on every day that is not a session of the line. Both charts
 * snap to the same {@link GridRow} day and resolve it with `rowAtOrBefore`, so a given x shows
 * the same session of a cycle in both tooltips. When a cycle's session is its valuation floor or
 * volume exhaustion, a row beneath it names the marker with its glyph and selecting value
 * ("Valuation floor · P/E 10.55×").
 *
 * A marker glyph spans several days, so the hovered offset, taken from the pointer's x, is often
 * not the marked session. While the pointer is on a glyph, `hoveredMarker` is that marker, and a
 * line directly under the title names it with its glyph and its full `<title>` text ("2017 Cycle:
 * Valuation floor, P/E 7.85×, 4 Aug 2017"), so hovering any part of a marker identifies it. The
 * tooltip is then drawn even when no row falls at or before the hovered offset.
 */
function CycleChartTooltip({
  active,
  label,
  series,
  metric,
  hoveredMarker,
}: CycleChartTooltipProps): ReactElement | null {
  const x = tooltipOffset(label);
  if (active !== true || x === null) return null;

  const entries: CycleTooltipEntry[] = [];
  for (const item of series) {
    const observed = rowAtOrBefore(item.observed, x);
    if (observed !== undefined) {
      entries.push({
        type: "line",
        key: item.key + "-observed",
        eventDate: item.cycle.event_date,
        name: displayLabel(item.cycle.label),
        color: item.color,
        width: item.strokeWidth,
        row: observed,
      });
      for (const kind of MARKER_KINDS) {
        const marker = item.cycle.markers?.[kind] ?? null;
        if (
          marker !== null &&
          marker.offset_days === observed.point.offset_days &&
          marker.trade_date === observed.point.trade_date
        ) {
          entries.push({
            type: "marker",
            key: `${item.key}-${kind}`,
            kind,
            eventDate: item.cycle.event_date,
            color: item.color,
            text: `${MARKER_META[kind].label} \u00b7 ${markerMetricText(marker)}`,
          });
        }
      }
    }
    if (item.projection !== null) {
      const projected = rowAtOrBefore(item.projection, x);
      if (projected !== undefined) {
        entries.push({
          type: "line",
          key: item.key + "-projection",
          eventDate: item.cycle.event_date,
          name: "Projection \u00b7 " + displayLabel(item.cycle.label),
          color: item.color,
          width: item.projectionStrokeWidth,
          row: projected,
        });
      }
    }
  }
  if (entries.length === 0 && hoveredMarker === null) return null;

  return (
    <div className={TOOLTIP_CLASS}>
      <p className={TOOLTIP_TITLE_CLASS}>On or before day {formatOffset(x)} from event</p>
      {hoveredMarker !== null ? (
        <p
          data-testid="cycle-tooltip-hovered-marker"
          data-event-date={hoveredMarker.cycle.event_date}
          className={TOOLTIP_HOVERED_MARKER_CLASS}
        >
          <span className={TOOLTIP_MARKER_SLOT_CLASS}>
            <svg
              viewBox="0 0 16 16"
              aria-hidden="true"
              focusable="false"
              className={MARKER_SWATCH_CLASS}
            >
              <MarkerGlyph kind={hoveredMarker.kind} cx={8} cy={8} stroke={hoveredMarker.color} />
            </svg>
          </span>
          <span>{markerTitle(hoveredMarker)}</span>
        </p>
      ) : null}
      {entries.length > 0 ? (
        <ul className={TOOLTIP_LIST_CLASS}>
          {entries.map((entry) =>
            entry.type === "marker" ? (
              <li
                key={entry.key}
                data-testid="cycle-tooltip-marker"
                data-event-date={entry.eventDate}
                className={TOOLTIP_MARKER_ROW_CLASS}
              >
                <span className={TOOLTIP_MARKER_SLOT_CLASS}>
                  <svg
                    viewBox="0 0 16 16"
                    aria-hidden="true"
                    focusable="false"
                    className={MARKER_SWATCH_CLASS}
                  >
                    <MarkerGlyph kind={entry.kind} cx={8} cy={8} stroke={entry.color} />
                  </svg>
                </span>
                <span>{entry.text}</span>
              </li>
            ) : (
              <li
                key={entry.key}
                data-testid="cycle-tooltip-row"
                data-event-date={entry.eventDate}
                data-series={entry.row.kind}
                className={TOOLTIP_ROW_CLASS}
              >
                <span className={TOOLTIP_NAME_CLASS}>
                  <LineSwatch
                    color={entry.color}
                    width={entry.width}
                    dash={entry.row.kind === "projection" ? PROJECTION_DASH : undefined}
                  />
                  <span>{entry.name}</span>
                  <span className={TOOLTIP_META_CLASS}>
                    {entry.row.kind === "observed"
                      ? formatDate(entry.row.point.trade_date) + " \u00b7 "
                      : ""}
                    {formatOffset(entry.row.x) + " d"}
                  </span>
                </span>
                <span data-part="value" className={TOOLTIP_VALUE_CLASS}>
                  {formatRowValue(entry.row, metric)}
                </span>
              </li>
            ),
          )}
        </ul>
      ) : null}
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
          <li
            key={overlay.field}
            data-testid="cycle-overlay-tooltip-row"
            data-series={overlay.field}
            className={TOOLTIP_ROW_CLASS}
          >
            <span className={TOOLTIP_NAME_CLASS}>
              <LineSwatch
                color={overlay.color}
                width={OVERLAY_STROKE_WIDTH}
                dash={overlay.dash}
                linecap={overlay.linecap}
              />
              <span>{overlay.label}</span>
            </span>
            <span data-part="value" className={TOOLTIP_VALUE_CLASS}>
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
 * Controls (rendered in every state, so the events can be edited while empty or after an error): a
 * "±180 days" or "±12 months" window preset, removable event chips with a custom-date input (at
 * most 8 events), and a metric toggle across price index, P/E index, turnover index and
 * cumulative foreign flow.
 *
 * States: when the request names no event and no cycle is stored, the API's `no_event_dates` 422
 * is shown as the empty state ("No data yet — run the seeder or an ingestion job"), without a
 * Retry, and adding an event date from it sends a new request. Every other failure shows the API's
 * message and a Retry.
 *
 * Views: one solid line per cycle against calendar days from the event, a dashed "Projection (mean
 * of …)" line for each partial or awaiting cycle that carries a projection, a reference line at the
 * event, and the valuation-floor and volume-exhaustion markers, each named at the top of the chart
 * tooltip while the pointer is on its glyph; an inflation and 91/364-day T-bill overlay for a
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
  // The key of the main-chart marker whose glyph is under the pointer. Set and cleared only by
  // the glyph's pointer handlers, the chart's mouse leave and the controls that change the chart.
  const [hoveredMarkerKey, setHoveredMarkerKey] = useState<string | null>(null);
  // The label of every event date a response has named, by date, recorded whenever the events
  // change, so a re-added stored date keeps its label while the new result loads.
  const [knownLabels, setKnownLabels] = useState<Readonly<Record<string, string>>>({});
  const removeButtonRefs = useRef(new Map<string, HTMLButtonElement>());
  const dateInputRef = useRef<HTMLInputElement>(null);
  const pendingChipFocus = useRef<ChipFocusTarget | null>(null);
  // The result body mounts only once data has loaded, so it is held in state, not a ref object.
  const [bodyElement, setBodyElement] = useState<HTMLDivElement | null>(null);
  const chartMetrics = useChartMetrics(bodyElement);

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
  // `no_event_dates` means neither the request nor the stored cycles hold an event, so it is the
  // empty state: a Retry could never succeed and the API's message names a storage table. Every
  // other failure keeps the error state with its message and Retry.
  const noEvents = state === "error" && error?.code === NO_EVENT_DATES_CODE;
  const frameState: WidgetState = noEvents ? "empty" : state;

  // An earlier payload's stored cycles are gone once the API reports none, so no chip shows them.
  const cycles = noEvents ? undefined : data?.cycles;
  const chips = eventDates ?? cycles?.map((cycle) => cycle.event_date) ?? [];
  const chartModel = useMemo(() => buildChartModel(cycles ?? [], metric), [cycles, metric]);
  // A key the current chart no longer draws names no marker, so it is never shown.
  const hoveredMarker =
    hoveredMarkerKey === null
      ? null
      : (chartModel.markers.find((spec) => spec.key === hoveredMarkerKey) ?? null);

  // The overlay shows the chosen cycle while it still has points, otherwise the first that does.
  // Its swatch takes the cycle's colour by its position among all cycles, as on the main chart,
  // not among the candidates.
  const overlay = useMemo(() => {
    const allCycles = cycles ?? [];
    const candidates = allCycles.filter((cycle) => cycle.points.length > 0);
    const selected =
      candidates.find((cycle) => cycle.event_date === overlayEventDate) ?? candidates[0];
    const selectedIndex = selected === undefined ? -1 : allCycles.indexOf(selected);
    const rows = (selected?.points ?? []).map(
      (point): OverlayRow => ({
        x: point.offset_days,
        inflation_yoy_pct: toChartNumber(point.inflation_yoy_pct),
        tbill_91d_pct: toChartNumber(point.tbill_91d_pct),
        tbill_364d_pct: toChartNumber(point.tbill_364d_pct),
        point,
      }),
    );
    const hasCoordinate = rows.some((row) =>
      OVERLAY_SERIES.some((series) => row[series.field] !== null),
    );
    const yAxis = hasCoordinate ? AUTO_VALUE_AXIS : emptyValueAxis(0);
    return { candidates, selected, selectedIndex, rows, yAxis };
  }, [cycles, overlayEventDate]);
  const overlayCandidates = overlay.candidates;
  const overlayCycle = overlay.selected;
  const overlayCycleIndex = overlay.selectedIndex;
  const overlayRows = overlay.rows;
  const overlayYAxis = overlay.yAxis;

  // Both charts take one chart-level row per day of the shared domain (see GridRow).
  const mainGridRows = useMemo(
    () =>
      buildGridRows(
        chartModel.xDomain,
        chartModel.series.flatMap((item) => [
          { field: item.observedField, rows: item.observed },
          ...(item.projection !== null
            ? [{ field: item.projectionField, rows: item.projection }]
            : []),
        ]),
      ),
    [chartModel],
  );
  const overlayGridRows = useMemo(
    () =>
      buildGridRows(
        chartModel.xDomain,
        OVERLAY_SERIES.map((series) => ({
          field: series.field,
          rows: overlayRows.map((row) => ({ x: row.x, y: row[series.field] })),
        })),
      ),
    [chartModel.xDomain, overlayRows],
  );

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

  // A glyph removed or redrawn while the pointer rests on it, as when a keyboard control changes
  // the chart, never receives pointerleave; each control that changes the chart therefore clears
  // the hovered marker, so a marker drawn again later is not named until the pointer enters it.
  // `handleWindowChange` below does the same when it applies a new preset.
  function handleMetricChange(value: Metric): void {
    setMetric(value);
    setHoveredMarkerKey(null);
  }

  function handleRetry(): void {
    setHoveredMarkerKey(null);
    refetch();
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
    setHoveredMarkerKey(null);
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
    setHoveredMarkerKey(null);
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
      setHoveredMarkerKey(null);
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
              className={
                windowPreset === option.value ? TOGGLE_BUTTON_PRESSED_CLASS : TOGGLE_BUTTON_CLASS
              }
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
              onClick={() => handleMetricChange(option.value)}
              className={
                metric === option.value ? TOGGLE_BUTTON_PRESSED_CLASS : TOGGLE_BUTTON_CLASS
              }
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

      <div
        className={
          chips.length === 0 && frameState === "loading" ? EVENTS_GROUP_LOADING_CLASS : GROUP_CLASS
        }
      >
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
                      className={CHIP_REMOVE_BUTTON_CLASS}
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
            data-empty={customDate === ""}
            className={DATE_INPUT_CLASS}
          />
          <button type="submit" data-testid="cycle-add-date" className={SECONDARY_BUTTON_CLASS}>
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
        {describeUpdateStatus(frameState, data, isFetching)}
      </p>
    </div>
  );

  const metricOption = METRIC_OPTIONS.find((option) => option.value === metric);
  const drawnProjections = chartModel.series.filter((item) => item.projection !== null);
  const hasPlot =
    chartModel.series.some((item) => item.observed.length > 0) || drawnProjections.length > 0;
  const mainChartDescription = describeMainChart(chartModel);
  const hasAnyProjection = (cycles ?? []).some((cycle) => cycle.projection !== null);

  // Chart text and the axis sizes that hold it follow the user's root font size at every body
  // width, as every other text of the widget does through rem units. A body too narrow for the
  // scaled layout (a phone with a large text setting) scrolls the charts instead of shrinking
  // their text (see `minChartWidth`).
  const bodyWidth = chartMetrics.width;
  const textScale = chartMetrics.rem / DEFAULT_REM_PX;
  const axisTick = { ...AXIS_TICK, fontSize: AXIS_TICK.fontSize * textScale };
  const axisLabelStyle = { ...AXIS_LABEL_STYLE, fontSize: AXIS_LABEL_STYLE.fontSize * textScale };
  const xAxisHeight = X_AXIS_HEIGHT * textScale;
  // The right margin holds half of the last offset tick label, so it grows with the text too.
  const chartMargin = { ...CHART_MARGIN, right: CHART_MARGIN.right * textScale };
  const valuePadding = AXIS_EDGE_PADDING * Math.max(1, textScale);
  const yAxisPadding = { top: valuePadding, bottom: valuePadding };
  // On a narrow body the value axis narrows too, identically on both charts so their days align,
  // and the offset axis gets only as many ticks as the plot width holds.
  const compactYAxis = bodyWidth !== null && bodyWidth < COMPACT_BODY_REM * chartMetrics.rem;
  const yAxisWidth = (compactYAxis ? Y_AXIS_COMPACT_WIDTH : Y_AXIS_WIDTH) * textScale;
  // Chart px outside the plot: the value axis, both chart margins and the offset axis padding.
  const plotInset = yAxisWidth + chartMargin.left + chartMargin.right + 2 * AXIS_EDGE_PADDING;
  const offsetTickSpacing = OFFSET_TICK_SPACING_REM * DEFAULT_REM_PX * textScale;
  /**
   * The narrowest width in px that holds the scaled chart layout: `plotInset` plus a plot
   * of {@link MIN_OFFSET_TICKS} offset ticks at the scaled {@link OFFSET_TICK_SPACING_REM}
   * spacing, 188px at the default text size with the compact value axis. Both charts render at
   * least this wide, so they keep identical widths and a given day at the same x in each; on a
   * narrower body each scrolls horizontally inside its own wrapper.
   */
  const minChartWidth = plotInset + MIN_OFFSET_TICKS * offsetTickSpacing;
  const needsScroll = bodyWidth !== null && bodyWidth < minChartWidth;
  const chartWidth = bodyWidth === null ? null : Math.max(bodyWidth, minChartWidth);
  const tickBudget =
    chartWidth === null
      ? MAX_OFFSET_TICKS
      : Math.floor((chartWidth - plotInset) / offsetTickSpacing);
  const maxOffsetTicks = Math.min(MAX_OFFSET_TICKS, Math.max(MIN_OFFSET_TICKS, tickBudget));
  const xTicks = useMemo(
    () => offsetTicks(chartModel.xDomain, maxOffsetTicks),
    [chartModel.xDomain, maxOffsetTicks],
  );
  // Axis and event labels shared by both charts.
  const offsetAxisLabel = {
    value: "Days from event",
    position: "insideBottom",
    ...axisLabelStyle,
  } as const;
  // The label sits left of the event line, unless 0 is the domain's left edge, where it goes
  // right of it, off the Y axis.
  const eventLabel = {
    value: "Event",
    position: chartModel.xDomain?.[0] === 0 ? "insideTopLeft" : "insideTopRight",
    ...axisLabelStyle,
  } as const;
  // The rotated title's centre moves in from the chart's left edge as the text grows, so its
  // glyphs stay inside the chart.
  const valueAxisTitle = (value: string) =>
    ({
      value,
      angle: -90,
      position: "insideLeft",
      textAnchor: "middle",
      offset: (chartMargin.left + AXIS_TITLE_OFFSET) * textScale - chartMargin.left,
      ...axisLabelStyle,
    }) as const;

  const body = (
    <div
      ref={setBodyElement}
      className={isFetching ? BODY_FETCHING_CLASS : BODY_CLASS}
      aria-busy={isFetching}
    >
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
          <div
            data-testid="cycle-main-chart"
            role={needsScroll ? "region" : undefined}
            aria-label={
              needsScroll
                ? (metricOption?.label ?? "") + " chart, scrolls horizontally"
                : undefined
            }
            tabIndex={needsScroll ? 0 : undefined}
            className={needsScroll ? MAIN_CHART_SCROLL_CLASS : MAIN_CHART_CLASS}
          >
            <ResponsiveContainer width="100%" height="100%" minWidth={minChartWidth}>
              <LineChart
                data={mainGridRows}
                margin={chartMargin}
                title={(metricOption?.label ?? "") + " of each cycle by days from the event"}
                desc={mainChartDescription}
                onMouseLeave={() => setHoveredMarkerKey(null)}
              >
                <CartesianGrid stroke={CHART_COLORS.grid} strokeDasharray="3 3" />
                <XAxis
                  type="number"
                  dataKey="x"
                  domain={chartModel.xDomain ?? ["dataMin", "dataMax"]}
                  padding={X_AXIS_PADDING}
                  allowDecimals={false}
                  ticks={xTicks}
                  tick={axisTick}
                  stroke={CHART_COLORS.axis}
                  tickFormatter={formatOffsetTick}
                  height={xAxisHeight}
                  label={offsetAxisLabel}
                />
                <YAxis
                  type="number"
                  domain={chartModel.yAxis.domain}
                  ticks={chartModel.yAxis.ticks}
                  allowDataOverflow={chartModel.yAxis.allowDataOverflow}
                  padding={yAxisPadding}
                  tick={axisTick}
                  stroke={CHART_COLORS.axis}
                  width={yAxisWidth}
                  tickFormatter={
                    metric === "cumulative_foreign_net_kes"
                      ? formatKesAxisTick
                      : formatIndexAxisTick
                  }
                  label={valueAxisTitle(metricOption?.axisLabel ?? "")}
                />
                {/* filterNull is off: Recharts hides the tooltip when every payload value is
                    null, but the content resolves its own rows and shows a null as "—". */}
                <Tooltip
                  isAnimationActive={false}
                  filterNull={false}
                  cursor={{ stroke: CHART_COLORS.muted }}
                  content={(props) => (
                    <CycleChartTooltip
                      active={props.active}
                      label={props.label}
                      series={chartModel.series}
                      metric={metric}
                      hoveredMarker={hoveredMarker}
                    />
                  )}
                />
                <ReferenceLine x={0} stroke={CHART_COLORS.axis} label={eventLabel} />
                {/* activeDot is off: a line has a value only on its own sessions, so the dot
                    would vanish on other days, while the tooltip shows each line's session on
                    or before the hovered day. Recharts copies a Line's data-* props onto its
                    path.recharts-line-curve, so each drawn path names its cycle and series. */}
                {chartModel.series.map((item) =>
                  item.observed.length > 0 ? (
                    <Line
                      key={item.key + "-observed"}
                      data-testid="cycle-line"
                      data-event-date={item.cycle.event_date}
                      data-series="observed"
                      dataKey={item.observedField}
                      name={displayLabel(item.cycle.label)}
                      type="linear"
                      stroke={item.color}
                      strokeWidth={item.strokeWidth}
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
                    data-testid="cycle-line"
                    data-event-date={item.cycle.event_date}
                    data-series="projection"
                    dataKey={item.projectionField}
                    name={item.projectionName ?? "Projection"}
                    type="linear"
                    stroke={item.color}
                    strokeWidth={item.projectionStrokeWidth}
                    strokeDasharray={PROJECTION_DASH}
                    dot={false}
                    activeDot={false}
                    connectNulls
                    isAnimationActive={false}
                  />
                ))}
                {/* Recharts 3 has no `isFront`: a ReferenceDot's default z-index (600) already
                    draws it above every Line (400). Each dot sits at its marker's true session;
                    its shape lays out all the markers, in cycle then kind order, and moves this
                    glyph vertically to the nearest candidate within the bounded search that
                    clears the glyphs laid out before it, or to the least-overlapping one when
                    none does. The leaders back to the moved glyphs' sessions are drawn by
                    MarkerConnectors in the layer just below (599), beneath every glyph. */}
                <MarkerConnectors markers={chartModel.markers} />
                {chartModel.markers.map((spec) => (
                  <ReferenceDot
                    key={spec.key}
                    x={spec.marker.offset_days}
                    y={spec.y}
                    r={MARKER_RADIUS}
                    ifOverflow="extendDomain"
                    shape={markerShape(spec, chartModel.markers, setHoveredMarkerKey)}
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
              <LineSwatch color={item.color} width={item.strokeWidth} />
              {/* One text run, so the label and its note wrap together on a narrow card. */}
              <span>
                {displayLabel(item.cycle.label)}
                {item.observed.length === 0 ? (
                  <>
                    {" "}
                    <span className={TOOLTIP_META_CLASS}>
                      {`(${noSessionNote(item.cycle, noSessionCause(item.cycle, data))})`}
                    </span>
                  </>
                ) : null}
              </span>
            </li>
          ))}
          {drawnProjections.map((item) => (
            <li
              key={item.key + "-projection"}
              data-testid="cycle-projection-legend"
              data-event-date={item.cycle.event_date}
              className={LEGEND_ITEM_CLASS}
            >
              <LineSwatch
                color={item.color}
                width={item.projectionStrokeWidth}
                dash={PROJECTION_DASH}
              />
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
            <li className={TOOLTIP_META_CLASS}>
              Each marker is outlined in its cycle&apos;s colour
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
              <LineSwatch
                color={seriesColor(overlayCycleIndex)}
                width={seriesStrokeWidth(overlayCycleIndex)}
              />
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
            <div
              data-testid="cycle-overlay-chart"
              role={needsScroll ? "region" : undefined}
              aria-label={
                needsScroll ? "Inflation and T-bill overlay chart, scrolls horizontally" : undefined
              }
              tabIndex={needsScroll ? 0 : undefined}
              className={needsScroll ? OVERLAY_CHART_SCROLL_CLASS : OVERLAY_CHART_CLASS}
            >
              <ResponsiveContainer width="100%" height="100%" minWidth={minChartWidth}>
                <LineChart
                  data={overlayGridRows}
                  margin={chartMargin}
                  title={
                    "12-month inflation and 91- and 364-day T-bill yields around " +
                    displayLabel(overlayCycle.label)
                  }
                  desc="Percentage points as of each session, on the main chart's day axis."
                >
                  <CartesianGrid stroke={CHART_COLORS.grid} strokeDasharray="3 3" />
                  {/* The overlay shares the main chart's offset domain, ticks and padding (and
                      its Y axis width), so both charts place a given day at the same position. */}
                  <XAxis
                    type="number"
                    dataKey="x"
                    domain={chartModel.xDomain ?? ["dataMin", "dataMax"]}
                    padding={X_AXIS_PADDING}
                    allowDecimals={false}
                    ticks={xTicks}
                    tick={axisTick}
                    stroke={CHART_COLORS.axis}
                    tickFormatter={formatOffsetTick}
                    height={xAxisHeight}
                    label={offsetAxisLabel}
                  />
                  <YAxis
                    type="number"
                    domain={overlayYAxis.domain}
                    ticks={overlayYAxis.ticks}
                    allowDataOverflow={overlayYAxis.allowDataOverflow}
                    padding={yAxisPadding}
                    tick={axisTick}
                    stroke={CHART_COLORS.axis}
                    width={yAxisWidth}
                    tickFormatter={formatPctAxisTick}
                    label={valueAxisTitle("Rate (%)")}
                  />
                  {/* filterNull is off, as on the main chart, so null rates show as "—". */}
                  <Tooltip
                    isAnimationActive={false}
                    filterNull={false}
                    cursor={{ stroke: CHART_COLORS.muted }}
                    content={(props) => (
                      <OverlayTooltip
                        active={props.active}
                        label={props.label}
                        rows={overlayRows}
                      />
                    )}
                  />
                  <ReferenceLine x={0} stroke={CHART_COLORS.axis} label={eventLabel} />
                  {/* activeDot is off, as on the main chart: a rate has a value only on the
                      cycle's sessions, while the tooltip shows the session on or before the
                      hovered day. As there, the data-* props name each drawn path's series. */}
                  {OVERLAY_SERIES.map((overlay) => (
                    <Line
                      key={overlay.field}
                      data-testid="cycle-overlay-line"
                      data-series={overlay.field}
                      dataKey={overlay.field}
                      name={overlay.label}
                      type="linear"
                      stroke={overlay.color}
                      strokeWidth={OVERLAY_STROKE_WIDTH}
                      strokeDasharray={overlay.dash}
                      strokeLinecap={overlay.linecap}
                      dot={false}
                      activeDot={false}
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
                  <LineSwatch
                    color={overlay.color}
                    width={OVERLAY_STROKE_WIDTH}
                    dash={overlay.dash}
                    linecap={overlay.linecap}
                  />
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
                    <span className={CYCLE_CELL_CLASS}>
                      <span className={CYCLE_SWATCH_SLOT_CLASS}>
                        <LineSwatch color={seriesColor(index)} width={seriesStrokeWidth(index)} />
                      </span>
                      <span>
                        <span className={CYCLE_LABEL_CLASS}>{displayLabel(cycle.label)}</span>
                        {labelNamesDate(cycle.label, cycle.event_date) ? null : (
                          <>
                            {" "}
                            <span className={CYCLE_DATE_CLASS}>
                              {formatDate(cycle.event_date)}
                            </span>
                          </>
                        )}
                      </span>
                    </span>
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
                  <td
                    className={cycle.flags.length > 0 ? TD_FLAGS_CLASS : TD_FLAGS_EMPTY_CLASS}
                  >
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
      state={frameState}
      error={noEvents ? null : error}
      onRetry={noEvents ? undefined : handleRetry}
      className={className}
      skeletonClassName={SKELETON_HEIGHT_CLASS}
      controls={controls}
    >
      {body}
    </WidgetFrame>
  );
}

