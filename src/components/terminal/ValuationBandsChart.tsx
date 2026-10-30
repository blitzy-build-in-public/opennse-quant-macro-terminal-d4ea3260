"use client";

import { useCallback, useMemo } from "react";
import type { ReactElement } from "react";
import {
  CartesianGrid,
  ComposedChart,
  Line,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { TooltipContentProps } from "recharts";

import { TerminalApiError } from "./lib/apiClient";
import { AXIS_TICK, CHART_COLORS } from "./lib/chartTheme";
import {
  formatDate,
  formatDecimal,
  formatInteger,
  formatMonth,
  toChartNumber,
} from "./lib/format";
import { useTerminalQuery } from "./lib/useTerminalQuery";
import { PartialCoverageNote, WidgetFrame } from "./shared/WidgetFrame";
import { KPI_LABEL_CLASS, KPI_TILE_CLASS, KPI_VALUE_CLASS } from "./shared/headingAndTileClasses";
import type {
  BandMetric,
  BandStats,
  SeriesPoint,
  SubjectType,
  TerminalWidgetProps,
  ValuationBandsResponse,
  WidgetState,
} from "./types";

/** Props of {@link ValuationBandsChart}. */
export interface ValuationBandsChartProps extends TerminalWidgetProps {
  /**
   * NSE ticker of the counter to band, for example `"SCOM"`. Pass exactly one of `ticker` and
   * `sector`. An empty or blank string counts as not passed, so a page can forward its selection
   * before anything is selected.
   */
  ticker?: string;
  /**
   * Sector to band as one capitalization-weighted aggregate, spelled as stored, for example
   * `"Banking"`. Pass exactly one of `ticker` and `sector`; an empty or blank string counts as not
   * passed.
   */
  sector?: string;
  /** The banded multiple: `"pe"` (P/E) or `"pb"` (P/B). Default `"pe"`. */
  metric?: BandMetric;
  /**
   * Lookback in whole calendar years, ending at the latest trading session, from 1 to 15.
   * Default 5. The API validates the range; a value outside it shows the error state.
   */
  lookbackYears?: number;
}

const BANDS_PATH = "/market/valuation-bands";
const DEFAULT_METRIC: BandMetric = "pe";
const DEFAULT_LOOKBACK_YEARS = 5;
const NO_SUBJECT_MESSAGE = "Select a counter or sector to see its valuation bands";

const METRIC_LABELS: Record<BandMetric, string> = { pe: "P/E", pb: "P/B" };
const SUBJECT_TYPE_LABELS: Record<SubjectType, string> = { ticker: "Counter", sector: "Sector" };

/** Explanation of every non-`ok` status, shown above the still-drawn series. */
const STATUS_NOTES: Readonly<Record<string, string>> = {
  insufficient_history: "Fewer than 60 valid observations — bands not computed",
  no_positive_earnings: "No positive earnings in the lookback — P/E undefined",
  no_positive_equity: "No positive book equity in the lookback — P/B undefined",
};

// Passing both subjects is a programming error in the host page. It is reported locally, with no
// request and no Retry, because sending the same props again cannot succeed.
const BOTH_SUBJECTS_ERROR = new TerminalApiError({
  status: 0,
  code: "exactly_one_subject",
  message: "Pass exactly one of ticker or sector",
});

// Every class below is a complete static string, so Tailwind's source scan and a host's `@source`
// directive or `content` glob find each one.
const BODY_CLASS =
  "flex flex-col gap-3 motion-safe:transition-opacity motion-safe:duration-150 motion-safe:ease-out";
// While a new lookback for the same subject loads, the previous bands stay on screen, dimmed.
const BODY_UPDATING_CLASS =
  "flex flex-col gap-3 opacity-60 motion-safe:transition-opacity motion-safe:duration-150 motion-safe:ease-out";
// The loaded body's height for SCOM, measured on the standalone screener page with the seeded data
// at each viewport width where it reflows, so the card keeps its height when the bands arrive
// after each change of subject.
const SKELETON_HEIGHT_CLASS =
  "h-[59rem] min-[330px]:h-[38.5rem] min-[466px]:h-[33.375rem] min-[602px]:h-[28.25rem] min-[1024px]:h-[33.375rem] min-[1188px]:h-[28.25rem]";
const META_CLASS = "text-xs text-slate-400";
const UPDATING_CLASS = "text-sky-300";
const STATUS_NOTE_CLASS = "text-xs text-amber-300";
// Recharts' accessibility layer renders the chart as `<svg class="recharts-surface" tabindex="0">`,
// which the keyboard reaches and navigates with the arrow keys. Recharts 3 also renders its z-index
// layers as `<g tabindex="-1">`: Tab never reaches them, but a mouse click on plotted data focuses
// one, and a click on bare plot area focuses the surface. The browser outlines a focused SVG
// element even for a mouse click, and an arrow key pressed after the click makes that element
// :focus-visible. So the layers' outline and the surface's pointer-focus outline are hidden, and
// the widgets' sky-400 ring is drawn on the surface whenever it or a descendant is :focus-visible:
// it appears for keyboard use and not for a mouse click. The solid style is explicit because
// Tailwind 3's `outline-2` sets only the width.
// In forced-colors mode browsers keep an SVG's own colours (`forced-color-adjust:
// preserve-parent-color`) while the card behind it takes the user's Canvas colour, so colours
// chosen for the slate-900 surface can fade out on a light Canvas. Under forced colors only, the
// ring is drawn in Highlight, as the page's other controls are; every chart text (tick labels,
// axis title and band labels), the axis and tick lines and the hover cursor in CanvasText; and
// the weekly line and its active dot in Highlight. Band lines that need it take a system colour
// through BAND_LINES. The grid, which only guides the eye as every band and tick is labelled, is
// drawn in CanvasText at 30% opacity: about 2:1 on any Canvas, faint as it is on the slate-900
// surface, where its slate-700 would be darker than the ±1σ lines on a white Canvas.
const CHART_CLASS =
  "h-64 w-full [&_g[tabindex='-1']]:outline-none [&_.recharts-surface:focus:not(:focus-visible)]:outline-none [&_.recharts-surface:is(:focus-visible,:has(:focus-visible))]:[outline-style:solid] [&_.recharts-surface:is(:focus-visible,:has(:focus-visible))]:outline-2 [&_.recharts-surface:is(:focus-visible,:has(:focus-visible))]:outline-offset-2 [&_.recharts-surface:is(:focus-visible,:has(:focus-visible))]:outline-sky-400 forced-colors:[&_.recharts-surface:is(:focus-visible,:has(:focus-visible))]:outline-[color:Highlight] forced-colors:[&_text]:[fill:CanvasText] forced-colors:[&_.recharts-cartesian-axis-line]:[stroke:CanvasText] forced-colors:[&_.recharts-cartesian-axis-tick-line]:[stroke:CanvasText] forced-colors:[&_.recharts-tooltip-cursor]:[stroke:CanvasText] forced-colors:[&_.recharts-line-curve]:[stroke:Highlight] forced-colors:[&_.recharts-active-dot_.recharts-dot]:[fill:Highlight] forced-colors:[&_.recharts-cartesian-grid_line]:[stroke:CanvasText] forced-colors:[&_.recharts-cartesian-grid_line]:[stroke-opacity:0.3]";
// The custom tooltip mirrors TOOLTIP_CONTENT_STYLE (slate-900 surface, slate-700 border, slate-300
// text, 12px) with utility classes instead of an inline style.
const TOOLTIP_CLASS =
  "rounded border border-slate-700 bg-slate-900 px-2 py-1 text-xs text-slate-300 shadow-lg";
const TOOLTIP_VALUE_CLASS = "font-medium tabular-nums text-slate-100";
// Auto-filling columns reflow with the widget's own width, whatever the host's layout or viewport.
// Each statistic is a KPI tile shared with the other widgets; an 8rem track keeps the widest
// value, a 100.00% percentile, on one line inside its tile.
const STATS_CLASS = "m-0 grid grid-cols-[repeat(auto-fill,minmax(8rem,1fr))] gap-2";
const STAT_VALUE_CLASS = `${KPI_VALUE_CLASS} text-slate-100`;
const FLAGS_CLASS = "flex flex-wrap gap-1";
const FLAG_CLASS = "rounded bg-slate-800 px-1.5 py-0.5 font-mono text-xs text-slate-400";

/** Which subject the props select. */
type SubjectSelection =
  | { kind: "none" }
  | { kind: "both" }
  | { kind: "one"; type: SubjectType; name: string };

/** One plotted point: the float coordinate for Recharts and the exact string for display. */
interface BandsRow {
  date: string;
  y: number | null;
  raw: string | null;
}

/** Decimal-valued statistics, each formatted from the API's exact string. */
type DecimalStatName = Exclude<keyof BandStats, "observations">;

const STAT_ITEMS: ReadonlyArray<{
  name: DecimalStatName;
  label: string;
  kind: "ratio" | "pct";
}> = [
  { name: "current", label: "Current", kind: "ratio" },
  { name: "current_percentile", label: "Percentile", kind: "pct" },
  { name: "mean", label: "Mean", kind: "ratio" },
  { name: "stdev", label: "Std dev", kind: "ratio" },
  { name: "min", label: "Min", kind: "ratio" },
  { name: "median", label: "Median", kind: "ratio" },
  { name: "max", label: "Max", kind: "ratio" },
];

/**
 * Horizontal band lines. A rich multiple (+2σ) is rose and a cheap one (−2σ) emerald; every label
 * names its line, so colour is never the only cue. The ±1σ labels use slate-400 rather than the
 * slate-500 stroke, which is below 4.5:1 for 10px text on the slate-900 surface.
 *
 * Under forced colors, where the chart keeps these strokes on the user's Canvas, the slate-300
 * mean and the emerald −2σ (1.49:1 and 2.47:1 on a white Canvas) are drawn in CanvasText, the
 * mean still solid and the −2σ still dashed. The rose +2σ and the slate-500 ±1σ keep their
 * strokes, above 3:1 on a white and on a black Canvas. Every label is CanvasText there.
 */
const BAND_LINES: ReadonlyArray<{
  name: "plus_2sd" | "plus_1sd" | "mean" | "minus_1sd" | "minus_2sd";
  label: string;
  stroke: string;
  labelFill: string;
  dashed: boolean;
  /** `ReferenceLine` class drawing the line in a system colour under forced colors. */
  className?: string;
}> = [
  {
    name: "plus_2sd",
    label: "+2σ",
    stroke: CHART_COLORS.negative,
    labelFill: CHART_COLORS.negative,
    dashed: true,
  },
  {
    name: "plus_1sd",
    label: "+1σ",
    stroke: CHART_COLORS.muted,
    labelFill: CHART_COLORS.axis,
    dashed: true,
  },
  {
    name: "mean",
    label: "Mean",
    stroke: CHART_COLORS.text,
    labelFill: CHART_COLORS.text,
    dashed: false,
    className: "forced-colors:[&_.recharts-reference-line-line]:[stroke:CanvasText]",
  },
  {
    name: "minus_1sd",
    label: "−1σ",
    stroke: CHART_COLORS.muted,
    labelFill: CHART_COLORS.axis,
    dashed: true,
  },
  {
    name: "minus_2sd",
    label: "−2σ",
    stroke: CHART_COLORS.positive,
    labelFill: CHART_COLORS.positive,
    dashed: true,
    className: "forced-colors:[&_.recharts-reference-line-line]:[stroke:CanvasText]",
  },
];

const BAND_DASH = "4 4";
// Band labels sit in the right margin, outside the plot, because inside it the weekly line runs
// through them wherever the multiple crosses a band. 76px holds a label such as "+2σ 123.45×".
// The 4px left margin keeps the ascent of the rotated axis title inside the svg.
const CHART_MARGIN = { top: 8, right: 76, bottom: 0, left: 4 };

/**
 * Title of the multiple axis: the metric and its `×` unit, as in "P/E (×)". It is rotated and
 * centred on the axis height in a column of its own at the axis's left edge; the axis is sized
 * to its content (`width="auto"`), which counts the title, the gap and the widest tick label, so
 * the title never overlaps or crowds out a tick label such as "1,234.00".
 */
function yAxisTitle(metric: BandMetric) {
  return {
    value: `${METRIC_LABELS[metric]} (×)`,
    angle: -90,
    position: "insideLeft" as const,
    // Centres the rotated title on the axis; the position alone anchors it at its start.
    textAnchor: "middle" as const,
    fill: CHART_COLORS.axis,
    fontSize: 11,
  };
}

const Y_AXIS_TITLES: Readonly<Record<BandMetric, ReturnType<typeof yAxisTitle>>> = {
  pe: yAxisTitle("pe"),
  pb: yAxisTitle("pb"),
};

/** The trimmed value, or `undefined` when it is absent, empty or blank. */
function presentText(value: string | undefined): string | undefined {
  if (value === undefined) return undefined;
  const trimmed = value.trim();
  return trimmed === "" ? undefined : trimmed;
}

function selectSubject(ticker: string | undefined, sector: string | undefined): SubjectSelection {
  const tickerName = presentText(ticker);
  const sectorName = presentText(sector);
  if (tickerName !== undefined && sectorName !== undefined) return { kind: "both" };
  if (tickerName !== undefined) return { kind: "one", type: "ticker", name: tickerName };
  if (sectorName !== undefined) return { kind: "one", type: "sector", name: sectorName };
  return { kind: "none" };
}

/** An `ok` response always holds at least 60 values; any other status is explained, not empty. */
function isEmptyBands(data: ValuationBandsResponse): boolean {
  return data.series.length === 0 && data.status === "ok";
}

/**
 * Whether a payload is the one the props ask for. Compared case-insensitively, because the API
 * returns the canonical ticker spelling.
 */
function describes(
  data: ValuationBandsResponse,
  subject: { type: SubjectType; name: string },
  metric: BandMetric,
): boolean {
  return (
    data.subject_type === subject.type &&
    data.metric === metric &&
    data.subject.toUpperCase() === subject.name.toUpperCase()
  );
}

function statusNoteOf(status: string): string | null {
  if (status === "ok") return null;
  return STATUS_NOTES[status] ?? `Bands not computed (${status})`;
}

function toRows(series: readonly SeriesPoint[]): BandsRow[] {
  return series.map((point) => ({
    date: point.date,
    y: toChartNumber(point.value),
    raw: point.value,
  }));
}

function isBandsRow(value: unknown): value is BandsRow {
  return (
    typeof value === "object" &&
    value !== null &&
    "date" in value &&
    typeof value.date === "string" &&
    "raw" in value &&
    (typeof value.raw === "string" || value.raw === null)
  );
}

function monthTick(value: unknown): string {
  return typeof value === "string" ? formatMonth(value) : "";
}

/** The fewest fraction digits of a multiple-axis tick: the 2 dp of the API's ratios. */
const RATIO_TICK_MIN_FRACTION_DIGITS = 2;

/**
 * `YAxis` tick of the multiple axis, grouped and signed by `formatDecimal`, so the axis reads like
 * the 2 dp tooltip and band labels. A tick of `axis` (see {@link ratioAxis}) is written from its
 * exact decimal text, with the axis's one `fractionDigits` on every tick (`2.25`, `4.50`,
 * `20.00`, `100,000,000,000,000.10`, `−2.50`), so no float rounding reaches the label at any
 * magnitude. A tick with no exact text, one of Recharts' automatic domain, is written as a plain
 * decimal rounded to {@link RATIO_TICK_MIN_FRACTION_DIGITS}, which removes float noise such as
 * `0.15000000000000002`, and never in exponent notation. The `×` unit is in the axis title.
 * Non-numbers give `""`.
 */
function formatRatioTick(value: unknown, axis: RatioAxis | null): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "";
  const exact = axis?.tickTexts.get(value);
  if (exact !== undefined) return formatDecimal(exact, "ratio", { unit: false });
  // toFixed switches to exponent notation from 10^21; BigInt prints every digit of such an
  // integral value, whose fraction digits are all zero.
  const text =
    Math.abs(value) >= 1e21
      ? BigInt(Math.round(value)).toString() + "." + "0".repeat(RATIO_TICK_MIN_FRACTION_DIGITS)
      : value.toFixed(RATIO_TICK_MIN_FRACTION_DIGITS);
  return formatDecimal(text, "ratio", { unit: false });
}

/**
 * Round tick steps of one decade, ascending: 1, 2, 2.5 and 5 × 10^k. Each is an integer count of
 * `units` of 10^(k + shift), so every tick is built from integers.
 */
const NICE_STEPS: ReadonlyArray<{ units: number; shift: number }> = [
  { units: 1, shift: 0 },
  { units: 2, shift: 0 },
  { units: 25, shift: -1 },
  { units: 5, shift: 0 },
];
/** Intervals the multiple axis aims for: the step whose axis has the nearest count is used. */
const RATIO_AXIS_TARGET_INTERVALS = 5;
/** Most intervals a candidate step may give, which bounds the ticks one axis builds. */
const RATIO_AXIS_MAX_INTERVALS = 50;
/**
 * Largest magnitude, 2^50, of a tick's integer count of its step's power of ten. Up to it every
 * count and tick index is an exact safe integer, with an eighth of `Number.MAX_SAFE_INTEGER` to
 * spare; ticks whose counts differ, by at least 1 in 2^50, are distinct floats, which resolve 1 in
 * 2^53; and a float index such as `low / step` is within a quarter of its exact value.
 */
const RATIO_AXIS_MAX_TICK_COUNT = 2 ** 50;
/**
 * Least clearance, in steps, between an axis bound and the nearest point or band line, so neither
 * is drawn on, or a pixel or two from, the axis baseline or the plot's top edge.
 */
const RATIO_AXIS_CLEARANCE_STEPS = 0.1;
/** Spread, as a share of their magnitude, of values that are all equal (1 when they are 0). */
const RATIO_AXIS_FLAT_PAD = 0.1;

/** The explicit domain and ticks of the multiple axis, and the fraction digits of its ticks. */
interface RatioAxis {
  domain: [number, number];
  ticks: number[];
  fractionDigits: number;
  /** Each tick's exact decimal text, with `fractionDigits` fraction digits, keyed by the tick. */
  tickTexts: ReadonlyMap<number, string>;
}

/**
 * `units × 10^exponent`. An integer times or divided by an exact power of ten is rounded once, to
 * the float nearest the decimal value, so a tick such as `0.3` carries no accumulated noise.
 */
function scaledByPowerOfTen(units: number, exponent: number): number {
  return exponent >= 0 ? units * 10 ** exponent : units / 10 ** -exponent;
}

/**
 * `count × 10^exponent` as a plain decimal string with `fractionDigits` fraction digits, or more
 * where the value needs them, written from the digits of the safe integer `count`, so it is exact
 * at any magnitude: `(-25, -1, 2)` gives `"-2.50"` and `(1, 14, 2)` `"100000000000000.00"`.
 */
function scaledDecimalText(count: number, exponent: number, fractionDigits: number): string {
  const sign = count < 0 ? "-" : "";
  const shift = Math.max(0, -exponent);
  const padded = String(Math.abs(count)).padStart(shift + 1, "0");
  let whole = padded.slice(0, padded.length - shift);
  if (exponent > 0 && count !== 0) whole += "0".repeat(exponent);
  const fraction = padded.slice(padded.length - shift).padEnd(fractionDigits, "0");
  return sign + whole + (fraction === "" ? "" : "." + fraction);
}

/** Every value the multiple axis must show: each weekly point and each drawn band line. */
function axisValues(rows: readonly BandsRow[], stats: BandStats | null): Array<number | null> {
  const values = rows.map((row) => row.y);
  if (stats !== null) {
    for (const band of BAND_LINES) values.push(toChartNumber(stats[band.name]));
  }
  return values;
}

/**
 * Domain and ticks of the multiple axis over `values` (see {@link axisValues}), or null when none
 * is finite or no step qualifies, which leaves the axis to Recharts' automatic domain.
 *
 * - The step is a round 1, 2, 2.5 or 5 × 10^k: the one whose axis has the interval count nearest
 *   {@link RATIO_AXIS_TARGET_INTERVALS}, and of two equally near the one spanning less.
 * - The floor is the largest multiple of the step, and the ceiling the smallest, that lies at
 *   least {@link RATIO_AXIS_CLEARANCE_STEPS} of a step beyond the lowest and the highest value,
 *   so no point or band line is drawn on the axis baseline or the plot's top edge. When every
 *   value is positive the floor is never below 0, which still lies strictly below them.
 * - Values that are all equal are first spread by {@link RATIO_AXIS_FLAT_PAD} of their
 *   magnitude either side, or by 1 at zero.
 * - Each tick is an integer count of the step's power of ten, scaled once by that power, so ticks
 *   carry no float noise, and `fractionDigits` is what the step needs, at least
 *   {@link RATIO_TICK_MIN_FRACTION_DIGITS}. `tickTexts` writes each tick exactly from its count.
 * - The bound: a step qualifies only when the floor's and the ceiling's counts are at most
 *   {@link RATIO_AXIS_MAX_TICK_COUNT} (2^50) in magnitude, it gives at most
 *   {@link RATIO_AXIS_MAX_INTERVALS} intervals, every tick is finite and every value lies strictly
 *   between the floor and the ceiling. The search spans at most four decades and starts no finer
 *   than the values' magnitude allows, so a step qualifies for any finite values whose span and
 *   ceiling stay below `Number.MAX_VALUE` and whose step stays above 10^-308, and the ticks come
 *   from a count loop of at most RATIO_AXIS_MAX_INTERVALS + 1 iterations. Where no step
 *   qualifies, as at `Number.MAX_VALUE` itself, the result is null.
 *
 * @example ratioAxis([0.75, 0.83, 1.33]) gives domain `[0.6, 1.4]`, ticks `0.6, 0.8, 1, 1.2, 1.4`
 * and 2 fraction digits.
 */
function ratioAxis(values: Iterable<number | null>): RatioAxis | null {
  let lowest = Number.POSITIVE_INFINITY;
  let highest = Number.NEGATIVE_INFINITY;
  for (const value of values) {
    if (value === null || !Number.isFinite(value)) continue;
    if (value < lowest) lowest = value;
    if (value > highest) highest = value;
  }
  if (lowest > highest) return null;
  const pad = lowest === highest ? Math.abs(lowest) * RATIO_AXIS_FLAT_PAD || 1 : 0;
  const low = lowest - pad;
  const high = highest + pad;
  const span = high - low;
  if (!Number.isFinite(span) || span <= 0) return null;

  // Steps from a hundredth to fifty times the power of ten at or below the span cover every
  // interval count near the target. A step below 10^finest needs counts above the bound at the
  // values' magnitude, so the search starts no finer; it reaches 10^(finest + 2), whose counts are
  // within a tenth of the bound, so a step with few intervals is always among the candidates.
  const decade = Math.floor(Math.log10(span));
  const magnitude = Math.max(Math.abs(low), Math.abs(high));
  const finest = Math.floor(Math.log10(magnitude / RATIO_AXIS_MAX_TICK_COUNT));
  const first = Math.max(decade - 2, finest);
  const last = Math.max(decade + 1, finest + 2);
  let best: {
    lower: number;
    intervals: number;
    units: number;
    exponent: number;
    miss: number;
    extent: number;
  } | null = null;
  for (let exponent = first; exponent <= last; exponent += 1) {
    for (const nice of NICE_STEPS) {
      const stepExponent = exponent + nice.shift;
      const step = scaledByPowerOfTen(nice.units, stepExponent);
      if (!Number.isFinite(step) || step <= 0) continue;
      const clearance = RATIO_AXIS_CLEARANCE_STEPS * step;
      let lower = Math.floor(low / step - RATIO_AXIS_CLEARANCE_STEPS);
      let upper = Math.ceil(high / step + RATIO_AXIS_CLEARANCE_STEPS);
      // Near the count bound these quotients are only within a quarter of their exact value, so
      // the clearance is checked on the bound ticks and restored by one more step where rounding
      // lost it.
      if (low - scaledByPowerOfTen(lower * nice.units, stepExponent) < clearance) lower -= 1;
      if (scaledByPowerOfTen(upper * nice.units, stepExponent) - high < clearance) upper += 1;
      if (lowest > 0) lower = Math.max(0, lower);
      const floorTick = scaledByPowerOfTen(lower * nice.units, stepExponent);
      const ceilingTick = scaledByPowerOfTen(upper * nice.units, stepExponent);
      const intervals = upper - lower;
      const extent = ceilingTick - floorTick;
      // The negated comparisons also reject NaN and infinite counts; an infinite extent means a
      // bound tick, or the distance between them, overflows.
      if (
        !(Math.abs(lower * nice.units) <= RATIO_AXIS_MAX_TICK_COUNT) ||
        !(Math.abs(upper * nice.units) <= RATIO_AXIS_MAX_TICK_COUNT) ||
        intervals > RATIO_AXIS_MAX_INTERVALS ||
        !Number.isFinite(extent) ||
        !(floorTick < lowest && highest < ceilingTick)
      ) {
        continue;
      }
      const miss = Math.abs(intervals - RATIO_AXIS_TARGET_INTERVALS);
      if (best === null || miss < best.miss || (miss === best.miss && extent < best.extent)) {
        best = { lower, intervals, units: nice.units, exponent: stepExponent, miss, extent };
      }
    }
  }
  if (best === null) return null;

  const fractionDigits = Math.max(RATIO_TICK_MIN_FRACTION_DIGITS, -best.exponent);
  const ticks: number[] = [];
  const tickTexts = new Map<number, string>();
  for (let offset = 0; offset <= best.intervals; offset += 1) {
    const count = (best.lower + offset) * best.units;
    // `+ 0` turns -0 into 0.
    const tick = scaledByPowerOfTen(count, best.exponent) + 0;
    ticks.push(tick);
    tickTexts.set(tick, scaledDecimalText(count, best.exponent, fractionDigits));
  }
  const floor = ticks[0];
  const ceiling = ticks[ticks.length - 1];
  if (floor === undefined || ceiling === undefined) return null;
  return { domain: [floor, ceiling], ticks, fractionDigits, tickTexts };
}

/** Tooltip of the hovered week: its date and the multiple formatted from the exact string. */
function BandsTooltip({ active, payload }: TooltipContentProps): ReactElement | null {
  const entry = payload[0];
  if (!active || entry === undefined) return null;
  const row: unknown = entry.payload;
  if (!isBandsRow(row)) return null;
  const seriesName = typeof entry.name === "string" ? entry.name : "";
  return (
    <div className={TOOLTIP_CLASS}>
      <p>{formatDate(row.date)}</p>
      <p>
        {seriesName}{" "}
        <span className={TOOLTIP_VALUE_CLASS}>{formatDecimal(row.raw, "ratio")}</span>
      </p>
    </div>
  );
}

function StatsStrip({ stats }: { stats: BandStats }): ReactElement {
  return (
    <dl className={STATS_CLASS}>
      {STAT_ITEMS.map((item) => (
        <div key={item.name} data-testid={"bands-stat-" + item.name} className={KPI_TILE_CLASS}>
          <dt className={KPI_LABEL_CLASS}>{item.label}</dt>
          <dd className={STAT_VALUE_CLASS}>{formatDecimal(stats[item.name], item.kind)}</dd>
        </div>
      ))}
      <div data-testid="bands-stat-observations" className={KPI_TILE_CLASS}>
        <dt className={KPI_LABEL_CLASS}>Observations</dt>
        <dd className={STAT_VALUE_CLASS}>{formatInteger(stats.observations)}</dd>
      </div>
    </dl>
  );
}

/** Text alternative of the chart for its SVG `<desc>`. */
function chartDescription(metricLabel: string, stats: BandStats | null): string {
  if (stats === null) return `Weekly ${metricLabel}; band statistics not computed.`;
  return (
    `Weekly ${metricLabel} with bands: mean ${formatDecimal(stats.mean, "ratio")}, ` +
    `±1σ ${formatDecimal(stats.minus_1sd, "ratio")} to ${formatDecimal(stats.plus_1sd, "ratio")}, ` +
    `±2σ ${formatDecimal(stats.minus_2sd, "ratio")} to ${formatDecimal(stats.plus_2sd, "ratio")}; ` +
    `current ${formatDecimal(stats.current, "ratio")}.`
  );
}

function BandsBody({
  data,
  updating,
}: {
  data: ValuationBandsResponse;
  /** True while the same subject reloads with other inputs, such as a new lookback. */
  updating: boolean;
}): ReactElement {
  const metricLabel = METRIC_LABELS[data.metric];
  const rows = useMemo(() => toRows(data.series), [data.series]);
  const { stats, coverage, flags } = data;
  const yAxis = useMemo(() => ratioAxis(axisValues(rows, stats)), [rows, stats]);
  const formatTick = useCallback((value: unknown) => formatRatioTick(value, yAxis), [yAxis]);
  const statusNote = statusNoteOf(data.status);

  return (
    <div className={updating ? BODY_UPDATING_CLASS : BODY_CLASS} aria-busy={updating}>
      <p className={META_CLASS} data-testid="bands-meta">
        {SUBJECT_TYPE_LABELS[data.subject_type]} {data.subject} · {metricLabel} · as of{" "}
        {formatDate(data.as_of)}
        {updating ? (
          <span className={UPDATING_CLASS} role="status" data-testid="bands-updating">
            {" "}
            · Updating…
          </span>
        ) : null}
      </p>
      {statusNote !== null ? (
        <p className={STATUS_NOTE_CLASS} data-testid="bands-status-note">
          {statusNote}
        </p>
      ) : null}
      {coverage.is_partial_coverage ? (
        <PartialCoverageNote
          testId="bands-partial-coverage"
          coverageStart={coverage.coverage_start}
        />
      ) : null}
      {rows.length > 0 ? (
        <div className={CHART_CLASS} data-testid="bands-chart">
          <ResponsiveContainer width="100%" height="100%">
            <ComposedChart
              data={rows}
              margin={CHART_MARGIN}
              title={`${metricLabel} of ${data.subject}`}
              desc={chartDescription(metricLabel, stats)}
            >
              <CartesianGrid stroke={CHART_COLORS.grid} strokeDasharray="3 3" vertical={false} />
              <XAxis
                dataKey="date"
                tickFormatter={monthTick}
                minTickGap={32}
                tick={AXIS_TICK}
                stroke={CHART_COLORS.axis}
              />
              <YAxis
                domain={yAxis === null ? ["auto", "auto"] : yAxis.domain}
                ticks={yAxis?.ticks}
                tickFormatter={formatTick}
                tick={AXIS_TICK}
                stroke={CHART_COLORS.axis}
                // Sized to the widest tick label plus the title's own column.
                width="auto"
                label={Y_AXIS_TITLES[data.metric]}
              />
              <Tooltip content={BandsTooltip} />
              {stats !== null
                ? BAND_LINES.map((band) => {
                    const value = stats[band.name];
                    const y = toChartNumber(value);
                    if (y === null) return null;
                    return (
                      <ReferenceLine
                        key={band.name}
                        y={y}
                        className={band.className}
                        stroke={band.stroke}
                        strokeDasharray={band.dashed ? BAND_DASH : undefined}
                        ifOverflow="extendDomain"
                        label={{
                          value: `${band.label} ${formatDecimal(value, "ratio")}`,
                          position: "right",
                          fill: band.labelFill,
                          fontSize: 10,
                        }}
                      />
                    );
                  })
                : null}
              <Line
                type="linear"
                dataKey="y"
                name={metricLabel}
                connectNulls
                dot={false}
                isAnimationActive={false}
                stroke={CHART_COLORS.accent}
                strokeWidth={2}
              />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      ) : null}
      {stats !== null ? <StatsStrip stats={stats} /> : null}
      {flags.length > 0 ? (
        <ul className={FLAGS_CLASS} aria-label="Flags" data-testid="bands-flags">
          {flags.map((flag, index) => (
            <li key={`${flag}-${index}`}>
              <code className={FLAG_CLASS}>{flag}</code>
            </li>
          ))}
        </ul>
      ) : null}
    </div>
  );
}

/**
 * Historical valuation bands of one counter or one sector from `GET /market/valuation-bands`:
 * the weekly P/E or P/B line against horizontal reference lines at the mean and mean ±1σ and ±2σ,
 * with a statistics strip (current value and its percentile, mean, standard deviation, min,
 * median, max and observation count).
 *
 * States, on the root's `data-state`:
 * - no subject: `empty`, prompting a selection, and no request;
 * - both `ticker` and `sector`: `error` (`exactly_one_subject`), no request and no Retry;
 * - while a different subject or metric loads: `loading`, so a previous counter's bands never
 *   appear under the new title; while only the lookback changes, the previous bands stay
 *   `ready`, dimmed and marked "Updating…";
 * - `ready` for every status. A non-`ok` status keeps the series and explains why the bands are
 *   missing, and partial coverage of the lookback is noted with its first covered date.
 *
 * Every displayed number is the API's exact decimal string; floats are used only to place points.
 */
export function ValuationBandsChart({
  ticker,
  sector,
  metric = DEFAULT_METRIC,
  lookbackYears = DEFAULT_LOOKBACK_YEARS,
  apiBaseUrl,
  getHeaders,
  className,
}: ValuationBandsChartProps): ReactElement {
  const selection = selectSubject(ticker, sector);
  const subject = selection.kind === "one" ? selection : null;

  const query = useTerminalQuery<ValuationBandsResponse>({
    path: subject === null ? null : BANDS_PATH,
    query: {
      ticker: subject?.type === "ticker" ? subject.name : undefined,
      sector: subject?.type === "sector" ? subject.name : undefined,
      metric,
      lookback_years: lookbackYears,
    },
    apiBaseUrl,
    getHeaders,
    isEmpty: isEmptyBands,
  });

  let state: WidgetState = query.state;
  let error: TerminalApiError | null = query.error;
  let onRetry: (() => void) | undefined = query.refetch;
  if (selection.kind === "both") {
    state = "error";
    error = BOTH_SUBJECTS_ERROR;
    onRetry = undefined;
  } else if (
    subject !== null &&
    query.isFetching &&
    query.data !== undefined &&
    !describes(query.data, subject, metric)
  ) {
    state = "loading";
  }

  const shown = state === "ready" ? query.data : undefined;
  const metricLabel = METRIC_LABELS[metric];
  const subjectLabel = shown?.subject ?? subject?.name;
  const title =
    subjectLabel === undefined ? `${metricLabel} bands` : `${metricLabel} bands — ${subjectLabel}`;

  return (
    <WidgetFrame
      testId="terminal-valuation-bands-chart"
      title={title}
      state={state}
      error={error}
      onRetry={onRetry}
      emptyMessage={subject === null ? NO_SUBJECT_MESSAGE : undefined}
      skeletonClassName={SKELETON_HEIGHT_CLASS}
      className={className}
    >
      {shown !== undefined ? <BandsBody data={shown} updating={query.isFetching} /> : null}
    </WidgetFrame>
  );
}
