"use client";

import { useMemo, useState } from "react";
import type { FocusEvent, ReactElement } from "react";
import {
  Bar,
  CartesianGrid,
  ComposedChart,
  Line,
  Rectangle,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { BarShapeProps, TooltipContentProps } from "recharts";

import { AXIS_TICK, CHART_COLORS } from "./lib/chartTheme";
import {
  compareDecimal,
  formatDate,
  formatDecimal,
  formatInteger,
  formatMonth,
  toChartNumber,
} from "./lib/format";
import { useTerminalQuery } from "./lib/useTerminalQuery";
import { WidgetFrame } from "./shared/WidgetFrame";
import { KPI_LABEL_CLASS, KPI_TILE_CLASS, KPI_VALUE_CLASS } from "./shared/headingAndTileClasses";
import type {
  DecimalString,
  FlowPoint,
  FlowsResponse,
  FlowTotals,
  TerminalWidgetProps,
} from "./types";

/** Props of {@link NetFlowChart}. */
export interface NetFlowChartProps extends TerminalWidgetProps {
  /**
   * NSE ticker whose flows are shown, for example `"SCOM"`. Give at most one of `ticker` and
   * `sector`: neither means the whole market, and both is rejected by the server with the coded
   * 422 `conflicting_subjects`. An empty string counts as absent; an unknown ticker is a 404.
   */
  ticker?: string;
  /**
   * Sector name as stored, for example `"Banking"`, to show the flows of its constituents. Give at
   * most one of `ticker` and `sector`. An empty string counts as absent; an unknown sector is a
   * 404.
   */
  sector?: string;
  /**
   * Market sessions summed by each point's rolling foreign net flow. Default 20; the server
   * accepts 5–250 and answers any other value with a 422 shown in the error state.
   */
  rollingWindow?: number;
}

const DEFAULT_ROLLING_WINDOW = 20;
const MARKET_LABEL = "Market";
/** `FlowsResponse.subject` when neither a ticker nor a sector was requested. */
const MARKET_SUBJECT = "market";
/** The chart surface's `<desc>`: what is drawn, and how to read it from the keyboard. */
const CHART_DESCRIPTION =
  "Bars: daily foreign net flow in KES on the left axis. Line: cumulative foreign net flow in KES on the right axis. Use the arrow keys to move between sessions; the selected session's details appear below the chart.";
const NO_FLOW_MESSAGE =
  "No foreign-flow data in this range — per-ticker foreign flow is not published on public pages; import it with the CSV importer";
const LOCAL_FLOW_NOTE = "Local net flow is estimated as the negative of foreign net flow";

/** U+2212 minus sign, matching the formatted decimals of `lib/format.ts`. */
const MINUS_SIGN = "\u2212";

// Every class below is a complete static string, never assembled from fragments, so Tailwind's
// source scan and a host's `@source` directive or `content` glob find each one. Only the default
// slate, emerald, rose and sky families are used, plus CSS system colours inside `forced-colors:`
// rules, where they stand for the user's own palette.
const BODY_CLASS =
  "flex flex-col gap-3 motion-safe:transition-opacity motion-safe:duration-200 motion-safe:ease-out";
// A superseded result stays on screen, dimmed, while the request for a new subject is pending.
const BODY_UPDATING_CLASS =
  "flex flex-col gap-3 opacity-60 motion-safe:transition-opacity motion-safe:duration-200 motion-safe:ease-out";
const UPDATING_CLASS = "text-xs text-slate-400";
const META_CLASS =
  "flex flex-wrap items-center justify-between gap-x-4 gap-y-1 text-xs text-slate-400";
const COVERAGE_CLASS = "font-medium tabular-nums text-slate-200";
const NO_DATA_CLASS = "rounded border border-slate-800 bg-slate-950 p-3 text-sm text-slate-300";
const FIGURE_CLASS = "m-0 flex flex-col gap-2";
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
// ring is drawn in Highlight, as the page's other controls are; every chart text, the axis and
// tick lines and the hover cursor in CanvasText; and the cumulative line and its active dot in
// Highlight. The rose bars and the slate-500 zero line keep their colours, which stay above 3:1
// on a white and on a black Canvas, and the emerald bars take BAR_POSITIVE_CLASS. The grid,
// which only guides the eye as every value is labelled on the axes and in the readout, is drawn
// in CanvasText at 30% opacity: about 2:1 on any Canvas, faint as it is on the slate-900 surface,
// where its slate-700 would be darker than the zero line on a white Canvas.
const CHART_CLASS =
  "h-64 w-full [&_g[tabindex='-1']]:outline-none [&_.recharts-surface:focus:not(:focus-visible)]:outline-none [&_.recharts-surface:is(:focus-visible,:has(:focus-visible))]:[outline-style:solid] [&_.recharts-surface:is(:focus-visible,:has(:focus-visible))]:outline-2 [&_.recharts-surface:is(:focus-visible,:has(:focus-visible))]:outline-offset-2 [&_.recharts-surface:is(:focus-visible,:has(:focus-visible))]:outline-sky-400 forced-colors:[&_.recharts-surface:is(:focus-visible,:has(:focus-visible))]:outline-[color:Highlight] forced-colors:[&_text]:[fill:CanvasText] forced-colors:[&_.recharts-cartesian-axis-line]:[stroke:CanvasText] forced-colors:[&_.recharts-cartesian-axis-tick-line]:[stroke:CanvasText] forced-colors:[&_.recharts-tooltip-cursor]:[stroke:CanvasText] forced-colors:[&_.recharts-line-curve]:[stroke:Highlight] forced-colors:[&_.recharts-active-dot_.recharts-dot]:[fill:Highlight] forced-colors:[&_.recharts-cartesian-grid_line]:[stroke:CanvasText] forced-colors:[&_.recharts-cartesian-grid_line]:[stroke-opacity:0.3]";
// Under forced colors the emerald bars, 2.47:1 on a white Canvas, are drawn in CanvasText, as is
// their legend swatch.
const BAR_POSITIVE_CLASS = "forced-colors:[fill:CanvasText]";
const LEGEND_CLASS = "flex flex-col gap-1 text-xs";
const LEGEND_LIST_CLASS =
  "m-0 flex list-none flex-wrap items-center gap-x-4 gap-y-1 p-0 text-slate-300";
const LEGEND_ITEM_CLASS = "flex items-center gap-1.5";
// Forced colors would paint these HTML swatches Canvas and hide the key, so there they opt out of
// the adjustment (`forced-color-adjust-none`) and match their series as the chart draws them:
// CanvasText for buying, rose with a CanvasText edge for selling, and Highlight for the line.
const SWATCH_POSITIVE_CLASS =
  "inline-block h-2.5 w-2.5 rounded-sm bg-emerald-500 forced-colors:forced-color-adjust-none forced-colors:bg-[color:CanvasText]";
const SWATCH_NEGATIVE_CLASS =
  "inline-block h-2.5 w-2.5 rounded-sm bg-rose-500 forced-colors:forced-color-adjust-none forced-colors:border forced-colors:border-solid forced-colors:border-[color:CanvasText]";
const SWATCH_LINE_CLASS =
  "inline-block h-0.5 w-4 rounded-sm bg-sky-400 forced-colors:forced-color-adjust-none forced-colors:bg-[color:Highlight]";
const LEGEND_NOTE_CLASS = "m-0 text-slate-400";
// The note on its own, when no chart (and so no legend) is drawn.
const STANDALONE_NOTE_CLASS = "m-0 text-xs text-slate-400";
// Columns follow the widget's own width rather than the viewport, so the cards keep room for a
// full KES amount whether a host places the widget in a narrow column or across the page.
const TOTALS_CLASS = "m-0 grid grid-cols-[repeat(auto-fit,minmax(12rem,1fr))] gap-2";
// Each total is a KPI tile shared with the other widgets; its value wraps a long KES amount
// inside a narrow tile, and only the tone colour is this widget's own.
const TOTAL_VALUE_POSITIVE_CLASS = `${KPI_VALUE_CLASS} text-emerald-300`;
const TOTAL_VALUE_NEGATIVE_CLASS = `${KPI_VALUE_CLASS} text-rose-300`;
const TOTAL_VALUE_NEUTRAL_CLASS = `${KPI_VALUE_CLASS} text-slate-100`;
const FLAGS_CLASS = "m-0 flex list-none flex-wrap gap-1.5 p-0";
const FLAG_CODE_CLASS = "rounded bg-slate-800 px-1.5 py-0.5 font-mono text-xs text-slate-400";
// The session readout beneath the plot. The Recharts `<Tooltip>` renders its content into this
// element through its `portal` prop, which positions nothing, so the readout sits in the figure's
// flow between the plot and the legend rather than floating over the bars: it never hides the
// session it describes and stays inside the card at any width. It takes no pointer events, as the
// floating tooltip did: React bubbles a portal's events to the chart, so pointer moves and presses
// on the readout would otherwise reach the chart's own handlers.
const READOUT_CLASS = "pointer-events-none";
// The readout's panel, styled like the totals (slate-950 surface, slate-800 border, 12px slate-300
// text) with utility classes: this content is rendered by the widget, not by the Recharts default
// content that takes `contentStyle`, so it needs no inline style. Recharts hides its tooltip wrapper
// (`visibility: hidden`) after Escape until the pointer or the keyboard moves; `visible` keeps this
// readout, which covers nothing, on screen instead of leaving a blank space below the chart.
const TOOLTIP_CLASS =
  "visible flex flex-col gap-1.5 rounded border border-slate-800 bg-slate-950 px-3 py-2 text-xs text-slate-300";
const TOOLTIP_TITLE_CLASS = "m-0 font-medium text-slate-100";
// Columns follow the widget's own width rather than the viewport, as the totals' do. Each value
// has its own line beneath its label, so the panel keeps its height from one session to the next.
const TOOLTIP_LIST_CLASS =
  "m-0 grid grid-cols-[repeat(auto-fit,minmax(9rem,1fr))] gap-x-3 gap-y-1.5";
const TOOLTIP_ENTRY_CLASS = "flex min-w-0 flex-col";
const TOOLTIP_TERM_CLASS = "text-slate-400";
// `[overflow-wrap:anywhere]` lets a long KES amount wrap inside a narrow card, as the totals' do.
const TOOLTIP_VALUE_CLASS = "m-0 tabular-nums text-slate-100 [overflow-wrap:anywhere]";

/** One plotted session: chart coordinates beside the exact point they were derived from. */
interface FlowChartRow {
  /** Trade date, the category of the x-axis. */
  date: string;
  /** Daily foreign net flow as a bar coordinate; null leaves a gap. */
  bar: number | null;
  /** Cumulative foreign net flow as a line coordinate. */
  line: number | null;
  /** The API point, whose strings are what the tooltip displays. */
  point: FlowPoint;
}

/** A prop value with surrounding whitespace removed, or `undefined` when nothing is left. */
function presentOrUndefined(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed === undefined || trimmed === "" ? undefined : trimmed;
}

/** Bar colour from the exact sign of the daily foreign net string; muted when it is null. */
function barFill(value: DecimalString | null): string {
  const sign = compareDecimal(value, "0");
  if (sign === null) return CHART_COLORS.muted;
  return sign >= 0 ? CHART_COLORS.positive : CHART_COLORS.negative;
}

/** Text classes of a signed total: emerald above zero, rose below, slate at zero or null. */
function signedValueClass(value: DecimalString | null): string {
  const sign = compareDecimal(value, "0");
  if (sign === 1) return TOTAL_VALUE_POSITIVE_CLASS;
  if (sign === -1) return TOTAL_VALUE_NEGATIVE_CLASS;
  return TOTAL_VALUE_NEUTRAL_CLASS;
}

/**
 * The first plotted date of each calendar month, used as the x-axis ticks so every month label
 * appears once, rather than once per sampled session.
 */
function monthStartTicks(rows: readonly FlowChartRow[]): string[] {
  const ticks: string[] = [];
  let previousMonth = "";
  for (const row of rows) {
    const month = row.date.slice(0, 7);
    if (month !== previousMonth) {
      ticks.push(row.date);
      previousMonth = month;
    }
  }
  return ticks;
}

/** Axis abbreviation steps, largest first. */
const AXIS_UNITS = [
  { divisor: 1e12, suffix: "T" },
  { divisor: 1e9, suffix: "B" },
  { divisor: 1e6, suffix: "M" },
  { divisor: 1e3, suffix: "K" },
] as const;

/**
 * Abbreviates a KES axis coordinate, such as `"12M"` or `"−3B"`. Axis ticks are generated
 * positions, not values from the API, so they are formatted from the coordinate itself;
 * every data value is shown exactly, from its string, in the tooltip and the totals.
 */
function formatAxisAmount(value: unknown): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "";
  const magnitude = Math.abs(value);
  if (magnitude === 0) return "0";
  const sign = value < 0 ? MINUS_SIGN : "";
  for (let index = 0; index < AXIS_UNITS.length; index += 1) {
    const unit = AXIS_UNITS[index];
    if (unit === undefined || magnitude < unit.divisor) continue;
    const scaled = Number((magnitude / unit.divisor).toFixed(1));
    // 999.95M rounds to 1000.0M; show it as 1B instead.
    const larger = index > 0 ? AXIS_UNITS[index - 1] : undefined;
    if (scaled >= 1000 && larger !== undefined) {
      return sign + trimZeroFraction((magnitude / larger.divisor).toFixed(1)) + larger.suffix;
    }
    return sign + trimZeroFraction(scaled.toFixed(1)) + unit.suffix;
  }
  return sign + trimZeroFraction(magnitude.toFixed(1));
}

/** Drops a `.0` fraction, so `"12.0"` reads `"12"`. */
function trimZeroFraction(text: string): string {
  return text.endsWith(".0") ? text.slice(0, -2) : text;
}

/** Tick intervals on each value axis: five ticks, Recharts' default `tickCount`. */
const VALUE_AXIS_INTERVALS = 4;
/** Nice step mantissas 1, 2, 2.5 and 5, times ten so each is an integer; smallest first. */
const NICE_STEP_MANTISSAS = [10, 20, 25, 50] as const;
/** Lowest step exponent searched: 0.1 (10 × 10^−2) is the finest step labelled to one decimal. */
const STEP_EXPONENT_FLOOR = -2;
/**
 * Relative difference under which a label read back counts as its tick: far above float rounding,
 * far below the 5 × 10^−5 of the smallest change a one-decimal label can make.
 */
const LABEL_TOLERANCE = 1e-9;

/** Explicit domain and ticks of one value axis. */
interface ValueAxisScale {
  /** The lowest and the highest tick. */
  domain: readonly [number, number];
  /** Every tick from the lowest to the highest, zero included. */
  ticks: readonly number[];
}

/** Scales of the bar (left) and cumulative-line (right) axes, sharing zero and every tick row. */
interface AlignedZeroScales {
  left: ValueAxisScale;
  right: ValueAxisScale;
}

/** A tick step of `mantissa × 10^exponent`, kept as integers so ticks carry no float noise. */
interface NiceStep {
  mantissa: number;
  exponent: number;
}

/** A series' lowest and highest coordinate, each widened to include zero. */
interface ZeroInclusiveExtent {
  lo: number;
  hi: number;
}

/**
 * The lowest and highest finite value, widened to include zero; null when no value is finite or
 * every one is zero, since such a series gives its axis no span to align.
 */
function zeroInclusiveExtent(values: readonly (number | null)[]): ZeroInclusiveExtent | null {
  let lo = 0;
  let hi = 0;
  for (const value of values) {
    if (value === null || !Number.isFinite(value)) continue;
    lo = Math.min(lo, value);
    hi = Math.max(hi, value);
  }
  return lo === hi ? null : { lo, hi };
}

/**
 * `index × step`, parsed from its decimal form so that 3 × 0.2 is 0.6 rather than
 * 0.6000000000000001.
 */
function stepMultiple(step: NiceStep, index: number): number {
  return Number(`${index * step.mantissa}e${step.exponent}`);
}

/** The ticks from `negative` steps below zero to `positive` steps above it. */
function stepTicks(step: NiceStep, negative: number, positive: number): number[] {
  const ticks: number[] = [];
  for (let index = -negative; index <= positive; index += 1) {
    ticks.push(stepMultiple(step, index));
  }
  return ticks;
}

/**
 * Whether {@link formatAxisAmount} labels `value` exactly: the label read back through its unit
 * suffix is the value itself, so no tick is rounded to a neighbouring amount (0.25 would read
 * "0.3") and no two ticks share a label.
 */
function isAxisLabelExact(value: number): boolean {
  if (!Number.isFinite(value)) return false;
  const label = formatAxisAmount(value);
  const negative = label.startsWith(MINUS_SIGN);
  const unsigned = negative ? label.slice(MINUS_SIGN.length) : label;
  const unit = AXIS_UNITS.find((candidate) => unsigned.endsWith(candidate.suffix));
  const digits = unit === undefined ? unsigned : unsigned.slice(0, -unit.suffix.length);
  const magnitude = Number(digits) * (unit?.divisor ?? 1);
  const labelled = negative ? -magnitude : magnitude;
  return Math.abs(labelled - value) <= Math.abs(value) * LABEL_TOLERANCE;
}

/**
 * The smallest step of 1, 2, 2.5 or 5 × 10^k at which `negative` steps below zero reach `lo` and
 * `positive` steps above it reach `hi`, among the steps whose every tick {@link formatAxisAmount}
 * labels exactly. Null when the split leaves a side that has values without a step, or when no
 * step fits.
 */
function smallestNiceStep(
  extent: ZeroInclusiveExtent,
  negative: number,
  positive: number,
): NiceStep | null {
  if ((extent.lo < 0 && negative === 0) || (extent.hi > 0 && positive === 0)) return null;
  const needed = Math.max(
    negative > 0 ? -extent.lo / negative : 0,
    positive > 0 ? extent.hi / positive : 0,
  );
  if (!(needed > 0) || !Number.isFinite(needed)) return null;
  // With `needed` in [10^decade, 10^(decade + 1)), the answer is 1, 2, 2.5 or 5 × 10^decade
  // (exponent decade − 1) or 10^(decade + 1) (exponent decade), and a logarithm rounded across
  // a power of ten still leaves it inside those two exponents.
  const decade = Math.floor(Math.log10(needed));
  const lastExponent = Math.max(decade, STEP_EXPONENT_FLOOR);
  for (
    let exponent = Math.max(decade - 1, STEP_EXPONENT_FLOOR);
    exponent <= lastExponent;
    exponent += 1
  ) {
    for (const mantissa of NICE_STEP_MANTISSAS) {
      const step: NiceStep = { mantissa, exponent };
      const bottom = stepMultiple(step, -negative);
      const top = stepMultiple(step, positive);
      if (!Number.isFinite(bottom) || !Number.isFinite(top)) continue;
      if (bottom > extent.lo || top < extent.hi) continue;
      if (stepTicks(step, negative, positive).every(isAxisLabelExact)) return step;
    }
  }
  return null;
}

/** The domain and ticks of `step` from `negative` steps below zero to `positive` above it. */
function alignedAxisScale(step: NiceStep, negative: number, positive: number): ValueAxisScale {
  return {
    domain: [stepMultiple(step, -negative), stepMultiple(step, positive)],
    ticks: stepTicks(step, negative, positive),
  };
}

/**
 * Domains and ticks for the daily bars' (left) and the cumulative line's (right) axes that put
 * both zeros, and every tick, on the same pixel rows, so the line crosses the zero reference line
 * exactly where the cumulative flow changes sign.
 *
 * Both axes split their {@link VALUE_AXIS_INTERVALS} intervals the same way around zero, `n`
 * below and the rest above, with no interval below zero when neither series is negative and none
 * above it when neither is positive. Each axis takes the smallest nice step that holds its own
 * extent ({@link smallestNiceStep}), and the split kept is the one wasting the least span: the
 * least sum, over the two axes, of the axis span divided by the data's zero-inclusive extent.
 *
 * The rows' values are chart coordinates only; the ticks are labelled by
 * {@link formatAxisAmount}, exactly. Null, leaving both axes to Recharts' own domains and ticks,
 * when either series has no finite value or is zero throughout, or no split fits.
 */
function alignedZeroScales(rows: readonly FlowChartRow[]): AlignedZeroScales | null {
  const bars = zeroInclusiveExtent(rows.map((row) => row.bar));
  const lines = zeroInclusiveExtent(rows.map((row) => row.line));
  if (bars === null || lines === null) return null;

  let best: (AlignedZeroScales & { waste: number }) | null = null;
  for (let negative = 0; negative <= VALUE_AXIS_INTERVALS; negative += 1) {
    const positive = VALUE_AXIS_INTERVALS - negative;
    const barStep = smallestNiceStep(bars, negative, positive);
    const lineStep = smallestNiceStep(lines, negative, positive);
    if (barStep === null || lineStep === null) continue;
    const left = alignedAxisScale(barStep, negative, positive);
    const right = alignedAxisScale(lineStep, negative, positive);
    const waste =
      (left.domain[1] - left.domain[0]) / (bars.hi - bars.lo) +
      (right.domain[1] - right.domain[0]) / (lines.hi - lines.lo);
    if (Number.isFinite(waste) && (best === null || waste < best.waste)) {
      best = { left, right, waste };
    }
  }
  return best === null ? null : { left: best.left, right: best.right };
}

/** Whether a Recharts payload entry carries one of this chart's rows. */
function isFlowChartRow(value: unknown): value is FlowChartRow {
  return typeof value === "object" && value !== null && "date" in value && "point" in value;
}

/**
 * Daily bar shape coloured by the exact sign of its own session's foreign net string: emerald at
 * or above zero, rose below it, muted when the value is null or the entry is not one of this
 * chart's rows. An emerald bar also carries {@link BAR_POSITIVE_CLASS}, its forced-colors fill.
 * Used instead of `Cell`, which the pinned Recharts 3.10 deprecates for removal in 4.0.
 */
function renderFlowBar(props: BarShapeProps): ReactElement {
  const row: unknown = props.payload;
  const fill = isFlowChartRow(row) ? barFill(row.point.foreign_net_kes) : CHART_COLORS.muted;
  const className = fill === CHART_COLORS.positive ? BAR_POSITIVE_CLASS : props.className;
  return <Rectangle {...props} fill={fill} className={className} />;
}

/** One labelled value of the session readout's description list, its value beneath its label. */
function TooltipEntry({ label, value }: { label: string; value: string }): ReactElement {
  return (
    <div className={TOOLTIP_ENTRY_CLASS}>
      <dt className={TOOLTIP_TERM_CLASS}>{label}</dt>
      <dd className={TOOLTIP_VALUE_CLASS}>{value}</dd>
    </div>
  );
}

/** Props of {@link FlowTooltip}. */
interface FlowTooltipProps {
  /** The active session's payload entries, passed by Recharts when it clones the content. */
  payload?: TooltipContentProps["payload"];
  /** The session shown while Recharts has no payload yet: the latest, the readout's default. */
  fallback: FlowChartRow | undefined;
}

/**
 * Content of the session readout below the chart: the hovered, tapped or keyboard-selected
 * session, then the last one inspected, and the latest session before any. Every amount is
 * formatted from the API's exact string, never from the chart coordinate.
 */
function FlowTooltip({ payload, fallback }: FlowTooltipProps): ReactElement | null {
  const entryRow: unknown = payload?.[0]?.payload;
  const row = isFlowChartRow(entryRow) ? entryRow : fallback;
  if (row === undefined) return null;
  const point = row.point;
  return (
    <div className={TOOLTIP_CLASS}>
      <p className={TOOLTIP_TITLE_CLASS}>
        <span className={TOOLTIP_TERM_CLASS}>Session</span> {formatDate(point.trade_date)}
      </p>
      <dl className={TOOLTIP_LIST_CLASS}>
        <TooltipEntry
          label="Foreign net"
          value={formatDecimal(point.foreign_net_kes, "kes", { signed: true })}
        />
        <TooltipEntry
          label="Local net (est.)"
          value={formatDecimal(point.local_net_kes, "kes", { signed: true })}
        />
        <TooltipEntry
          label="Cumulative foreign net"
          value={formatDecimal(point.cumulative_foreign_net_kes, "kes", { signed: true })}
        />
        <TooltipEntry
          label="Rolling foreign net"
          value={formatDecimal(point.rolling_foreign_net_kes, "kes", { signed: true })}
        />
        <TooltipEntry label="Turnover" value={formatDecimal(point.turnover_kes, "kes")} />
        <TooltipEntry
          label="% of turnover"
          value={formatDecimal(point.foreign_net_pct_of_turnover, "pct", { signed: true })}
        />
      </dl>
    </div>
  );
}

/**
 * Keeps focus that a click or tap gives the chart from reaching Recharts, in the capture phase.
 * Recharts starts its keyboard position at the first session when the chart first gains focus,
 * and that position outranks the session last pointed at once the pointer leaves, so a press on a
 * bar or an axis would move the readout to the first session. Keyboard focus, which matches
 * `:focus-visible`, still reaches Recharts, and the arrow keys work after a click either way.
 */
function stopPointerFocus(event: FocusEvent<HTMLElement>): void {
  const target = event.target;
  if (target instanceof Element && !target.matches(":focus-visible")) event.stopPropagation();
}

/** Range totals: foreign and local net, foreign net as a share of turnover, and session counts. */
function FlowTotalsStrip({ totals }: { totals: FlowTotals }): ReactElement {
  return (
    <dl className={TOTALS_CLASS} data-testid="flows-totals">
      <div className={KPI_TILE_CLASS}>
        <dt className={KPI_LABEL_CLASS}>Foreign net</dt>
        <dd className={signedValueClass(totals.foreign_net_kes)} data-testid="flows-total-foreign">
          {formatDecimal(totals.foreign_net_kes, "kes", { signed: true })}
        </dd>
      </div>
      <div className={KPI_TILE_CLASS}>
        <dt className={KPI_LABEL_CLASS}>Local net (est.)</dt>
        <dd className={signedValueClass(totals.local_net_kes)} data-testid="flows-total-local">
          {formatDecimal(totals.local_net_kes, "kes", { signed: true })}
        </dd>
      </div>
      <div className={KPI_TILE_CLASS}>
        <dt className={KPI_LABEL_CLASS}>Foreign net % of turnover</dt>
        <dd
          className={signedValueClass(totals.foreign_net_pct_of_turnover)}
          data-testid="flows-total-pct"
        >
          {formatDecimal(totals.foreign_net_pct_of_turnover, "pct", { signed: true })}
        </dd>
      </div>
      <div className={KPI_TILE_CLASS}>
        <dt className={KPI_LABEL_CLASS}>Sessions with data</dt>
        <dd className={TOTAL_VALUE_NEUTRAL_CLASS} data-testid="flows-sessions">
          {formatInteger(totals.sessions_with_data)} / {formatInteger(totals.sessions_in_range)}
        </dd>
      </div>
    </dl>
  );
}

/** Props of {@link NetFlowBody}. */
interface NetFlowBodyProps {
  data: FlowsResponse;
  rollingWindow: number;
  isUpdating: boolean;
}

/** The loaded widget: range and coverage, the chart with its legend, totals and flags. */
function NetFlowBody({ data, rollingWindow, isUpdating }: NetFlowBodyProps): ReactElement {
  const rows = useMemo<FlowChartRow[]>(
    () =>
      data.series.map((point) => ({
        date: point.trade_date,
        bar: toChartNumber(point.foreign_net_kes),
        line: toChartNumber(point.cumulative_foreign_net_kes),
        point,
      })),
    [data.series],
  );
  const ticks = useMemo(() => monthStartTicks(rows), [rows]);
  // Null leaves both value axes on Recharts' own domains and ticks.
  const valueScales = useMemo(() => alignedZeroScales(rows), [rows]);

  const noFlowData = data.status === "no_flow_data";
  // Without flow data the chart is kept only when it still has turnover rows to show.
  const showChart =
    rows.length > 0 && (!noFlowData || data.series.some((point) => point.turnover_kes !== null));
  const rangeText = `${formatDate(data.start)} – ${formatDate(data.end)}`;
  const subjectText = data.subject === MARKET_SUBJECT ? MARKET_LABEL : data.subject;
  // The session readout's element, held in state so that the `<Tooltip>` renders only once it can
  // portal into it: without a portal, Recharts would float the content over the plot.
  const [readoutElement, setReadoutElement] = useState<HTMLDivElement | null>(null);
  // Before any session is inspected, the readout shows the latest one.
  const latestIndex = rows.length - 1;
  const latestRow = rows[latestIndex];

  return (
    <div className={isUpdating ? BODY_UPDATING_CLASS : BODY_CLASS} aria-busy={isUpdating}>
      <div className={META_CLASS}>
        <p className="m-0" data-testid="flows-range">
          {rangeText} · rolling {formatInteger(rollingWindow)} sessions
        </p>
        <span className={COVERAGE_CLASS} data-testid="flows-coverage">
          Coverage {formatDecimal(data.coverage_pct, "pct")}
        </span>
      </div>

      {noFlowData ? (
        <p className={NO_DATA_CLASS} data-testid="flows-no-data">
          {NO_FLOW_MESSAGE}
        </p>
      ) : null}

      {showChart ? (
        <figure className={FIGURE_CLASS} onFocusCapture={stopPointerFocus}>
          <div className={CHART_CLASS} data-testid="flows-chart">
            <ResponsiveContainer width="100%" height="100%">
              <ComposedChart
                // Keyed by subject, so a new ticker, sector or the market starts its readout and
                // cursor on its own latest session, not on the session last inspected before.
                key={data.subject}
                data={rows}
                margin={{ top: 8, right: 4, bottom: 0, left: 4 }}
                title={`Foreign net flow — ${subjectText}, ${rangeText}`}
                desc={CHART_DESCRIPTION}
              >
                {/* Bound to the left axis, so a grid line runs along each of its tick rows; with
                    aligned scales the right axis shares every one of those rows. */}
                <CartesianGrid
                  yAxisId="left"
                  stroke={CHART_COLORS.grid}
                  strokeDasharray="3 3"
                  vertical={false}
                />
                <XAxis
                  dataKey="date"
                  ticks={ticks}
                  tickFormatter={formatMonth}
                  minTickGap={24}
                  tick={AXIS_TICK}
                  stroke={CHART_COLORS.axis}
                />
                <YAxis
                  yAxisId="left"
                  domain={valueScales?.left.domain}
                  ticks={valueScales?.left.ticks}
                  tickFormatter={formatAxisAmount}
                  tick={AXIS_TICK}
                  stroke={CHART_COLORS.axis}
                  width={56}
                />
                <YAxis
                  yAxisId="right"
                  orientation="right"
                  domain={valueScales?.right.domain}
                  ticks={valueScales?.right.ticks}
                  tickFormatter={formatAxisAmount}
                  tick={AXIS_TICK}
                  stroke={CHART_COLORS.axis}
                  width={56}
                />
                {/* `active` keeps the readout and the cursor on the last session inspected after
                    the pointer leaves; `defaultIndex` selects the latest session before any is
                    inspected; `filterNull={false}` keeps a session whose bar and line are both
                    null, so the readout still names it and shows its turnover. A composed chart
                    draws the cursor as a vertical line, stroked slate-400 rather than Recharts'
                    default #ccc, which is outside the palette. */}
                {readoutElement !== null ? (
                  <Tooltip
                    content={<FlowTooltip fallback={latestRow} />}
                    cursor={{ stroke: CHART_COLORS.axis }}
                    isAnimationActive={false}
                    portal={readoutElement}
                    active
                    defaultIndex={latestIndex}
                    filterNull={false}
                  />
                ) : null}
                <ReferenceLine y={0} yAxisId="left" stroke={CHART_COLORS.muted} />
                <Bar
                  dataKey="bar"
                  yAxisId="left"
                  name="Daily foreign net"
                  isAnimationActive={false}
                  shape={renderFlowBar}
                />
                <Line
                  dataKey="line"
                  yAxisId="right"
                  type="linear"
                  dot={false}
                  isAnimationActive={false}
                  stroke={CHART_COLORS.accent}
                  strokeWidth={2}
                  name="Cumulative foreign net"
                />
              </ComposedChart>
            </ResponsiveContainer>
          </div>
          <div ref={setReadoutElement} className={READOUT_CLASS} data-testid="flows-readout" />
          <figcaption className={LEGEND_CLASS}>
            <ul className={LEGEND_LIST_CLASS}>
              <li className={LEGEND_ITEM_CLASS}>
                <span aria-hidden="true" className={SWATCH_POSITIVE_CLASS} />
                Daily foreign net buying (bars, left axis)
              </li>
              <li className={LEGEND_ITEM_CLASS}>
                <span aria-hidden="true" className={SWATCH_NEGATIVE_CLASS} />
                Daily foreign net selling (bars, left axis)
              </li>
              <li className={LEGEND_ITEM_CLASS}>
                <span aria-hidden="true" className={SWATCH_LINE_CLASS} />
                Cumulative foreign net (line, right axis)
              </li>
            </ul>
            <p className={LEGEND_NOTE_CLASS}>{LOCAL_FLOW_NOTE}</p>
          </figcaption>
        </figure>
      ) : (
        <p className={STANDALONE_NOTE_CLASS}>{LOCAL_FLOW_NOTE}</p>
      )}

      {data.totals !== null ? <FlowTotalsStrip totals={data.totals} /> : null}

      {data.flags.length > 0 ? (
        <ul aria-label="Flags" className={FLAGS_CLASS} data-testid="flows-flags">
          {data.flags.map((flag, index) => (
            <li key={`${index}:${flag}`}>
              <code className={FLAG_CODE_CLASS}>{flag}</code>
            </li>
          ))}
        </ul>
      ) : null}
    </div>
  );
}

/**
 * Foreign versus local net capital flow of one ticker, one sector or the whole market, from
 * `GET /market/flows`: daily foreign net flow as bars coloured by sign, the cumulative foreign net
 * flow as a line, coverage of the sessions with flow data, and range totals. Local net flow is the
 * negative of foreign net flow, since aggregate buys equal aggregate sells.
 *
 * Per-ticker foreign flow is not published on public pages, so a range without it reports
 * `no_flow_data` with an inline explanation instead of the empty state. With no props the whole
 * market is shown.
 */
export function NetFlowChart({
  ticker,
  sector,
  rollingWindow = DEFAULT_ROLLING_WINDOW,
  apiBaseUrl,
  getHeaders,
  className,
}: NetFlowChartProps): ReactElement {
  const subjectTicker = presentOrUndefined(ticker);
  const subjectSector = presentOrUndefined(sector);

  const { state, data, error, isFetching, refetch } = useTerminalQuery<FlowsResponse>({
    path: "/market/flows",
    query: { ticker: subjectTicker, sector: subjectSector, rolling_window: rollingWindow },
    apiBaseUrl,
    getHeaders,
    isEmpty: (response) => response.series.length === 0 && response.status !== "no_flow_data",
  });

  // The title names the requested subject rather than `data.subject`, because the previous
  // subject's result stays on screen while a new one loads.
  const subjectLabel = subjectTicker ?? subjectSector ?? MARKET_LABEL;
  const isUpdating = isFetching && state === "ready";

  return (
    <WidgetFrame
      testId="terminal-net-flow-chart"
      title={`Foreign vs local net flow — ${subjectLabel}`}
      state={state}
      error={error}
      onRetry={refetch}
      className={className}
      skeletonClassName="h-64"
      actions={
        isUpdating ? (
          <span role="status" className={UPDATING_CLASS}>
            Updating…
          </span>
        ) : null
      }
    >
      {data !== undefined ? (
        <NetFlowBody data={data} rollingWindow={rollingWindow} isUpdating={isUpdating} />
      ) : null}
    </WidgetFrame>
  );
}
