"use client";

import { Fragment, useMemo } from "react";
import type { ReactElement, ReactNode } from "react";
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { TooltipContentProps } from "recharts";

import { AXIS_TICK, CHART_COLORS, SERIES_PALETTE } from "./lib/chartTheme";
import {
  compareDecimal,
  formatDate,
  formatDecimal,
  formatMonth,
  shiftMonth,
  toChartNumber,
} from "./lib/format";
import { useTerminalQuery } from "./lib/useTerminalQuery";
import { WidgetFrame } from "./shared/WidgetFrame";
import type {
  Instrument,
  MacroOverviewResponse,
  TerminalWidgetProps,
  YieldCurvePoint,
  YieldCurveSnapshot,
} from "./types";

/** Props of {@link YieldCurveChart}. */
export interface YieldCurveChartProps extends TerminalWidgetProps {
  /**
   * Month-ends of history requested from `GET /macro/overview` as `history_months` (1–240).
   * Default 24. A comparison curve older than this window has no snapshot and is not drawn.
   * Widgets mounted together with the same value share one request.
   */
  historyMonths?: number;
  /**
   * Ages of the historical curves drawn against the current one, in whole months before the
   * latest month-end of the history window. Default `[6, 12]`. A value without a month-end
   * snapshot in the window is skipped, as is any value that is not a positive integer.
   */
  compareMonthsAgo?: number[];
  /** Host-supplied overview. While it is defined the widget sends no request. */
  data?: MacroOverviewResponse;
}

/** One plotted point of a curve: axis coordinates plus the untouched API point. */
interface PlottedPoint {
  /** Tenor in years, a chart coordinate only. */
  x: number;
  /** Yield in percentage points, a chart coordinate only. */
  y: number;
  /** The API point, whose decimal strings are what the tooltip shows. */
  point: YieldCurvePoint;
}

/** One tooltip block: a curve and every point it has at the hovered tenor. */
interface TenorEntry {
  /** The curve's {@link CurveSeries.id}. */
  seriesId: string;
  /** The curve's name. */
  name: string;
  /** The curve's colour. */
  color: string;
  /** The curve's API points at this tenor, in server order; never empty. */
  points: readonly YieldCurvePoint[];
}

/**
 * One row of the chart-level data that every curve is drawn from. The rows run through the union
 * of the drawn curves' tenors in ascending order, tenors being matched by exact decimal equality.
 * A tenor at which a curve has `k` points spans `k` rows at the same `x`, the `j`-th row holding
 * each curve's `j`-th point there, so every point is drawn and repeated tenors join in server
 * order.
 */
interface ChartRow {
  /** Tenor in years, a chart coordinate only. */
  x: number;
  /**
   * Yield coordinate of each curve at this row, keyed by {@link CurveSeries.id}; null where the
   * curve has no point at this row, which the line bridges and marks with no dot.
   */
  y: Readonly<Record<string, number | null>>;
  /**
   * What the tooltip lists at this tenor: each curve with at least one point at it, in legend
   * order. Every row of one tenor holds the same entries, so the tooltip is complete whichever
   * of them Recharts resolves the hovered tenor to.
   */
  entries: readonly TenorEntry[];
}

/** One drawn curve with its legend identity. */
interface CurveSeries {
  /** `current`, or `m<k>` for the curve `k` months ago; written to the legend's `data-series`. */
  id: string;
  /** Legend and tooltip name. */
  name: string;
  /** Stroke and dot colour. */
  color: string;
  /** Line width in px. */
  strokeWidth: number;
  /** The points that have both chart coordinates, in server order. */
  points: PlottedPoint[];
  /**
   * The `Line`'s data key: this curve's yield coordinate in a {@link ChartRow}, or null. It is
   * created once per chart model, so its identity stays stable across renders.
   */
  yieldOf: (row: ChartRow) => number | null;
}

/** The drawn curves and the chart-level rows they are plotted from. */
interface ChartModel {
  series: CurveSeries[];
  rows: ChartRow[];
}

/** A tenor of the union across the curves, and each curve's points at it in server order. */
interface TenorGroup {
  /** The tenor as the first point at it wrote it. */
  tenor: string;
  /** Its chart coordinate. */
  x: number;
  /** Points at this tenor by {@link CurveSeries.id}. */
  pointsBySeries: Map<string, PlottedPoint[]>;
}

const DEFAULT_HISTORY_MONTHS = 24;

// Module-level, so the default keeps one identity and never invalidates the chart model memo.
const DEFAULT_COMPARE_MONTHS_AGO: readonly number[] = [6, 12];

const CURRENT_STROKE_WIDTH = 2;
const COMPARISON_STROKE_WIDTH = 1.5;

const INSTRUMENT_LABELS: Record<Instrument, string> = {
  tbill: "Treasury bill",
  bond: "Treasury bond",
};

// Every class below is a complete static string, so Tailwind's source scan and a host's
// `@source` directive or `content` glob generate each one. `m-0`, `p-0` and `list-none` keep the
// layout intact in a host that has no CSS reset.
const FIGURE_CLASS = "m-0 flex flex-col gap-3";
// Recharts' accessibility layer renders the chart as `<svg class="recharts-surface" tabindex="0">`,
// which the keyboard reaches and navigates with the arrow keys. Recharts 3 also renders its z-index
// layers as `<g tabindex="-1">`: Tab never reaches them, but a mouse click on plotted data focuses
// one, and a click on bare plot area focuses the surface. The browser outlines a focused SVG
// element even for a mouse click, and an arrow key pressed after the click makes that element
// :focus-visible. So the layers' outline and the surface's pointer-focus outline are hidden, and
// the widgets' sky-400 ring is drawn on the surface whenever it or a descendant is :focus-visible:
// it appears for keyboard use and not for a mouse click. The solid style is explicit because
// Tailwind 3's `outline-2` sets only the width.
const CHART_CLASS =
  "h-64 w-full [&_g[tabindex='-1']]:outline-none [&_.recharts-surface:focus:not(:focus-visible)]:outline-none [&_.recharts-surface:is(:focus-visible,:has(:focus-visible))]:[outline-style:solid] [&_.recharts-surface:is(:focus-visible,:has(:focus-visible))]:outline-2 [&_.recharts-surface:is(:focus-visible,:has(:focus-visible))]:outline-offset-2 [&_.recharts-surface:is(:focus-visible,:has(:focus-visible))]:outline-sky-400";
const CAPTION_CLASS = "flex flex-col gap-2";
const LEGEND_CLASS = "m-0 flex list-none flex-wrap gap-x-4 gap-y-1 p-0 text-xs text-slate-300";
const LEGEND_ITEM_CLASS = "flex items-center gap-2";
// The keys alone link each curve name to its line, so they keep their colours under forced colours.
const SWATCH_CLASS = "h-2 w-4 shrink-0 forced-color-adjust-none";
const NOTE_CLASS = "m-0 text-xs text-slate-400";
// The tooltip shades are those of chartTheme's TOOLTIP_CONTENT_STYLE (slate-900 surface, slate-700
// border, slate-300 text at 12px), written as utilities because the widgets style their own HTML
// with Tailwind classes only, never inline styles.
const TOOLTIP_CLASS =
  "rounded border border-slate-700 bg-slate-900 px-3 py-2 text-xs text-slate-300 shadow-lg";
const TOOLTIP_LIST_CLASS = "m-0 flex list-none flex-col gap-2 p-0";
const TOOLTIP_SERIES_CLASS = "m-0 flex items-center gap-2 font-semibold text-slate-100";
const TOOLTIP_LINE_CLASS = "m-0";
const TOOLTIP_DOT_CLASS = "h-2 w-2 shrink-0";

const NOTE_TEXT = "T-bills from CBK rates; bonds are FXD issues bucketed by remaining tenor";

function isCurveEmpty(data: MacroOverviewResponse): boolean {
  return data.yield_curve.current.length === 0;
}

/**
 * The API points that have both chart coordinates, in server order. A point whose tenor or yield
 * has no chart coordinate is not drawn.
 */
function toPlottedPoints(points: readonly YieldCurvePoint[]): PlottedPoint[] {
  const plotted: PlottedPoint[] = [];
  for (const point of points) {
    const x = toChartNumber(point.tenor_years);
    const y = toChartNumber(point.yield_pct);
    if (x !== null && y !== null) {
      plotted.push({ x, y, point });
    }
  }
  return plotted;
}

/** Whether exact decimal comparison can read the tenor text. */
function isComparableTenor(tenor: string): boolean {
  return compareDecimal(tenor, tenor) !== null;
}

/**
 * Whether two tenor texts are the same tenor: exactly equal decimals, or, for text the exact
 * comparison cannot read, identical text. Chart numbers are coordinates only and are never
 * compared (lib/format.ts).
 */
function isSameTenor(a: string, b: string): boolean {
  const order = compareDecimal(a, b);
  return order === null ? a === b : order === 0;
}

/**
 * Where a new comparable tenor goes among the groups: before the first comparable tenor above
 * it, so the comparable tenors stay in ascending exact order.
 */
function insertionIndex(groups: readonly TenorGroup[], tenor: string): number {
  const index = groups.findIndex((group) => compareDecimal(group.tenor, tenor) === 1);
  return index === -1 ? groups.length : index;
}

/**
 * Groups the points of every curve by tenor, as the union of the curves' tenors in ascending
 * exact order. A tenor the exact comparison cannot read is never matched to another tenor: it
 * forms its own group, shared only with identical text, placed right after the tenor of the
 * point its curve sends before it, so it keeps the server's ascending-tenor order.
 */
function groupByTenor(series: readonly CurveSeries[]): TenorGroup[] {
  const groups: TenorGroup[] = [];
  for (const curve of series) {
    // The group holding this curve's previous point; null before its first point.
    let previous: TenorGroup | null = null;
    for (const plotted of curve.points) {
      const tenor = plotted.point.tenor_years;
      let group = groups.find((candidate) => isSameTenor(candidate.tenor, tenor));
      if (group === undefined) {
        group = { tenor, x: plotted.x, pointsBySeries: new Map() };
        const afterPrevious = previous === null ? 0 : groups.indexOf(previous) + 1;
        const index = isComparableTenor(tenor) ? insertionIndex(groups, tenor) : afterPrevious;
        groups.splice(index, 0, group);
      }
      const atTenor = group.pointsBySeries.get(curve.id);
      if (atTenor === undefined) {
        group.pointsBySeries.set(curve.id, [plotted]);
      } else {
        atTenor.push(plotted);
      }
      previous = group;
    }
  }
  return groups;
}

/**
 * Builds the chart-level rows from the tenor groups: per tenor, as many rows as the most points
 * any curve has there, each holding every curve's coordinate or null and the tenor's tooltip
 * entries.
 */
function buildRows(series: readonly CurveSeries[]): ChartRow[] {
  const rows: ChartRow[] = [];
  for (const group of groupByTenor(series)) {
    const entries: TenorEntry[] = [];
    let depth = 0;
    for (const curve of series) {
      const atTenor = group.pointsBySeries.get(curve.id);
      if (atTenor !== undefined && atTenor.length > 0) {
        entries.push({
          seriesId: curve.id,
          name: curve.name,
          color: curve.color,
          points: atTenor.map((plotted) => plotted.point),
        });
        depth = Math.max(depth, atTenor.length);
      }
    }
    for (let position = 0; position < depth; position += 1) {
      const y: Record<string, number | null> = {};
      for (const curve of series) {
        y[curve.id] = group.pointsBySeries.get(curve.id)?.[position]?.y ?? null;
      }
      rows.push({ x: group.x, y, entries });
    }
  }
  return rows;
}

/** The data key of the curve `id`: its yield coordinate in a row, or null where it has none. */
function yieldOfSeries(id: string): (row: ChartRow) => number | null {
  return (row) => row.y[id] ?? null;
}

/** Latest `observation_date` among the points (ISO dates order as text), or null when empty. */
function latestObservationDate(points: readonly YieldCurvePoint[]): string | null {
  let latest: string | null = null;
  for (const point of points) {
    if (latest === null || point.observation_date > latest) {
      latest = point.observation_date;
    }
  }
  return latest;
}

/** The requested comparison ages that are positive integers, first occurrence of each only. */
function comparisonAges(compareMonthsAgo: readonly number[]): number[] {
  const ages: number[] = [];
  for (const months of compareMonthsAgo) {
    if (Number.isInteger(months) && months >= 1 && !ages.includes(months)) {
      ages.push(months);
    }
  }
  return ages;
}

/**
 * Stroke colour of the comparison at `position` in the requested ages. Palette entry 0 is the
 * accent, which the current curve uses, so comparisons start at entry 1. The colour follows the
 * requested position rather than the drawn order, so a curve keeps its colour when another
 * comparison has no snapshot.
 */
function comparisonColor(position: number): string {
  const span = SERIES_PALETTE.length - 1;
  if (span < 1) {
    return CHART_COLORS.muted;
  }
  return SERIES_PALETTE[1 + (position % span)] ?? CHART_COLORS.muted;
}

function comparisonName(months: number, snapshot: YieldCurveSnapshot): string {
  const unit = months === 1 ? "month" : "months";
  return `${months} ${unit} ago (${formatMonth(snapshot.as_of)})`;
}

/**
 * Builds the current curve and, for each requested age `k`, the month-end snapshot `k` months
 * before the anchor: the last history snapshot's `as_of`, or the current curve's latest
 * observation date when the history is empty. Months are shifted as `YYYY-MM` text, so no
 * calendar object or time zone is involved. An age without a matching snapshot, or whose
 * snapshot has no plottable point, draws no line.
 */
function buildSeries(
  data: MacroOverviewResponse | undefined,
  compareMonthsAgo: readonly number[],
): CurveSeries[] {
  if (data === undefined) {
    return [];
  }
  const { current, history } = data.yield_curve;
  const series: CurveSeries[] = [
    {
      id: "current",
      name: "Current",
      color: CHART_COLORS.accent,
      strokeWidth: CURRENT_STROKE_WIDTH,
      points: toPlottedPoints(current),
      yieldOf: yieldOfSeries("current"),
    },
  ];

  const anchor = history[history.length - 1]?.as_of ?? latestObservationDate(current);
  if (anchor === null) {
    return series;
  }

  comparisonAges(compareMonthsAgo).forEach((months, position) => {
    const target = shiftMonth(anchor, -months);
    const snapshot = history.find((candidate) => candidate.as_of.slice(0, 7) === target);
    if (snapshot === undefined) {
      return;
    }
    const points = toPlottedPoints(snapshot.points);
    if (points.length === 0) {
      return;
    }
    const id = "m" + months;
    series.push({
      id,
      name: comparisonName(months, snapshot),
      color: comparisonColor(position),
      strokeWidth: COMPARISON_STROKE_WIDTH,
      points,
      yieldOf: yieldOfSeries(id),
    });
  });
  return series;
}

/**
 * Builds the drawn curves and the chart-level rows they share. Every `Line` reads the same rows,
 * so Recharts resolves the hovered tenor to one row for all curves, and a curve without a point
 * there contributes no tooltip entry and no active dot.
 */
function buildChartModel(
  data: MacroOverviewResponse | undefined,
  compareMonthsAgo: readonly number[],
): ChartModel {
  const series = buildSeries(data, compareMonthsAgo);
  return { series, rows: buildRows(series) };
}

function isChartRow(value: unknown): value is ChartRow {
  return (
    typeof value === "object" &&
    value !== null &&
    "x" in value &&
    typeof value.x === "number" &&
    "y" in value &&
    typeof value.y === "object" &&
    value.y !== null &&
    "entries" in value &&
    Array.isArray(value.entries)
  );
}

/**
 * The row of the hovered tenor. Every curve is drawn from the same chart-level rows, so each
 * payload entry carries that row; the first one found is used.
 */
function hoveredRow(payload: TooltipContentProps["payload"]): ChartRow | undefined {
  for (const entry of payload) {
    const row: unknown = entry.payload;
    if (isChartRow(row)) {
      return row;
    }
  }
  return undefined;
}

/**
 * Tooltip of the chart. It lists only the curves that have a point at the hovered tenor, taken
 * from that tenor's chart row rather than from each curve's own data, so no entry comes from
 * another tenor. Each curve is named once, followed by every point it has at the tenor, with the
 * API's own strings: the yield through `formatDecimal`, the observation date and the provenance
 * of that point.
 */
function YieldCurveTooltip({ active, payload }: TooltipContentProps): ReactNode {
  if (!active) {
    return null;
  }
  const row = hoveredRow(payload);
  if (row === undefined || row.entries.length === 0) {
    return null;
  }
  return (
    <div className={TOOLTIP_CLASS}>
      <ul className={TOOLTIP_LIST_CLASS}>
        {row.entries.map(({ seriesId, name, color, points }) => (
          <li key={seriesId}>
            <p className={TOOLTIP_SERIES_CLASS}>
              <svg
                viewBox="0 0 8 8"
                aria-hidden="true"
                focusable="false"
                className={TOOLTIP_DOT_CLASS}
              >
                <circle cx="4" cy="4" r="4" fill={color} />
              </svg>
              <span>{name}</span>
            </p>
            {/* The curve's points at this tenor are a fixed list, in server order. */}
            {points.map((point, index) => (
              <Fragment key={index}>
                <p className={TOOLTIP_LINE_CLASS}>
                  {point.tenor_label}: {formatDecimal(point.yield_pct, "pct")}
                </p>
                <p className={TOOLTIP_LINE_CLASS}>
                  {INSTRUMENT_LABELS[point.instrument] ?? point.instrument}
                  {point.bond_code ? ` ${point.bond_code}` : ""}
                </p>
                {/* One line for date and provenance keeps a three-curve tooltip within the chart height. */}
                <p className={TOOLTIP_LINE_CLASS}>
                  Observed {formatDate(point.observation_date)} · Source: {point.source}
                </p>
              </Fragment>
            ))}
          </li>
        ))}
      </ul>
    </div>
  );
}

/**
 * Tick label of the tenor axis. The tick is a chart coordinate generated by the axis, not an
 * API value, so it is printed from the number at no more than 2 decimals: `0.25` → `0.25y`.
 */
function formatTenorTick(value: number): string {
  if (!Number.isFinite(value)) {
    return "";
  }
  return `${Math.round(value * 100) / 100}y`;
}

/**
 * Yield curve visualizer: the current CBK curve against the curves of `compareMonthsAgo`
 * months earlier, on a numeric tenor axis in years, with each point's observation date and
 * source in its tooltip.
 *
 * Fetches `GET /macro/overview?history_months=<historyMonths>` through the terminal API (or uses
 * `data`), and is `empty` when the current curve has no point. T-bill points come from the CBK
 * rates series; bond points are FXD issues bucketed by remaining tenor. Below the chart, an HTML
 * legend lists each drawn curve as `data-testid="yield-curve-series"` with `data-series`
 * (`current`, `m6`, `m12`, …) and `data-point-count`.
 */
export function YieldCurveChart({
  historyMonths = DEFAULT_HISTORY_MONTHS,
  compareMonthsAgo,
  data,
  apiBaseUrl,
  getHeaders,
  className,
}: YieldCurveChartProps): ReactElement {
  const query = useTerminalQuery<MacroOverviewResponse>({
    path: "/macro/overview",
    query: { history_months: historyMonths },
    apiBaseUrl,
    getHeaders,
    data,
    isEmpty: isCurveEmpty,
  });
  const ages = compareMonthsAgo ?? DEFAULT_COMPARE_MONTHS_AGO;
  const { series, rows } = useMemo(() => buildChartModel(query.data, ages), [query.data, ages]);

  return (
    <WidgetFrame
      testId="terminal-yield-curve-chart"
      title="Yield curve"
      state={query.state}
      error={query.error}
      onRetry={query.refetch}
      className={className}
      skeletonClassName="h-64"
    >
      <figure className={FIGURE_CLASS}>
        <div className={CHART_CLASS}>
          <ResponsiveContainer width="100%" height="100%">
            <LineChart
              data={rows}
              margin={{ top: 8, right: 16, bottom: 4, left: 4 }}
              title="Yield curve"
              desc="Yield in percent against tenor in years, for the current curve and each comparison curve listed below the chart."
            >
              <CartesianGrid stroke={CHART_COLORS.grid} strokeDasharray="3 3" />
              <XAxis
                type="number"
                dataKey="x"
                domain={[0, "dataMax"]}
                tick={AXIS_TICK}
                stroke={CHART_COLORS.axis}
                tickFormatter={formatTenorTick}
                // Six ticks give a 25-year curve 5-year steps; the default five give 7-year steps.
                tickCount={6}
                height={40}
                label={{
                  value: "Tenor (years)",
                  position: "insideBottom",
                  fill: CHART_COLORS.axis,
                  fontSize: 11,
                }}
              />
              {/* No axis data key: the domain is taken from every Line's yield values. */}
              <YAxis
                type="number"
                domain={["auto", "auto"]}
                tick={AXIS_TICK}
                stroke={CHART_COLORS.axis}
                // Whole-percent ticks; Recharts' decimal steps (14.4, 15.3, …) read poorly.
                allowDecimals={false}
                width={52}
                label={{
                  value: "Yield (%)",
                  angle: -90,
                  position: "insideLeft",
                  // Centres the rotated label on the axis; the position alone anchors it at its start.
                  textAnchor: "middle",
                  fill: CHART_COLORS.axis,
                  fontSize: 11,
                }}
              />
              <Tooltip
                content={YieldCurveTooltip}
                cursor={{ stroke: CHART_COLORS.muted, strokeDasharray: "3 3" }}
                isAnimationActive={false}
              />
              {series.map((curve) => (
                <Line
                  key={curve.id}
                  dataKey={curve.yieldOf}
                  name={curve.name}
                  type="linear"
                  // A row at a tenor this curve lacks is null: the line runs on through it, and
                  // neither a dot nor an active dot is drawn there.
                  connectNulls
                  stroke={curve.color}
                  strokeWidth={curve.strokeWidth}
                  dot={{ r: 3, fill: curve.color, stroke: curve.color, strokeWidth: 1 }}
                  activeDot={{
                    r: 5,
                    fill: curve.color,
                    stroke: CHART_COLORS.tooltipBg,
                    strokeWidth: 2,
                  }}
                  isAnimationActive={false}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>
        <figcaption className={CAPTION_CLASS}>
          <ul className={LEGEND_CLASS} aria-label="Curves">
            {series.map((curve) => (
              <li
                key={curve.id}
                data-testid="yield-curve-series"
                data-series={curve.id}
                data-point-count={curve.points.length}
                className={LEGEND_ITEM_CLASS}
              >
                <svg
                  viewBox="0 0 16 8"
                  aria-hidden="true"
                  focusable="false"
                  className={SWATCH_CLASS}
                >
                  <line
                    x1="0"
                    y1="4"
                    x2="16"
                    y2="4"
                    stroke={curve.color}
                    strokeWidth={curve.strokeWidth}
                  />
                  <circle cx="8" cy="4" r="2.5" fill={curve.color} />
                </svg>
                <span>{curve.name}</span>
              </li>
            ))}
          </ul>
          <p className={NOTE_CLASS}>{NOTE_TEXT}</p>
        </figcaption>
      </figure>
    </WidgetFrame>
  );
}
