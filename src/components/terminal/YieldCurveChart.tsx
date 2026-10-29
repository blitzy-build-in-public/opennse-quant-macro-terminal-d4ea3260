"use client";

import { useMemo } from "react";
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

/** One plotted point: axis coordinates plus the untouched API point the tooltip displays. */
interface ChartRow {
  /** Tenor in years, a chart coordinate only. */
  x: number;
  /** Yield in percentage points, a chart coordinate only. */
  y: number;
  /** The API point, whose decimal strings are what the tooltip shows. */
  point: YieldCurvePoint;
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
  /** Points in ascending tenor. */
  points: ChartRow[];
}

const DEFAULT_HISTORY_MONTHS = 24;

// Module-level, so the default keeps one identity and never invalidates the series memo.
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
// A mouse click focuses one of Recharts' internal SVG layers (tabindex -1), and Chrome then draws
// its focus ring around part of the plot. The ring is removed only for focus that is not
// :focus-visible, so keyboard focus on the chart surface keeps its indicator.
const CHART_CLASS = "h-64 w-full [&_*:focus:not(:focus-visible)]:outline-none";
const CAPTION_CLASS = "flex flex-col gap-2";
const LEGEND_CLASS = "m-0 flex list-none flex-wrap gap-x-4 gap-y-1 p-0 text-xs text-slate-300";
const LEGEND_ITEM_CLASS = "flex items-center gap-2";
const SWATCH_CLASS = "h-2 w-4 shrink-0";
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
 * Turns API points into plotted rows in ascending tenor, dropping a point whose tenor or yield
 * has no chart coordinate. The order is decided by exact decimal comparison, because chart
 * numbers are coordinates only and are never compared (lib/format.ts); a pair the exact
 * comparison cannot read keeps the server's ascending-tenor order.
 */
function toRows(points: readonly YieldCurvePoint[]): ChartRow[] {
  const rows: ChartRow[] = [];
  for (const point of points) {
    const x = toChartNumber(point.tenor_years);
    const y = toChartNumber(point.yield_pct);
    if (x !== null && y !== null) {
      rows.push({ x, y, point });
    }
  }
  return rows.sort((a, b) => compareDecimal(a.point.tenor_years, b.point.tenor_years) ?? 0);
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
      points: toRows(current),
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
    const points = toRows(snapshot.points);
    if (points.length === 0) {
      return;
    }
    series.push({
      id: "m" + months,
      name: comparisonName(months, snapshot),
      color: comparisonColor(position),
      strokeWidth: COMPARISON_STROKE_WIDTH,
      points,
    });
  });
  return series;
}

function isChartRow(value: unknown): value is ChartRow {
  if (typeof value !== "object" || value === null || !("point" in value)) {
    return false;
  }
  const point: unknown = value.point;
  return (
    typeof point === "object" &&
    point !== null &&
    "tenor_label" in point &&
    "yield_pct" in point &&
    "observation_date" in point &&
    "source" in point
  );
}

/** One tooltip block: the hovered point of one curve. */
interface TooltipEntry {
  key: string;
  name: string;
  color: string;
  point: YieldCurvePoint;
}

/**
 * Tooltip of the chart. The tooltip follows the tenor axis, so it lists every curve with a
 * point at the hovered tenor, each named, with the API's own strings: the yield through
 * `formatDecimal`, the observation date and the provenance of that point.
 */
function YieldCurveTooltip({ active, payload }: TooltipContentProps): ReactNode {
  if (!active) {
    return null;
  }
  const entries: TooltipEntry[] = [];
  payload.forEach((entry, index) => {
    const row: unknown = entry.payload;
    if (isChartRow(row)) {
      entries.push({
        key: `${entry.graphicalItemId}:${index}`,
        name: typeof entry.name === "string" ? entry.name : String(entry.name ?? ""),
        color: entry.color ?? entry.stroke ?? CHART_COLORS.text,
        point: row.point,
      });
    }
  });
  if (entries.length === 0) {
    return null;
  }
  return (
    <div className={TOOLTIP_CLASS}>
      <ul className={TOOLTIP_LIST_CLASS}>
        {entries.map(({ key, name, color, point }) => (
          <li key={key}>
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
  const series = useMemo(() => buildSeries(query.data, ages), [query.data, ages]);

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
              <YAxis
                type="number"
                dataKey="y"
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
                  data={curve.points}
                  dataKey="y"
                  name={curve.name}
                  type="linear"
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
