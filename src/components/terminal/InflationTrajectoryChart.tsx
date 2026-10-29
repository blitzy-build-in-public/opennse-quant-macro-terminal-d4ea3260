"use client";

import { useMemo } from "react";
import type { ReactElement, ReactNode } from "react";
import {
  CartesianGrid,
  ComposedChart,
  Line,
  ReferenceArea,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { TooltipContentProps } from "recharts";

import { AXIS_TICK, CHART_COLORS } from "./lib/chartTheme";
import { formatDate, formatDecimal, formatMonth, toChartNumber } from "./lib/format";
import { useTerminalQuery } from "./lib/useTerminalQuery";
import { WidgetFrame } from "./shared/WidgetFrame";
import type { DecimalString, MacroOverviewResponse, TerminalWidgetProps } from "./types";

/** Props of {@link InflationTrajectoryChart}. */
export interface InflationTrajectoryChartProps extends TerminalWidgetProps {
  /**
   * Number of month-ends of history to plot, ending at the latest observation month. Sent as
   * `history_months` (the API accepts 1–240). Default 24, the value `MacroSnapshot` and
   * `YieldCurveChart` use, so the three widgets share one `/macro/overview` request.
   */
  historyMonths?: number;
  /** Whether to shade CBK's 2.5–7.5% inflation target band. Default `true`. */
  showTargetBand?: boolean;
  /**
   * A `GET /macro/overview` response supplied by the host. While it is defined the widget sends
   * no request and plots this payload.
   */
  data?: MacroOverviewResponse;
}

const DEFAULT_HISTORY_MONTHS = 24;

// CBK's medium-term inflation target is 5% with a ±2.5 percentage-point tolerance band. The
// bounds are policy constants, not API values, so they are plotted as plain numbers.
const TARGET_BAND_LOWER_PCT = 2.5;
const TARGET_BAND_UPPER_PCT = 7.5;
const TARGET_BAND_LABEL = "CBK target 2.5–7.5%";
const TARGET_BAND_FILL_OPACITY = 0.12;

const INFLATION_LABEL = "12-month inflation";
const CBR_LABEL = "Central Bank Rate";

// Every class below is a complete static string, never assembled from fragments, so Tailwind's
// source scan and a host's `@source` directive or `content` glob find each one. The swatch
// colours mirror the chart strokes: amber-400 (`CHART_COLORS.warning`), sky-400
// (`CHART_COLORS.accent`) and emerald-500 (`CHART_COLORS.positive`).
// Recharts 3 renders its z-index layers as `<g tabindex="-1">`, which a mouse click focuses and
// Chrome then outlines. Those layers are never keyboard-reachable, so hiding their outline loses
// no keyboard indicator; the chart's own `<svg tabindex="0">` keeps its focus ring.
const CHART_CLASS = "h-64 w-full [&_g[tabindex='-1']]:outline-none";
// `list-none` and `ps-0` keep the legend flat in a host without Tailwind's preflight reset.
const LEGEND_CLASS =
  "mt-3 mb-0 flex list-none flex-wrap items-center gap-x-4 gap-y-1 ps-0 text-xs text-slate-300";
const LEGEND_ITEM_CLASS = "flex items-center gap-1.5";
const INFLATION_SWATCH_CLASS = "h-0.5 w-4 shrink-0 rounded-full bg-amber-400";
const CBR_SWATCH_CLASS = "h-0.5 w-4 shrink-0 rounded-full bg-sky-400";
const BAND_SWATCH_CLASS =
  "h-2.5 w-4 shrink-0 rounded-sm border border-emerald-500/60 bg-emerald-500/15";
const PARTIAL_COVERAGE_CLASS = "mt-2 mb-0 text-xs text-amber-300";
// Mirrors `TOOLTIP_CONTENT_STYLE` (slate-900 surface, slate-700 border, slate-300 12px text) with
// classes, since this custom tooltip is an HTML element rather than Recharts' default content.
const TOOLTIP_CLASS =
  "rounded border border-slate-700 bg-slate-900 px-2 py-1.5 text-xs text-slate-300";
const TOOLTIP_TITLE_CLASS = "mt-0 mb-1 font-medium text-slate-100";
const TOOLTIP_LIST_CLASS = "m-0 grid grid-cols-[auto_auto] items-center gap-x-3 gap-y-0.5";
const TOOLTIP_TERM_CLASS = "flex items-center gap-1.5";
const TOOLTIP_VALUE_CLASS = "m-0 text-end tabular-nums text-slate-100";

/** One month-end of the chart: both series at that date, as chart numbers and exact strings. */
interface ChartRow {
  /** ISO month-end date, the category of the x-axis. */
  date: string;
  /** 12-month inflation as a chart coordinate; null leaves a gap that `connectNulls` bridges. */
  inflation: number | null;
  /** 12-month inflation exactly as the API sent it, for display. */
  inflationRaw: DecimalString | null;
  /** Central Bank Rate as a chart coordinate. */
  cbr: number | null;
  /** Central Bank Rate exactly as the API sent it, for display. */
  cbrRaw: DecimalString | null;
}

/** The plotted rows with the number of points each line draws. */
interface ChartModel {
  rows: ChartRow[];
  inflationPoints: number;
  cbrPoints: number;
}

const EMPTY_MODEL: ChartModel = { rows: [], inflationPoints: 0, cbrPoints: 0 };

/** The widget has nothing to plot when neither series holds an observation. */
function isOverviewEmpty(data: MacroOverviewResponse): boolean {
  return data.inflation_series.length === 0 && data.rates_series.length === 0;
}

/** Compares ISO dates as text: `YYYY-MM-DD` sorts lexically in calendar order. */
function compareIsoDates(a: ChartRow, b: ChartRow): number {
  if (a.date < b.date) return -1;
  if (a.date > b.date) return 1;
  return 0;
}

/**
 * Merges the inflation series and the rates series into one row per date string, oldest first.
 * Dates are joined and sorted as strings and never parsed into `Date` objects, so the result
 * does not depend on the runtime's time zone and server and client renders agree.
 */
function buildChartModel(data: MacroOverviewResponse | undefined): ChartModel {
  if (data === undefined) return EMPTY_MODEL;

  const byDate = new Map<string, ChartRow>();
  const rowAt = (date: string): ChartRow => {
    const existing = byDate.get(date);
    if (existing !== undefined) return existing;
    const created: ChartRow = {
      date,
      inflation: null,
      inflationRaw: null,
      cbr: null,
      cbrRaw: null,
    };
    byDate.set(date, created);
    return created;
  };

  for (const point of data.inflation_series) {
    const row = rowAt(point.date);
    row.inflationRaw = point.value;
    row.inflation = toChartNumber(point.value);
  }
  for (const point of data.rates_series) {
    const row = rowAt(point.observation_date);
    row.cbrRaw = point.cbr_rate_pct;
    row.cbr = toChartNumber(point.cbr_rate_pct);
  }

  const rows = Array.from(byDate.values()).sort(compareIsoDates);
  let inflationPoints = 0;
  let cbrPoints = 0;
  for (const row of rows) {
    if (row.inflation !== null) inflationPoints += 1;
    if (row.cbr !== null) cbrPoints += 1;
  }
  return { rows, inflationPoints, cbrPoints };
}

/** X-axis tick: the month of an ISO date, as `Sep 2026`. */
function formatMonthTick(value: unknown): string {
  if (typeof value === "string") return formatMonth(value);
  return typeof value === "number" ? String(value) : "";
}

/** Narrows a Recharts tooltip entry's `payload`, typed `any` by Recharts, to a chart row. */
function isChartRow(value: unknown): value is ChartRow {
  return (
    typeof value === "object" &&
    value !== null &&
    "date" in value &&
    typeof value.date === "string" &&
    "inflationRaw" in value &&
    "cbrRaw" in value
  );
}

/**
 * Tooltip content: the hovered month-end with both rates formatted from the API's exact strings,
 * never from the chart coordinates.
 */
function InflationTooltip({ active, payload }: TooltipContentProps): ReactNode {
  if (!active) return null;
  const row: unknown = payload[0]?.payload;
  if (!isChartRow(row)) return null;
  return (
    <div className={TOOLTIP_CLASS}>
      <p className={TOOLTIP_TITLE_CLASS}>{formatDate(row.date)}</p>
      <dl className={TOOLTIP_LIST_CLASS}>
        <dt className={TOOLTIP_TERM_CLASS}>
          <span aria-hidden="true" className={INFLATION_SWATCH_CLASS} />
          {INFLATION_LABEL}
        </dt>
        <dd className={TOOLTIP_VALUE_CLASS}>{formatDecimal(row.inflationRaw, "pct")}</dd>
        <dt className={TOOLTIP_TERM_CLASS}>
          <span aria-hidden="true" className={CBR_SWATCH_CLASS} />
          {CBR_LABEL}
        </dt>
        <dd className={TOOLTIP_VALUE_CLASS}>{formatDecimal(row.cbrRaw, "pct")}</dd>
      </dl>
    </div>
  );
}

/** Text of the partial-coverage note; a window without any observation has no start date. */
function partialCoverageText(coverageStart: string | null): string {
  return coverageStart === null
    ? "Partial coverage: no observations in the requested window"
    : `Partial coverage from ${formatDate(coverageStart)}`;
}

/**
 * 12-month inflation against the Central Bank Rate over the last `historyMonths` month-ends,
 * with CBK's 2.5–7.5% target band shaded behind both lines (`GET /macro/overview`).
 *
 * The root carries `data-testid="terminal-inflation-trajectory-chart"` and `data-state`. The
 * HTML legend lists each series as `data-testid="inflation-legend-item"` with `data-series`
 * (`inflation`, `cbr`, `target_band`) and, for the lines, `data-point-count`. When the stored
 * history starts after the requested window, a "Partial coverage from <date>" note is shown.
 *
 * Chart coordinates are the only float use; the tooltip formats the API's decimal strings.
 * Works with default props alone, and needs no provider, layout or global CSS from its host.
 */
export function InflationTrajectoryChart(props: InflationTrajectoryChartProps): ReactElement {
  const {
    apiBaseUrl,
    getHeaders,
    className,
    historyMonths = DEFAULT_HISTORY_MONTHS,
    showTargetBand = true,
    data: providedData,
  } = props;

  // The query is exactly `{ history_months }`, so the URL matches the other macro widgets' and
  // the API client merges their concurrent requests into one.
  const { state, data, error, refetch } = useTerminalQuery<MacroOverviewResponse>({
    path: "/macro/overview",
    query: { history_months: historyMonths },
    apiBaseUrl,
    getHeaders,
    data: providedData,
    isEmpty: isOverviewEmpty,
  });

  const model = useMemo(() => buildChartModel(data), [data]);
  const coverage = data?.coverage;

  return (
    <WidgetFrame
      testId="terminal-inflation-trajectory-chart"
      title="Inflation trajectory"
      state={state}
      error={error}
      onRetry={refetch}
      className={className}
      skeletonClassName="h-64"
    >
      <div className={CHART_CLASS}>
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart
            data={model.rows}
            margin={{ top: 8, right: 16, bottom: 0, left: 0 }}
            title={`${INFLATION_LABEL} and ${CBR_LABEL}, percent`}
            desc={
              showTargetBand
                ? `Monthly ${INFLATION_LABEL} and ${CBR_LABEL} with the ${TARGET_BAND_LABEL} band`
                : `Monthly ${INFLATION_LABEL} and ${CBR_LABEL}`
            }
          >
            <CartesianGrid stroke={CHART_COLORS.grid} strokeDasharray="3 3" />
            <XAxis
              dataKey="date"
              type="category"
              tickFormatter={formatMonthTick}
              tick={AXIS_TICK}
              stroke={CHART_COLORS.axis}
              minTickGap={24}
            />
            <YAxis
              tick={AXIS_TICK}
              stroke={CHART_COLORS.axis}
              width={44}
              label={{
                value: "%",
                angle: -90,
                position: "insideLeft",
                fill: CHART_COLORS.axis,
                fontSize: 11,
              }}
            />
            {showTargetBand ? (
              <ReferenceArea
                y1={TARGET_BAND_LOWER_PCT}
                y2={TARGET_BAND_UPPER_PCT}
                fill={CHART_COLORS.positive}
                fillOpacity={TARGET_BAND_FILL_OPACITY}
                ifOverflow="extendDomain"
                // Bottom of the band: inflation rarely runs near 2.5% and the CBR sits above
                // the band, so the lines seldom reach the label there. Recharts draws labels
                // above the lines, and a stroke in the widget's surface colour painted under
                // the glyphs keeps the text legible wherever a line or gridline still crosses.
                label={{
                  value: TARGET_BAND_LABEL,
                  position: "insideBottomRight",
                  fill: CHART_COLORS.positive,
                  fontSize: 11,
                  stroke: CHART_COLORS.tooltipBg,
                  strokeWidth: 3,
                  strokeLinejoin: "round",
                  paintOrder: "stroke",
                }}
              />
            ) : null}
            <Line
              dataKey="inflation"
              name={INFLATION_LABEL}
              type="monotone"
              stroke={CHART_COLORS.warning}
              strokeWidth={2}
              dot={false}
              connectNulls
              isAnimationActive={false}
            />
            <Line
              dataKey="cbr"
              name={CBR_LABEL}
              type="stepAfter"
              stroke={CHART_COLORS.accent}
              strokeWidth={2}
              dot={false}
              connectNulls
              isAnimationActive={false}
            />
            <Tooltip
              content={InflationTooltip}
              cursor={{ stroke: CHART_COLORS.muted }}
              isAnimationActive={false}
            />
          </ComposedChart>
        </ResponsiveContainer>
      </div>
      <ul className={LEGEND_CLASS} aria-label="Legend">
        <li
          data-testid="inflation-legend-item"
          data-series="inflation"
          data-point-count={model.inflationPoints}
          className={LEGEND_ITEM_CLASS}
        >
          <span aria-hidden="true" className={INFLATION_SWATCH_CLASS} />
          {INFLATION_LABEL}
        </li>
        <li
          data-testid="inflation-legend-item"
          data-series="cbr"
          data-point-count={model.cbrPoints}
          className={LEGEND_ITEM_CLASS}
        >
          <span aria-hidden="true" className={CBR_SWATCH_CLASS} />
          {CBR_LABEL}
        </li>
        {showTargetBand ? (
          <li
            data-testid="inflation-legend-item"
            data-series="target_band"
            className={LEGEND_ITEM_CLASS}
          >
            <span aria-hidden="true" className={BAND_SWATCH_CLASS} />
            {TARGET_BAND_LABEL}
          </li>
        ) : null}
      </ul>
      {coverage?.is_partial_coverage ? (
        <p data-testid="inflation-partial-coverage" className={PARTIAL_COVERAGE_CLASS}>
          {partialCoverageText(coverage.coverage_start)}
        </p>
      ) : null}
    </WidgetFrame>
  );
}
