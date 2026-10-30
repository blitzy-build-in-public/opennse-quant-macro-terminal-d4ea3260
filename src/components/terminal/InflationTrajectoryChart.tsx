"use client";

import { useCallback, useMemo, useState } from "react";
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
import { PartialCoverageNote, WidgetFrame } from "./shared/WidgetFrame";
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
// The band has no edge, but a 16×8 key at the band's faint fill would barely show, so the key
// adds an edge of the same colour.
const BAND_SWATCH_EDGE_OPACITY = 0.6;
// Under forced colours the band label's surface-coloured halo would smear over the forced
// background, so it is dropped there. Recharts 3.10.1 replaces its own `recharts-label` class
// with a label's `className` instead of adding to it, so the library class is repeated here to
// keep the label matching `.recharts-label` selectors.
const TARGET_BAND_LABEL_CLASS = "recharts-label forced-colors:stroke-none";

const INFLATION_LABEL = "12-month inflation";
const CBR_LABEL = "Central Bank Rate";
// Both lines' width in px, which their legend and tooltip keys repeat.
const LINE_STROKE_WIDTH = 2;

// A line with exactly one plottable point has no segment to stroke, so that point is drawn as a
// dot in the line's colour; it would otherwise be invisible. Longer lines stay dot-free.
const INFLATION_LONE_POINT_DOT = {
  r: 3,
  fill: CHART_COLORS.warning,
  stroke: CHART_COLORS.warning,
  strokeWidth: 1,
};
const CBR_LONE_POINT_DOT = {
  r: 3,
  fill: CHART_COLORS.accent,
  stroke: CHART_COLORS.accent,
  strokeWidth: 1,
};

// The axis titles, their sizes and the chart's margins follow YieldCurveChart, its sibling on the
// overview, so both cards label their axes alike.
const Y_AXIS_WIDTH_PX = 52;
const X_AXIS_HEIGHT_PX = 40;
// The widest month label ("May 2026") is about 48px at the axis's 11px in common sans-serif
// faces; 52px leaves headroom for a wider host font.
const MONTH_TICK_WIDTH_PX = 52;
// Clear space kept between neighbouring month labels; monthTicks adds it to the label width.
const MONTH_TICK_GAP_PX = 24;
// The latest month is labelled centred on the plot's right edge, and the chart's SVG clips what
// is drawn past its own edge, so the right margin holds half a label.
const CHART_MARGIN = { top: 8, right: MONTH_TICK_WIDTH_PX / 2, bottom: 4, left: 4 };
// Chart width assumed until ResponsiveContainer reports the real one; it only decides which
// months the first frame labels.
const FALLBACK_CHART_WIDTH_PX = 480;
// Fraction digits of a generated y-axis tick: the server's 2 dp for both rates.
const RATE_TICK_PLACES = 2;

// Every class below is a complete static string, never assembled from fragments, so Tailwind's
// source scan and a host's `@source` directive or `content` glob find each one.
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
// `list-none` and `ps-0` keep the legend flat in a host without Tailwind's preflight reset.
const LEGEND_CLASS =
  "mt-3 mb-0 flex list-none flex-wrap items-center gap-x-4 gap-y-1 ps-0 text-xs text-slate-300";
const LEGEND_ITEM_CLASS = "flex items-center gap-2";
// The keys are inline SVG in the chart's own colours, the form YieldCurveChart's legend uses. A
// CSS background would be replaced by the forced background colour and vanish under forced
// colours, so the keys also opt out of colour forcing: they are the only link from a series name
// to its line.
const SWATCH_CLASS = "h-2 w-4 shrink-0 forced-color-adjust-none";
// The loading skeleton's heights are the loaded chart and legend, measured on the standalone page
// with the seeded data at each viewport width where they reflow, so the card keeps its height
// when the data arrives.
const SKELETON_HEIGHT_CLASS = "h-[20.25rem] min-[323px]:h-[19rem] min-[473px]:h-[17.75rem]";
// Mirrors `TOOLTIP_CONTENT_STYLE` (slate-900 surface, slate-700 border, slate-300 12px text) with
// classes, since this custom tooltip is an HTML element rather than Recharts' default content.
const TOOLTIP_CLASS =
  "rounded border border-slate-700 bg-slate-900 px-2 py-1.5 text-xs text-slate-300";
const TOOLTIP_TITLE_CLASS = "mt-0 mb-1 font-medium text-slate-100";
const TOOLTIP_LIST_CLASS = "m-0 grid grid-cols-[auto_auto] items-center gap-x-3 gap-y-0.5";
const TOOLTIP_TERM_CLASS = "flex items-center gap-2";
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

/**
 * The widget has nothing to plot when neither line has a point: no inflation value and no CBR
 * value that {@link buildChartModel} turns into a chart coordinate. Null values, and rates rows
 * that carry only interbank or T-bill rates, therefore leave the widget empty, while a single
 * plottable value of either series keeps the chart. It reads the chart model's own point counts,
 * so the empty state and the legend's `data-point-count` always agree.
 */
function isOverviewEmpty(data: MacroOverviewResponse): boolean {
  const model = buildChartModel(data);
  return model.inflationPoints === 0 && model.cbrPoints === 0;
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

/**
 * The months that the x-axis labels and the vertical gridlines mark: every `step`-th row counted
 * back from the latest, so the latest month is always labelled, with `step` the smallest that
 * keeps neighbouring labels {@link MONTH_TICK_WIDTH_PX} plus {@link MONTH_TICK_GAP_PX} apart.
 * Recharts spaces the rows evenly from the plot's left edge to its right edge, and a single row
 * sits alone in the middle. The axis and the grid both take this list, so every dashed gridline
 * sits on a labelled tick; left to Recharts, the grid thins the months with its own label
 * measurements and keeps a different set than the axis.
 */
function monthTicks(rows: readonly ChartRow[], chartWidth: number): string[] {
  const lastIndex = rows.length - 1;
  if (lastIndex < 0) return [];
  const plotWidth = chartWidth - CHART_MARGIN.left - CHART_MARGIN.right - Y_AXIS_WIDTH_PX;
  const rowSpacing = lastIndex > 0 && plotWidth > 0 ? plotWidth / lastIndex : 0;
  // Without room between rows (one row, or a plot with no width) only the latest is labelled.
  const step =
    rowSpacing > 0
      ? Math.max(1, Math.ceil((MONTH_TICK_WIDTH_PX + MONTH_TICK_GAP_PX) / rowSpacing))
      : rows.length;
  const ticks: string[] = [];
  for (let index = lastIndex; index >= 0; index -= step) {
    const row = rows[index];
    if (row !== undefined) ticks.push(row.date);
  }
  return ticks.reverse();
}

/** X-axis tick: the month of an ISO date, as `Sep 2026`. */
function formatMonthTick(value: unknown): string {
  if (typeof value === "string") return formatMonth(value);
  return typeof value === "number" ? String(value) : "";
}

/**
 * Y-axis tick, in percentage points without the `%` that the axis title "Rate (%)" already
 * carries: `8`, `2.5`, `−2`. A tick is a generated axis coordinate with no server string behind
 * it, so it is written here as a plain decimal string, rounded to {@link RATE_TICK_PLACES} places
 * with trailing fraction zeros and a bare `.` removed and never in exponent notation (`0.3` for
 * `0.30000000000000004`), and then grouped and signed by `formatDecimal`, as the tooltip's exact
 * rates are. A tick that is not a finite number has no label.
 */
function formatRateTick(value: unknown): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "";
  // `toFixed` switches to exponent notation from 1e21; a float that large is integral, so BigInt
  // prints all of its digits instead.
  const fixed =
    Math.abs(value) < 1e21 ? value.toFixed(RATE_TICK_PLACES) : BigInt(value).toString();
  const text = fixed.replace(/(\.\d*?)0+$/, "$1").replace(/\.$/, "");
  return formatDecimal(text, "pct", { unit: false });
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

/** Key of a line series: a stroke in the line's colour and width. Neither line draws dots. */
function LineSwatch({ color }: { color: string }): ReactElement {
  return (
    <svg viewBox="0 0 16 8" aria-hidden="true" focusable="false" className={SWATCH_CLASS}>
      <line x1="0" y1="4" x2="16" y2="4" stroke={color} strokeWidth={LINE_STROKE_WIDTH} />
    </svg>
  );
}

/** Key of the target band: the band's fill inside an edge of the same colour. */
function BandSwatch(): ReactElement {
  return (
    <svg viewBox="0 0 16 8" aria-hidden="true" focusable="false" className={SWATCH_CLASS}>
      <rect
        x="0.5"
        y="0.5"
        width="15"
        height="7"
        rx="1"
        fill={CHART_COLORS.positive}
        fillOpacity={TARGET_BAND_FILL_OPACITY}
        stroke={CHART_COLORS.positive}
        strokeOpacity={BAND_SWATCH_EDGE_OPACITY}
        strokeWidth={1}
      />
    </svg>
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
          <LineSwatch color={CHART_COLORS.warning} />
          {INFLATION_LABEL}
        </dt>
        <dd className={TOOLTIP_VALUE_CLASS}>{formatDecimal(row.inflationRaw, "pct")}</dd>
        <dt className={TOOLTIP_TERM_CLASS}>
          <LineSwatch color={CHART_COLORS.accent} />
          {CBR_LABEL}
        </dt>
        <dd className={TOOLTIP_VALUE_CLASS}>{formatDecimal(row.cbrRaw, "pct")}</dd>
      </dl>
    </div>
  );
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
  // The width ResponsiveContainer last measured, rounded as Recharts rounds the chart's own; null
  // until the first measurement.
  const [chartWidth, setChartWidth] = useState<number | null>(null);
  const handleResize = useCallback((width: number) => {
    if (Number.isFinite(width)) setChartWidth(Math.round(width));
  }, []);
  const ticks = useMemo(
    () => monthTicks(model.rows, chartWidth ?? FALLBACK_CHART_WIDTH_PX),
    [model.rows, chartWidth],
  );

  return (
    <WidgetFrame
      testId="terminal-inflation-trajectory-chart"
      title="Inflation trajectory"
      state={state}
      error={error}
      onRetry={refetch}
      className={className}
      skeletonClassName={SKELETON_HEIGHT_CLASS}
    >
      <div className={CHART_CLASS}>
        <ResponsiveContainer width="100%" height="100%" onResize={handleResize}>
          <ComposedChart
            data={model.rows}
            margin={CHART_MARGIN}
            title={`${INFLATION_LABEL} and ${CBR_LABEL}, percent`}
            desc={
              showTargetBand
                ? `Monthly ${INFLATION_LABEL} and ${CBR_LABEL} with the ${TARGET_BAND_LABEL} band`
                : `Monthly ${INFLATION_LABEL} and ${CBR_LABEL}`
            }
          >
            {/* Vertical lines only at the labelled months, without the extra edge lines. */}
            <CartesianGrid
              stroke={CHART_COLORS.grid}
              strokeDasharray="3 3"
              verticalValues={ticks}
            />
            <XAxis
              dataKey="date"
              type="category"
              ticks={ticks}
              // Every listed month is labelled: the list is already thinned to the plot's width.
              interval={0}
              tickFormatter={formatMonthTick}
              tick={AXIS_TICK}
              stroke={CHART_COLORS.axis}
              height={X_AXIS_HEIGHT_PX}
              label={{
                value: "Month-end",
                position: "insideBottom",
                fill: CHART_COLORS.axis,
                fontSize: 11,
              }}
            />
            <YAxis
              tickFormatter={formatRateTick}
              tick={AXIS_TICK}
              stroke={CHART_COLORS.axis}
              width={Y_AXIS_WIDTH_PX}
              label={{
                value: "Rate (%)",
                angle: -90,
                position: "insideLeft",
                // Centres the rotated label on the axis; the position alone anchors it at its start.
                textAnchor: "middle",
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
                  className: TARGET_BAND_LABEL_CLASS,
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
              strokeWidth={LINE_STROKE_WIDTH}
              dot={model.inflationPoints === 1 ? INFLATION_LONE_POINT_DOT : false}
              connectNulls
              isAnimationActive={false}
            />
            <Line
              dataKey="cbr"
              name={CBR_LABEL}
              type="stepAfter"
              stroke={CHART_COLORS.accent}
              strokeWidth={LINE_STROKE_WIDTH}
              dot={model.cbrPoints === 1 ? CBR_LONE_POINT_DOT : false}
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
          <LineSwatch color={CHART_COLORS.warning} />
          {INFLATION_LABEL}
        </li>
        <li
          data-testid="inflation-legend-item"
          data-series="cbr"
          data-point-count={model.cbrPoints}
          className={LEGEND_ITEM_CLASS}
        >
          <LineSwatch color={CHART_COLORS.accent} />
          {CBR_LABEL}
        </li>
        {showTargetBand ? (
          <li
            data-testid="inflation-legend-item"
            data-series="target_band"
            className={LEGEND_ITEM_CLASS}
          >
            <BandSwatch />
            {TARGET_BAND_LABEL}
          </li>
        ) : null}
      </ul>
      {coverage?.is_partial_coverage ? (
        <PartialCoverageNote
          testId="inflation-partial-coverage"
          coverageStart={coverage.coverage_start}
          className="mt-2 mb-0"
        />
      ) : null}
    </WidgetFrame>
  );
}
