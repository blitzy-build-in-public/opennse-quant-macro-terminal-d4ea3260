"use client";

import { useMemo } from "react";
import type { ReactElement } from "react";
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
  "Bars: daily foreign net flow in KES on the left axis. Line: cumulative foreign net flow in KES on the right axis. Use the arrow keys to move between sessions.";
const NO_FLOW_MESSAGE =
  "No foreign-flow data in this range — per-ticker foreign flow is not published on public pages; import it with the CSV importer";
const LOCAL_FLOW_NOTE = "Local net flow is estimated as the negative of foreign net flow";

/** U+2212 minus sign, matching the formatted decimals of `lib/format.ts`. */
const MINUS_SIGN = "\u2212";

// Every class below is a complete static string, never assembled from fragments, so Tailwind's
// source scan and a host's `@source` directive or `content` glob find each one. Only the default
// slate, emerald, rose and sky families are used.
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
const CHART_CLASS =
  "h-64 w-full [&_g[tabindex='-1']]:outline-none [&_.recharts-surface:focus:not(:focus-visible)]:outline-none [&_.recharts-surface:is(:focus-visible,:has(:focus-visible))]:[outline-style:solid] [&_.recharts-surface:is(:focus-visible,:has(:focus-visible))]:outline-2 [&_.recharts-surface:is(:focus-visible,:has(:focus-visible))]:outline-offset-2 [&_.recharts-surface:is(:focus-visible,:has(:focus-visible))]:outline-sky-400";
const LEGEND_CLASS = "flex flex-col gap-1 text-xs";
const LEGEND_LIST_CLASS =
  "m-0 flex list-none flex-wrap items-center gap-x-4 gap-y-1 p-0 text-slate-300";
const LEGEND_ITEM_CLASS = "flex items-center gap-1.5";
const SWATCH_POSITIVE_CLASS = "inline-block h-2.5 w-2.5 rounded-sm bg-emerald-500";
const SWATCH_NEGATIVE_CLASS = "inline-block h-2.5 w-2.5 rounded-sm bg-rose-500";
const SWATCH_LINE_CLASS = "inline-block h-0.5 w-4 rounded-sm bg-sky-400";
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
// Mirrors TOOLTIP_CONTENT_STYLE (slate-900 surface, slate-700 border, slate-300 text, 12px) with
// utility classes: this content is rendered by the widget, not by the Recharts default content that
// takes `contentStyle`, so it needs no inline style.
const TOOLTIP_CLASS =
  "flex flex-col gap-1 rounded border border-slate-700 bg-slate-900 px-2.5 py-2 text-xs text-slate-300 shadow-lg";
const TOOLTIP_TITLE_CLASS = "m-0 font-medium text-slate-100";
const TOOLTIP_LIST_CLASS = "m-0 grid grid-cols-[auto_auto] gap-x-3 gap-y-0.5";
const TOOLTIP_TERM_CLASS = "text-slate-400";
const TOOLTIP_VALUE_CLASS = "m-0 text-end tabular-nums text-slate-100";

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
 * Abbreviates a KES axis coordinate, such as `"12M"` or `"−3B"`. Axis ticks are positions that
 * Recharts generates, not values from the API, so they are formatted from the coordinate itself;
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

/** Whether a Recharts payload entry carries one of this chart's rows. */
function isFlowChartRow(value: unknown): value is FlowChartRow {
  return typeof value === "object" && value !== null && "date" in value && "point" in value;
}

/**
 * Daily bar shape coloured by the exact sign of its own session's foreign net string: emerald at
 * or above zero, rose below it, muted when the value is null or the entry is not one of this
 * chart's rows. Used instead of `Cell`, which the pinned Recharts 3.10 deprecates for removal in
 * 4.0.
 */
function renderFlowBar(props: BarShapeProps): ReactElement {
  const row: unknown = props.payload;
  const fill = isFlowChartRow(row) ? barFill(row.point.foreign_net_kes) : CHART_COLORS.muted;
  return <Rectangle {...props} fill={fill} />;
}

/** One labelled value of the tooltip's description list. */
function TooltipEntry({ label, value }: { label: string; value: string }): ReactElement {
  return (
    <>
      <dt className={TOOLTIP_TERM_CLASS}>{label}</dt>
      <dd className={TOOLTIP_VALUE_CLASS}>{value}</dd>
    </>
  );
}

/**
 * Tooltip content for the hovered or keyboard-focused session. Every amount is formatted from the
 * API's exact string, never from the chart coordinate.
 */
function FlowTooltip({ active, payload }: TooltipContentProps): ReactElement | null {
  if (!active) return null;
  const row: unknown = payload[0]?.payload;
  if (!isFlowChartRow(row)) return null;
  const point = row.point;
  return (
    <div className={TOOLTIP_CLASS}>
      <p className={TOOLTIP_TITLE_CLASS}>{formatDate(point.trade_date)}</p>
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

  const noFlowData = data.status === "no_flow_data";
  // Without flow data the chart is kept only when it still has turnover rows to show.
  const showChart =
    rows.length > 0 && (!noFlowData || data.series.some((point) => point.turnover_kes !== null));
  const rangeText = `${formatDate(data.start)} – ${formatDate(data.end)}`;
  const subjectText = data.subject === MARKET_SUBJECT ? MARKET_LABEL : data.subject;

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
        <figure className={FIGURE_CLASS}>
          <div className={CHART_CLASS} data-testid="flows-chart">
            <ResponsiveContainer width="100%" height="100%">
              <ComposedChart
                data={rows}
                margin={{ top: 8, right: 4, bottom: 0, left: 4 }}
                title={`Foreign net flow — ${subjectText}, ${rangeText}`}
                desc={CHART_DESCRIPTION}
              >
                <CartesianGrid stroke={CHART_COLORS.grid} strokeDasharray="3 3" vertical={false} />
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
                  tickFormatter={formatAxisAmount}
                  tick={AXIS_TICK}
                  stroke={CHART_COLORS.axis}
                  width={56}
                />
                <YAxis
                  yAxisId="right"
                  orientation="right"
                  tickFormatter={formatAxisAmount}
                  tick={AXIS_TICK}
                  stroke={CHART_COLORS.axis}
                  width={56}
                />
                <Tooltip
                  content={FlowTooltip}
                  cursor={{ fill: CHART_COLORS.grid, fillOpacity: 0.4 }}
                  isAnimationActive={false}
                />
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
