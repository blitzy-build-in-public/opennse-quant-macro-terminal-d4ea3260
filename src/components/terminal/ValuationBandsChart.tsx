"use client";

import { useMemo } from "react";
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
import { WidgetFrame } from "./shared/WidgetFrame";
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
const META_CLASS = "text-xs text-slate-400";
const UPDATING_CLASS = "text-sky-300";
const STATUS_NOTE_CLASS = "text-xs text-amber-300";
const PARTIAL_NOTE_CLASS = "text-xs text-slate-400";
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
// The custom tooltip mirrors TOOLTIP_CONTENT_STYLE (slate-900 surface, slate-700 border, slate-300
// text, 12px) with utility classes instead of an inline style.
const TOOLTIP_CLASS =
  "rounded border border-slate-700 bg-slate-900 px-2 py-1 text-xs text-slate-300 shadow-lg";
const TOOLTIP_VALUE_CLASS = "font-medium tabular-nums text-slate-100";
// Auto-filling columns reflow with the widget's own width, whatever the host's layout or viewport.
const STATS_CLASS = "grid grid-cols-[repeat(auto-fill,minmax(7rem,1fr))] gap-x-4 gap-y-2";
const STAT_TERM_CLASS = "text-xs text-slate-400";
const STAT_VALUE_CLASS = "text-sm font-medium tabular-nums text-slate-100";
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
 */
const BAND_LINES: ReadonlyArray<{
  name: "plus_2sd" | "plus_1sd" | "mean" | "minus_1sd" | "minus_2sd";
  label: string;
  stroke: string;
  labelFill: string;
  dashed: boolean;
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
  },
];

const BAND_DASH = "4 4";
// Band labels sit in the right margin, outside the plot, because inside it the weekly line runs
// through them wherever the multiple crosses a band. 76px holds a label such as "+2σ 123.45×".
const CHART_MARGIN = { top: 8, right: 76, bottom: 0, left: 0 };
const Y_AXIS_LABEL = {
  value: "×",
  position: "insideTopLeft" as const,
  fill: CHART_COLORS.axis,
  fontSize: 11,
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

/** Fraction digits kept from a generated tick position: enough for any step a ratio axis uses. */
const RATIO_TICK_FRACTION_DIGITS = 6;

/**
 * `YAxis` tick of the multiple axis (`4`, `12.5`, `1,234.5`, `−2`). A tick is a generated axis
 * coordinate with no server string behind it, so it is written here as a plain decimal — at most
 * {@link RATIO_TICK_FRACTION_DIGITS} fraction digits with trailing zeros dropped, which removes
 * float noise such as `0.15000000000000002`, and never in exponent notation — and then grouped and
 * signed by `formatDecimal`. The `×` unit is left to the axis label. Non-numbers give `""`.
 */
function formatRatioTick(value: unknown): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "";
  // toFixed switches to exponent notation from 10^21; BigInt prints every digit of such an
  // integral value.
  const text =
    Math.abs(value) >= 1e21
      ? BigInt(Math.round(value)).toString()
      : value.toFixed(RATIO_TICK_FRACTION_DIGITS).replace(/0+$/, "").replace(/\.$/, "");
  return formatDecimal(text, "ratio", { unit: false });
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
        <div key={item.name} data-testid={"bands-stat-" + item.name}>
          <dt className={STAT_TERM_CLASS}>{item.label}</dt>
          <dd className={STAT_VALUE_CLASS}>{formatDecimal(stats[item.name], item.kind)}</dd>
        </div>
      ))}
      <div data-testid="bands-stat-observations">
        <dt className={STAT_TERM_CLASS}>Observations</dt>
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
        <p className={PARTIAL_NOTE_CLASS} data-testid="bands-partial-coverage">
          Partial coverage from {formatDate(coverage.coverage_start)}
        </p>
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
                domain={["auto", "auto"]}
                tickFormatter={formatRatioTick}
                tick={AXIS_TICK}
                stroke={CHART_COLORS.axis}
                width={48}
                label={Y_AXIS_LABEL}
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
      skeletonClassName="h-64"
      className={className}
    >
      {shown !== undefined ? <BandsBody data={shown} updating={query.isFetching} /> : null}
    </WidgetFrame>
  );
}
