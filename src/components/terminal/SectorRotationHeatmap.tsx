"use client";

import { useId, useState } from "react";
import type { ReactElement } from "react";

import {
  EMPTY_VALUE,
  compareDecimal,
  formatDate,
  formatDecimal,
  formatInteger,
  formatMonth,
} from "./lib/format";
import { useTerminalQuery } from "./lib/useTerminalQuery";
import { WidgetFrame } from "./shared/WidgetFrame";
import type {
  DecimalString,
  HeatmapCell,
  HeatmapInterval,
  SectorHeatmapResponse,
  SectorHeatmapRow,
  TerminalWidgetProps,
} from "./types";

/** Props of {@link SectorRotationHeatmap}. */
export interface SectorRotationHeatmapProps extends TerminalWidgetProps {
  /**
   * Period boundaries: the last market session on or before each month-end (`"month"`) or each
   * Friday (`"week"`). Default `"month"`.
   */
  interval?: HeatmapInterval;
  /**
   * Number of periods ending at the latest stored session, an integer from 1 to 36. Default 12.
   * The API rejects other values with a 422, which the widget shows as its error state.
   */
  periods?: number;
  /**
   * A `GET /market/sector-heatmap` response supplied by the host. While it is defined the widget
   * sends no request and renders it as given.
   */
  data?: SectorHeatmapResponse;
}

/** What the sector cells show: the return minus the market's, or the sector's own return. */
type HeatmapMode = "relative" | "absolute";

/** Colour bucket of a cell value, in percent, by the thresholds −5, −2, 0, 2 and 5. */
type Bucket =
  | "strongNegative"
  | "negative"
  | "slightNegative"
  | "flat"
  | "slightPositive"
  | "positive"
  | "strongPositive"
  | "noData";

// Every class below is a complete static string, never assembled from fragments, so Tailwind's
// source scan and a host's `@source` directive or `content` glob find each one.
const BUCKET_CLASSES: Record<Bucket, string> = {
  strongNegative: "bg-rose-700 text-rose-50",
  negative: "bg-rose-800/80 text-rose-100",
  slightNegative: "bg-rose-950/70 text-rose-200",
  flat: "bg-slate-800 text-slate-200",
  slightPositive: "bg-emerald-950/70 text-emerald-200",
  positive: "bg-emerald-800/80 text-emerald-100",
  strongPositive: "bg-emerald-700 text-emerald-50",
  noData: "bg-slate-800 text-slate-500",
};

/** Legend entries in scale order; the labels use the U+2212 minus sign of the cell values. */
const LEGEND: ReadonlyArray<{ bucket: Bucket; label: string }> = [
  { bucket: "strongNegative", label: "≤ −5" },
  { bucket: "negative", label: "−5 to −2" },
  { bucket: "slightNegative", label: "−2 to 0" },
  { bucket: "flat", label: "0" },
  { bucket: "slightPositive", label: "0 to 2" },
  { bucket: "positive", label: "2 to 5" },
  { bucket: "strongPositive", label: "≥ 5" },
  { bucket: "noData", label: "No data" },
];

// The toggle keeps the compact size of the card's other text controls; its pressed variant is the
// sky accent used for focus and selection elsewhere in the terminal.
/* BLITZY [A11Y]: the mode toggle is about 26px tall (px-3 py-1 text-xs), above WCAG 2.2 AA's 24px
   target minimum and below the 44px guideline, matching WidgetFrame's Retry button. Flagged for
   designer review. */
const TOGGLE_CLASS =
  "rounded border border-slate-700 bg-slate-800 px-3 py-1 text-xs font-medium text-slate-100 hover:bg-slate-700 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400 motion-safe:transition-colors motion-safe:duration-150 motion-safe:ease-out";
const TOGGLE_PRESSED_CLASS =
  "rounded border border-sky-700 bg-sky-900 px-3 py-1 text-xs font-medium text-sky-100 hover:bg-sky-800 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400 motion-safe:transition-colors motion-safe:duration-150 motion-safe:ease-out";
// The scroller is a keyboard-focusable region, so the periods can be scrolled without a pointer,
// and a size container, so the caption text can wrap to its visible width.
const SCROLL_CLASS =
  "@container overflow-x-auto rounded focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400";
const TABLE_CLASS = "w-full border-collapse text-xs tabular-nums";
const CAPTION_CLASS = "caption-top pb-2 text-start text-xs text-slate-400";
// A caption box is as wide as its table, so on a narrow screen its text would run under the
// scroller's edge. The text instead wraps to the scroller's width and stays pinned while the
// periods scroll.
const CAPTION_TEXT_CLASS = "sticky start-0 inline-block max-w-[100cqi]";
// The sector column stays in view while the periods scroll horizontally; its opaque surface hides
// the cells passing beneath it.
const CORNER_HEADER_CLASS =
  "sticky start-0 z-10 whitespace-nowrap bg-slate-900 px-2 py-1.5 text-start font-medium text-slate-300";
const PERIOD_HEADER_CLASS = "whitespace-nowrap px-2 py-1.5 text-end font-medium text-slate-300";
const ROW_HEADER_CLASS =
  "sticky start-0 z-10 whitespace-nowrap bg-slate-900 px-2 py-1.5 text-start font-medium text-slate-200";
const CELL_CLASS = "whitespace-nowrap border border-slate-900 px-2 py-1.5 text-end";
// The market row is set apart from the sectors by a heavier rule above it.
const MARKET_HEADER_CLASS =
  "sticky start-0 z-10 whitespace-nowrap border-bs-2 border-bs-slate-600 bg-slate-900 px-2 py-1.5 text-start font-semibold text-slate-100";
const MARKET_CELL_CLASS =
  "whitespace-nowrap border border-slate-900 border-bs-2 border-bs-slate-600 px-2 py-1.5 text-end font-semibold";
const LEGEND_CLASS = "mt-3 flex flex-wrap items-center gap-x-4 gap-y-2 text-xs text-slate-400";
const LEGEND_LIST_CLASS = "flex flex-wrap items-center gap-x-3 gap-y-1";
const LEGEND_ITEM_CLASS = "flex items-center gap-1";
const SWATCH_CLASS = "inline-block h-3 w-3 rounded-sm";

const NO_CELL_TITLE = "No data for this period";

/**
 * Bucket of a percentage, compared exactly as a scaled integer, never as a float. A null value,
 * or text that is not a plain decimal, is `noData`: colouring it would claim a comparison that
 * was never made.
 */
function bucketOf(value: DecimalString | null): Bucket {
  const vsZero = compareDecimal(value, "0");
  if (vsZero === null) return "noData";
  if (vsZero === 0) return "flat";
  if (vsZero < 0) {
    const vsMinusFive = compareDecimal(value, "-5");
    if (vsMinusFive !== null && vsMinusFive <= 0) return "strongNegative";
    const vsMinusTwo = compareDecimal(value, "-2");
    if (vsMinusTwo !== null && vsMinusTwo <= 0) return "negative";
    return "slightNegative";
  }
  const vsFive = compareDecimal(value, "5");
  if (vsFive !== null && vsFive >= 0) return "strongPositive";
  const vsTwo = compareDecimal(value, "2");
  if (vsTwo !== null && vsTwo >= 0) return "positive";
  return "slightPositive";
}

/**
 * Background and text classes of a cell value: rose below zero, slate at zero, emerald above,
 * deeper beyond ±2 and ±5 percent; `≤ −5`, `(−5, −2]`, `(−2, 0)`, `0`, `(0, 2)`, `[2, 5)`, `≥ 5`.
 */
function bucketClass(value: DecimalString | null): string {
  return BUCKET_CLASSES[bucketOf(value)];
}

function isEmptyHeatmap(data: SectorHeatmapResponse): boolean {
  return data.period_ends.length === 0 || (data.sectors.length === 0 && data.market.length === 0);
}

/**
 * Cells keyed by `period_end`, so each lands in its own column whatever the array order. The
 * first cell of a repeated period wins.
 */
function cellsByPeriod(cells: readonly HeatmapCell[]): Map<string, HeatmapCell> {
  const byPeriod = new Map<string, HeatmapCell>();
  for (const cell of cells) {
    if (!byPeriod.has(cell.period_end)) byPeriod.set(cell.period_end, cell);
  }
  return byPeriod;
}

function formatReturn(value: DecimalString | null): string {
  return formatDecimal(value, "pct", { signed: true });
}

function constituentsText(count: number): string {
  return count === 1 ? "1 constituent" : `${formatInteger(count)} constituents`;
}

function sectorCellTitle(cell: HeatmapCell | undefined): string {
  if (cell === undefined) return NO_CELL_TITLE;
  return `Return ${formatReturn(cell.return_pct)} · vs market ${formatReturn(
    cell.relative_return_pct,
  )} · ${constituentsText(cell.constituents)}`;
}

/** The market's relative return is null by definition, so its title leaves that part out. */
function marketCellTitle(cell: HeatmapCell | undefined): string {
  if (cell === undefined) return NO_CELL_TITLE;
  return `Return ${formatReturn(cell.return_pct)} · ${constituentsText(cell.constituents)}`;
}

interface ValueCellProps {
  /** The value shown and coloured: a percentage, or null for a dash on a neutral surface. */
  value: DecimalString | null;
  /** Pointer tooltip with the absolute and relative returns and the constituent count. */
  title: string;
  /** Layout classes of the cell, without its colour bucket. */
  className: string;
  testId: string;
}

function ValueCell({ value, title, className, testId }: ValueCellProps): ReactElement {
  const missing = value === null || value === "";
  return (
    <td data-testid={testId} title={title} className={`${className} ${bucketClass(value)}`}>
      {missing ? (
        <>
          <span aria-hidden="true">{EMPTY_VALUE}</span>
          <span className="sr-only">No data</span>
        </>
      ) : (
        formatReturn(value)
      )}
    </td>
  );
}

interface SectorRowProps {
  row: SectorHeatmapRow;
  periodEnds: readonly string[];
  mode: HeatmapMode;
}

function SectorRow({ row, periodEnds, mode }: SectorRowProps): ReactElement {
  const byPeriod = cellsByPeriod(row.cells);
  return (
    <tr data-testid="heatmap-row" data-sector={row.sector}>
      <th scope="row" className={ROW_HEADER_CLASS}>
        {row.sector}
      </th>
      {periodEnds.map((period, index) => {
        const cell = byPeriod.get(period);
        const value =
          cell === undefined
            ? null
            : mode === "relative"
              ? cell.relative_return_pct
              : cell.return_pct;
        return (
          <ValueCell
            key={`${index}:${period}`}
            testId="heatmap-cell"
            value={value}
            title={sectorCellTitle(cell)}
            className={CELL_CLASS}
          />
        );
      })}
    </tr>
  );
}

interface HeatmapTableProps {
  data: SectorHeatmapResponse;
  mode: HeatmapMode;
}

function HeatmapTable({ data, mode }: HeatmapTableProps): ReactElement {
  const captionId = useId();
  const isWeekly = data.interval === "week";
  const marketByPeriod = cellsByPeriod(data.market);
  return (
    <>
      <div role="region" aria-labelledby={captionId} tabIndex={0} className={SCROLL_CLASS}>
        <table className={TABLE_CLASS}>
          <caption id={captionId} className={CAPTION_CLASS}>
            <span className={CAPTION_TEXT_CLASS}>
              {`Capitalization-weighted sector returns (%), ${
                isWeekly ? "weekly" : "monthly"
              } periods ending ${formatDate(data.as_of)}`}
            </span>
          </caption>
          <thead>
            <tr>
              <th scope="col" className={CORNER_HEADER_CLASS}>
                Sector
              </th>
              {data.period_ends.map((period, index) => (
                <th
                  key={`${index}:${period}`}
                  scope="col"
                  data-testid="heatmap-period"
                  className={PERIOD_HEADER_CLASS}
                >
                  <time dateTime={period}>{isWeekly ? formatDate(period) : formatMonth(period)}</time>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.sectors.map((row, index) => (
              <SectorRow
                key={`${index}:${row.sector}`}
                row={row}
                periodEnds={data.period_ends}
                mode={mode}
              />
            ))}
          </tbody>
          <tfoot>
            <tr data-testid="heatmap-market-row">
              <th scope="row" className={MARKET_HEADER_CLASS}>
                Market
              </th>
              {data.period_ends.map((period, index) => {
                const cell = marketByPeriod.get(period);
                // The market row always shows its absolute return: its relative return is null by
                // definition, so the mode toggle never applies to it. Its cells carry their own test
                // id, so `heatmap-cell` counts exactly sectors × periods.
                return (
                  <ValueCell
                    key={`${index}:${period}`}
                    testId="heatmap-market-cell"
                    value={cell === undefined ? null : cell.return_pct}
                    title={marketCellTitle(cell)}
                    className={MARKET_CELL_CLASS}
                  />
                );
              })}
            </tr>
          </tfoot>
        </table>
      </div>
      <div className={LEGEND_CLASS}>
        <p data-testid="heatmap-mode-description">
          {mode === "relative"
            ? "Showing: Relative to market (sector return minus market return). Market row: absolute return."
            : "Showing: Absolute return (sector return over each period). Market row: absolute return."}
        </p>
        <ul aria-label="Colour scale, %" className={LEGEND_LIST_CLASS}>
          {LEGEND.map(({ bucket, label }) => (
            <li key={bucket} className={LEGEND_ITEM_CLASS}>
              <span aria-hidden="true" className={`${SWATCH_CLASS} ${BUCKET_CLASSES[bucket]}`} />
              <span>{label}</span>
            </li>
          ))}
        </ul>
      </div>
    </>
  );
}

/**
 * Sector rotation heatmap: one row per sector and one column per period, each cell the
 * capitalization-weighted return of the sector's constituents over the period
 * (`GET /market/sector-heatmap`). Cells show, and are coloured by, the return relative to the
 * market by default; the header toggle switches them to the absolute return without a new
 * request. A final Market row always shows the whole market's absolute return.
 *
 * Values are displayed and bucketed from the API's decimal strings, never converted to numbers.
 * The grid is a semantic `<table>` with row and column headers, so it reads without a chart.
 */
export function SectorRotationHeatmap({
  interval = "month",
  periods = 12,
  data,
  apiBaseUrl,
  getHeaders,
  className,
}: SectorRotationHeatmapProps): ReactElement {
  const [mode, setMode] = useState<HeatmapMode>("relative");
  const query = useTerminalQuery<SectorHeatmapResponse>({
    path: "/market/sector-heatmap",
    query: { interval, periods },
    apiBaseUrl,
    getHeaders,
    data,
    isEmpty: isEmptyHeatmap,
  });
  const isAbsolute = mode === "absolute";

  // A toggle button keeps one label while `aria-pressed` carries its state, as the WAI-ARIA
  // toggle-button pattern requires; the current mode is named in words beside the legend.
  const toggle = (
    <button
      type="button"
      data-testid="heatmap-mode-toggle"
      aria-pressed={isAbsolute}
      onClick={() => setMode((current) => (current === "absolute" ? "relative" : "absolute"))}
      className={isAbsolute ? TOGGLE_PRESSED_CLASS : TOGGLE_CLASS}
    >
      Absolute return
    </button>
  );

  return (
    <WidgetFrame
      testId="terminal-sector-rotation-heatmap"
      title="Sector rotation"
      state={query.state}
      error={query.error}
      onRetry={query.refetch}
      className={className}
      skeletonClassName="h-56"
      actions={toggle}
    >
      {query.data === undefined ? null : <HeatmapTable data={query.data} mode={mode} />}
    </WidgetFrame>
  );
}
