"use client";

import type { ReactElement } from "react";

import { EMPTY_VALUE, compareDecimal, formatDecimal } from "./lib/format";
import type { DecimalString, SensitivityCell, SensitivityMatrixData } from "./types";

/**
 * Props of {@link SensitivityMatrix}. The component is presentational and never fetches, so it
 * takes no `apiBaseUrl` or `getHeaders`.
 */
export interface SensitivityMatrixProps {
  /**
   * The `sensitivity` object of `IpoValuationResponse` from `POST /ipo/valuation`: WACC rows (%),
   * base EBITDA margin columns (%), and one blended fair value cell per row and column.
   */
  data: SensitivityMatrixData;
  /**
   * Offer price, KES per share, as the API's decimal string. A cell is green when its fair value
   * is at or above the offer and rose when below it.
   */
  offerPriceKes: DecimalString;
  /**
   * The assumptions currently applied. The cell whose WACC and base margin equal these values
   * numerically (`"18"` matches `"18.00"`) is outlined and marked `aria-current`.
   */
  selected?: {
    /** Current WACC, %. */
    waccPct: DecimalString;
    /** Current base EBITDA margin, %. */
    ebitdaMarginPct: DecimalString;
  };
  /** Extra classes merged onto the root element. */
  className?: string;
}

const ROOT_CLASS = "bg-slate-900 text-slate-100 overflow-x-auto";

// Every class below is a complete static string, never assembled from fragments, so Tailwind's
// source scan and a host's `@source` directive find each one.
const HEADER_CLASS = "whitespace-nowrap px-2 py-1.5 font-medium text-slate-300";
const CELL_CLASS = "border border-slate-900 px-2 py-1.5 text-end align-top";
const TONE_AT_OR_ABOVE_OFFER = "bg-emerald-900/60 text-emerald-100";
const TONE_BELOW_OFFER = "bg-rose-900/60 text-rose-100";
const TONE_NOT_COMPARABLE = "bg-slate-800 text-slate-400";
const SELECTED_CLASS = "ring-2 ring-inset ring-sky-400";

/** An axis value with a React key that is unique within its axis. */
interface AxisEntry {
  value: DecimalString;
  key: string;
}

/**
 * Pairs each axis value with a key: the value itself, or the value suffixed with its index when
 * it repeats an earlier one, so a duplicated axis value cannot produce colliding keys.
 */
function axisEntries(values: readonly DecimalString[]): AxisEntry[] {
  const seen = new Set<string>();
  return values.map((value, index) => {
    const key = seen.has(value) ? `${value}#${index}` : value;
    seen.add(value);
    return { value, key };
  });
}

/** Whether the cell sits at the selected WACC and base margin, compared as exact decimals. */
function isSelectedCell(
  cell: SensitivityCell,
  selected: SensitivityMatrixProps["selected"],
): boolean {
  return (
    selected !== undefined &&
    compareDecimal(cell.wacc_pct, selected.waccPct) === 0 &&
    compareDecimal(cell.ebitda_margin_pct, selected.ebitdaMarginPct) === 0
  );
}

/**
 * Background and text classes of a cell against the offer. A null fair value, or one the offer
 * cannot be compared with exactly (unparsable text on either side), is neutral: colouring it
 * would claim a comparison that was never made.
 */
function toneClass(cell: SensitivityCell, offerPriceKes: DecimalString): string {
  const comparison = compareDecimal(cell.fair_value_per_share_kes, offerPriceKes);
  if (comparison === null) return TONE_NOT_COMPARABLE;
  return comparison >= 0 ? TONE_AT_OR_ABOVE_OFFER : TONE_BELOW_OFFER;
}

/** Hover text of a cell: the reason codes of a null cell, otherwise the upside and any flags. */
function cellTitle(cell: SensitivityCell, offerText: string): string {
  const flags = cell.flags.join(", ");
  if (cell.fair_value_per_share_kes === null) return flags === "" ? "Not computable" : flags;
  const upsideText = formatDecimal(cell.upside_pct, "pct", { signed: true });
  const upside = `Upside ${upsideText} against offer KES ${offerText}`;
  return flags === "" ? upside : `${upside}; ${flags}`;
}

interface MatrixCellProps {
  cell: SensitivityCell;
  offerPriceKes: DecimalString;
  offerText: string;
  selected: SensitivityMatrixProps["selected"];
}

/** One blended fair value with its upside beneath it, coloured against the offer. */
function MatrixCell({ cell, offerPriceKes, offerText, selected }: MatrixCellProps): ReactElement {
  const isSelected = isSelectedCell(cell, selected);
  const className = isSelected
    ? `${CELL_CLASS} ${toneClass(cell, offerPriceKes)} ${SELECTED_CLASS}`
    : `${CELL_CLASS} ${toneClass(cell, offerPriceKes)}`;
  const flags = cell.flags.join(", ");

  return (
    <td
      data-testid="sensitivity-cell"
      data-wacc={cell.wacc_pct}
      data-margin={cell.ebitda_margin_pct}
      data-selected={isSelected ? "true" : "false"}
      aria-current={isSelected ? "true" : undefined}
      title={cellTitle(cell, offerText)}
      className={className}
    >
      {cell.fair_value_per_share_kes === null ? (
        <>
          <span aria-hidden="true">{EMPTY_VALUE}</span>
          <span className="sr-only">
            {flags === "" ? "Not computable" : `Not computable: ${flags}`}
          </span>
        </>
      ) : (
        <>
          <span className="block font-medium">
            {formatDecimal(cell.fair_value_per_share_kes, "per_share", { unit: false })}
          </span>
          <span className="block text-xs">
            {formatDecimal(cell.upside_pct, "pct", { signed: true })}
          </span>
          {flags === "" ? null : <span className="sr-only">{flags}</span>}
        </>
      )}
    </td>
  );
}

/**
 * WACC × base EBITDA margin table of blended fair values per share (KES), each coloured against
 * the offer price: emerald at or above it, rose below it, slate when not computable.
 *
 * A pure render of the server's decimal strings: values are displayed with `formatDecimal` and
 * compared with the exact `compareDecimal`, never converted to numbers. It holds no state or
 * effects, so server and client output match on hydration. `IpoValuationSandbox` renders it, and
 * a host that calls `POST /ipo/valuation` itself can render it from the response's `sensitivity`.
 */
export function SensitivityMatrix({
  data,
  offerPriceKes,
  selected,
  className,
}: SensitivityMatrixProps): ReactElement {
  const rootClass =
    className === undefined || className === "" ? ROOT_CLASS : `${ROOT_CLASS} ${className}`;

  if (data.rows.length === 0 || data.cols.length === 0) {
    return (
      <div data-testid="terminal-sensitivity-matrix" className={rootClass}>
        <p className="text-sm text-slate-400">No sensitivity data</p>
      </div>
    );
  }

  const offerText = formatDecimal(offerPriceKes, "per_share", { unit: false });
  const rows = axisEntries(data.rows);
  const cols = axisEntries(data.cols);

  return (
    <div data-testid="terminal-sensitivity-matrix" className={rootClass}>
      <table className="min-w-full border-collapse text-sm tabular-nums">
        <caption className="pbe-2 text-start text-sm font-medium text-slate-300">
          Fair value per share (KES) by WACC × base EBITDA margin
        </caption>
        <thead>
          <tr>
            {/* w-px with the headers' nowrap shrinks the label column to its text, so a wide
                container gives its spare width to the value columns rather than the labels. */}
            <th scope="col" className={`${HEADER_CLASS} w-px text-start`}>
              WACC <span aria-hidden="true">↓</span> / Margin <span aria-hidden="true">→</span>
            </th>
            {cols.map((col) => (
              <th key={col.key} scope="col" className={`${HEADER_CLASS} text-end`}>
                {formatDecimal(col.value, "pct")}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => {
            const rowCells = data.cells[i];
            return (
              <tr key={row.key}>
                <th scope="row" className={`${HEADER_CLASS} text-start`}>
                  {formatDecimal(row.value, "pct")}
                </th>
                {cols.map((col, j) => {
                  const cellKey = `${row.key}|${col.key}`;
                  const cell = rowCells?.[j];
                  return cell === undefined ? (
                    <td key={cellKey} className={`${CELL_CLASS} ${TONE_NOT_COMPARABLE}`} />
                  ) : (
                    <MatrixCell
                      key={cellKey}
                      cell={cell}
                      offerPriceKes={offerPriceKes}
                      offerText={offerText}
                      selected={selected}
                    />
                  );
                })}
              </tr>
            );
          })}
        </tbody>
      </table>
      <p className="mbs-2 text-xs text-slate-400">
        Green: at or above offer KES {offerText}; rose: below
      </p>
    </div>
  );
}
