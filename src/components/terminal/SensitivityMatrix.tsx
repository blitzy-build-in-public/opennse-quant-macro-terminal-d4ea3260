"use client";

import { useId, useRef, useState } from "react";
import type { ReactElement, ReactNode } from "react";

import { EMPTY_VALUE, compareDecimal, formatDecimal } from "./lib/format";
import { SECTION_HEADING_CLASS } from "./shared/headingAndTileClasses";
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

// The root sets its own surface and font family, so a host's body font never reaches the table.
// It does not scroll and uses no containment, so in a host that sizes it by its content (a
// non-growing flex item, an inline block, an auto grid track) it keeps the table's width.
// `min-w-0` lets a flex or grid host narrower than the table still shrink it, leaving the table
// to scroll inside its region; in block flow it changes nothing.
const ROOT_CLASS = "min-w-0 bg-slate-900 text-slate-100 font-sans";
// The table's description: the visible text above the scroll region, and the screen-reader-only
// caption that names both the table and the region. A visible caption box is as wide as its
// table, so on a narrow screen its text would run past the scrollport; above the scroller the
// text wraps to the root's width instead.
const CAPTION_TEXT = "Fair value per share (KES) by WACC × base EBITDA margin";
// Only the table scrolls. The scroller is a keyboard-focusable region named by the caption, so
// the columns can be scrolled without a pointer even in a browser that never focuses a scroll
// container by itself. `relative` makes it the containing block of the absolutely positioned
// `sr-only` caption, which would otherwise escape the overflow clip and widen the host page.
const SCROLL_REGION_CLASS =
  "relative overflow-x-auto rounded focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400";

// Every class below is a complete static string, never assembled from fragments, so Tailwind's
// source scan and a host's `@source` directive find each one.
// Column headers, the corner included, and the WACC row headers use the terminal's table header
// tokens: muted slate-400 above the columns, brighter slate-200 beside the rows.
const COL_HEADER_CLASS = "whitespace-nowrap px-2 py-1.5 font-medium text-slate-400";
// The label column (the corner and the WACC row headers) stays pinned to the scroll region's start
// edge while the value columns scroll beneath it, so every value keeps its WACC label in view. The
// card's slate-900 surface makes it opaque, and z-10 paints it over the cells passing under it,
// the positioned explained cells included. A 1 px slate-700 `::after` line on its end edge,
// positioned by the sticky header, moves with the column, so a partly scrolled value never sits
// flush against a label. Row headers align to the top, as the two-line cells do, so each label
// lines up with its row's fair values. The corner's end border, slate-900 like the surface and so
// unseen, takes the same half of the collapsed border that the cells' borders give each row
// header, so the corner's divider lines up exactly with the rows' divider.
const CORNER_HEADER_CLASS =
  "sticky start-0 z-10 w-px whitespace-nowrap border-e border-slate-900 bg-slate-900 px-2 py-1.5 text-start font-medium text-slate-400 after:pointer-events-none after:absolute after:inset-y-0 after:end-0 after:w-px after:bg-slate-700";
const ROW_HEADER_CLASS =
  "sticky start-0 z-10 whitespace-nowrap bg-slate-900 px-2 py-1.5 text-start align-top font-medium text-slate-200 after:pointer-events-none after:absolute after:inset-y-0 after:end-0 after:w-px after:bg-slate-700";
const CELL_CLASS = "border border-slate-900 px-2 py-1.5 text-end align-top";
const TONE_AT_OR_ABOVE_OFFER = "bg-emerald-900/60 text-emerald-100";
const TONE_BELOW_OFFER = "bg-rose-900/60 text-rose-100";
const TONE_NOT_COMPARABLE = "bg-slate-800 text-slate-400";
const SELECTED_CLASS = "ring-2 ring-inset ring-sky-400";
// A cell with an explanation positions its keyboard tooltip.
const EXPLAINED_CELL_CLASS = "relative";
// The explanation trigger looks like the plain cell content, even in a host without a CSS reset,
// and shows a slate-100 focus ring distinct from the selected cell's sky ring. The transparent
// outline (written out, since `outline-none` differs between Tailwind 3 and 4) keeps a focus
// indicator in forced-colors mode, where box-shadow rings are not drawn. Hover underlines the
// value (dotted), pressing dims it and a disabled trigger fades with a not-allowed cursor; none of
// these touches the cell's comparison colours.
const EXPLANATION_TRIGGER_CLASS =
  "peer m-0 block w-full cursor-help appearance-none rounded border-0 bg-transparent p-0 text-end text-inherit decoration-dotted underline-offset-2 [font:inherit] [letter-spacing:inherit] enabled:hover:underline enabled:active:opacity-80 disabled:cursor-not-allowed disabled:opacity-50 focus-visible:ring-2 focus-visible:ring-slate-100 focus-visible:[outline:2px_solid_transparent]";
// z-20 keeps the keyboard tooltip above the pinned label column (z-10), which a tooltip below a
// first-row cell, or one extending leftward from a right-half cell, can overlap.
const EXPLANATION_TIP_CLASS =
  "pointer-events-none absolute z-20 w-max max-w-[14rem] whitespace-normal break-words rounded-md border border-slate-600 bg-slate-950 px-2 py-1 text-start text-xs font-normal text-slate-100 shadow-lg";
// A tooltip sits above its cell, except on the first body row. The table's scroll region is a
// scroll container, and content overflowing a scroll container's top can never be scrolled to,
// so a multi-line tooltip above a first-row cell, with only the header row over it, would lose
// its top. Below the cell, any overflow past the region's bottom stays reachable, and focusing
// scrolls it into view.
const EXPLANATION_TIP_ABOVE = "bottom-full mb-1";
const EXPLANATION_TIP_BELOW = "top-full mt-1";
// Left-half columns extend the tooltip rightward and right-half columns leftward, so it spreads
// across the table rather than past the scroll region's left edge, which clips it.
const EXPLANATION_TIP_ANCHOR_LEFT = "left-0";
const EXPLANATION_TIP_ANCHOR_RIGHT = "right-0";
const EXPLANATION_TIP_ON_FOCUS = "hidden peer-focus-visible:block";
const EXPLANATION_TIP_DISMISSED = "hidden";

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

/**
 * Explanation of a cell, its pointer tooltip (`title`) and, for a null or flagged cell, its
 * keyboard tooltip: the reason codes of a null cell, otherwise the signed upside against the
 * offer and any flags. A negative upside is labelled "Downside" and any other (zero, positive, or
 * not comparable exactly) "Upside", the same wording as the sandbox's upside card.
 */
function cellTitle(cell: SensitivityCell, offerText: string): string {
  const flags = cell.flags.join(", ");
  if (cell.fair_value_per_share_kes === null) return flags === "" ? "Not computable" : flags;
  const label = compareDecimal(cell.upside_pct, "0") === -1 ? "Downside" : "Upside";
  const upsideText = formatDecimal(cell.upside_pct, "pct", { signed: true });
  const upside = `${label} ${upsideText} vs offer KES ${offerText}`;
  return flags === "" ? upside : `${upside}; ${flags}`;
}

/** Props of {@link CellExplanation}. */
interface CellExplanationProps {
  /** The explanation text, the same as the cell's `title`. */
  explanation: string;
  /**
   * Anchors the tooltip to the cell's right edge so that it extends leftward, for a cell in the
   * right half of the columns; otherwise it is anchored to the left edge and extends rightward.
   */
  anchorRight: boolean;
  /**
   * Places the tooltip below the cell, for a cell in the first body row, where the table's scroll
   * region would cut off the top of a tooltip above it; otherwise it is placed above.
   */
  placeBelow: boolean;
  /** The cell's visible and screen-reader content, rendered unchanged inside the trigger. */
  children: ReactNode;
}

/**
 * Brings a keyboard-focused explanation into view inside the matrix's scroll region. Neither the
 * browser's focus scroll nor `scrollIntoView` knows about the pinned label column, so either can
 * leave the focused value beneath it. The region is therefore first scrolled until the button's
 * cell clears its row's label; the cell's padding keeps the focus ring, drawn outside the button,
 * clear of the label too. The tooltip, which paints above the label column, is then shifted along
 * the row just enough to fit inside the region, and finally scrolled into view, which also brings
 * a below-cell tooltip running past the region's bottom into view. A tooltip without a box, as on
 * a pointer focus, which does not show it, scrolls and shifts nothing.
 */
function revealExplanation(button: HTMLElement, tip: HTMLElement): void {
  tip.style.transform = "";
  if (tip.getClientRects().length === 0) return;
  const region = button.closest<HTMLElement>('[role="region"]');
  const label = button.closest("tr")?.querySelector<HTMLElement>(":scope > th");
  if (region !== null && label !== null && label !== undefined) {
    const regionRect = region.getBoundingClientRect();
    const innerLeft = regionRect.left + region.clientLeft;
    const innerRight = innerLeft + region.clientWidth;
    // The label column sits at the inline start: the left edge, or the right one in RTL. How far
    // the cell reaches beneath the label, and past the region's inline end edge:
    const labelRect = label.getBoundingClientRect();
    const cellRect = (button.closest("td") ?? button).getBoundingClientRect();
    const rtl = getComputedStyle(region).direction === "rtl";
    const underLabel = rtl ? cellRect.right - labelRect.left : labelRect.right - cellRect.left;
    const pastEnd = rtl ? innerLeft - cellRect.left : cellRect.right - innerRight;
    // A growing scrollLeft moves the content left. Whole pixels, rounded towards a clear cell,
    // since the region may snap a fractional scroll offset.
    const towardEnd = rtl ? -1 : 1;
    if (underLabel > 0) {
      region.scrollLeft -= towardEnd * Math.ceil(underLabel);
    } else if (pastEnd > 0) {
      // Never so far that a cell wider than the space beside the labels slips beneath them.
      region.scrollLeft += towardEnd * Math.min(Math.ceil(pastEnd), Math.floor(-underLabel));
    }
    const tipRect = tip.getBoundingClientRect();
    let shift = 0;
    if (tipRect.right > innerRight) shift = innerRight - tipRect.right;
    if (tipRect.left + shift < innerLeft) shift = innerLeft - tipRect.left;
    if (shift !== 0) tip.style.transform = `translateX(${shift}px)`;
  }
  tip.scrollIntoView({ block: "nearest", inline: "nearest" });
}

/**
 * Keyboard access to a cell's explanation, which a pointer user gets from the cell's `title`. The
 * cell's content becomes a focusable button, described by a tooltip that shows while the button
 * has keyboard focus: above the cell, or below it on the first body row, since the table's scroll
 * region cannot scroll to a tooltip overflowing its top. Focusing brings the button and its
 * tooltip into view with {@link revealExplanation}, so neither a narrow, horizontally scrolled
 * region nor the pinned label column cuts them off. Escape hides the tooltip without moving focus,
 * so it no longer covers neighbouring cells, and leaving the button restores it for the next visit.
 */
function CellExplanation({
  explanation,
  anchorRight,
  placeBelow,
  children,
}: CellExplanationProps): ReactElement {
  const tipId = useId();
  const tipRef = useRef<HTMLSpanElement>(null);
  const [dismissed, setDismissed] = useState(false);
  const placementClass = placeBelow ? EXPLANATION_TIP_BELOW : EXPLANATION_TIP_ABOVE;
  const anchorClass = anchorRight ? EXPLANATION_TIP_ANCHOR_RIGHT : EXPLANATION_TIP_ANCHOR_LEFT;
  const visibilityClass = dismissed ? EXPLANATION_TIP_DISMISSED : EXPLANATION_TIP_ON_FOCUS;

  return (
    <>
      <button
        type="button"
        aria-describedby={tipId}
        className={EXPLANATION_TRIGGER_CLASS}
        onFocus={(event) => {
          // The browser scrolls only the button into view, and without regard to the pinned
          // label column. The tooltip has no box unless keyboard focus shows it, so a pointer
          // focus scrolls nothing.
          if (tipRef.current !== null) revealExplanation(event.currentTarget, tipRef.current);
        }}
        onKeyDown={(event) => {
          if (event.key === "Escape") setDismissed(true);
        }}
        onBlur={() => {
          // The shift fitted the tooltip to this visit's scroll position; the next visit refits it.
          if (tipRef.current !== null) tipRef.current.style.transform = "";
          setDismissed(false);
        }}
      >
        {children}
      </button>
      <span
        ref={tipRef}
        role="tooltip"
        id={tipId}
        className={`${EXPLANATION_TIP_CLASS} ${placementClass} ${anchorClass} ${visibilityClass}`}
      >
        {explanation}
      </span>
    </>
  );
}

interface MatrixCellProps {
  cell: SensitivityCell;
  offerPriceKes: DecimalString;
  offerText: string;
  selected: SensitivityMatrixProps["selected"];
  /** Whether the cell's column lies in the right half of the columns. */
  inRightHalf: boolean;
  /** Whether the cell lies in the first body row. */
  inFirstRow: boolean;
}

/**
 * One blended fair value with its upside beneath it, coloured against the offer. A null or
 * flagged cell also offers its explanation to keyboard users through {@link CellExplanation}.
 */
function MatrixCell({
  cell,
  offerPriceKes,
  offerText,
  selected,
  inRightHalf,
  inFirstRow,
}: MatrixCellProps): ReactElement {
  const isSelected = isSelectedCell(cell, selected);
  const isExplained = cell.fair_value_per_share_kes === null || cell.flags.length > 0;
  const baseClass = isExplained ? `${CELL_CLASS} ${EXPLAINED_CELL_CLASS}` : CELL_CLASS;
  const className = isSelected
    ? `${baseClass} ${toneClass(cell, offerPriceKes)} ${SELECTED_CLASS}`
    : `${baseClass} ${toneClass(cell, offerPriceKes)}`;
  const flags = cell.flags.join(", ");
  const title = cellTitle(cell, offerText);

  const content =
    cell.fair_value_per_share_kes === null ? (
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
        {/* Same size as the fair value, as every td in the terminal's tables shares one size; the
            upside reads as the secondary line by its lighter weight. */}
        <span className="block">{formatDecimal(cell.upside_pct, "pct", { signed: true })}</span>
        {flags === "" ? null : <span className="sr-only">{flags}</span>}
      </>
    );

  return (
    <td
      data-testid="sensitivity-cell"
      data-wacc={cell.wacc_pct}
      data-margin={cell.ebitda_margin_pct}
      data-selected={isSelected ? "true" : "false"}
      aria-current={isSelected ? "true" : undefined}
      title={title}
      className={className}
    >
      {isExplained ? (
        <CellExplanation explanation={title} anchorRight={inRightHalf} placeBelow={inFirstRow}>
          {content}
        </CellExplanation>
      ) : (
        content
      )}
    </td>
  );
}

/**
 * WACC × base EBITDA margin table of blended fair values per share (KES), each coloured against
 * the offer price: emerald at or above it, rose below it, slate when not computable.
 *
 * A pure render of the server's decimal strings: values are displayed with `formatDecimal` and
 * compared with the exact `compareDecimal`, never converted to numbers. Rendering is a pure
 * function of the props apart from each cell explanation's Escape-dismissed flag, which starts
 * false, so server and client output still match on hydration; it runs no effects.
 * `IpoValuationSandbox` renders it, and a host that calls `POST /ipo/valuation` itself can render
 * it from the response's `sensitivity`.
 */
export function SensitivityMatrix({
  data,
  offerPriceKes,
  selected,
  className,
}: SensitivityMatrixProps): ReactElement {
  const captionId = useId();
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
      {/* The visible description; screen readers get the same text once, as the caption. */}
      <p aria-hidden="true" className={`${SECTION_HEADING_CLASS} pb-2 text-start`}>
        {CAPTION_TEXT}
      </p>
      <div role="region" aria-labelledby={captionId} tabIndex={0} className={SCROLL_REGION_CLASS}>
        <table className="min-w-full border-collapse text-xs tabular-nums">
          <caption id={captionId} className="sr-only">{CAPTION_TEXT}</caption>
          <thead>
            <tr>
              {/* w-px with the headers' nowrap shrinks the label column to its text, so a wide
                  container gives its spare width to the value columns rather than the labels. */}
              <th scope="col" className={CORNER_HEADER_CLASS}>
                WACC <span aria-hidden="true">↓</span> / Margin <span aria-hidden="true">→</span>
              </th>
              {cols.map((col) => (
                <th key={col.key} scope="col" className={`${COL_HEADER_CLASS} text-end`}>
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
                  <th scope="row" className={ROW_HEADER_CLASS}>
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
                        inRightHalf={j >= cols.length / 2}
                        inFirstRow={i === 0}
                      />
                    );
                  })}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      {/* Outside the scroll region, so the legend stays in view while the columns scroll. */}
      <p className="mt-2 text-xs text-slate-400">
        Green: at or above offer KES {offerText}; rose: below
      </p>
    </div>
  );
}
