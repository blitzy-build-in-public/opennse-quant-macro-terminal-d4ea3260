"use client";

import { useCallback, useEffect, useId, useLayoutEffect, useMemo, useRef, useState } from "react";
import type { ReactElement } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Rectangle,
  ReferenceLine,
  ResponsiveContainer,
  Text,
  Tooltip,
  useActiveTooltipCoordinate,
  XAxis,
  YAxis,
} from "recharts";
import type { BarShapeProps, TooltipContentProps, XAxisTickContentProps } from "recharts";

import { SensitivityMatrix } from "./SensitivityMatrix";
import { apiSend, fieldErrorMap, isAbortError, TerminalApiError } from "./lib/apiClient";
import { AXIS_TICK, CHART_COLORS } from "./lib/chartTheme";
import {
  checkDecimalRange,
  compareDecimal,
  formatDecimal,
  formatScaled,
  parseScaled,
  toChartNumber,
} from "./lib/format";
import type { DecimalKind, DecimalRangeRule } from "./lib/format";
import { useTerminalQuery } from "./lib/useTerminalQuery";
import {
  FIELD_LABEL_CLASS,
  SECONDARY_BUTTON_CLASS,
  TEXT_FIELD_CLASS,
} from "./shared/controlStyles";
import { FieldErrorText, WidgetFrame } from "./shared/WidgetFrame";
import {
  KPI_LABEL_CLASS,
  KPI_TILE_CLASS,
  KPI_VALUE_CLASS,
  SECTION_HEADING_CLASS,
} from "./shared/headingAndTileClasses";
import type {
  DecimalString,
  IpoScenario,
  IpoValuationRequest,
  IpoValuationResponse,
  TerminalWidgetProps,
} from "./types";

/** Props of {@link IpoValuationSandbox}. */
export interface IpoValuationSandboxProps extends TerminalWidgetProps {
  /**
   * Preset to open, matched exactly against `IpoScenario.company_name`, for example
   * `"Quickmart Supermarket"`. When it is absent or matches no preset, the first preset returned
   * by `GET /ipo/scenarios` is used. A preset picked in the widget's own selector, shown when
   * several presets exist, takes precedence.
   */
  scenarioName?: string;
  /**
   * Milliseconds to wait after the last input change before posting the assumptions to
   * `POST /ipo/valuation`. Default 150. Negative and non-finite values send without delay.
   */
  debounceMs?: number;
}

// ---------------------------------------------------------------------------------------------
// Field configuration (single-field ranges of the valuation request)
// ---------------------------------------------------------------------------------------------

/** Assumptions adjusted with a slider, in display order. */
const SLIDER_FIELDS = [
  "offer_price_kes",
  "revenue_growth_pct",
  "ebitda_margin_pct",
  "margin_expansion_pct",
  "wacc_pct",
  "terminal_growth_pct",
  "net_debt_kes",
  "nwc_pct_revenue",
  "peer_ev_ebitda_multiple",
  "dcf_weight_pct",
] as const;
type SliderField = (typeof SLIDER_FIELDS)[number];

/** Advanced assumptions typed into a text input, in display order. */
const TEXT_FIELDS = [
  "capex_pct_revenue",
  "da_pct_revenue",
  "tax_rate_pct",
  "forecast_years",
  "base_revenue_kes",
  "shares_outstanding",
  "shares_offered",
  "primary_proceeds_kes",
] as const;
type TextField = (typeof TEXT_FIELDS)[number];
type InputField = SliderField | TextField;

/**
 * A slider holds an integer count of 10^-scale units, so its position, bounds and step are
 * integer strings and the value is serialized with BigInt arithmetic: position `"1800"` at
 * scale 2 is sent as `"18.00"`, and no float is ever involved.
 */
interface SliderSpec {
  label: string;
  /** Decimal places of one slider unit. */
  scale: number;
  /** Inclusive lower bound, in slider units. */
  min: string;
  /** Inclusive upper bound, in slider units. */
  max: string;
  /** Slider step, in slider units; `max − min` is a whole number of steps. */
  step: string;
  /** How the current value is displayed beside the slider. */
  kind: DecimalKind;
  /** Replaces the kind's unit affixes, for values in percentage points. */
  suffix?: string;
}

const SLIDERS: Record<SliderField, SliderSpec> = {
  offer_price_kes: {
    label: "Offer price (KES)",
    scale: 2,
    min: "50",
    max: "5000",
    step: "5",
    kind: "per_share",
  },
  revenue_growth_pct: {
    label: "Revenue growth (%/yr)",
    scale: 2,
    min: "-2000",
    max: "4000",
    step: "50",
    kind: "pct",
  },
  ebitda_margin_pct: {
    label: "Base EBITDA margin (%)",
    scale: 2,
    min: "50",
    max: "1500",
    step: "25",
    kind: "pct",
  },
  margin_expansion_pct: {
    label: "Margin expansion by year N (pp)",
    scale: 2,
    min: "-300",
    max: "300",
    step: "25",
    kind: "pct",
    suffix: " pp",
  },
  wacc_pct: { label: "WACC (%)", scale: 2, min: "800", max: "3000", step: "25", kind: "pct" },
  terminal_growth_pct: {
    label: "Terminal growth (%)",
    scale: 2,
    min: "0",
    max: "1000",
    step: "25",
    kind: "pct",
  },
  net_debt_kes: {
    label: "Net debt (KES)",
    scale: 0,
    min: "-20000000000",
    max: "40000000000",
    step: "250000000",
    kind: "kes",
  },
  nwc_pct_revenue: {
    label: "Working capital (% of Δrevenue)",
    scale: 2,
    min: "-1500",
    max: "1500",
    step: "50",
    kind: "pct",
  },
  peer_ev_ebitda_multiple: {
    label: "Peer EV/EBITDA (×)",
    scale: 2,
    min: "100",
    max: "2000",
    step: "25",
    kind: "ratio",
  },
  dcf_weight_pct: { label: "DCF weight (%)", scale: 0, min: "0", max: "100", step: "5", kind: "pct" },
};

/**
 * A text input keeps the typed string unchanged. It is checked against the field's single-field
 * range before anything is sent; cross-field rules (shares offered within shares outstanding,
 * primary proceeds within the offer size) are the server's, reported as coded field errors. A
 * decimal text is sent exactly as typed and the server reads only plain decimal text, so spaces
 * around it fail the same check.
 */
interface TextSpec {
  label: string;
  inputMode: "decimal" | "numeric";
  /** Single-field range; `integer` fields are sent as JSON integers, the rest as strings. */
  rule: DecimalRangeRule;
  /** Unit of the digit-grouped echo shown beneath a valid long value, or null for none. */
  echo: "kes" | "shares" | null;
}

const MAX_SHARE_COUNT = "1000000000000";

const TEXTS: Record<TextField, TextSpec> = {
  capex_pct_revenue: {
    label: "Capex (% of revenue)",
    inputMode: "decimal",
    rule: { scale: 4, min: "0", max: "10" },
    echo: null,
  },
  da_pct_revenue: {
    label: "D&A (% of revenue)",
    inputMode: "decimal",
    rule: { scale: 4, min: "0", max: "10" },
    echo: null,
  },
  tax_rate_pct: {
    label: "Tax rate (%)",
    inputMode: "decimal",
    rule: { scale: 4, min: "0", max: "50" },
    echo: null,
  },
  forecast_years: {
    label: "Forecast years",
    inputMode: "numeric",
    rule: { scale: 0, integer: true, min: "3", max: "10" },
    echo: null,
  },
  base_revenue_kes: {
    label: "Base revenue (KES)",
    inputMode: "decimal",
    rule: { scale: 2, min: "0", minExclusive: true, max: "100000000000000" },
    echo: "kes",
  },
  shares_outstanding: {
    label: "Shares outstanding (post-offer)",
    inputMode: "numeric",
    rule: { scale: 0, integer: true, min: "1", max: MAX_SHARE_COUNT },
    echo: "shares",
  },
  shares_offered: {
    label: "Shares offered",
    inputMode: "numeric",
    rule: { scale: 0, integer: true, min: "0", max: MAX_SHARE_COUNT },
    echo: "shares",
  },
  primary_proceeds_kes: {
    label: "Primary proceeds (KES)",
    inputMode: "decimal",
    rule: { scale: 2, min: "0" },
    echo: "kes",
  },
};

/** Every request field that has an input, for routing server field errors. */
const INPUT_FIELDS: ReadonlySet<string> = new Set<string>([...SLIDER_FIELDS, ...TEXT_FIELDS]);

type SliderPositions = Record<SliderField, string>;
/**
 * Per slider, the exact preset value it holds until the user moves it, or null once it is
 * serialized from its position. Only a preset value off the slider grid is held.
 */
type HeldSliderValues = Record<SliderField, DecimalString | null>;
type TextValues = Record<TextField, string>;
type LocalErrors = Partial<Record<InputField, string>>;

// ---------------------------------------------------------------------------------------------
// Styles
// ---------------------------------------------------------------------------------------------

// Every class name is written out in full, never assembled from fragments, so Tailwind's source
// scan and a host's `@source` directive or `content` glob find each one. Focus indicators are
// outlines, which forced-colors mode keeps. Labels, the text inputs, the preset selector and the
// valuation's Retry button use the shared compact controls of `shared/controlStyles` (26px tall),
// so they match the other widgets' fields and buttons: a slate-500 field border (about 3.7:1 on
// the slate-900 card, 4.2:1 on the slate-950 fill) lightens to slate-400 on hover and slate-300
// when pressed, and an input with `aria-invalid` keeps a rose border (rose-400, then rose-300 and
// rose-200). Hover and press change a field only while it is enabled; a disabled one dims and
// shows a not-allowed cursor. Only the sliders keep a 44px (h-11) box, because a slider's hit area
// is the empty space around its thin track. Section headings and output cards take the classes
// shared with the other widgets from ./shared/headingAndTileClasses.
/* BLITZY [A11Y]: the text inputs, the preset selector and Retry are 26px tall, at or above WCAG
   2.2 AA's 24px target minimum and below the 44px guideline, the size of every other widget's
   fields and buttons. Flagged for designer review. */
const FOCUS_CLASS =
  "focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400";
// The slider's 44px box is mostly empty space around its track, so its focus outline is drawn
// inside the box, where it can never overlap the label row above.
const RANGE_CLASS =
  "block h-11 w-full cursor-pointer rounded accent-sky-400 enabled:hover:accent-sky-300 enabled:active:accent-sky-200 disabled:cursor-not-allowed disabled:opacity-50 focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-sky-400";
const TEXT_INPUT_CLASS = `mt-1 block w-full tabular-nums ${TEXT_FIELD_CLASS}`;
// The layout follows the widget's own width, never the viewport's: a host may place the widget in
// a narrow card on a wide screen, and Tailwind 3 has no container-query variants without a plugin.
// The inputs column starts at 24rem and the output column takes the rest while it keeps at least
// 28rem; otherwise the output wraps beneath the inputs and both span the widget. The card and
// advanced-input grids fit as many tracks as their own width allows, the cards at most three (a
// fourth 30% track never fits). A card track is at least 12rem, so an 18px KES amount of up to
// 11 integer digits (tens of billions) wraps only between "KES" and the number, never inside it.
const WORKSPACE_CLASS = "flex flex-wrap gap-6";
const INPUTS_COLUMN_CLASS = "flex min-w-0 grow basis-96 flex-col gap-6";
const OUTPUT_COLUMN_CLASS = "flex min-w-[min(100%,28rem)] grow-[999] basis-0 flex-col gap-6";
const ADVANCED_GRID_CLASS = "mt-2 grid grid-cols-[repeat(auto-fill,minmax(11rem,1fr))] gap-3";
const CARD_GRID_CLASS = "grid grid-cols-[repeat(auto-fill,minmax(max(12rem,30%),1fr))] gap-3";
const OUTPUT_CLASS =
  "flex min-w-0 flex-col gap-4 motion-safe:transition-opacity motion-safe:duration-200 motion-safe:ease-out";
// The loaded workspace's height and the first valuation output's height (cards, chart and matrix),
// measured on the standalone page with the seeded preset at each viewport width where they reflow,
// so the card keeps its height when the presets arrive and again when the first result does.
const SKELETON_HEIGHT_CLASS =
  "h-[205.34375rem] min-[323px]:h-[204.09375rem] min-[353px]:h-[202.84375rem] min-[363px]:h-[201.84375rem] min-[429px]:h-[200.59375rem] min-[430px]:h-[181.59375rem] min-[435px]:h-[180.59375rem] min-[437px]:h-[179.59375rem] min-[439px]:h-[178.34375rem] min-[462px]:h-[162.09375rem] min-[489px]:h-[156.84375rem] min-[524px]:h-[155.59375rem] min-[595px]:h-[154.34375rem] min-[618px]:h-[150.21875rem] min-[660px]:h-[149.21875rem] min-[666px]:h-[141.96875rem] min-[707px]:h-[138.46875rem] min-[739px]:h-[137.21875rem] min-[806px]:h-[131.84375rem] min-[922px]:h-[89.59375rem] min-[932px]:h-[88.34375rem] min-[1003px]:h-[87.09375rem] min-[1075px]:h-[79.84375rem] min-[1116px]:h-[76.34375rem] min-[1147px]:h-[75.09375rem]";
const PENDING_OUTPUT_CLASS =
  "h-[85.96875rem] rounded bg-slate-800 motion-safe:animate-pulse min-[435px]:h-[84.96875rem] min-[437px]:h-[83.96875rem] min-[462px]:h-[67.71875rem] min-[489px]:h-[62.46875rem] min-[666px]:h-[55.21875rem] min-[707px]:h-[51.71875rem] min-[922px]:h-[62.46875rem] min-[1075px]:h-[55.21875rem] min-[1116px]:h-[51.71875rem]";
// Recharts' accessibility layer renders the chart as `<svg class="recharts-surface" tabindex="0">`,
// which the keyboard reaches and navigates with the arrow keys, and renders its z-index layers as
// `<g tabindex="-1">`: Tab never reaches them, but a mouse click on a bar focuses one, and a click
// on bare plot area focuses the surface. A key pressed after the click, such as an arrow key or the
// Escape that dismisses the tooltip, makes that element :focus-visible. So the layers' outline and
// any pointer-focus outline are hidden, and the controls' sky-400 ring is drawn on the surface
// whenever it or a descendant is :focus-visible: it appears for keyboard use and not for a mouse
// click. The solid style is explicit because Tailwind 3's `outline-2` sets only the width. When Tab
// scrolls the chart into view, the svg's scroll-my-2 keeps the whole ring (2px wide at a 2px
// offset) inside the viewport, with room to spare even when a host's root font size makes the
// rem-based margin smaller than 8px.
const FAIR_VALUE_CHART_CLASS =
  "mt-2 h-56 w-full [&_g[tabindex='-1']]:outline-none [&_*:focus:not(:focus-visible)]:outline-none [&_svg]:scroll-my-2 [&_.recharts-surface:is(:focus-visible,:has(:focus-visible))]:[outline-style:solid] [&_.recharts-surface:is(:focus-visible,:has(:focus-visible))]:outline-2 [&_.recharts-surface:is(:focus-visible,:has(:focus-visible))]:outline-offset-2 [&_.recharts-surface:is(:focus-visible,:has(:focus-visible))]:outline-sky-400";
// The chart tooltip mirrors TOOLTIP_CONTENT_STYLE (slate-900 surface, slate-700 border, slate-300
// text, 12px) with utility classes: the widget renders this content itself, in place of the
// Recharts default content that takes `contentStyle`.
const TOOLTIP_CLASS =
  "flex flex-col gap-1 whitespace-nowrap rounded border border-slate-700 bg-slate-900 px-2.5 py-2 text-xs text-slate-300 shadow-lg";
const TOOLTIP_TITLE_CLASS = "m-0 font-medium text-slate-100";
const TOOLTIP_ROW_CLASS = "m-0";
const TOOLTIP_VALUE_CLASS = "tabular-nums text-slate-100";
const FLAG_CLASS =
  "rounded border border-amber-800 bg-amber-950 px-2 py-0.5 font-mono text-xs text-amber-200";
const GENERAL_ERROR_CLASS =
  "flex flex-col gap-1 rounded border border-rose-900 bg-slate-950 p-3 text-sm text-rose-300";
const RETRY_CLASS = `${SECONDARY_BUTTON_CLASS} mt-1 self-start`;

// ---------------------------------------------------------------------------------------------
// Pure helpers (BigInt scaled integers; the only float boundary is the chart's toChartNumber)
// ---------------------------------------------------------------------------------------------

const ONE = BigInt(1);
const TWO = BigInt(2);

/** 10^exponent as a BigInt. */
function pow10(exponent: number): bigint {
  return BigInt("1" + "0".repeat(exponent));
}

/** Number of characters after the first `.`, or 0 when there is none. */
function fractionDigits(text: string): number {
  const dot = text.indexOf(".");
  return dot === -1 ? 0 : text.length - dot - 1;
}

/**
 * Snaps `scaled`, a value in slider units multiplied by `unit`, to the nearest step counted from
 * `min` and clamps it to [min, max]. Returns the position in slider units as an integer string.
 */
function snapToGrid(spec: SliderSpec, scaled: bigint, unit: bigint): string {
  const min = BigInt(spec.min) * unit;
  const max = BigInt(spec.max) * unit;
  const step = BigInt(spec.step) * unit;
  let snapped: bigint;
  if (scaled <= min) {
    snapped = min;
  } else if (scaled >= max) {
    snapped = max;
  } else {
    const steps = (scaled - min + step / TWO) / step;
    snapped = min + steps * step;
    if (snapped > max) snapped = max;
  }
  return (snapped / unit).toString();
}

/**
 * Slider position of a preset value. The value is parsed at its own precision, so a server value
 * such as `"18.0000"` sits on its step, and the thumb of a value off the slider grid sits on the
 * nearest step while {@link heldPresetValue} keeps the value itself. A preset the API validated
 * always parses; text that does not parse starts the slider at its lower bound.
 */
function presetPosition(spec: SliderSpec, value: DecimalString): string {
  const text = value.trim();
  const precision = Math.max(spec.scale, fractionDigits(text));
  const unit = pow10(precision - spec.scale);
  const parsed = parseScaled(text, precision);
  return snapToGrid(spec, parsed ?? BigInt(spec.min) * unit, unit);
}

/** Slider position from a range input's value, or null when the value is not an integer. */
function inputPosition(spec: SliderSpec, raw: string): string | null {
  const parsed = parseScaled(raw, 0);
  return parsed === null ? null : snapToGrid(spec, parsed, ONE);
}

/**
 * The exact value a slider holds from its preset, or null when there is none to hold: the trimmed
 * preset text when it differs numerically from its position serialized at the slider's scale,
 * that is when it lies off the slider grid. An offer of `"7.52"` on the 0.05 grid is held while
 * the thumb sits at 7.50; `"7.50"`, `"18.0000"` and `"4000000000.00"` equal their positions and
 * are not held, so they are sent as `"7.50"`, `"18.00"` and `"4000000000"`. Text that does not
 * parse is not held either and leaves the slider at its lower bound.
 */
function heldPresetValue(spec: SliderSpec, value: DecimalString): DecimalString | null {
  const text = value.trim();
  const serialized = formatScaled(BigInt(presetPosition(spec, text)), spec.scale);
  const comparison = compareDecimal(serialized, text);
  return comparison === null || comparison === 0 ? null : text;
}

/**
 * A slider's current value as the plain decimal string sent to the API: its held preset value
 * while it has one, else its position serialized at the slider's scale.
 */
function sliderValue(
  positions: SliderPositions,
  held: HeldSliderValues,
  field: SliderField,
): DecimalString {
  return held[field] ?? formatScaled(BigInt(positions[field]), SLIDERS[field].scale);
}

/**
 * A slider's current value, as {@link sliderValue} returns it, for display: for example
 * `"18.00%"`, `"KES 7.50"`, `"0.50 pp"`, or a held preset value such as `"KES 4,100,000,000.00"`.
 */
function sliderDisplay(field: SliderField, value: DecimalString): string {
  const spec = SLIDERS[field];
  return spec.suffix === undefined
    ? formatDecimal(value, spec.kind)
    : formatDecimal(value, spec.kind, { unit: false }) + spec.suffix;
}

/**
 * Slider positions of a preset. The thumb of a value off the slider grid sits on the nearest
 * step, and {@link initialHeldValues} keeps the value itself until that slider is moved.
 */
function initialPositions(assumptions: IpoValuationRequest): SliderPositions {
  const positions = {} as SliderPositions;
  for (const field of SLIDER_FIELDS) {
    positions[field] = presetPosition(SLIDERS[field], assumptions[field]);
  }
  return positions;
}

/**
 * The exact preset values the sliders hold, null for each slider whose preset value is on its
 * grid (see {@link heldPresetValue}).
 */
function initialHeldValues(assumptions: IpoValuationRequest): HeldSliderValues {
  const held = {} as HeldSliderValues;
  for (const field of SLIDER_FIELDS) {
    held[field] = heldPresetValue(SLIDERS[field], assumptions[field]);
  }
  return held;
}

/** Decimal strings exactly as received; integers in their plain decimal form. */
function initialTexts(assumptions: IpoValuationRequest): TextValues {
  const texts = {} as TextValues;
  for (const field of TEXT_FIELDS) {
    texts[field] = String(assumptions[field]);
  }
  return texts;
}

/** A range bound with digit grouping and no unit, for error messages. */
function formatBound(bound: string | undefined): string {
  return formatDecimal(bound, "kes", { unit: false });
}

/**
 * The message for a text value its field's own check refuses, or null when it is valid. A decimal
 * text is sent exactly as typed, and the server refuses spaces around a decimal, so they are
 * refused here before anything is sent. A count is sent as a JSON integer, never as its text, so
 * spaces around it are harmless.
 */
function localErrorMessage(spec: TextSpec, value: string): string | null {
  const { rule } = spec;
  const text = value.trim();
  if (rule.integer !== true && text !== "" && text !== value) {
    return "Remove the spaces around the number";
  }
  switch (checkDecimalRange(value, rule)) {
    case "ok":
      return null;
    case "invalid": {
      if (rule.integer === true) return "Enter a whole number";
      // A well-formed decimal is rejected only for carrying more decimal places than the field.
      return parseScaled(text, fractionDigits(text)) === null
        ? "Enter a valid number"
        : `Use at most ${rule.scale} decimal places`;
    }
    case "below_min":
      return rule.minExclusive === true
        ? `Must be greater than ${formatBound(rule.min)}`
        : `Must be at least ${formatBound(rule.min)}`;
    case "above_max":
      return `Must be at most ${formatBound(rule.max)}`;
  }
}

function collectLocalErrors(texts: TextValues): LocalErrors {
  const errors: LocalErrors = {};
  for (const field of TEXT_FIELDS) {
    const message = localErrorMessage(TEXTS[field], texts[field]);
    if (message !== null) errors[field] = message;
  }
  return errors;
}

/**
 * A validated integer text as a JSON integer. The single-field check has already accepted it as
 * a plain integer of at most 10^12, below 2^53, so the conversion is exact.
 */
function toJsonInteger(text: string): number {
  return Number(text.trim());
}

/**
 * The `POST /ipo/valuation` body for valid inputs: sliders as their held preset values, else
 * serialized from their positions; decimal texts exactly as typed, which the local check has
 * already found free of surrounding spaces; integer texts as JSON integers. The sensitivity axes are omitted, so the server's
 * 5 WACC × 7 margin defaults apply.
 */
function buildRequest(
  positions: SliderPositions,
  held: HeldSliderValues,
  texts: TextValues,
): IpoValuationRequest {
  return {
    offer_price_kes: sliderValue(positions, held, "offer_price_kes"),
    revenue_growth_pct: sliderValue(positions, held, "revenue_growth_pct"),
    ebitda_margin_pct: sliderValue(positions, held, "ebitda_margin_pct"),
    margin_expansion_pct: sliderValue(positions, held, "margin_expansion_pct"),
    wacc_pct: sliderValue(positions, held, "wacc_pct"),
    terminal_growth_pct: sliderValue(positions, held, "terminal_growth_pct"),
    net_debt_kes: sliderValue(positions, held, "net_debt_kes"),
    nwc_pct_revenue: sliderValue(positions, held, "nwc_pct_revenue"),
    peer_ev_ebitda_multiple: sliderValue(positions, held, "peer_ev_ebitda_multiple"),
    dcf_weight_pct: sliderValue(positions, held, "dcf_weight_pct"),
    capex_pct_revenue: texts.capex_pct_revenue,
    da_pct_revenue: texts.da_pct_revenue,
    tax_rate_pct: texts.tax_rate_pct,
    forecast_years: toJsonInteger(texts.forecast_years),
    base_revenue_kes: texts.base_revenue_kes,
    shares_outstanding: toJsonInteger(texts.shares_outstanding),
    shares_offered: toJsonInteger(texts.shares_offered),
    primary_proceeds_kes: texts.primary_proceeds_kes,
  };
}

/**
 * Digit-grouped reading of a valid long value, such as `KES 45,000,000,000.00` or
 * `500,000,000 shares`; a count equal to one, `"1"` or `"01"`, reads `1 share`.
 */
function echoText(spec: TextSpec, value: string): string | null {
  const text = value.trim();
  if (spec.echo === "kes") return formatDecimal(text, "kes");
  if (spec.echo === "shares") {
    const noun = compareDecimal(text, "1") === 0 ? " share" : " shares";
    return formatDecimal(text, "kes", { unit: false }) + noun;
  }
  return null;
}

/** Text colour of a value compared with zero or with the offer: emerald at or above, rose below. */
function toneClass(comparison: -1 | 0 | 1 | null): string {
  if (comparison === null) return "text-slate-100";
  return comparison >= 0 ? "text-emerald-400" : "text-rose-400";
}

// ---------------------------------------------------------------------------------------------
// Fair value by method chart
// ---------------------------------------------------------------------------------------------

/** One bar: the method's fair value string, its approximate bar height and its colour. */
interface FairValueBar {
  method: string;
  raw: DecimalString | null;
  /** Bar height only (toChartNumber); never displayed. */
  y: number | null;
  fill: string;
}

function fairValueBar(method: string, raw: DecimalString | null, offer: DecimalString): FairValueBar {
  const comparison = compareDecimal(raw, offer);
  let fill: string = CHART_COLORS.muted;
  if (comparison !== null) fill = comparison >= 0 ? CHART_COLORS.positive : CHART_COLORS.negative;
  return { method, raw, y: toChartNumber(raw), fill };
}

function isFairValueBar(value: unknown): value is FairValueBar {
  return typeof value === "object" && value !== null && "method" in value && "fill" in value;
}

/**
 * Bar shape coloured from its own data entry: emerald at or above the offer, rose below it. Used
 * instead of `Cell`, which the pinned Recharts 3.10 deprecates for removal in 4.0.
 */
function renderFairValueBar(props: BarShapeProps): ReactElement {
  const entry: unknown = props.payload;
  return <Rectangle {...props} fill={isFairValueBar(entry) ? entry.fill : CHART_COLORS.muted} />;
}

/**
 * Tooltip of the active bar: its method and the exact fair value string, never the bar's float
 * height. It renders nothing while the tooltip is inactive or dismissed, so no stale text stays in
 * the hidden box. It has no live region: Recharts' default content announces every arrow key
 * assertively, and the chart's description already gives each value.
 */
function FairValueTooltipContent({ active, payload }: TooltipContentProps): ReactElement | null {
  const entry: unknown = payload[0]?.payload;
  if (!active || !isFairValueBar(entry)) return null;
  return (
    <div className={TOOLTIP_CLASS}>
      <p className={TOOLTIP_TITLE_CLASS}>{entry.method}</p>
      <p className={TOOLTIP_ROW_CLASS}>
        Fair value:{" "}
        <span className={TOOLTIP_VALUE_CLASS}>{formatDecimal(entry.raw, "per_share")}</span>
      </p>
    </div>
  );
}

/** The active tooltip coordinate when Escape was pressed, (0, 0) when there was none. */
interface TooltipDismissal {
  x: number;
  y: number;
}

/**
 * The chart's Tooltip, which Escape dismisses together with its cursor band. Recharts' own Escape
 * handling only hides the tooltip box and leaves the band drawn. This applies the same rule (a
 * document-level Escape dismisses until the active coordinate changes, so the next arrow key or
 * pointer move onto another position shows both again) and switches the whole Tooltip off while
 * it holds, so the band never shows without the tooltip, nor the tooltip without the band.
 */
function FairValueTooltip(): ReactElement {
  const coordinate = useActiveTooltipCoordinate();
  const x = coordinate?.x ?? 0;
  const y = coordinate?.y ?? 0;
  const [dismissal, setDismissal] = useState<TooltipDismissal | null>(null);
  useEffect(() => {
    function dismissOnEscape(event: KeyboardEvent): void {
      if (event.key === "Escape") setDismissal({ x, y });
    }
    document.addEventListener("keydown", dismissOnEscape);
    return () => document.removeEventListener("keydown", dismissOnEscape);
  }, [x, y]);
  const dismissed = dismissal !== null && dismissal.x === x && dismissal.y === y;
  // A move away ends the dismissal, so coming back to the same bar later shows it again.
  if (dismissal !== null && !dismissed) setDismissal(null);
  return (
    <Tooltip
      active={dismissed ? false : undefined}
      cursor={{ fill: CHART_COLORS.grid, fillOpacity: 0.35 }}
      content={FairValueTooltipContent}
    />
  );
}

/** Y-axis ticks are generated chart coordinates with no source string, labelled at 2 dp. */
function formatAxisTick(value: number): string {
  return Number.isFinite(value) ? formatDecimal(value.toFixed(2), "per_share", { unit: false }) : "";
}

/**
 * Each method's x-axis labels, longest first. The Y axis widens with its longest tick (over 20
 * characters for the largest valuations the inputs allow) and the right margin holds the offer
 * label, so a narrow chart's bands can be narrower than "Comparables", and at the extremes than
 * "Comps". The tooltip and the chart's description keep the full names.
 */
const METHOD_AXIS_LABELS: Readonly<Record<string, readonly string[]>> = {
  DCF: ["DCF", "D"],
  Comparables: ["Comparables", "Comps", "C"],
  Blended: ["Blended", "Blend", "B"],
};

/** Least room a label leaves in its band: Recharts' default `minTickGap` between ticks. */
const METHOD_LABEL_GAP = 5;

/**
 * One bar's x-axis label: the longest of its method's labels whose rendered width, measured in
 * the host page's font, fits the bar's category band with {@link METHOD_LABEL_GAP} to spare (the
 * shortest when none does). Each label then stays within its own band, so neighbours never
 * overlap.
 */
function MethodAxisTick(props: XAxisTickContentProps): ReactElement {
  const method = String(props.payload.value);
  const labels = METHOD_AXIS_LABELS[method] ?? [method];
  // Recharts sets a category tick's `offset` to half its band.
  const band = 2 * (props.payload.offset ?? 0);
  const textRef = useRef<SVGTextElement>(null);
  // Rendered width of each label measured so far.
  const [widths, setWidths] = useState<Readonly<Record<string, number>>>({});
  // A label not measured yet is rendered so it can be measured; without a band, the full name.
  const label =
    labels.find((candidate) => {
      const width = widths[candidate];
      return width === undefined || !(band > 0) || width + METHOD_LABEL_GAP <= band;
    }) ??
    labels[labels.length - 1] ??
    method;

  // Measures the rendered label before paint, so one too wide for its band is replaced by the
  // next before it is ever drawn. Without SVG text layout (jsdom) the full name stays.
  useLayoutEffect(() => {
    const text = textRef.current;
    if (text === null || typeof text.getComputedTextLength !== "function") return;
    const width = text.getComputedTextLength();
    if (widths[label] !== width) setWidths((previous) => ({ ...previous, [label]: width }));
  }, [label, widths]);

  // A host web font that loads after the measurement changes every width: measure again (where
  // the CSS Font Loading API exists).
  useEffect(() => {
    if (!("fonts" in document)) return undefined;
    const fonts = document.fonts;
    const remeasure = (): void => setWidths({});
    fonts.addEventListener("loadingdone", remeasure);
    return () => fonts.removeEventListener("loadingdone", remeasure);
  }, []);

  return (
    <Text ref={textRef} {...props} {...AXIS_TICK}>
      {label}
    </Text>
  );
}

function renderMethodTick(props: XAxisTickContentProps): ReactElement {
  return <MethodAxisTick {...props} />;
}

// In forced-colors mode browsers keep an SVG's own colours, so the amber offer line and its label,
// chosen for the slate-900 surface, would fade on a light Canvas (about 1.7:1 on white). Under
// forced colours only, both are drawn in CanvasText, the line still dashed and drawn 2px wide,
// because the grid keeps its own dark dashed stroke and a 1px offer line would read as one more
// grid line. The `ReferenceLine` class lands on the layer holding the line, while Recharts draws
// the label in a layer of its own, so the label takes its own class. Recharts 3.10.1 replaces its
// `recharts-label` class with a label's `className` instead of adding to it, so the library class
// is repeated to keep the label matching `.recharts-label` selectors.
const OFFER_LINE_CLASS =
  "forced-colors:[&_.recharts-reference-line-line]:[stroke:CanvasText] forced-colors:[&_.recharts-reference-line-line]:stroke-2";
const OFFER_LABEL_CLASS = "recharts-label forced-colors:[fill:CanvasText]";

function FairValueChart({ response }: { response: IpoValuationResponse }): ReactElement {
  const offer = response.offer_price_kes;
  const bars = [
    fairValueBar("DCF", response.dcf.fair_value_per_share_kes, offer),
    fairValueBar("Comparables", response.comparables.fair_value_per_share_kes, offer),
    fairValueBar("Blended", response.blended_fair_value_per_share_kes, offer),
  ];
  const offerY = toChartNumber(offer);
  const offerText = formatDecimal(offer, "per_share");
  // The chart's accessible description, read when keyboard focus reaches the chart.
  const description = `${bars
    .map((bar) => `${bar.method} ${formatDecimal(bar.raw, "per_share")}`)
    .join(", ")}; offer ${offerText}`;

  return (
    <figure className="m-0">
      <figcaption className={SECTION_HEADING_CLASS}>
        Fair value per share by method (KES) against the offer
      </figcaption>
      <div className={FAIR_VALUE_CHART_CLASS} data-testid="ipo-fair-value-chart">
        <ResponsiveContainer width="100%" height="100%">
          {/* The right margin holds the offer line's label beside the plot, where it cannot
              cover the Blended bar. The Y axis is as wide as its longest tick label, which
              Recharts rounds to a whole pixel; the 1px left margin keeps that label's first
              glyph inside the svg. Every bar keeps an x-axis label (interval 0). */}
          <BarChart
            data={bars}
            title="Fair value per share by method against the offer price"
            desc={description}
            margin={{ top: 12, right: 96, bottom: 0, left: 1 }}
          >
            <CartesianGrid stroke={CHART_COLORS.grid} strokeDasharray="3 3" vertical={false} />
            <XAxis
              dataKey="method"
              tick={renderMethodTick}
              interval={0}
              stroke={CHART_COLORS.axis}
            />
            <YAxis
              tick={AXIS_TICK}
              stroke={CHART_COLORS.axis}
              tickFormatter={formatAxisTick}
              width="auto"
            />
            <FairValueTooltip />
            <Bar dataKey="y" name="Fair value" isAnimationActive={false} shape={renderFairValueBar} />
            {offerY === null ? null : (
              <ReferenceLine
                y={offerY}
                className={OFFER_LINE_CLASS}
                stroke={CHART_COLORS.warning}
                strokeDasharray="4 3"
                ifOverflow="extendDomain"
                label={{
                  value: `Offer ${offerText}`,
                  position: "right",
                  className: OFFER_LABEL_CLASS,
                  fill: CHART_COLORS.warning,
                  fontSize: 11,
                }}
              />
            )}
          </BarChart>
        </ResponsiveContainer>
      </div>
    </figure>
  );
}

// ---------------------------------------------------------------------------------------------
// Inputs
// ---------------------------------------------------------------------------------------------

interface SliderControlProps {
  field: SliderField;
  inputId: string;
  /** Position in slider units, an integer string: the thumb's step, nearest a held value. */
  position: string;
  /**
   * The value sent to the API (see {@link sliderValue}), shown beside the slider and as its
   * `aria-valuetext`: a held preset value exactly, else the position serialized.
   */
  value: DecimalString;
  error: string | undefined;
  onPositionChange: (field: SliderField, raw: string) => void;
}

function SliderControl({
  field,
  inputId,
  position,
  value,
  error,
  onPositionChange,
}: SliderControlProps): ReactElement {
  const spec = SLIDERS[field];
  const display = sliderDisplay(field, value);
  const errorId = `${inputId}-error`;
  return (
    <div>
      <div className="flex items-baseline justify-between gap-2">
        <label htmlFor={inputId} className={FIELD_LABEL_CLASS}>
          {spec.label}
        </label>
        <output
          htmlFor={inputId}
          data-testid={`ipo-value-${field}`}
          className="text-xs font-medium tabular-nums text-slate-100"
        >
          {display}
        </output>
      </div>
      <input
        type="range"
        id={inputId}
        data-testid={`ipo-input-${field}`}
        min={spec.min}
        max={spec.max}
        step={spec.step}
        value={position}
        aria-valuetext={display}
        aria-invalid={error ? true : undefined}
        aria-describedby={error ? errorId : undefined}
        onChange={(event) => onPositionChange(field, event.target.value)}
        className={RANGE_CLASS}
      />
      <FieldErrorText field={field} id={errorId} message={error} />
    </div>
  );
}

interface TextControlProps {
  field: TextField;
  inputId: string;
  value: string;
  /** The local range error, else the server's error for this field. */
  error: string | undefined;
  /** Whether the value passes its single-field check, which enables the grouped echo. */
  locallyValid: boolean;
  onValueChange: (field: TextField, value: string) => void;
}

function TextControl({
  field,
  inputId,
  value,
  error,
  locallyValid,
  onValueChange,
}: TextControlProps): ReactElement {
  const spec = TEXTS[field];
  const errorId = `${inputId}-error`;
  const hintId = `${inputId}-hint`;
  const echo = locallyValid ? echoText(spec, value) : null;
  const describedBy = [error ? errorId : null, echo === null ? null : hintId]
    .filter((id): id is string => id !== null)
    .join(" ");
  return (
    <div>
      <label htmlFor={inputId} className={FIELD_LABEL_CLASS}>
        {spec.label}
      </label>
      <input
        type="text"
        id={inputId}
        inputMode={spec.inputMode}
        autoComplete="off"
        spellCheck={false}
        data-testid={`ipo-input-${field}`}
        value={value}
        aria-invalid={error ? true : undefined}
        aria-describedby={describedBy === "" ? undefined : describedBy}
        onChange={(event) => onValueChange(field, event.target.value)}
        className={TEXT_INPUT_CLASS}
      />
      {echo === null ? null : (
        <p id={hintId} className="mt-1 text-xs tabular-nums text-slate-400 break-words">
          {echo}
        </p>
      )}
      <FieldErrorText field={field} id={errorId} message={error} />
    </div>
  );
}

// ---------------------------------------------------------------------------------------------
// Outputs
// ---------------------------------------------------------------------------------------------

/** A settled valuation together with the request it answers. */
interface ValuationResult {
  request: IpoValuationRequest;
  response: IpoValuationResponse;
  /** The body as posted, which is the request key the result answers. */
  key: string;
  /** The `apiBaseUrl` the request was sent to; a result from another API is never reused. */
  apiBaseUrl: string | undefined;
}

interface OutputCardProps {
  label: string;
  value: string;
  testId?: string;
  /** Text colour class of the value; slate when omitted. */
  toneClassName?: string;
  /** Pointer tooltip, and screen-reader text, explaining a missing value. */
  explanation?: string;
}

function OutputCard({
  label,
  value,
  testId,
  toneClassName = "text-slate-100",
  explanation,
}: OutputCardProps): ReactElement {
  return (
    <div className={KPI_TILE_CLASS}>
      <dt className={KPI_LABEL_CLASS}>{label}</dt>
      <dd
        data-testid={testId}
        title={explanation}
        className={`${KPI_VALUE_CLASS} ${toneClassName}`}
      >
        {value}
        {explanation === undefined ? null : <span className="sr-only">: {explanation}</span>}
      </dd>
    </div>
  );
}

interface ValuationOutputProps {
  result: ValuationResult;
  isRecalculating: boolean;
}

/** Output cards, flags, the method chart and the sensitivity matrix of one settled valuation. */
function ValuationOutput({ result, isRecalculating }: ValuationOutputProps): ReactElement {
  const { request, response } = result;
  const upsideComparison = compareDecimal(response.upside_pct, "0");
  const compsValue = response.comparables.fair_value_per_share_kes;
  const compsFlags = response.comparables.flags.join(", ");
  return (
    <div
      data-testid="ipo-valuation-output"
      className={isRecalculating ? `${OUTPUT_CLASS} opacity-60` : OUTPUT_CLASS}
    >
      {/* The live region covers the headline figures only: announcing the 35-cell matrix and
          the chart on every slider change would drown them out. */}
      <div aria-live="polite" className="flex flex-col gap-3">
        <dl className={CARD_GRID_CLASS}>
          <OutputCard
            label="Blended fair value"
            testId="ipo-blended-fair-value"
            value={formatDecimal(response.blended_fair_value_per_share_kes, "per_share")}
          />
          <OutputCard
            label="DCF fair value"
            testId="ipo-dcf-fair-value"
            value={formatDecimal(response.dcf.fair_value_per_share_kes, "per_share")}
          />
          <OutputCard
            label="Comparables fair value"
            testId="ipo-comps-fair-value"
            value={formatDecimal(compsValue, "per_share")}
            explanation={
              compsValue === null ? (compsFlags === "" ? "Not computable" : compsFlags) : undefined
            }
          />
          <OutputCard
            label={upsideComparison === -1 ? "Downside vs offer" : "Upside vs offer"}
            testId="ipo-upside"
            value={formatDecimal(response.upside_pct, "pct", { signed: true })}
            toneClassName={toneClass(upsideComparison)}
          />
          <OutputCard
            label="Implied EV/EBITDA at offer"
            testId="ipo-implied-ev-ebitda"
            value={formatDecimal(response.implied_ev_ebitda_at_offer, "ratio")}
          />
          <OutputCard
            label="Implied market cap at offer"
            testId="ipo-implied-market-cap"
            value={formatDecimal(response.implied_market_cap_at_offer_kes, "kes")}
          />
          <OutputCard
            label="Offer size"
            testId="ipo-offer-size"
            value={formatDecimal(response.offer_size_kes, "kes")}
          />
          <OutputCard
            label="Primary proceeds"
            testId="ipo-primary-proceeds"
            value={formatDecimal(response.primary_proceeds_kes, "kes")}
          />
          <OutputCard
            label="Secondary proceeds"
            testId="ipo-secondary-proceeds"
            value={formatDecimal(response.secondary_proceeds_kes, "kes")}
          />
        </dl>
        {response.flags.length === 0 ? null : (
          <ul aria-label="Valuation flags" className="flex flex-wrap gap-1.5">
            {response.flags.map((flag, index) => (
              <li key={`${flag}-${index}`} data-testid="ipo-flag" className={FLAG_CLASS}>
                {flag}
              </li>
            ))}
          </ul>
        )}
      </div>
      <FairValueChart response={response} />
      <SensitivityMatrix
        data={response.sensitivity}
        offerPriceKes={request.offer_price_kes}
        selected={{ waccPct: request.wacc_pct, ebitdaMarginPct: request.ebitda_margin_pct }}
      />
    </div>
  );
}

// ---------------------------------------------------------------------------------------------
// Workspace: the inputs and outputs of one preset
// ---------------------------------------------------------------------------------------------

const RECALCULATION_FAILED = "Could not recalculate the valuation";
// The Valuation heading takes focus from a Retry button that disappears while focused, so it
// draws the controls' keyboard focus outline.
const VALUATION_HEADING_CLASS = `${SECTION_HEADING_CLASS} ${FOCUS_CLASS}`;

/** Whether the element is rendered and holds focus, in whichever document or shadow root. */
function holdsFocus(element: HTMLElement | null): boolean {
  return element !== null && element.matches(":focus");
}

interface SandboxWorkspaceProps {
  scenario: IpoScenario;
  apiBaseUrl: string | undefined;
  getHeaders: TerminalWidgetProps["getHeaders"];
  debounceMs: number;
}

/**
 * Inputs initialized from one preset, and the server's valuation of their current values. The
 * parent remounts it with `key={scenario.id}` when the preset changes, so its state starts from
 * the new preset's assumptions through `useState` initializers. A slider whose preset value lies
 * off its grid holds that exact value, shown and sent unchanged with its thumb on the nearest
 * step, until the user moves that slider.
 *
 * Recalculation: every change updates only its own input state, so a slider's `<output>` follows
 * each input event. The derived request key changes, and once it has been stable for
 * `debounceMs` the body is posted, unless it is the body of the result on screen, valued by the
 * same `apiBaseUrl`. Inputs that return to that result, inside the debounce or after a rejected
 * request, post nothing: the result is their valuation, so it shows undimmed without
 * "Recalculating…", and the server's field and general errors from the inputs in between are
 * cleared at once. The comparison is byte for byte on the body, which carries typed text
 * unchanged, so primary proceeds typed as `"0"` differ from the preset's `"0.00"` and are posted.
 * The moment the key changes, before the debounce elapses, the request in flight is aborted and
 * its sequence number retired; that includes a change to a null key, when a text input fails its
 * single-field check and nothing is sent, and unmounting. A response or error for inputs that
 * have since changed is therefore never applied. A newer request, such as a Retry of the same
 * key, likewise aborts the one in flight, and the sequence counter discards any response that is
 * not the latest. The last valid result stays on screen, dimmed with "Recalculating…", until the
 * latest request settles. A field error from the server appears beneath its input and leaves the
 * last result in place. When the general error is cleared, by a settled request or by a return
 * to the result on screen, while its Retry button holds focus, focus moves to the Valuation
 * heading.
 */
function SandboxWorkspace({
  scenario,
  apiBaseUrl,
  getHeaders,
  debounceMs,
}: SandboxWorkspaceProps): ReactElement {
  const idPrefix = useId();
  const [positions, setPositions] = useState<SliderPositions>(() =>
    initialPositions(scenario.assumptions),
  );
  const [heldValues, setHeldValues] = useState<HeldSliderValues>(() =>
    initialHeldValues(scenario.assumptions),
  );
  const [texts, setTexts] = useState<TextValues>(() => initialTexts(scenario.assumptions));
  const [result, setResult] = useState<ValuationResult | null>(null);
  const [serverFieldErrors, setServerFieldErrors] = useState<Record<string, string>>({});
  const [generalError, setGeneralError] = useState<string | null>(null);
  /**
   * Key of the latest request once it has settled; null while it is pending. When the inputs
   * return to the result on screen, it becomes that result's key without a request being sent.
   */
  const [settledKey, setSettledKey] = useState<string | null>(null);

  const controllerRef = useRef<AbortController | null>(null);
  const sequenceRef = useRef(0);
  const retryButtonRef = useRef<HTMLButtonElement | null>(null);
  const valuationHeadingRef = useRef<HTMLHeadingElement>(null);
  // Set when the general error is cleared, and with it the Retry button, while that button holds
  // focus: by a settled request, or without one when the inputs or the API return to the result
  // on screen (see attachRetryButton). The effect after the commit then moves focus to the
  // Valuation heading rather than leaving it to fall back to the document body.
  const focusValuationRef = useRef(false);
  // The request options are read through a ref so that a host passing a new `getHeaders`
  // function on every render does not restart the debounce, which would re-post endlessly.
  const optionsRef = useRef({ apiBaseUrl, getHeaders });
  useEffect(() => {
    optionsRef.current = { apiBaseUrl, getHeaders };
  });

  const localErrors = useMemo(() => collectLocalErrors(texts), [texts]);
  const hasLocalErrors = Object.keys(localErrors).length > 0;
  /** The serialized request body, or null while a text input is invalid. */
  const requestKey = useMemo(
    () => (hasLocalErrors ? null : JSON.stringify(buildRequest(positions, heldValues, texts))),
    [hasLocalErrors, positions, heldValues, texts],
  );
  /** Key of the result on screen; null when there is none or another API computed it. */
  const displayedKey = result !== null && result.apiBaseUrl === apiBaseUrl ? result.key : null;
  /** Whether the inputs serialize to the very body of the result on screen, their valuation. */
  const showsDisplayedResult = requestKey !== null && requestKey === displayedKey;
  const hasServerFieldErrors = Object.keys(serverFieldErrors).length > 0;
  // Back at the result on screen, the inputs' request has settled to that result, and no server
  // error from the inputs in between applies. The state is adjusted during render, as in
  // FairValueTooltip, rather than in an effect; the pass that follows finds nothing left to
  // adjust, so the render settles at once.
  if (
    showsDisplayedResult &&
    (settledKey !== requestKey || generalError !== null || hasServerFieldErrors)
  ) {
    setSettledKey(requestKey);
    if (generalError !== null) setGeneralError(null);
    if (hasServerFieldErrors) setServerFieldErrors({});
  }
  const isRecalculating = requestKey !== null && !showsDisplayedResult && requestKey !== settledKey;

  // The Retry button's ref. React detaches a removed element's ref before taking the element out
  // of the document, so a general error cleared without a request while Retry holds focus is
  // seen here and hands focus to the Valuation heading, as a settled request's does.
  const attachRetryButton = useCallback((node: HTMLButtonElement | null): void => {
    if (node === null && holdsFocus(retryButtonRef.current)) focusValuationRef.current = true;
    retryButtonRef.current = node;
  }, []);

  const send = useCallback(async (key: string): Promise<void> => {
    controllerRef.current?.abort();
    const controller = new AbortController();
    controllerRef.current = controller;
    sequenceRef.current += 1;
    const sequence = sequenceRef.current;
    setSettledKey(null);
    // Decoded from the key, so the body posted is exactly the one the key identifies.
    const request = JSON.parse(key) as IpoValuationRequest;
    const options = optionsRef.current;
    try {
      const response = await apiSend<IpoValuationResponse>("POST", "/ipo/valuation", request, {
        apiBaseUrl: options.apiBaseUrl,
        getHeaders: options.getHeaders,
        signal: controller.signal,
      });
      if (controller.signal.aborted || sequence !== sequenceRef.current) return;
      focusValuationRef.current = holdsFocus(retryButtonRef.current);
      setResult({ request, response, key, apiBaseUrl: options.apiBaseUrl });
      setServerFieldErrors({});
      setGeneralError(null);
      setSettledKey(key);
    } catch (error) {
      if (isAbortError(error) || controller.signal.aborted || sequence !== sequenceRef.current) {
        return;
      }
      const fieldErrors = fieldErrorMap(error);
      if (Object.keys(fieldErrors).length > 0) {
        focusValuationRef.current = holdsFocus(retryButtonRef.current);
        setServerFieldErrors(fieldErrors);
        setGeneralError(null);
      } else {
        setServerFieldErrors({});
        setGeneralError(error instanceof TerminalApiError ? error.message : RECALCULATION_FAILED);
      }
      setSettledKey(key);
    } finally {
      if (controllerRef.current === controller) controllerRef.current = null;
    }
  }, []);

  /**
   * Aborts the request in flight and retires its sequence number, so neither its response nor
   * its error is applied, even one that has already arrived and is waiting to be handled.
   */
  const abortInFlight = useCallback((): void => {
    controllerRef.current?.abort();
    controllerRef.current = null;
    sequenceRef.current += 1;
  }, []);

  // `apiBaseUrl` is a dependency so that pointing the widget at another API revalues the
  // current assumptions there; `send` reads it from the ref, updated by the effect above. The
  // cleanup runs whenever the request key changes, to a new body or to null, so it retires the
  // request for the superseded inputs at once rather than when the next one is posted. The body
  // of the result on screen is never posted again: that result already answers it, from this
  // API, and nothing is in flight for it to retire.
  useEffect(() => {
    if (requestKey === null || requestKey === displayedKey) return undefined;
    const timer = setTimeout(() => {
      void send(requestKey);
    }, debounceMs);
    return () => {
      clearTimeout(timer);
      abortInFlight();
    };
  }, [requestKey, displayedKey, debounceMs, apiBaseUrl, send, abortInFlight]);

  useEffect(() => abortInFlight, [abortInFlight]);

  // Runs after the commit that cleared the general error, once the Retry button has left the DOM.
  useEffect(() => {
    if (!focusValuationRef.current) return;
    focusValuationRef.current = false;
    valuationHeadingRef.current?.focus();
  }, [generalError]);

  const handlePositionChange = useCallback((field: SliderField, raw: string): void => {
    const next = inputPosition(SLIDERS[field], raw);
    if (next === null) return;
    setPositions((previous) => (previous[field] === next ? previous : { ...previous, [field]: next }));
    // Moving a slider releases its held preset value, so it is serialized from its position from
    // now on; the other sliders keep theirs.
    setHeldValues((previous) =>
      previous[field] === null ? previous : { ...previous, [field]: null },
    );
  }, []);

  const handleTextChange = useCallback((field: TextField, value: string): void => {
    setTexts((previous) => (previous[field] === value ? previous : { ...previous, [field]: value }));
  }, []);

  const unmatchedErrors = Object.entries(serverFieldErrors).filter(
    ([field]) => !INPUT_FIELDS.has(field),
  );
  const fieldError = (field: InputField): string | undefined =>
    localErrors[field] ?? serverFieldErrors[field];
  const currency = scenario.currency || "KES";
  const assumptionsHeadingId = `${idPrefix}-assumptions`;
  const advancedHeadingId = `${idPrefix}-advanced`;
  const valuationHeadingId = `${idPrefix}-valuation`;
  const risksHeadingId = `${idPrefix}-risks`;

  return (
    <div className="flex flex-col gap-4">
      <div>
        <p data-testid="ipo-company-name" className="text-base font-semibold text-slate-100">
          {scenario.company_name}
        </p>
        <p className="text-xs text-slate-400">
          {scenario.sector === null ? null : `${scenario.sector} · `}
          Amounts in {currency}; presets are read-only and every valuation is computed by the
          server
        </p>
      </div>

      <div className={WORKSPACE_CLASS}>
        <div className={INPUTS_COLUMN_CLASS}>
          <section aria-labelledby={assumptionsHeadingId}>
            <h3 id={assumptionsHeadingId} className={SECTION_HEADING_CLASS}>
              Assumptions
            </h3>
            <div className="mt-2 flex flex-col gap-2">
              {SLIDER_FIELDS.map((field) => (
                <SliderControl
                  key={field}
                  field={field}
                  inputId={`${idPrefix}-${field}`}
                  position={positions[field]}
                  value={sliderValue(positions, heldValues, field)}
                  error={fieldError(field)}
                  onPositionChange={handlePositionChange}
                />
              ))}
            </div>
          </section>

          <section aria-labelledby={advancedHeadingId}>
            <h3 id={advancedHeadingId} className={SECTION_HEADING_CLASS}>
              Advanced assumptions
            </h3>
            <div className={ADVANCED_GRID_CLASS}>
              {TEXT_FIELDS.map((field) => (
                <TextControl
                  key={field}
                  field={field}
                  inputId={`${idPrefix}-${field}`}
                  value={texts[field]}
                  error={fieldError(field)}
                  locallyValid={localErrors[field] === undefined}
                  onValueChange={handleTextChange}
                />
              ))}
            </div>
          </section>
        </div>

        <div className={OUTPUT_COLUMN_CLASS}>
          {/* The heading row is as tall as the 16px heading, and its content follows 8px below,
              as the Assumptions heading's does, so both columns' headings and first rows line
              up. The status text is 16px tall too, so it shifts nothing when it appears. */}
          <section aria-labelledby={valuationHeadingId} className="flex flex-col gap-2">
            <div className="flex items-baseline justify-between gap-2">
              <h3
                ref={valuationHeadingRef}
                id={valuationHeadingId}
                tabIndex={-1}
                className={VALUATION_HEADING_CLASS}
              >
                Valuation
              </h3>
              <p role="status" className="text-xs text-sky-300">
                {isRecalculating ? (
                  <span data-testid="ipo-recalculating">Recalculating…</span>
                ) : null}
              </p>
            </div>

            {generalError === null && unmatchedErrors.length === 0 ? null : (
              <div role="alert" data-testid="ipo-general-error" className={GENERAL_ERROR_CLASS}>
                {generalError === null ? null : <p>{generalError}</p>}
                {unmatchedErrors.map(([field, message]) => (
                  <p key={field}>
                    {message} ({field})
                  </p>
                ))}
                {generalError === null || requestKey === null ? null : (
                  <button
                    ref={attachRetryButton}
                    type="button"
                    data-testid="ipo-retry"
                    onClick={() => {
                      void send(requestKey);
                    }}
                    className={RETRY_CLASS}
                  >
                    Retry
                  </button>
                )}
              </div>
            )}

            {result !== null ? (
              <ValuationOutput result={result} isRecalculating={isRecalculating} />
            ) : isRecalculating ? (
              <div data-testid="ipo-valuation-pending">
                {/* Sized to the first valuation's output (cards, chart and matrix), so the card
                    keeps its height from the frame's skeleton through the first result. */}
                <div aria-hidden="true" className={PENDING_OUTPUT_CLASS} />
                <span className="sr-only">Calculating the fair value…</span>
              </div>
            ) : generalError === null ? (
              <p className="text-sm text-slate-400">
                Correct the highlighted assumptions to calculate a fair value.
              </p>
            ) : null}
          </section>

          {scenario.risk_scenarios.length === 0 ? null : (
            <section aria-labelledby={risksHeadingId}>
              <h3 id={risksHeadingId} className={SECTION_HEADING_CLASS}>
                Risk scenarios
              </h3>
              <ul className="mt-2 flex flex-col gap-2">
                {scenario.risk_scenarios.map((risk, index) => (
                  <li
                    key={`${risk.title}-${index}`}
                    data-testid="ipo-risk-scenario"
                    className="rounded border border-slate-800 bg-slate-950 p-3 text-sm"
                  >
                    <strong className="font-semibold text-slate-100">{risk.title}</strong>
                    <p className="mt-1 text-slate-300">{risk.description}</p>
                  </li>
                ))}
              </ul>
            </section>
          )}
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------------------------
// Widget
// ---------------------------------------------------------------------------------------------

function scenariosEmpty(scenarios: IpoScenario[]): boolean {
  return scenarios.length === 0;
}

/**
 * IPO valuation sandbox: sliders and text inputs for the assumptions of a read-only preset from
 * `GET /ipo/scenarios` (the seeded Quickmart Supermarket offer at KES 7.50), with the server's
 * DCF, comparables and blended fair values from `POST /ipo/valuation` recalculated shortly after
 * each change, a fair-value-by-method chart against the offer, the WACC × base EBITDA margin
 * sensitivity matrix and the preset's risk notes.
 *
 * There is no client-side valuation code: every figure is the server's `Decimal` result, shown
 * from its exact string. Inputs are held as text, and decimal fields travel as exact decimal
 * strings (slider positions are integer step counts serialized with BigInt arithmetic; decimal
 * text inputs are sent as typed). `forecast_years`, `shares_outstanding` and `shares_offered` are
 * sent as JSON integers that the local check has bounded to at most 10^12, below 2^53, so they
 * stay exact; amounts are KES.
 *
 * The root carries `data-testid="terminal-ipo-valuation-sandbox"` and `data-state` from the
 * presets request; a rejected valuation shows field errors beneath the inputs and keeps the root
 * `ready` with the last valid result on screen.
 */
export function IpoValuationSandbox({
  apiBaseUrl,
  getHeaders,
  className,
  scenarioName,
  debounceMs = 150,
}: IpoValuationSandboxProps): ReactElement {
  const { state, data, error, refetch } = useTerminalQuery<IpoScenario[]>({
    path: "/ipo/scenarios",
    apiBaseUrl,
    getHeaders,
    isEmpty: scenariosEmpty,
  });
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const selectId = useId();

  const scenarios = data ?? [];
  const scenario =
    scenarios.find((item) => item.id === selectedId) ??
    (scenarioName === undefined
      ? undefined
      : scenarios.find((item) => item.company_name === scenarioName)) ??
    scenarios[0];
  const delay = Number.isFinite(debounceMs) && debounceMs > 0 ? debounceMs : 0;

  const presetSelector =
    scenario !== undefined && scenarios.length > 1 ? (
      <div className="flex items-center gap-2">
        <label htmlFor={selectId} className={FIELD_LABEL_CLASS}>
          Preset
        </label>
        <select
          id={selectId}
          data-testid="ipo-scenario-select"
          value={scenario.id}
          onChange={(event) => setSelectedId(event.target.value)}
          className={TEXT_FIELD_CLASS}
        >
          {scenarios.map((item) => (
            <option key={item.id} value={item.id}>
              {item.company_name}
            </option>
          ))}
        </select>
      </div>
    ) : null;

  return (
    <WidgetFrame
      testId="terminal-ipo-valuation-sandbox"
      title="IPO valuation sandbox"
      state={state}
      error={error}
      onRetry={refetch}
      className={className}
      skeletonClassName={SKELETON_HEIGHT_CLASS}
      actions={presetSelector}
    >
      {scenario === undefined ? null : (
        <SandboxWorkspace
          key={scenario.id}
          scenario={scenario}
          apiBaseUrl={apiBaseUrl}
          getHeaders={getHeaders}
          debounceMs={delay}
        />
      )}
    </WidgetFrame>
  );
}
