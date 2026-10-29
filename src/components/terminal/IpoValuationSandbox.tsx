"use client";

import { useCallback, useEffect, useId, useMemo, useRef, useState } from "react";
import type { ReactElement } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Rectangle,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { BarShapeProps } from "recharts";

import { SensitivityMatrix } from "./SensitivityMatrix";
import { apiSend, fieldErrorMap, isAbortError, TerminalApiError } from "./lib/apiClient";
import { AXIS_TICK, CHART_COLORS, TOOLTIP_CONTENT_STYLE } from "./lib/chartTheme";
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
import { FieldErrorText, WidgetFrame } from "./shared/WidgetFrame";
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
 * primary proceeds within the offer size) are the server's, reported as coded field errors.
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
type TextValues = Record<TextField, string>;
type LocalErrors = Partial<Record<InputField, string>>;

// ---------------------------------------------------------------------------------------------
// Styles
// ---------------------------------------------------------------------------------------------

// Every class name is written out in full, never assembled from fragments, so Tailwind's source
// scan and a host's `@source` directive or `content` glob find each one. Inputs, the slider and
// the preset selector are 44px tall (h-11), the minimum touch target, since no design source
// sets a smaller one. Focus indicators are outlines, which forced-colors mode keeps.
const SECTION_HEADING_CLASS = "text-xs font-semibold uppercase tracking-wide text-slate-300";
const LABEL_CLASS = "text-xs font-medium text-slate-300";
const FOCUS_CLASS =
  "focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400";
// The slider's 44px box is mostly empty space around its track, so its focus outline is drawn
// inside the box, where it can never overlap the label row above.
const RANGE_CLASS =
  "block h-11 w-full cursor-pointer rounded accent-sky-400 focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-sky-400";
const TEXT_INPUT_BASE_CLASS =
  "mt-1 block h-11 w-full rounded border bg-slate-950 px-3 text-sm tabular-nums text-slate-100 motion-safe:transition-[border-color] motion-safe:duration-150 motion-safe:ease-out";
const TEXT_INPUT_CLASS = `${TEXT_INPUT_BASE_CLASS} border-slate-700 hover:border-slate-500 ${FOCUS_CLASS}`;
const TEXT_INPUT_INVALID_CLASS = `${TEXT_INPUT_BASE_CLASS} border-rose-500 ${FOCUS_CLASS}`;
const SELECT_CLASS = `h-11 rounded border border-slate-700 bg-slate-950 px-2 text-sm text-slate-100 hover:border-slate-500 ${FOCUS_CLASS}`;
const OUTPUT_CLASS =
  "flex min-w-0 flex-col gap-4 motion-safe:transition-opacity motion-safe:duration-200 motion-safe:ease-out";
const CARD_CLASS = "min-w-0 rounded border border-slate-800 bg-slate-950 p-3";
const CARD_PRIMARY_VALUE_CLASS = "mt-1 text-lg font-semibold tabular-nums break-words";
const CARD_VALUE_CLASS = "mt-1 text-sm font-medium tabular-nums break-words";
const FLAG_CLASS =
  "rounded border border-amber-800 bg-amber-950 px-2 py-0.5 font-mono text-xs text-amber-200";
const GENERAL_ERROR_CLASS =
  "flex flex-col gap-1 rounded border border-rose-900 bg-slate-950 p-3 text-sm text-rose-300";
const RETRY_CLASS = `mt-1 h-11 self-start rounded border border-slate-700 bg-slate-800 px-4 text-xs font-medium text-slate-100 hover:bg-slate-700 motion-safe:transition-[background-color] motion-safe:duration-150 motion-safe:ease-out ${FOCUS_CLASS}`;

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
 * such as `"18.0000"` or one off the slider grid still lands on the nearest step. A preset the
 * API validated always parses; text that does not parse starts the slider at its lower bound.
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

/** A slider's current value as the plain decimal string sent to the API. */
function sliderValue(positions: SliderPositions, field: SliderField): DecimalString {
  return formatScaled(BigInt(positions[field]), SLIDERS[field].scale);
}

/** A slider's current value for display, for example `"18.00%"`, `"KES 7.50"` or `"0.50 pp"`. */
function sliderDisplay(field: SliderField, position: string): string {
  const spec = SLIDERS[field];
  const value = formatScaled(BigInt(position), spec.scale);
  return spec.suffix === undefined
    ? formatDecimal(value, spec.kind)
    : formatDecimal(value, spec.kind, { unit: false }) + spec.suffix;
}

function initialPositions(assumptions: IpoValuationRequest): SliderPositions {
  const positions = {} as SliderPositions;
  for (const field of SLIDER_FIELDS) {
    positions[field] = presetPosition(SLIDERS[field], assumptions[field]);
  }
  return positions;
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

/** The message for a text value outside its single-field range, or null when it is valid. */
function localErrorMessage(spec: TextSpec, value: string): string | null {
  const { rule } = spec;
  switch (checkDecimalRange(value, rule)) {
    case "ok":
      return null;
    case "invalid": {
      if (rule.integer === true) return "Enter a whole number";
      const text = value.trim();
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
 * The `POST /ipo/valuation` body for valid inputs: sliders serialized from their positions,
 * decimal texts trimmed but otherwise unchanged, integer texts as JSON integers. The sensitivity
 * axes are omitted, so the server's 5 WACC × 7 margin defaults apply.
 */
function buildRequest(positions: SliderPositions, texts: TextValues): IpoValuationRequest {
  return {
    offer_price_kes: sliderValue(positions, "offer_price_kes"),
    revenue_growth_pct: sliderValue(positions, "revenue_growth_pct"),
    ebitda_margin_pct: sliderValue(positions, "ebitda_margin_pct"),
    margin_expansion_pct: sliderValue(positions, "margin_expansion_pct"),
    wacc_pct: sliderValue(positions, "wacc_pct"),
    terminal_growth_pct: sliderValue(positions, "terminal_growth_pct"),
    net_debt_kes: sliderValue(positions, "net_debt_kes"),
    nwc_pct_revenue: sliderValue(positions, "nwc_pct_revenue"),
    peer_ev_ebitda_multiple: sliderValue(positions, "peer_ev_ebitda_multiple"),
    dcf_weight_pct: sliderValue(positions, "dcf_weight_pct"),
    capex_pct_revenue: texts.capex_pct_revenue.trim(),
    da_pct_revenue: texts.da_pct_revenue.trim(),
    tax_rate_pct: texts.tax_rate_pct.trim(),
    forecast_years: toJsonInteger(texts.forecast_years),
    base_revenue_kes: texts.base_revenue_kes.trim(),
    shares_outstanding: toJsonInteger(texts.shares_outstanding),
    shares_offered: toJsonInteger(texts.shares_offered),
    primary_proceeds_kes: texts.primary_proceeds_kes.trim(),
  };
}

/** Digit-grouped reading of a valid long value, such as `KES 45,000,000,000.00`. */
function echoText(spec: TextSpec, value: string): string | null {
  const text = value.trim();
  if (spec.echo === "kes") return formatDecimal(text, "kes");
  if (spec.echo === "shares") return formatDecimal(text, "kes", { unit: false }) + " shares";
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

/** Tooltip text: the exact fair value string, never the bar's float height. */
function fairValueTooltip(
  _value: unknown,
  _name: unknown,
  item: { payload?: unknown },
): [string, string] {
  const entry = item.payload;
  return [formatDecimal(isFairValueBar(entry) ? entry.raw : null, "per_share"), "Fair value"];
}

/** Y-axis ticks are generated chart coordinates with no source string, labelled at 2 dp. */
function formatAxisTick(value: number): string {
  return Number.isFinite(value) ? formatDecimal(value.toFixed(2), "per_share", { unit: false }) : "";
}

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
      <div className="mt-2 h-56 w-full" data-testid="ipo-fair-value-chart">
        <ResponsiveContainer width="100%" height="100%">
          {/* The right margin holds the offer line's label beside the plot, where it cannot
              cover the Blended bar. */}
          <BarChart
            data={bars}
            title="Fair value per share by method against the offer price"
            desc={description}
            margin={{ top: 12, right: 96, bottom: 0, left: 0 }}
          >
            <CartesianGrid stroke={CHART_COLORS.grid} strokeDasharray="3 3" vertical={false} />
            <XAxis dataKey="method" tick={AXIS_TICK} stroke={CHART_COLORS.axis} />
            <YAxis
              tick={AXIS_TICK}
              stroke={CHART_COLORS.axis}
              tickFormatter={formatAxisTick}
              width={48}
            />
            <Tooltip
              cursor={{ fill: CHART_COLORS.grid, fillOpacity: 0.35 }}
              contentStyle={TOOLTIP_CONTENT_STYLE}
              labelStyle={{ color: CHART_COLORS.text }}
              itemStyle={{ color: CHART_COLORS.text }}
              formatter={fairValueTooltip}
            />
            <Bar dataKey="y" name="Fair value" isAnimationActive={false} shape={renderFairValueBar} />
            {offerY === null ? null : (
              <ReferenceLine
                y={offerY}
                stroke={CHART_COLORS.warning}
                strokeDasharray="4 3"
                ifOverflow="extendDomain"
                label={{
                  value: `Offer ${offerText}`,
                  position: "right",
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
  /** Position in slider units, an integer string. */
  position: string;
  error: string | undefined;
  onPositionChange: (field: SliderField, raw: string) => void;
}

function SliderControl({
  field,
  inputId,
  position,
  error,
  onPositionChange,
}: SliderControlProps): ReactElement {
  const spec = SLIDERS[field];
  const display = sliderDisplay(field, position);
  const errorId = `${inputId}-error`;
  return (
    <div>
      <div className="flex items-baseline justify-between gap-2">
        <label htmlFor={inputId} className={LABEL_CLASS}>
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
      <label htmlFor={inputId} className={LABEL_CLASS}>
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
        className={error ? TEXT_INPUT_INVALID_CLASS : TEXT_INPUT_CLASS}
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
}

interface OutputCardProps {
  label: string;
  value: string;
  testId?: string;
  /** Larger type for the headline per-share values. */
  primary?: boolean;
  /** Text colour class of the value; slate when omitted. */
  toneClassName?: string;
  /** Pointer tooltip, and screen-reader text, explaining a missing value. */
  explanation?: string;
}

function OutputCard({
  label,
  value,
  testId,
  primary = false,
  toneClassName = "text-slate-100",
  explanation,
}: OutputCardProps): ReactElement {
  return (
    <div className={CARD_CLASS}>
      <dt className="text-xs text-slate-400">{label}</dt>
      <dd
        data-testid={testId}
        title={explanation}
        className={`${primary ? CARD_PRIMARY_VALUE_CLASS : CARD_VALUE_CLASS} ${toneClassName}`}
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
        <dl className="grid grid-cols-2 gap-3 sm:grid-cols-3">
          <OutputCard
            label="Blended fair value"
            testId="ipo-blended-fair-value"
            value={formatDecimal(response.blended_fair_value_per_share_kes, "per_share")}
            primary
          />
          <OutputCard
            label="DCF fair value"
            testId="ipo-dcf-fair-value"
            value={formatDecimal(response.dcf.fair_value_per_share_kes, "per_share")}
            primary
          />
          <OutputCard
            label="Comparables fair value"
            testId="ipo-comps-fair-value"
            value={formatDecimal(compsValue, "per_share")}
            primary
            explanation={
              compsValue === null ? (compsFlags === "" ? "Not computable" : compsFlags) : undefined
            }
          />
          <OutputCard
            label={upsideComparison === -1 ? "Downside vs offer" : "Upside vs offer"}
            testId="ipo-upside"
            value={formatDecimal(response.upside_pct, "pct", { signed: true })}
            primary
            toneClassName={toneClass(upsideComparison)}
          />
          <OutputCard
            label="Implied EV/EBITDA at offer"
            testId="ipo-implied-ev-ebitda"
            value={formatDecimal(response.implied_ev_ebitda_at_offer, "ratio")}
            primary
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

interface SandboxWorkspaceProps {
  scenario: IpoScenario;
  apiBaseUrl: string | undefined;
  getHeaders: TerminalWidgetProps["getHeaders"];
  debounceMs: number;
}

/**
 * Inputs initialized from one preset, and the server's valuation of their current values. The
 * parent remounts it with `key={scenario.id}` when the preset changes, so its state starts from
 * the new preset's assumptions through `useState` initializers.
 *
 * Recalculation: every change updates only its own input state, so a slider's `<output>` follows
 * each input event. The derived request key changes, and once it has been stable for
 * `debounceMs` the body is posted. A newer request aborts the one in flight and a sequence
 * counter discards any response that is not the latest. The last valid result stays on screen,
 * dimmed with "Recalculating…", until the latest request settles. A field error from the server
 * appears beneath its input and leaves the last result in place; while any text input fails its
 * single-field check, nothing is sent.
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
  const [texts, setTexts] = useState<TextValues>(() => initialTexts(scenario.assumptions));
  const [result, setResult] = useState<ValuationResult | null>(null);
  const [serverFieldErrors, setServerFieldErrors] = useState<Record<string, string>>({});
  const [generalError, setGeneralError] = useState<string | null>(null);
  /** Key of the latest request once it has settled; null while it is pending. */
  const [settledKey, setSettledKey] = useState<string | null>(null);

  const controllerRef = useRef<AbortController | null>(null);
  const sequenceRef = useRef(0);
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
    () => (hasLocalErrors ? null : JSON.stringify(buildRequest(positions, texts))),
    [hasLocalErrors, positions, texts],
  );
  const isRecalculating = requestKey !== null && requestKey !== settledKey;

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
      if (sequence !== sequenceRef.current) return;
      setResult({ request, response });
      setServerFieldErrors({});
      setGeneralError(null);
      setSettledKey(key);
    } catch (error) {
      if (isAbortError(error) || sequence !== sequenceRef.current) return;
      const fieldErrors = fieldErrorMap(error);
      if (Object.keys(fieldErrors).length > 0) {
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

  const abortInFlight = useCallback((): void => {
    controllerRef.current?.abort();
    controllerRef.current = null;
  }, []);

  // `apiBaseUrl` is a dependency so that pointing the widget at another API revalues the
  // current assumptions there; `send` reads it from the ref, updated by the effect above.
  useEffect(() => {
    if (requestKey === null) return undefined;
    const timer = setTimeout(() => {
      void send(requestKey);
    }, debounceMs);
    return () => {
      clearTimeout(timer);
    };
  }, [requestKey, debounceMs, apiBaseUrl, send]);

  useEffect(() => abortInFlight, [abortInFlight]);

  const handlePositionChange = useCallback((field: SliderField, raw: string): void => {
    const next = inputPosition(SLIDERS[field], raw);
    if (next === null) return;
    setPositions((previous) => (previous[field] === next ? previous : { ...previous, [field]: next }));
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

      <div className="grid gap-6 lg:grid-cols-[minmax(0,24rem)_minmax(0,1fr)]">
        <div className="flex min-w-0 flex-col gap-6">
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
            <div className="mt-2 grid gap-3 sm:grid-cols-2">
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

        <div className="flex min-w-0 flex-col gap-6">
          <section aria-labelledby={valuationHeadingId} className="flex flex-col gap-3">
            <div className="flex min-h-5 items-center justify-between gap-2">
              <h3 id={valuationHeadingId} className={SECTION_HEADING_CLASS}>
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
                <div aria-hidden="true" className="h-96 rounded bg-slate-800 motion-safe:animate-pulse" />
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
 * from its exact string. Inputs are strings end to end (slider positions are integer step counts
 * serialized with BigInt arithmetic), and amounts are KES.
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
        <label htmlFor={selectId} className={LABEL_CLASS}>
          Preset
        </label>
        <select
          id={selectId}
          data-testid="ipo-scenario-select"
          value={scenario.id}
          onChange={(event) => setSelectedId(event.target.value)}
          className={SELECT_CLASS}
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
      skeletonClassName="h-[36rem]"
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
