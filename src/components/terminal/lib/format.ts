/**
 * Exact, locale-independent formatting and decimal arithmetic for the terminal widgets.
 *
 * The API serializes every `Decimal` as a JSON string already rounded once at the server
 * boundary ({@link DecimalString}). The display formatters here work on those strings digit by
 * digit and never convert them to a JavaScript number, so:
 * - a value is shown exactly at any magnitude, including 10^14 KES revenue inputs and totals
 *   beyond 2^53, which a float would silently round;
 * - the output does not depend on the runtime's locale data or time zone, so a server render and
 *   the browser's hydration produce identical text and never mismatch.
 *
 * Digits are grouped in the en-KE style (`1,234,567.00`), negatives use the Unicode minus sign
 * U+2212, money is labelled KES unless the kind says otherwise, and dates are formatted from
 * their ISO digits without a calendar object, so a date never shifts across a time zone.
 *
 * {@link toChartNumber} is the single float boundary of the widget library: it alone turns a
 * decimal string into a number, and only to place a point on a chart axis. Comparisons, range
 * checks and request serialization use the BigInt scaled-integer helpers at the end of this
 * module.
 *
 * The module has no side effects and imports only a type, so it stays inside the widget folder's
 * import boundary.
 */
import type { DecimalString } from "../types";

/** Placeholder shown for a missing value (U+2014 em dash); never "null" or "undefined". */
export const EMPTY_VALUE = "\u2014";

/** U+2212 minus sign: typographically matches `+` and digit widths, unlike ASCII `-`. */
const MINUS_SIGN = "\u2212";

/** Month abbreviations indexed by month number minus one. */
const MONTH_ABBREVIATIONS = [
  "Jan",
  "Feb",
  "Mar",
  "Apr",
  "May",
  "Jun",
  "Jul",
  "Aug",
  "Sep",
  "Oct",
  "Nov",
  "Dec",
] as const;

// ---------------------------------------------------------------------------------------------
// Display formatting
// ---------------------------------------------------------------------------------------------

/**
 * What a decimal value measures, which decides its unit affixes:
 * - `kes`: a KES amount, `KES 1,234,567.00`;
 * - `per_share`: a KES price or fair value per share, `KES 6.82`;
 * - `pct`: percentage points, `6.80%`;
 * - `ratio`: a multiple such as P/E or EV/EBITDA, `10.00×`;
 * - `z`: a dimensionless Z-score, `2.92`;
 * - `index`: a base-100 index level, `100.00`;
 * - `usd_m`: millions of US dollars, `USD 7,850.00 m`;
 * - `kes_per_usd`: an exchange rate, `129.20 KES/USD`.
 */
export type DecimalKind =
  | "kes"
  | "per_share"
  | "pct"
  | "ratio"
  | "z"
  | "index"
  | "usd_m"
  | "kes_per_usd";

/** Options for {@link formatDecimal}. */
export interface FormatOptions {
  /**
   * Whether to add the kind's unit affixes (`KES `, `%`, `×`, `USD … m`, ` KES/USD`).
   * Default `true`. With `false` only the signed, grouped number is returned.
   */
  unit?: boolean;
  /**
   * Whether to prefix `+` to a positive non-zero value, as for returns and upside. Default
   * `false`. Negative values always carry the minus sign and zero never carries a sign.
   */
  signed?: boolean;
}

/** Text placed before the sign and after the number for each kind. */
const UNIT_AFFIXES: Record<DecimalKind, { prefix: string; suffix: string }> = {
  kes: { prefix: "KES ", suffix: "" },
  per_share: { prefix: "KES ", suffix: "" },
  pct: { prefix: "", suffix: "%" },
  ratio: { prefix: "", suffix: "\u00d7" },
  z: { prefix: "", suffix: "" },
  index: { prefix: "", suffix: "" },
  usd_m: { prefix: "USD ", suffix: " m" },
  kes_per_usd: { prefix: "", suffix: " KES/USD" },
};

/** A plain decimal string: optional sign, integer digits, optional fraction digits. */
const DECIMAL_PATTERN = /^([+-]?)(\d+)(?:\.(\d+))?$/;

/** Removes redundant leading zeros, keeping at least one digit (`"007"` becomes `"7"`). */
function stripLeadingZeros(digits: string): string {
  const stripped = digits.replace(/^0+/, "");
  return stripped === "" ? "0" : stripped;
}

/** Inserts `,` every three digits from the right of an unsigned digit string. */
function groupDigits(digits: string): string {
  const head = digits.length % 3 || 3;
  let grouped = digits.slice(0, head);
  for (let i = head; i < digits.length; i += 3) {
    grouped += "," + digits.slice(i, i + 3);
  }
  return grouped;
}

/**
 * Formats an exact decimal string for display, without converting it to a number.
 *
 * - `null`, `undefined` and `""` give {@link EMPTY_VALUE}.
 * - A string that is not a plain decimal (`/^[+-]?\d+(\.\d+)?$/`, untrimmed) is returned
 *   unchanged, so unexpected server text is shown rather than hidden.
 * - Leading zeros of the integer part are removed and its digits grouped with `,`. The fraction
 *   is kept exactly as received, never rounded or padded: the server has already quantized it.
 * - Zero (every digit `0`, as in `"-0.00"`) carries no sign; a negative value carries U+2212; a
 *   positive value carries `+` only with `signed: true`.
 * - The sign follows a currency prefix and precedes a suffix: `KES −1,234.00`, `−9.11%`.
 *
 * @example formatDecimal("9484146704.50", "kes") === "KES 9,484,146,704.50"
 * @example formatDecimal("-9.11", "pct", { signed: true }) === "−9.11%"
 * @example formatDecimal("12.5", "pct", { signed: true }) === "+12.5%"
 * @example formatDecimal("0.00", "pct", { signed: true }) === "0.00%"
 * @example formatDecimal("33.25", "per_share") === "KES 33.25"
 * @example formatDecimal(null, "ratio") === "—"
 */
export function formatDecimal(
  value: DecimalString | null | undefined,
  kind: DecimalKind,
  options?: FormatOptions,
): string {
  if (value === null || value === undefined || value === "") return EMPTY_VALUE;
  const match = DECIMAL_PATTERN.exec(value);
  if (match === null) return value;

  const signText = match[1] ?? "";
  const integerDigits = stripLeadingZeros(match[2] ?? "0");
  const fractionDigits = match[3];
  const isZero = integerDigits === "0" && /^0*$/.test(fractionDigits ?? "");

  let sign = "";
  if (!isZero) {
    if (signText === "-") sign = MINUS_SIGN;
    else if (options?.signed === true) sign = "+";
  }
  const amount =
    sign + groupDigits(integerDigits) + (fractionDigits === undefined ? "" : "." + fractionDigits);
  if (options?.unit === false) return amount;

  const affixes = UNIT_AFFIXES[kind];
  return affixes.prefix + amount + affixes.suffix;
}

/**
 * Formats an integer count (shares, volume, sessions) with `,` grouping.
 *
 * The API bounds share counts and volumes to 10^12, below 2^53, so a JSON integer arrives exact
 * and its digits are printed exactly. A fractional input is truncated toward zero. `null`,
 * `undefined`, `NaN` and infinities give {@link EMPTY_VALUE}; zero, including `-0`, carries no
 * sign; negatives carry U+2212.
 *
 * @example formatInteger(2754329) === "2,754,329"
 * @example formatInteger(40065428000) === "40,065,428,000"
 */
export function formatInteger(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return EMPTY_VALUE;
  const whole = Math.trunc(value);
  if (whole === 0) return "0";
  // BigInt prints every digit of an integral number; String() switches to exponent notation
  // from 10^21, which grouping would garble. Both agree for every value the API can send.
  const digits = groupDigits(BigInt(Math.abs(whole)).toString());
  return whole < 0 ? MINUS_SIGN + digits : digits;
}

// ---------------------------------------------------------------------------------------------
// Dates (ISO strings handled as text, so no time-zone conversion can move a day)
// ---------------------------------------------------------------------------------------------

const DATE_PATTERN = /^(\d{4})-(\d{2})-(\d{2})$/;
const YEAR_MONTH_PATTERN = /^(\d{4})-(\d{2})$/;

/** Year and month of the first 7 characters of an ISO date, or null when they are not valid. */
function parseYearMonth(iso: string): { yearText: string; year: number; month: number } | null {
  const match = YEAR_MONTH_PATTERN.exec(iso.slice(0, 7));
  const yearText = match?.[1];
  const monthText = match?.[2];
  if (yearText === undefined || monthText === undefined) return null;
  const month = parseInt(monthText, 10);
  if (month < 1 || month > 12) return null;
  return { yearText, year: parseInt(yearText, 10), month };
}

/** Abbreviation of a month number from 1 to 12. */
function monthAbbreviation(month: number): string | undefined {
  return MONTH_ABBREVIATIONS[month - 1];
}

/**
 * Formats an ISO date as `<day> <Mon> <YYYY>`, the day without a leading zero.
 *
 * Only the first 10 characters are read, so an ISO datetime such as `"2026-09-30T12:00:00Z"` is
 * accepted and its calendar date is shown as written, never shifted by a time zone. `null`,
 * `undefined` and `""` give {@link EMPTY_VALUE}; any other input that is not `YYYY-MM-DD` with
 * month 01–12 and day 01–31 is returned unchanged.
 *
 * @example formatDate("2026-09-15") === "15 Sep 2026"
 * @example formatDate("2012-01-02") === "2 Jan 2012"
 */
export function formatDate(iso: string | null | undefined): string {
  if (iso === null || iso === undefined || iso === "") return EMPTY_VALUE;
  const match = DATE_PATTERN.exec(iso.slice(0, 10));
  const yearText = match?.[1];
  const monthText = match?.[2];
  const dayText = match?.[3];
  if (yearText === undefined || monthText === undefined || dayText === undefined) return iso;
  const day = parseInt(dayText, 10);
  const monthName = monthAbbreviation(parseInt(monthText, 10));
  if (monthName === undefined || day < 1 || day > 31) return iso;
  return `${day} ${monthName} ${yearText}`;
}

/**
 * Formats the month of `"YYYY-MM"`, `"YYYY-MM-DD"` or an ISO datetime as `<Mon> <YYYY>`.
 *
 * Accepts {@link shiftMonth} output. `null`, `undefined` and `""` give {@link EMPTY_VALUE};
 * input whose first 7 characters are not `YYYY-MM` with month 01–12 is returned unchanged.
 *
 * @example formatMonth("2026-09-30") === "Sep 2026"
 * @example formatMonth("2026-09") === "Sep 2026"
 */
export function formatMonth(iso: string | null | undefined): string {
  if (iso === null || iso === undefined || iso === "") return EMPTY_VALUE;
  const parsed = parseYearMonth(iso);
  if (parsed === null) return iso;
  return `${monthAbbreviation(parsed.month) ?? ""} ${parsed.yearText}`;
}

/**
 * Moves the month of an ISO date by `delta` whole months (truncated toward zero) and returns it
 * as `"YYYY-MM"`, the day being dropped. Used to select the `yield_curve.history` snapshot whose
 * `as_of` starts with the returned prefix.
 *
 * The input is returned unchanged when its first 7 characters are not `YYYY-MM` with month
 * 01–12, when `delta` is not finite, or when the result would fall outside years 0000–9999,
 * which `YYYY` cannot express.
 *
 * @example shiftMonth("2026-09-30", -6) === "2026-03"
 * @example shiftMonth("2026-09", -12) === "2025-09"
 * @example shiftMonth("2026-01-31", -1) === "2025-12"
 */
export function shiftMonth(iso: string, delta: number): string {
  const parsed = parseYearMonth(iso);
  if (parsed === null || !Number.isFinite(delta)) return iso;
  const total = parsed.year * 12 + (parsed.month - 1) + Math.trunc(delta);
  const year = Math.floor(total / 12);
  const month = total - year * 12 + 1;
  if (year < 0 || year > 9999) return iso;
  return `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}`;
}

// ---------------------------------------------------------------------------------------------
// Chart coordinates: the single float boundary
// ---------------------------------------------------------------------------------------------

/**
 * Converts a decimal string to a float, solely to place a point on a Recharts axis.
 *
 * This is the only decimal-to-float conversion in the widget library. The result is approximate
 * for magnitudes above 2^53, which at most moves a point by a sub-pixel amount. It must never be
 * displayed, summed, compared or sent back to the API: tooltips and axis labels format the
 * original string with {@link formatDecimal}, and arithmetic uses the scaled-integer helpers.
 *
 * Returns `null` for `null`, `undefined`, a blank string or text that does not convert to a
 * finite number, so Recharts leaves a gap instead of plotting a false zero.
 */
export function toChartNumber(value: DecimalString | null | undefined): number | null {
  if (value === null || value === undefined || value.trim() === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

// ---------------------------------------------------------------------------------------------
// Scaled integers (exact BigInt arithmetic for comparisons, range checks and request bodies)
// ---------------------------------------------------------------------------------------------

/** 10^scale as a BigInt, built from a string so no bigint literal syntax is required. */
function pow10(scale: number): bigint {
  return BigInt("1" + "0".repeat(scale));
}

/** Number of characters after the first `.` of the trimmed string, or 0 when there is none. */
function fractionLength(value: string): number {
  const trimmed = value.trim();
  const dot = trimmed.indexOf(".");
  return dot === -1 ? 0 : trimmed.length - dot - 1;
}

/** Whether `scale` is a usable number of decimal places. */
function isValidScale(scale: number): boolean {
  return Number.isInteger(scale) && scale >= 0;
}

/** Optional sign, optional integer digits, optional `.` with optional fraction digits. */
const SCALED_PATTERN = /^([+-]?)(\d*)(?:\.(\d*))?$/;
const ONLY_ZEROS_PATTERN = /^0*$/;
const INTEGER_PATTERN = /^-?\d+$/;

/**
 * Parses a decimal string into an integer count of 10^-scale units, exactly.
 *
 * The value is trimmed and must be an optional sign followed by digits with at most one `.`,
 * holding at least one digit: `"12."` and `".5"` are accepted, while `""`, `"."`, `"+"`, `"1e5"`,
 * `"1,000"` and `" - 1"` are not. Fraction digits beyond `scale` are accepted only when they are
 * all zeros, so the result is never rounded. Returns `null` for rejected text and for a `scale`
 * that is not a non-negative integer.
 *
 * @example parseScaled("18.00", 2) === BigInt("1800")
 * @example parseScaled("12.", 2) === BigInt("1200")
 * @example parseScaled(".5", 2) === BigInt("50")
 * @example parseScaled("50.0000", 0) === BigInt("50")
 * @example parseScaled("7.505", 2) === null
 */
export function parseScaled(value: string, scale: number): bigint | null {
  if (!isValidScale(scale)) return null;
  const match = SCALED_PATTERN.exec(value.trim());
  if (match === null) return null;
  const signText = match[1] ?? "";
  const integerDigits = match[2] ?? "";
  const fractionDigits = match[3] ?? "";
  if (integerDigits === "" && fractionDigits === "") return null;

  let scaledFraction: string;
  if (fractionDigits.length > scale) {
    if (!ONLY_ZEROS_PATTERN.test(fractionDigits.slice(scale))) return null;
    scaledFraction = fractionDigits.slice(0, scale);
  } else {
    scaledFraction = fractionDigits.padEnd(scale, "0");
  }
  const magnitude = BigInt((integerDigits || "0") + scaledFraction);
  return signText === "-" ? -magnitude : magnitude;
}

/**
 * Serializes a scaled integer as a plain ASCII decimal string for a request body: no grouping,
 * ASCII `-` for negatives, exactly `scale` fraction digits. The inverse of {@link parseScaled}.
 * Slider positions held as integer step counts are sent this way, so no float is involved.
 *
 * Throws a `RangeError` when `scale` is not a non-negative integer, a programmer error.
 *
 * @example formatScaled(BigInt("1800"), 2) === "18.00"
 * @example formatScaled(BigInt("-300"), 2) === "-3.00"
 * @example formatScaled(BigInt("5"), 2) === "0.05"
 * @example formatScaled(BigInt("4000000000"), 0) === "4000000000"
 */
export function formatScaled(scaled: bigint, scale: number): string {
  if (!isValidScale(scale)) {
    throw new RangeError(`formatScaled: scale must be a non-negative integer, got ${scale}`);
  }
  const zero = BigInt(0);
  const magnitude = scaled < zero ? -scaled : scaled;
  const divisor = pow10(scale);
  const integerPart = (magnitude / divisor).toString();
  const fractionPart = scale > 0 ? "." + (magnitude % divisor).toString().padStart(scale, "0") : "";
  return (scaled < zero ? "-" : "") + integerPart + fractionPart;
}

/**
 * Compares two decimal strings exactly, at the larger of their fraction lengths.
 *
 * Returns `-1`, `0` or `1` as `a` is below, equal to or above `b`, and `null` when either side is
 * `null`, `undefined` or not accepted by {@link parseScaled}. Trailing zeros do not matter.
 *
 * @example compareDecimal("18", "18.00") === 0
 * @example compareDecimal("6.82", "7.50") === -1
 * @example compareDecimal("-9.11", "0") === -1
 */
export function compareDecimal(
  a: string | null | undefined,
  b: string | null | undefined,
): -1 | 0 | 1 | null {
  if (a === null || a === undefined || b === null || b === undefined) return null;
  const scale = Math.max(fractionLength(a), fractionLength(b));
  const left = parseScaled(a, scale);
  const right = parseScaled(b, scale);
  if (left === null || right === null) return null;
  if (left < right) return -1;
  if (left > right) return 1;
  return 0;
}

/**
 * Single-field bounds of a typed input, checked exactly by {@link checkDecimalRange}.
 * `min` and `max` are trusted constants written as decimal strings.
 */
export interface DecimalRangeRule {
  /** Maximum number of non-zero fraction digits the value may carry. */
  scale: number;
  /** Inclusive lower bound, unless `minExclusive` is set. */
  min?: string;
  /** Inclusive upper bound. */
  max?: string;
  /** When true, a value equal to `min` is below the range. */
  minExclusive?: boolean;
  /** When true, the value must be a whole number written without a `.` or a `+` sign. */
  integer?: boolean;
}

/** Outcome of {@link checkDecimalRange}. */
export type DecimalRangeResult = "ok" | "invalid" | "below_min" | "above_max";

/** Parses a rule bound, throwing because an unparsable bound is a programmer error. */
function parseBound(bound: string, scale: number, name: "min" | "max"): bigint {
  const parsed = parseScaled(bound, scale);
  if (parsed === null) {
    throw new Error(
      `checkDecimalRange: rule.${name} "${bound}" is not a decimal at scale ${scale}`,
    );
  }
  return parsed;
}

/**
 * Checks a typed string against single-field bounds by exact scaled-integer comparison, before
 * the string is sent unchanged to the API. Cross-field rules, such as primary proceeds within the
 * offer size, are left to the server.
 *
 * Returns `"invalid"` when the value is not a decimal (see {@link parseScaled}), has more
 * non-zero fraction digits than `rule.scale`, or is not a plain integer under `rule.integer`;
 * otherwise `"below_min"`, `"above_max"` or `"ok"`. Throws an `Error` when `rule.scale`,
 * `rule.min` or `rule.max` is malformed, because the rule is a programmer-supplied constant.
 *
 * @example checkDecimalRange("0", { scale: 2, min: "0", minExclusive: true }) === "below_min"
 * @example checkDecimalRange("1.5", { scale: 0, integer: true, min: "1" }) === "invalid"
 * @example checkDecimalRange("50.01", { scale: 2, min: "0.00", max: "50.00" }) === "above_max"
 */
export function checkDecimalRange(value: string, rule: DecimalRangeRule): DecimalRangeResult {
  if (!isValidScale(rule.scale)) {
    throw new Error(
      `checkDecimalRange: rule.scale must be a non-negative integer, got ${rule.scale}`,
    );
  }
  const min = rule.min === undefined ? null : parseBound(rule.min, rule.scale, "min");
  const max = rule.max === undefined ? null : parseBound(rule.max, rule.scale, "max");

  if (rule.integer === true && !INTEGER_PATTERN.test(value.trim())) return "invalid";
  const parsed = parseScaled(value, rule.scale);
  if (parsed === null) return "invalid";

  if (min !== null && (parsed < min || (rule.minExclusive === true && parsed === min))) {
    return "below_min";
  }
  if (max !== null && parsed > max) return "above_max";
  return "ok";
}
