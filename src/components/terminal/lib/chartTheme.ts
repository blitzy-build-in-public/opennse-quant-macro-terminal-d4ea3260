/**
 * Colour and style constants for Recharts props (`stroke`, `fill`, `contentStyle`, `tick`),
 * which take colour strings rather than Tailwind classes.
 *
 * Each hex value is the sRGB approximation of a Tailwind default-palette shade (Tailwind v4
 * defines the palette in OKLCH), so chart strokes match the widgets' utility classes. Only the
 * slate, emerald, rose, amber and sky families are used: a host that customizes its Tailwind
 * colours must keep those families. No CSS variables or global CSS are read, so the values do
 * not depend on host styles.
 */
import type { CSSProperties } from "react";

/** Named chart colours, each mirroring one Tailwind default shade. */
export const CHART_COLORS = {
  /** slate-700: `CartesianGrid` `stroke`. */
  grid: "#334155",
  /** slate-400: axis lines and tick labels (`XAxis`/`YAxis` `stroke`, tick `fill`). */
  axis: "#94a3b8",
  /** slate-300: legend and tooltip text. */
  text: "#cbd5e1",
  /** slate-500: secondary series and reference lines (`ReferenceLine` `stroke`). */
  muted: "#64748b",
  /** slate-900: tooltip background, the widget surface colour. */
  tooltipBg: "#0f172a",
  /** slate-700: tooltip border. */
  tooltipBorder: "#334155",
  /** emerald-500: gains and inflows (`Bar`/`Cell` `fill`). */
  positive: "#10b981",
  /** rose-500: losses and outflows (`Bar`/`Cell` `fill`). */
  negative: "#f43f5e",
  /** sky-400: primary series (`Line` `stroke`) and the fallback series colour. */
  accent: "#38bdf8",
  /** amber-400: warnings and highlighted markers (`ReferenceDot` `fill`). */
  warning: "#fbbf24",
} as const;

/**
 * Line colours for multi-series charts, one per cycle line in `CycleBacktester` (the backtester
 * accepts at most 8 events): sky-400, emerald-400, amber-400, rose-400, then the 200 shades of
 * the same families. Use as `Line` `stroke`.
 *
 * Index it as `SERIES_PALETTE[i % SERIES_PALETTE.length] ?? CHART_COLORS.accent`:
 * `noUncheckedIndexedAccess` types every index read as `string | undefined`.
 */
export const SERIES_PALETTE: readonly string[] = [
  "#38bdf8",
  "#34d399",
  "#fbbf24",
  "#fb7185",
  "#bae6fd",
  "#a7f3d0",
  "#fde68a",
  "#fecdd3",
];

/** `Line` `strokeDasharray` for projection lines, so projections read as not observed. */
export const PROJECTION_DASH = "6 4";

/**
 * Recharts `<Tooltip contentStyle={...}>`: slate-900 background, slate-700 border and slate-300
 * text, matching the widget's own dark surface rather than any host style.
 */
export const TOOLTIP_CONTENT_STYLE: CSSProperties = {
  backgroundColor: CHART_COLORS.tooltipBg,
  border: "1px solid " + CHART_COLORS.tooltipBorder,
  color: CHART_COLORS.text,
  fontSize: 12,
};

/** `<XAxis tick={AXIS_TICK}>` / `<YAxis tick={AXIS_TICK}>`: slate-400 tick labels at 11px. */
export const AXIS_TICK = { fill: CHART_COLORS.axis, fontSize: 11 } as const;
