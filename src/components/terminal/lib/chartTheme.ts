/**
 * Colour and style constants for Recharts props (`stroke`, `fill`, `contentStyle`, `tick`),
 * which take colour strings rather than Tailwind classes.
 *
 * Each hex value is the sRGB approximation of a Tailwind default-palette shade, so chart strokes
 * match the widgets' utility classes. Only the slate, emerald, rose, amber and sky families are
 * used: a host that customizes its Tailwind colours must keep those families. No CSS variables
 * or global CSS are read, so the values do not depend on host styles.
 *
 * Source: the `--color-<family>-<shade>` OKLCH tokens in `tailwindcss/theme.css` of tailwindcss
 * 4.3.3, the pinned version. Each token is converted with the CSS Color 4 matrices, OKLCH to
 * OKLab to LMS (cubed) to linear sRGB, then sRGB gamma-encoded. Gamut policy: a channel outside
 * [0, 1] is clipped to that range on its own, which is how browsers paint an out-of-gamut
 * `oklch()` utility colour on an sRGB display; each channel is then rounded to the nearest 8-bit
 * value and written as lowercase `#rrggbb`. Seven shades lie outside sRGB and are clipped:
 * emerald-400, emerald-500, rose-200, rose-400, rose-500, sky-400 and amber-400. The compiled
 * stylesheet's legacy hex fallbacks, gamut-mapped by Lightning CSS and used only by browsers
 * without `lab()`/`oklch()` support, can differ by a few 8-bit units for clipped shades; these
 * values match the `lab()`/`oklch()` colour that supporting browsers paint for the utility
 * classes. Regenerate every value with this conversion whenever the tailwindcss pin changes.
 */
import type { CSSProperties } from "react";

/** Named chart colours, each mirroring one Tailwind default shade. */
export const CHART_COLORS = {
  /** slate-700, `oklch(37.2% 0.044 257.287)`: `CartesianGrid` `stroke`. */
  grid: "#314158",
  /**
   * slate-400, `oklch(70.4% 0.04 256.788)`: axis lines and tick labels (`XAxis`/`YAxis`
   * `stroke`, tick `fill`).
   */
  axis: "#90a1b9",
  /** slate-300, `oklch(86.9% 0.022 252.894)`: legend and tooltip text. */
  text: "#cad5e2",
  /**
   * slate-500, `oklch(55.4% 0.046 257.417)`: secondary series and reference lines
   * (`ReferenceLine` `stroke`).
   */
  muted: "#62748e",
  /** slate-900, `oklch(20.8% 0.042 265.755)`: tooltip background, the widget surface colour. */
  tooltipBg: "#0f172b",
  /** slate-700, `oklch(37.2% 0.044 257.287)`: tooltip border. */
  tooltipBorder: "#314158",
  /** emerald-500, `oklch(69.6% 0.17 162.48)`, clipped: gains and inflows (`Bar`/`Cell` `fill`). */
  positive: "#00bc7d",
  /** rose-500, `oklch(64.5% 0.246 16.439)`, clipped: losses and outflows (`Bar`/`Cell` `fill`). */
  negative: "#ff2056",
  /**
   * sky-400, `oklch(74.6% 0.16 232.661)`, clipped: primary series (`Line` `stroke`) and the
   * fallback series colour.
   */
  accent: "#00bcff",
  /**
   * amber-400, `oklch(82.8% 0.189 84.429)`, clipped: warnings and highlighted markers
   * (`ReferenceDot` `fill`).
   */
  warning: "#ffb900",
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
  "#00bcff", // sky-400, oklch(74.6% 0.16 232.661), clipped
  "#00d492", // emerald-400, oklch(76.5% 0.177 163.223), clipped
  "#ffb900", // amber-400, oklch(82.8% 0.189 84.429), clipped
  "#ff637e", // rose-400, oklch(71.2% 0.194 13.428), clipped
  "#b8e6fe", // sky-200, oklch(90.1% 0.058 230.902)
  "#a4f4cf", // emerald-200, oklch(90.5% 0.093 164.15)
  "#fee685", // amber-200, oklch(92.4% 0.12 95.746)
  "#ffccd3", // rose-200, oklch(89.2% 0.058 10.001), clipped
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
