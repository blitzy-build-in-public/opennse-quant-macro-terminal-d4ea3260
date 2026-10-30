/**
 * Class names shared by the widgets' in-card section headings and KPI tiles, so every card draws
 * them alike.
 *
 * Each constant is a complete static string, never assembled from fragments, so Tailwind's
 * source scan and a host's `@source` directive or `content` glob find every class. `m-0` cancels
 * the user-agent margins of headings, paragraphs and `dd` elements in a host that ships no CSS
 * reset.
 */

/**
 * An in-card section heading (`h3`, a `figcaption` or a visible table title): 12px semibold
 * uppercase slate-300, one step below the card title of `WidgetFrame` (14px semibold slate-200).
 */
export const SECTION_HEADING_CLASS =
  "m-0 text-xs font-semibold uppercase tracking-wide text-slate-300";

/**
 * The surface of one KPI tile, the `div` that groups a `dt` and its `dd` elements: slate-950 on
 * the slate-900 card, a 1px slate-800 border and 12px padding, its parts stacked 4px apart.
 * `min-w-0` lets the tile shrink inside a grid track so a long value wraps instead of overflowing.
 */
export const KPI_TILE_CLASS =
  "flex min-w-0 flex-col gap-1 rounded border border-slate-800 bg-slate-950 p-3";

/** The label (`dt`) of a KPI tile: 12px slate-400. */
export const KPI_LABEL_CLASS = "m-0 text-xs text-slate-400";

/**
 * The value (`dd`) of a KPI tile: 18px semibold tabular figures. Callers append the text colour,
 * such as `text-slate-100` for a neutral value or an emerald or rose shade for a signed one.
 * `[overflow-wrap:anywhere]` lets a long KES amount wrap inside a narrow tile in Tailwind 3 and 4.
 */
export const KPI_VALUE_CLASS = "m-0 text-lg font-semibold tabular-nums [overflow-wrap:anywhere]";
