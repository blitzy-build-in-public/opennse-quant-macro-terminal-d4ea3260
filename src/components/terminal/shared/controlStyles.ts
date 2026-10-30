/**
 * Shared Tailwind class strings for the widgets' form controls (text fields, selects, secondary
 * and toggle buttons, chips and ticker buttons), so each control type looks and behaves the same
 * in every widget.
 *
 * Every class token is written out complete and literal. A widget may join these strings with
 * its own complete tokens, but never builds a token from fragments, so Tailwind 3's `content`
 * glob and Tailwind 4's `@source` scan both find every class. Only the default palette is used.
 *
 * - Every control type has one size, in every widget: text fields, selects, secondary and toggle
 *   buttons and chips are 26px tall, ticker buttons 24px, and the chip remove button 24px square.
 * - Field borders are slate-500, which keeps at least 3:1 non-text contrast against the slate-900
 *   card (about 3.7:1) and the slate-950 field fill (about 4.2:1).
 * - Focus is a sky-400 outline at a 2px offset (the chip remove button draws it inside its own
 *   circle). Outlines survive forced-colors mode, which drops box shadows and replaces
 *   background colours with system colours.
 * - The `forced-colors:`, `aria-[…]:` and `data-[…]:` variants used here and by callers need
 *   Tailwind 3.4 or later, or Tailwind 4.
 *
 * Hover styles use the `hover:` variant, which Tailwind 4 applies only on devices that can hover;
 * every control also has an `active:` state one step past its hover, so a press shows on touch
 * screens too. Transitions run only when the user has not asked for reduced motion.
 */

/* BLITZY [A11Y]: every widget, the IPO sandbox and the anomaly feed included, uses these compact
   sizes: text fields, selects, secondary and toggle buttons and chips 26px tall, ticker buttons
   24px tall and the chip remove button 24px square. That is at or above WCAG 2.2 AA's 24px target
   minimum and below the 44px guideline, so dense filter rows, toolbars and lists stay readable.
   Flagged for designer review. */

/** Label of a text field or select: 12px medium slate-300, about 12:1 on the slate-900 card. */
export const FIELD_LABEL_CLASS = "text-xs font-medium text-slate-300";

/**
 * Compact text input or select, 26px tall with a slate-500 border on a slate-950 fill.
 *
 * Hover lightens the border to slate-400 and a press to slate-300, only while the field is
 * enabled. `aria-invalid="true"` turns the border rose-400, and its hover and press shades stay
 * rose: the `enabled:hover:` and `enabled:active:` selectors are more specific than
 * `aria-[invalid=true]:`, so without the `aria-[invalid=true]:enabled:hover:` and `…:active:`
 * tokens the slate hover and press borders would replace the invalid one. A disabled field dims
 * and shows a not-allowed cursor. No width is set: callers add `w-full min-w-0` where the field
 * fills its column. Native parts such as the calendar icon and the select arrow are drawn in the
 * dark scheme, except under forced colours, where `color-scheme: light dark` lets the browser draw
 * them for the forced palette, so a light palette never gets a white icon on its white Canvas.
 */
export const TEXT_FIELD_CLASS =
  "h-[26px] rounded border border-slate-500 bg-slate-950 px-2 text-sm text-slate-100 [color-scheme:dark] forced-colors:[color-scheme:light_dark] placeholder:text-slate-400 enabled:hover:border-slate-400 enabled:active:border-slate-300 aria-[invalid=true]:border-rose-400 aria-[invalid=true]:enabled:hover:border-rose-300 aria-[invalid=true]:enabled:active:border-rose-200 disabled:cursor-not-allowed disabled:opacity-50 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400 motion-safe:transition-colors motion-safe:duration-150 motion-safe:ease-out";

/**
 * Secondary text button, 26px tall: slate-100 text on slate-800, stepping to slate-700 on hover
 * and slate-600 when pressed (at least 6.9:1 in every state). Hover and press also lighten the
 * border to slate-500, so the edge stays visible once the fill reaches slate-700.
 */
export const SECONDARY_BUTTON_CLASS =
  "m-0 cursor-pointer rounded border border-slate-700 bg-slate-800 px-3 py-1 text-xs font-medium text-slate-100 hover:border-slate-500 hover:bg-slate-700 active:border-slate-500 active:bg-slate-600 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400 motion-safe:transition-colors motion-safe:duration-150 motion-safe:ease-out";

/** Unpressed toggle (`aria-pressed="false"`): identical to {@link SECONDARY_BUTTON_CLASS}. */
export const TOGGLE_BUTTON_CLASS = SECONDARY_BUTTON_CLASS;

/**
 * Pressed toggle button (`aria-pressed="true"`): sky-100 text on sky-900, stepping to sky-800 on
 * hover and sky-700 when pressed.
 *
 * The selection does not rely on hue alone: a 1px sky-400 border plus a 1px inset sky-400 ring
 * give a 2px bright edge, against the unpressed toggle's 1px slate-700 edge. Weight, padding and
 * width are unchanged, so toggling never shifts the layout.
 *
 * Under forced colours an unpressed toggle keeps `forced-color-adjust: auto` and renders
 * ButtonFace with ButtonText. A pressed toggle opts out (`forced-color-adjust: none`), because
 * with `auto` Chromium paints a Canvas-coloured backplate behind the label, which hides
 * HighlightText on a Highlight fill. Having opted out, it sets only system colours itself, in
 * every pointer state: a Highlight fill and border, a HighlightText label and inner ring, and a
 * Highlight focus outline, the colour the browser forces on the other controls' outlines.
 */
export const TOGGLE_BUTTON_PRESSED_CLASS =
  "m-0 cursor-pointer rounded border border-sky-400 bg-sky-900 px-3 py-1 text-xs font-medium text-sky-100 ring-1 ring-inset ring-sky-400 hover:bg-sky-800 active:bg-sky-700 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400 forced-colors:forced-color-adjust-none forced-colors:border-[Highlight] forced-colors:bg-[Highlight] forced-colors:text-[HighlightText] forced-colors:ring-[color:HighlightText] forced-colors:hover:bg-[Highlight] forced-colors:active:bg-[Highlight] forced-colors:focus-visible:outline-[color:Highlight] motion-safe:transition-colors motion-safe:duration-150 motion-safe:ease-out";

/**
 * Removable chip: a pill of 12px medium slate-100 text on slate-800. It is 26px tall with its
 * {@link CHIP_REMOVE_BUTTON_CLASS} button, which sits flush against the pill's end edge.
 */
export const CHIP_CLASS =
  "inline-flex items-center gap-1 rounded-full border border-slate-700 bg-slate-800 ps-2.5 text-xs font-medium text-slate-100";

/**
 * A chip's 24px round remove button: a slate-400 icon that lightens to slate-100 on a slate-700
 * hover and slate-600 pressed background. Both `disabled` and `aria-disabled="true"` dim it and
 * stop pointer events. The focus outline is drawn 2px inside the circle (`outline-offset-[-2px]`,
 * an arbitrary value that Tailwind 3 and Tailwind 4 both generate), so it never covers the chip's
 * text.
 */
export const CHIP_REMOVE_BUTTON_CLASS =
  "m-0 inline-flex h-6 w-6 shrink-0 cursor-pointer items-center justify-center rounded-full border-0 bg-transparent p-0 text-slate-400 hover:bg-slate-700 hover:text-slate-100 active:bg-slate-600 active:text-slate-100 disabled:pointer-events-none disabled:opacity-50 aria-disabled:pointer-events-none aria-disabled:opacity-50 focus-visible:outline-2 focus-visible:outline-offset-[-2px] focus-visible:outline-sky-400 motion-safe:transition-colors motion-safe:duration-150 motion-safe:ease-out";

/**
 * Ticker button, 24px tall: a 12px medium monospace sky-300 ticker that reads as text, with a
 * slate-800 hover and slate-700 pressed background under sky-200 text. 12px medium is the type of
 * the terminal tables' row headers, where the screener's ticker buttons sit, so a ticker reads the
 * same in a table and in a list. Its 4px inline padding frames the hover background, and its 64px
 * minimum width fits the usual 3–5 letter NSE ticker, so tickers stacked in a column start and end
 * on the same lines. It resets the margin, border and background, so it looks the same in a host
 * without a CSS reset.
 */
export const TICKER_BUTTON_CLASS =
  "m-0 inline-flex min-h-6 min-w-16 cursor-pointer items-center justify-start rounded border-0 bg-transparent px-1 font-mono text-xs font-medium text-sky-300 hover:bg-slate-800 hover:text-sky-200 active:bg-slate-700 active:text-sky-200 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400 motion-safe:transition-colors motion-safe:duration-150 motion-safe:ease-out";

/**
 * {@link TICKER_BUTTON_CLASS} for the selected ticker, whose row or list item is already tinted
 * slate-800: hover and press each step one shade further, to slate-700 (sky-200 at about 7.8:1)
 * and slate-600 (sky-200 at about 5.7:1), so both stay visible on the tint.
 */
export const TICKER_BUTTON_SELECTED_CLASS =
  "m-0 inline-flex min-h-6 min-w-16 cursor-pointer items-center justify-start rounded border-0 bg-transparent px-1 font-mono text-xs font-medium text-sky-300 hover:bg-slate-700 hover:text-sky-200 active:bg-slate-600 active:text-sky-200 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400 motion-safe:transition-colors motion-safe:duration-150 motion-safe:ease-out";
