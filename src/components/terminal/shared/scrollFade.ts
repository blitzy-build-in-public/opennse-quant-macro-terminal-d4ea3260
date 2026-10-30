/**
 * Overflow cue of the widgets' horizontal table scrollers: a positioned frame, an edge fade over
 * the scroller's inline end, and {@link useOverflowEnd}, which reports whether content lies
 * beyond that edge. Overlay scrollbars give no such cue, so without it a table cut off at the
 * card's edge reads as complete.
 *
 * A widget wraps its scroller in a {@link SCROLL_FRAME_CLASS} element, gives the scroller the
 * hook's ref and a `data-overflow-end="true|false"` attribute, and renders an `aria-hidden`
 * sibling after the scroller, inside the frame, with {@link SCROLL_FADE_CLASS} while the flag is
 * set and {@link SCROLL_FADE_HIDDEN_CLASS} otherwise.
 *
 * Every class token is written out complete and literal, never built from fragments, so Tailwind
 * 3's `content` glob and Tailwind 4's `@source` scan both find every class. Only the default
 * palette is used. The hook uses only `useState`, `useRef` and `useCallback`, so it runs on React
 * 18.2 and later. This module is internal to the widget folder and not re-exported from its
 * `index.ts`.
 */
import { useCallback, useRef, useState } from "react";

// Each scroller sits in a positioned frame with an edge fade over its inline end, shown while
// columns lie beyond that edge (see useOverflowEnd), since overlay scrollbars give no cue. The
// fade blends the card surface (slate-900) in over 2rem and ignores the pointer, so the cells
// beneath stay clickable. It stays inside the scroller's box, clear of its offset focus outline,
// and is fully transparent once the end is reached or when the table fits, so nothing stays
// covered. `bg-gradient-to-*` is the gradient direction Tailwind 3 and Tailwind 4 both generate.

/** The positioned frame around a scroller and its fade. It adds no spacing or size of its own. */
export const SCROLL_FRAME_CLASS = "relative";

/** The edge fade while columns lie beyond the scroller's inline end: fully opaque. */
export const SCROLL_FADE_CLASS =
  "pointer-events-none absolute inset-y-0 end-0 w-8 bg-gradient-to-l from-slate-900 to-transparent opacity-100 rtl:bg-gradient-to-r motion-safe:transition-opacity motion-safe:duration-150 motion-safe:ease-out";

/** The edge fade once the end is reached or the table fits: fully transparent. */
export const SCROLL_FADE_HIDDEN_CLASS =
  "pointer-events-none absolute inset-y-0 end-0 w-8 bg-gradient-to-l from-slate-900 to-transparent opacity-0 rtl:bg-gradient-to-r motion-safe:transition-opacity motion-safe:duration-150 motion-safe:ease-out";

/** Content hidden past a scroller's inline end by at most this many pixels is rounding. */
const OVERFLOW_TOLERANCE_PX = 1;

/**
 * Whether a horizontal scroller has content hidden beyond its inline-end edge, which drives the
 * edge fade of {@link SCROLL_FADE_CLASS}. Returns a callback ref for the scroller, so a scroller
 * that mounts after the widget, such as a table rendered only once its data arrives, is tracked
 * too, and the flag.
 *
 * The flag starts false, so server and client markup match and no fade shows before layout. It is
 * set only from the scroller's `scroll` events and from a `ResizeObserver` on the scroller and its
 * table, whose first observation arrives once they are laid out and whose later ones follow a
 * resize or a table that grows or shrinks with its rows. `Math.abs(scrollLeft)` keeps the end
 * right in right-to-left layouts, where `scrollLeft` runs from 0 to negative values. Listeners are
 * removed, and the flag cleared, when the scroller unmounts. Without `ResizeObserver` (non-browser
 * test environments) only scrolling updates the flag.
 */
export function useOverflowEnd(): [(node: HTMLElement | null) => void, boolean] {
  const [overflowEnd, setOverflowEnd] = useState(false);
  const detach = useRef<(() => void) | null>(null);
  const ref = useCallback((node: HTMLElement | null) => {
    detach.current?.();
    detach.current = null;
    if (node === null) {
      setOverflowEnd(false);
      return;
    }
    const update = (): void => {
      const hidden = node.scrollWidth - node.clientWidth - Math.abs(node.scrollLeft);
      setOverflowEnd(hidden > OVERFLOW_TOLERANCE_PX);
    };
    node.addEventListener("scroll", update, { passive: true });
    const observer = typeof ResizeObserver === "undefined" ? null : new ResizeObserver(update);
    if (observer !== null) {
      observer.observe(node);
      for (const child of Array.from(node.children)) {
        observer.observe(child);
      }
    }
    detach.current = () => {
      node.removeEventListener("scroll", update);
      observer?.disconnect();
    };
  }, []);
  return [ref, overflowEnd];
}
