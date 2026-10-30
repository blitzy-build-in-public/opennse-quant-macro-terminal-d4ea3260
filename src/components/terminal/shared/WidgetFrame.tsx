"use client";

import type { ReactElement, ReactNode } from "react";

import type { TerminalApiError } from "../lib/apiClient";
import type { WidgetState } from "../types";
import { SECONDARY_BUTTON_CLASS } from "./controlStyles";

/**
 * Default copy of the empty state, shown when a widget's `rows`, `items` or `series` is empty. It
 * points the operator at the fallback seeder and the ingestion jobs, the terminal's only data
 * sources.
 */
export const EMPTY_MESSAGE = "No data yet — run the seeder or an ingestion job";

/** Props of {@link WidgetFrame}, the card shell every fetching widget renders as its root. */
export interface WidgetFrameProps {
  /**
   * The root's `data-testid`, by convention `terminal-<widget-kebab-name>`, for example
   * `"terminal-yield-curve-chart"`.
   */
  testId: string;
  /**
   * Heading text of the card, also used as the root's accessible name, in the Retry button's
   * accessible name (`"Retry Yield curve"`) and in the status region's announcements, such as
   * `"Yield curve loaded"`.
   */
  title: string;
  /** Selects the body shown, and is written verbatim to the root's `data-state` attribute. */
  state: WidgetState;
  /**
   * The failed request's error. Only its `message` is shown, and only when `state` is `"error"`;
   * its `fieldErrors` belong beside the matching inputs, through {@link FieldErrorText}.
   */
  error?: TerminalApiError | null;
  /**
   * Called by the error state's Retry button, which is rendered only when this is provided.
   * Fetching widgets pass `useTerminalQuery(...).refetch`. Activating Retry first moves focus to
   * the card's root, which is focusable by script but not in the tab order, so focus stays in the
   * card when the retry replaces the error body.
   */
  onRetry?: () => void;
  /**
   * Replaces {@link EMPTY_MESSAGE} in the empty state, for example
   * `"Select a counter or sector to see its valuation bands"`. Either message is announced after
   * the card's `title` through the status region.
   */
  emptyMessage?: string;
  /**
   * Tailwind height classes for the loading skeleton. The skeleton replaces only the body, so
   * these size it to the loaded widget's body, and the card keeps its height when the data
   * arrives. They follow the loaded body's height across viewport widths: a base height for the
   * narrowest screens plus one `min-[<N>px]:` variant at each width where the body reflows, such
   * as `"h-[21.5rem] min-[457px]:h-[20.5rem] min-[487px]:h-[19.25rem]"`.
   * - Thresholds are integer `px`: Tailwind 3.2+ drops an arbitrary `min-*` variant whose unit
   *   differs from its `px` screens, and Tailwind 4 orders arbitrary breakpoints by their value.
   *   Named `sm:`, `md:` or `lg:` variants are not mixed in, so the cascade follows the
   *   thresholds in both versions.
   * - Heights are arbitrary `rem` values (the measured px / 16), which both versions generate,
   *   rather than Tailwind 4's computed spacing steps beyond Tailwind 3's fixed scale.
   * - A Tailwind 3 host without arbitrary `min-*` variants falls back to the base height.
   *
   * Default `"h-48"`. Every class must be complete and written literally in the calling widget's
   * source: Tailwind's scanner (and a host's `@source` or `content` glob) only generates classes
   * it can read literally, never computed ones.
   */
  skeletonClassName?: string;
  /** Extra classes merged onto the root element. */
  className?: string;
  /** Controls placed at the right of the header, such as toggles. */
  actions?: ReactNode;
  /**
   * Content rendered between the header and the body in every state, so filters and presets stay
   * usable while loading, when empty and after an error.
   */
  controls?: ReactNode;
  /** The widget's content, rendered only when `state` is `"ready"`. */
  children?: ReactNode;
}

/** Props of {@link FieldErrorText}. */
export interface FieldErrorTextProps {
  /**
   * The API's snake_case field name, used verbatim in `data-testid="field-error-<field>"`, for
   * example `"primary_proceeds_kes"`.
   */
  field: string;
  /** The message to show. Nothing is rendered when it is `undefined`, `null` or empty. */
  message?: string | null;
  /** Element id, so the input can reference the message through `aria-describedby`. */
  id?: string;
}

// Every class below is a complete static string, never assembled from fragments, so Tailwind's
// source scan and a host's `@source` directive or `content` glob find each one. The root sets its
// own surface and font family, so the widget never depends on a host's body styles or fonts. The
// root is the focus target Retry hands focus to, so it carries the same outline ring as Retry.
const ROOT_CLASS =
  "rounded-lg border border-slate-800 bg-slate-900 p-4 font-sans text-slate-100 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400";
const HEADER_CLASS = "mb-3 flex items-center justify-between gap-2";
const TITLE_CLASS = "text-sm font-semibold text-slate-200";
const ACTIONS_CLASS = "flex items-center gap-2";
const CONTROLS_CLASS = "mb-3";
// The pulse runs only when the user has not asked for reduced motion.
const SKELETON_CLASS = "motion-safe:animate-pulse rounded bg-slate-800";
const DEFAULT_SKELETON_HEIGHT = "h-48";
// Hides the status region in every state but `empty`, and the title prefix of the empty copy. The
// hidden region is taken out of the flow, so it adds no gap to a root a host lays out as flex.
const SR_ONLY_CLASS = "sr-only";
const EMPTY_CLASS = "text-sm text-slate-400";
const ERROR_CLASS = "rounded border border-rose-900 bg-slate-950 p-3 text-sm text-rose-300";
// Retry is the shared secondary button of `controlStyles`, so it matches the other widgets'
// secondary buttons: a pointer cursor, and hover and press states that lighten the border, with
// press one shade beyond hover. Its focus indicator is an outline rather than a box-shadow ring:
// forced-colors mode drops box shadows but keeps outlines, and Tailwind 4's `outline-none` would
// remove the outline entirely. `mt-3` overrides the shared class's `m-0` reset, because Tailwind
// emits the `margin-top` longhand after the `margin` shorthand.
/* BLITZY [A11Y]: Retry keeps the compact size of the card's other text controls (about 26px tall,
   above WCAG 2.2 AA's 24px target minimum, below the 44px guideline). Flagged for designer review. */
const RETRY_CLASS = `${SECONDARY_BUTTON_CLASS} mt-3`;
// `min-w-0` lets charts and tables shrink inside a host's grid or flex layout.
const READY_CLASS = "min-w-0";
const FIELD_ERROR_CLASS = "mt-1 text-xs text-rose-400";

const ERROR_FALLBACK_MESSAGE = "Something went wrong";

function joinClasses(...parts: Array<string | false | null | undefined>): string {
  return parts.filter(Boolean).join(" ");
}

// Content of the card's one status region, which stays mounted in every state so a screen reader
// announces each change politely. Loading and completion are short hidden phrases. The empty copy
// is the visible message itself, prefixed by the hidden title, so it is read once. An error leaves
// the region empty, because the error body's alert announces it.
function renderStatus({ state, title, emptyMessage }: WidgetFrameProps): ReactNode {
  switch (state) {
    case "loading":
      return "Loading…";
    case "empty":
      return (
        <>
          <span className={SR_ONLY_CLASS}>{title + ": "}</span>
          <p className={EMPTY_CLASS}>{emptyMessage ?? EMPTY_MESSAGE}</p>
        </>
      );
    case "error":
      return null;
    case "ready":
      return title + " loaded";
    default: {
      const unreachable: never = state;
      return unreachable;
    }
  }
}

function renderBody({
  state,
  title,
  error,
  onRetry,
  skeletonClassName,
  children,
}: WidgetFrameProps): ReactNode {
  switch (state) {
    case "loading":
      // Keyed apart from the ready body, so React mounts the loaded content in new nodes rather
      // than reusing the pulsing skeleton as its first element: when a reused node's animation
      // stops and its display type changes at once, Chrome records a layout shift from the
      // viewport's corner, although nothing moves.
      return (
        <div key="loading" aria-busy="true">
          <div
            aria-hidden="true"
            className={joinClasses(SKELETON_CLASS, skeletonClassName || DEFAULT_SKELETON_HEIGHT)}
          />
        </div>
      );
    case "empty":
      // The status region shows the empty copy, so the body adds nothing.
      return null;
    case "error":
      // Only the message is shown: field errors are placed beside their inputs by the widget, and
      // the code is for programs, not readers.
      return (
        <div role="alert" className={ERROR_CLASS}>
          <p>{error?.message || ERROR_FALLBACK_MESSAGE}</p>
          {onRetry ? (
            <button
              type="button"
              data-testid="widget-retry"
              // Names the card, so several failed widgets expose distinct Retry buttons; the
              // visible "Retry" starts the name, as WCAG's label-in-name criterion asks.
              aria-label={"Retry " + title}
              onClick={(event) => {
                // The retry replaces this error body, focused button included. Focus moves to the
                // card root first, so keyboard users keep their place in the card through the
                // loading, ready or repeated error state that follows.
                event.currentTarget.closest("section")?.focus({ preventScroll: true });
                onRetry();
              }}
              className={RETRY_CLASS}
            >
              Retry
            </button>
          ) : null}
        </div>
      );
    case "ready":
      return <div key="ready" className={READY_CLASS}>{children}</div>;
    default: {
      const unreachable: never = state;
      return unreachable;
    }
  }
}

/**
 * Card shell of a terminal widget: a titled `<section>` that carries the widget's
 * `data-testid` and `data-state`, optional header actions and always-visible controls, one
 * polite status region (`role="status"`) that stays mounted in every state, and a body chosen by
 * `state`:
 * - `loading`: a skeleton in place of the body, pulsing unless the user prefers reduced motion
 *   and sized by `skeletonClassName` to the loaded body's height so the card does not jump when
 *   the data arrives, marked `aria-busy`, while the status region announces "Loading…";
 * - `empty`: `emptyMessage`, or {@link EMPTY_MESSAGE}, shown inside the status region after the
 *   title, which only screen readers get, so the empty result is announced;
 * - `error`: an alert with the error's `message` and, when `onRetry` is given, a Retry button
 *   named "Retry" followed by the title;
 * - `ready`: `children`, while the status region announces the title followed by "loaded".
 *   Charts and tables stay outside the region, so only that phrase is announced.
 *
 * The root is focusable by script (`tabIndex={-1}`) but not in the tab order. Activating Retry
 * moves focus to it before retrying, so keyboard focus stays in the card when the error body is
 * replaced; the root then shows the same outline ring as Retry under `:focus-visible`.
 *
 * The output depends only on the props, with no hooks or effects, so server rendering and
 * hydration agree in any React 18.2+ host.
 */
export function WidgetFrame(props: WidgetFrameProps): ReactElement {
  const { testId, title, state, className, actions, controls } = props;
  return (
    <section
      data-testid={testId}
      data-state={state}
      aria-label={title}
      tabIndex={-1}
      className={joinClasses(ROOT_CLASS, className)}
    >
      <header className={HEADER_CLASS}>
        <h2 className={TITLE_CLASS}>{title}</h2>
        {actions ? <div className={ACTIONS_CLASS}>{actions}</div> : null}
      </header>
      {controls ? <div className={CONTROLS_CLASS}>{controls}</div> : null}
      {/* Unkeyed and always at this position, so React keeps the same live node across states. */}
      <div role="status" className={state === "empty" ? undefined : SR_ONLY_CLASS}>
        {renderStatus(props)}
      </div>
      {renderBody(props)}
    </section>
  );
}

/**
 * Error message for one request field, rendered beneath the input it names, as
 * `data-testid="field-error-<field>"`. Widgets pass the entry for that field from
 * `fieldErrorMap(error)` in `lib/apiClient.ts`. Returns `null` when there is no message.
 */
export function FieldErrorText({ field, message, id }: FieldErrorTextProps): ReactElement | null {
  if (!message) {
    return null;
  }
  return (
    <p id={id} data-testid={"field-error-" + field} role="alert" className={FIELD_ERROR_CLASS}>
      {message}
    </p>
  );
}
