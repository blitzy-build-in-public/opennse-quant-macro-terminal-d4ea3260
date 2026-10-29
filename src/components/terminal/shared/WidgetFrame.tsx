"use client";

import type { ReactElement, ReactNode } from "react";

import type { TerminalApiError } from "../lib/apiClient";
import type { WidgetState } from "../types";

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
  /** Heading text of the card, also used as the root's accessible name. */
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
   * Fetching widgets pass `useTerminalQuery(...).refetch`.
   */
  onRetry?: () => void;
  /**
   * Replaces {@link EMPTY_MESSAGE} in the empty state, for example
   * `"Select a counter or sector to see its valuation bands"`.
   */
  emptyMessage?: string;
  /**
   * One complete Tailwind height class for the loading skeleton, such as `"h-64"`, so the
   * skeleton matches the size of the loaded widget. Default `"h-48"`. It must be a literal class
   * written in the calling widget's source: Tailwind's scanner (and a host's `@source` or
   * `content` glob) only generates classes it can read literally, never computed ones.
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
// own surface and font family, so the widget never depends on a host's body styles or fonts.
const ROOT_CLASS = "rounded-lg border border-slate-800 bg-slate-900 p-4 font-sans text-slate-100";
const HEADER_CLASS = "mb-3 flex items-center justify-between gap-2";
const TITLE_CLASS = "text-sm font-semibold text-slate-200";
const ACTIONS_CLASS = "flex items-center gap-2";
const CONTROLS_CLASS = "mb-3";
// The pulse runs only when the user has not asked for reduced motion.
const SKELETON_CLASS = "motion-safe:animate-pulse rounded bg-slate-800";
const DEFAULT_SKELETON_HEIGHT = "h-48";
const EMPTY_CLASS = "text-sm text-slate-400";
const ERROR_CLASS = "rounded border border-rose-900 bg-slate-950 p-3 text-sm text-rose-300";
// The focus indicator is an outline rather than a box-shadow ring: forced-colors mode drops box
// shadows but keeps outlines, and Tailwind 4's `outline-none` would remove the outline entirely.
/* BLITZY [A11Y]: Retry keeps the compact size of the card's other text controls (about 26px tall,
   above WCAG 2.2 AA's 24px target minimum, below the 44px guideline). Flagged for designer review. */
const RETRY_CLASS =
  "mt-3 rounded border border-slate-700 bg-slate-800 px-3 py-1 text-xs font-medium text-slate-100 hover:bg-slate-700 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400 motion-safe:transition-colors motion-safe:duration-150 motion-safe:ease-out";
// `min-w-0` lets charts and tables shrink inside a host's grid or flex layout.
const READY_CLASS = "min-w-0";
const FIELD_ERROR_CLASS = "mt-1 text-xs text-rose-400";

const ERROR_FALLBACK_MESSAGE = "Something went wrong";

function joinClasses(...parts: Array<string | false | null | undefined>): string {
  return parts.filter(Boolean).join(" ");
}

function renderBody({
  state,
  error,
  onRetry,
  emptyMessage,
  skeletonClassName,
  children,
}: WidgetFrameProps): ReactNode {
  switch (state) {
    case "loading":
      return (
        <div role="status">
          <div
            aria-hidden="true"
            className={joinClasses(SKELETON_CLASS, skeletonClassName || DEFAULT_SKELETON_HEIGHT)}
          />
          <span className="sr-only">Loading…</span>
        </div>
      );
    case "empty":
      return <p className={EMPTY_CLASS}>{emptyMessage ?? EMPTY_MESSAGE}</p>;
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
              onClick={onRetry}
              className={RETRY_CLASS}
            >
              Retry
            </button>
          ) : null}
        </div>
      );
    case "ready":
      return <div className={READY_CLASS}>{children}</div>;
    default: {
      const unreachable: never = state;
      return unreachable;
    }
  }
}

/**
 * Card shell of a terminal widget: a titled `<section>` that carries the widget's
 * `data-testid` and `data-state`, optional header actions and always-visible controls, and a body
 * chosen by `state`:
 * - `loading`: a skeleton sized by `skeletonClassName`, announced as "Loading…";
 * - `empty`: `emptyMessage`, or {@link EMPTY_MESSAGE};
 * - `error`: the error's `message` and, when `onRetry` is given, a Retry button;
 * - `ready`: `children`.
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
      aria-busy={state === "loading"}
      aria-label={title}
      className={joinClasses(ROOT_CLASS, className)}
    >
      <header className={HEADER_CLASS}>
        <h2 className={TITLE_CLASS}>{title}</h2>
        {actions ? <div className={ACTIONS_CLASS}>{actions}</div> : null}
      </header>
      {controls ? <div className={CONTROLS_CLASS}>{controls}</div> : null}
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
