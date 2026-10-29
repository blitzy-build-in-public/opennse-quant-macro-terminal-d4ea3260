"use client";

import { useEffect, useId, useRef, useState } from "react";
import type { FormEvent, KeyboardEvent, MouseEvent, ReactElement } from "react";

import {
  apiSend,
  buildUrl,
  fieldErrorMap,
  isAbortError,
  pathSegment,
  TerminalApiError,
} from "./lib/apiClient";
import type { RequestOptions } from "./lib/apiClient";
import { formatDate } from "./lib/format";
import { useTerminalQuery } from "./lib/useTerminalQuery";
import { FieldErrorText, WidgetFrame } from "./shared/WidgetFrame";
import type { TerminalWidgetProps, Watchlist, WatchlistCreate } from "./types";

/** Props of {@link WatchlistManager}. */
export interface WatchlistManagerProps extends TerminalWidgetProps {
  /**
   * Called after every successful create, delete, ticker addition or ticker removal, with the
   * caller's full updated list of watchlists as the server returned it. Not called for the
   * initial load, for a failed request, or for a request abandoned because the widget unmounted
   * or its `apiBaseUrl` changed, so a host can keep, for example, a screener's watchlist selector
   * in step without refetching.
   */
  onWatchlistsChange?: (lists: Watchlist[]) => void;
}

/** Longest watchlist name the API accepts, counted in characters after trimming. */
const MAX_NAME_LENGTH = 100;
/** Most tickers `POST /watchlists` accepts. */
const MAX_CREATE_TICKERS = 100;
/** Longest ticker the API accepts, matching `terminal_equities.ticker`. */
const MAX_TICKER_LENGTH = 16;
/** Separators of the creation form's ticker list: commas and any whitespace. */
const TICKER_SEPARATORS = /[\s,]+/;

/**
 * Key of the general error line in the `errors` map. Field keys are API field names or
 * `ticker-<id>`, so this key, which starts with an underscore, never collides with one.
 */
const GENERAL_ERROR_KEY = "_general";

const NOT_FOUND_MESSAGE = "That watchlist no longer exists. The watchlists have been reloaded.";
const FALLBACK_ERROR_MESSAGE = "The request failed. Please try again.";

// Every class below is a complete static string, never assembled from fragments, so Tailwind's
// source scan and a host's `@source` directive or `content` glob find each one. Hover styles use
// Tailwind's `hover:` variant, which only applies on devices that can hover, and transitions run
// only when the user has not asked for reduced motion.
const BODY_CLASS = "flex flex-col gap-4";
const FORM_CLASS = "flex flex-col gap-3";
const FIELDS_CLASS = "flex flex-wrap gap-3";
const FIELD_CLASS = "flex min-w-0 flex-1 basis-48 flex-col gap-1";
const ADD_FIELD_CLASS = "flex min-w-0 flex-1 basis-32 flex-col gap-1";
const LABEL_CLASS = "text-xs font-medium text-slate-300";
// Input borders use slate-500, which keeps the 3:1 non-text contrast against the card surface.
// The error border uses the `aria-[invalid=true]:` variant because Tailwind 3 and Tailwind 4
// hosts both generate it; Tailwind 3 has no named `aria-invalid:` variant.
const INPUT_CLASS =
  "w-full min-w-0 rounded border border-slate-500 bg-slate-950 px-2 py-1 text-sm text-slate-100 placeholder:text-slate-400 aria-[invalid=true]:border-rose-400 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400";
/* BLITZY [A11Y]: buttons keep the compact size of the card's other text controls (about 26px
   tall, the chip remove buttons 24px square), meeting WCAG 2.2 AA's 24px target minimum but below
   the 44px guideline. Flagged for designer review. */
// While a request is in flight buttons are `aria-disabled` rather than `disabled`: a button that
// becomes `disabled` loses keyboard focus to the page body, whereas `aria-disabled` keeps it in
// place, and the handlers refuse to start a second request.
// Every button has a pressed `active:` state one step past its hover, so a press also shows on
// touch screens, which apply `:active` without `:hover`. Filled buttons step their background
// (slate 800 → 700 → 600, sky and rose 700 → 800 → 900) under slate-100 text, at least 5.3:1 in
// every state. The delete and chip remove buttons deepen their hover background and set their own
// light text (rose-200, slate-100), keeping 4.5:1 for text and 3:1 for the remove icon.
const PRIMARY_BUTTON_CLASS =
  "self-start rounded border border-sky-600 bg-sky-700 px-3 py-1 text-xs font-medium text-slate-100 hover:bg-sky-800 active:bg-sky-900 aria-disabled:pointer-events-none aria-disabled:opacity-50 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400 motion-safe:transition-colors motion-safe:duration-150 motion-safe:ease-out";
const SECONDARY_BUTTON_CLASS =
  "rounded border border-slate-700 bg-slate-800 px-3 py-1 text-xs font-medium text-slate-100 hover:bg-slate-700 active:bg-slate-600 aria-disabled:pointer-events-none aria-disabled:opacity-50 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400 motion-safe:transition-colors motion-safe:duration-150 motion-safe:ease-out";
const DELETE_BUTTON_CLASS =
  "rounded border border-rose-800 bg-slate-900 px-3 py-1 text-xs font-medium text-rose-300 hover:bg-rose-950 active:bg-rose-900 active:text-rose-200 aria-disabled:pointer-events-none aria-disabled:opacity-50 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400 motion-safe:transition-colors motion-safe:duration-150 motion-safe:ease-out";
const CONFIRM_BUTTON_CLASS =
  "rounded border border-rose-600 bg-rose-700 px-3 py-1 text-xs font-medium text-slate-100 hover:bg-rose-800 active:bg-rose-900 aria-disabled:pointer-events-none aria-disabled:opacity-50 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400 motion-safe:transition-colors motion-safe:duration-150 motion-safe:ease-out";
const GENERAL_ERROR_CLASS =
  "rounded border border-rose-900 bg-slate-950 px-3 py-2 text-xs text-rose-300";
const EMPTY_LISTS_CLASS = "text-sm text-slate-400";
const LISTS_CLASS = "flex flex-col gap-3";
const LIST_ITEM_CLASS = "rounded border border-slate-800 bg-slate-950 p-3";
const LIST_HEADER_CLASS = "flex flex-wrap items-start justify-between gap-2";
const LIST_TITLE_CLASS = "min-w-0";
const LIST_NAME_CLASS = "text-sm font-medium text-slate-100 break-words";
const LIST_META_CLASS = "text-xs text-slate-400";
const LIST_ACTIONS_CLASS = "flex items-center gap-2";
const CHIPS_CLASS = "mt-2 flex flex-wrap gap-1.5";
const NO_TICKERS_CLASS = "mt-2 text-xs text-slate-400";
const CHIP_CLASS =
  "inline-flex items-center gap-1 rounded-full border border-slate-700 bg-slate-800 ps-2.5 text-xs font-medium text-slate-100";
const CHIP_REMOVE_CLASS =
  "inline-flex size-6 items-center justify-center rounded-full text-slate-400 hover:bg-slate-700 hover:text-slate-100 active:bg-slate-600 active:text-slate-100 aria-disabled:pointer-events-none aria-disabled:opacity-50 focus-visible:outline-2 focus-visible:outline-offset-1 focus-visible:outline-sky-400 motion-safe:transition-colors motion-safe:duration-150 motion-safe:ease-out";
const CHIP_ICON_CLASS = "size-3";
const ADD_FORM_CLASS = "mt-3";
const ADD_ROW_CLASS = "flex flex-wrap items-end gap-2";
const STATUS_CLASS = "sr-only";

/** The mutation a request performs, which decides where its errors are shown. */
type Mutation =
  | { kind: "create" }
  | { kind: "delete"; list: Watchlist }
  | { kind: "add"; list: Watchlist }
  | { kind: "remove"; list: Watchlist };

/**
 * The lists the widget shows once a mutation has succeeded, together with the fetched payload
 * they were derived from. They apply only while the query still holds that payload, so a later
 * refetch, after a Retry or a vanished watchlist, replaces them without an effect.
 */
interface LocalLists {
  base: Watchlist[] | undefined;
  lists: Watchlist[];
}

/** Result of parsing the creation form's ticker text. */
type ParsedTickers = { ok: true; tickers: string[] } | { ok: false; message: string };

/** Key of the add-ticker error of one watchlist in the `errors` map. */
function tickerErrorKey(watchlistId: string): string {
  return "ticker-" + watchlistId;
}

/** Length in characters (code points), as the API counts it, rather than UTF-16 units. */
function characterCount(text: string): number {
  return Array.from(text).length;
}

/**
 * Parse comma- or whitespace-separated tickers: upper-cased, de-duplicated in input order, each
 * 1–16 characters, at most 100. Empty text gives no tickers.
 */
function parseTickers(text: string): ParsedTickers {
  const tickers: string[] = [];
  const seen = new Set<string>();
  for (const part of text.split(TICKER_SEPARATORS)) {
    const ticker = part.toUpperCase();
    if (ticker === "" || seen.has(ticker)) continue;
    if (characterCount(ticker) > MAX_TICKER_LENGTH) {
      return { ok: false, message: `${ticker} is longer than ${MAX_TICKER_LENGTH} characters.` };
    }
    seen.add(ticker);
    tickers.push(ticker);
  }
  if (tickers.length > MAX_CREATE_TICKERS) {
    return { ok: false, message: `List at most ${MAX_CREATE_TICKERS} tickers.` };
  }
  return { ok: true, tickers };
}

/** The validation message of a watchlist name, or `null` when it is acceptable. */
function nameProblem(trimmedName: string): string | null {
  const length = characterCount(trimmedName);
  if (length === 0) return "Enter a name for the watchlist.";
  if (length > MAX_NAME_LENGTH) return `Use at most ${MAX_NAME_LENGTH} characters.`;
  return null;
}

/** The validation message of a ticker to add, or `null` when it is acceptable. */
function tickerProblem(ticker: string): string | null {
  if (ticker === "") return "Enter a ticker to add.";
  if (TICKER_SEPARATORS.test(ticker)) return "Enter one ticker at a time.";
  if (characterCount(ticker) > MAX_TICKER_LENGTH) {
    return `Use at most ${MAX_TICKER_LENGTH} characters.`;
  }
  return null;
}

/** Order watchlists by name, as `GET /watchlists` does. Runs in event handlers only. */
function sortByName(lists: Watchlist[]): Watchlist[] {
  return [...lists].sort((a, b) => {
    const byLocale = a.name.localeCompare(b.name, "en");
    if (byLocale !== 0) return byLocale;
    if (a.name === b.name) return 0;
    return a.name < b.name ? -1 : 1;
  });
}

/** Replace the watchlist with the server's copy, or add it when it is not shown yet. */
function withList(lists: Watchlist[], updated: Watchlist): Watchlist[] {
  if (!lists.some((list) => list.id === updated.id)) {
    return sortByName([...lists, updated]);
  }
  return lists.map((list) => (list.id === updated.id ? updated : list));
}

/** Where an error attached to an API field is shown, or `null` for the general error line. */
function keyForField(field: string, mutation: Mutation): string | null {
  if (mutation.kind === "create") {
    if (field === "name") return "name";
    // FastAPI's standard 422 names a failing list element by its index, the last `loc` entry.
    if (field === "tickers" || /^\d+$/.test(field)) return "tickers";
    return null;
  }
  if (mutation.kind === "add" && field === "ticker") return tickerErrorKey(mutation.list.id);
  return null;
}

/** Where a coded error without a field is shown, or `null` for the general error line. */
function keyForCode(code: string, mutation: Mutation): string | null {
  if (code === "watchlist_name_taken" && mutation.kind === "create") return "name";
  if (code === "unknown_ticker") {
    if (mutation.kind === "create") return "tickers";
    if (mutation.kind === "add") return tickerErrorKey(mutation.list.id);
  }
  return null;
}

/** Map a failed mutation's error to the messages shown beside its inputs or on the error line. */
function errorsFor(error: TerminalApiError, mutation: Mutation): Record<string, string> {
  const result: Record<string, string> = {};
  const byField = Object.entries(fieldErrorMap(error));
  if (byField.length === 0) {
    result[keyForCode(error.code, mutation) ?? GENERAL_ERROR_KEY] =
      error.message || FALLBACK_ERROR_MESSAGE;
    return result;
  }
  for (const [field, message] of byField) {
    const key = keyForField(field, mutation) ?? GENERAL_ERROR_KEY;
    if (result[key] === undefined) result[key] = message;
  }
  return result;
}

/** A copy of `record` without `key`. */
function withoutKey(record: Record<string, string>, key: string): Record<string, string> {
  const next = { ...record };
  delete next[key];
  return next;
}

/**
 * Whether `element` holds keyboard focus. Focus is moved off a control that a mutation removes
 * only for keyboard users: moving it for a tap would open a touch keyboard nobody asked for.
 */
function hasKeyboardFocus(element: Element): boolean {
  try {
    return element.matches(":focus-visible");
  } catch {
    // Browsers without `:focus-visible` support reject the selector.
    return element.ownerDocument.activeElement === element;
  }
}

/** The remove icon of a ticker chip: a monochrome cross sized by CSS. */
function RemoveIcon(): ReactElement {
  return (
    <svg viewBox="0 0 16 16" aria-hidden="true" focusable="false" className={CHIP_ICON_CLASS}>
      <path
        d="M4 4l8 8M12 4l-8 8"
        fill="none"
        stroke="currentColor"
        strokeWidth="2"
        strokeLinecap="round"
      />
    </svg>
  );
}

/**
 * Custom watchlists of the calling principal: create a list (optionally with tickers), delete a
 * list after a two-step confirmation, and add or remove single tickers.
 *
 * The widget has no owner field and no login: the API scopes every watchlist to the principal
 * the host resolves, and the host's credentials reach it only through `getHeaders`. The server
 * owns the state. The widget loads `GET /watchlists` once and afterwards mirrors each mutation's
 * response (`POST /watchlists`, `DELETE /watchlists/{id}`, `PUT` and `DELETE
 * /watchlists/{id}/items/{ticker}`), with no optimistic update to roll back.
 *
 * An empty list of watchlists is a ready state, so the creation form is always offered. A failed
 * initial load shows the frame's error state with Retry; a failed mutation shows its message
 * beneath the input it concerns (`field-error-name`, `field-error-tickers`,
 * `field-error-ticker-<id>`) until that input is edited or the next request succeeds, or else on
 * the widget's own error line, and the lists stay usable.
 *
 * One mutation runs at a time. Unmounting the widget, or changing its `apiBaseUrl` to a
 * different API target, aborts the pending one; a new `getHeaders` identity does not, because
 * hosts often pass an inline function. Aborting only stops the widget tracking the response: it
 * cannot undo a write the server has already committed, which a later `GET /watchlists` from
 * that API returns. The response of an abandoned mutation changes nothing on screen, moves no
 * focus, shows no error and never reaches `onWatchlistsChange`, and the controls are released at
 * once. A successful create or ticker addition clears only the inputs that still hold the
 * submitted text, so a draft typed while the request was pending survives.
 *
 * Rendering depends only on props and state, with no dates or locale formatting; the one effect
 * only aborts a pending mutation. Server-rendered markup therefore hydrates without a mismatch.
 */
export function WatchlistManager({
  apiBaseUrl,
  getHeaders,
  className,
  onWatchlistsChange,
}: WatchlistManagerProps): ReactElement {
  const query = useTerminalQuery<Watchlist[]>({
    path: "/watchlists",
    apiBaseUrl,
    getHeaders,
    isEmpty: () => false,
  });

  const [local, setLocal] = useState<LocalLists | null>(null);
  const [name, setName] = useState("");
  const [tickersText, setTickersText] = useState("");
  const [addTicker, setAddTicker] = useState<Record<string, string>>({});
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [busy, setBusy] = useState(false);
  const [confirmDeleteId, setConfirmDeleteId] = useState<string | null>(null);
  const [notice, setNotice] = useState("");
  // Set synchronously by the handlers, so a second submit arriving before the busy state renders
  // cannot start an overlapping request. Read and written only in event handlers and callbacks.
  const inFlight = useRef(false);
  // The controller of the pending mutation, or `null` when none is pending. Read and written only
  // in event handlers, request callbacks and effect cleanup.
  const mutationController = useRef<AbortController | null>(null);
  const nameInputRef = useRef<HTMLInputElement>(null);
  const baseId = useId();
  // The API target, normalized as the watchlist query normalizes it, so an `apiBaseUrl` naming
  // the same base (the default spelled out, or a trailing slash) is not a new target.
  const target = buildUrl(apiBaseUrl, "/watchlists");

  // Abandons the pending mutation when the widget unmounts or its API target changes. Aborting
  // only stops the widget tracking the response: a write the server has already committed stays
  // committed. The cleanup also releases the busy state, so a widget repointed mid-request
  // accepts its next mutation at once. Nothing is recorded across cleanups, so StrictMode's
  // mount, cleanup and mount sequence leaves the widget fully working.
  useEffect(() => {
    return () => {
      const controller = mutationController.current;
      if (controller === null) return;
      mutationController.current = null;
      inFlight.current = false;
      controller.abort();
      setBusy(false);
    };
  }, [target]);

  const lists = local !== null && local.base === query.data ? local.lists : (query.data ?? []);

  /**
   * Run one mutation. On success the returned lists become the shown lists, every error is
   * cleared, `onSuccess` clears the inputs that still hold the submitted text and returns the
   * status announcement, and `onWatchlistsChange` receives the lists. On failure the error is
   * placed by {@link errorsFor}; `watchlist_not_found` instead drops the vanished list and
   * reloads the watchlists.
   *
   * The request carries the signal of its own controller, which the abort effect aborts on
   * unmount or a target change. A completion whose controller was aborted, or is no longer the
   * pending one, is ignored entirely: the widget stopped tracking that response, although the
   * server may have committed its write. An `AbortError` is a cancellation rather than a failed
   * save, so it shows no error.
   */
  function runMutation(
    mutation: Mutation,
    request: (options: RequestOptions) => Promise<Watchlist[]>,
    onSuccess: () => string,
  ): void {
    if (inFlight.current) return;
    inFlight.current = true;
    const controller = new AbortController();
    mutationController.current = controller;
    const base = query.data;
    setBusy(true);
    setConfirmDeleteId(null);
    setNotice("");

    /** Claim this completion, or report `false` when the widget abandoned the request. */
    const settle = (): boolean => {
      if (controller.signal.aborted || mutationController.current !== controller) return false;
      mutationController.current = null;
      inFlight.current = false;
      setBusy(false);
      return true;
    };

    request({ apiBaseUrl, getHeaders, signal: controller.signal }).then(
      (next) => {
        if (!settle()) return;
        setLocal({ base, lists: next });
        setErrors({});
        setNotice(onSuccess());
        onWatchlistsChange?.(next);
      },
      (error: unknown) => {
        if (!settle()) return;
        // An abort the widget did not start, such as one raised by the host's `getHeaders`, is a
        // cancellation rather than a failed save, so it only releases the controls.
        if (isAbortError(error)) return;
        if (error instanceof TerminalApiError && error.code === "watchlist_not_found") {
          if (mutation.kind !== "create") {
            const vanishedId = mutation.list.id;
            setLocal({ base, lists: lists.filter((list) => list.id !== vanishedId) });
          }
          setErrors({ [GENERAL_ERROR_KEY]: NOT_FOUND_MESSAGE });
          query.refetch();
          return;
        }
        setErrors(
          error instanceof TerminalApiError
            ? errorsFor(error, mutation)
            : { [GENERAL_ERROR_KEY]: FALLBACK_ERROR_MESSAGE },
        );
      },
    );
  }

  function handleCreate(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault();
    if (inFlight.current) return;
    const trimmedName = name.trim();
    const nameMessage = nameProblem(trimmedName);
    const parsed = parseTickers(tickersText);
    if (nameMessage !== null || !parsed.ok) {
      const problems: Record<string, string> = {};
      if (nameMessage !== null) problems.name = nameMessage;
      if (!parsed.ok) problems.tickers = parsed.message;
      setErrors(problems);
      setNotice("");
      return;
    }
    const body: WatchlistCreate =
      parsed.tickers.length > 0
        ? { name: trimmedName, tickers: parsed.tickers }
        : { name: trimmedName };
    // The inputs stay editable while the request is pending, so success clears each one only if
    // it still holds exactly the text submitted, and a draft typed in the meantime survives.
    const submittedName = name;
    const submittedTickers = tickersText;
    runMutation(
      { kind: "create" },
      async (options) => {
        const created = await apiSend<Watchlist>("POST", "/watchlists", body, options);
        return sortByName([...lists.filter((list) => list.id !== created.id), created]);
      },
      () => {
        setName((current) => (current === submittedName ? "" : current));
        setTickersText((current) => (current === submittedTickers ? "" : current));
        return `Created watchlist ${trimmedName}`;
      },
    );
  }

  function handleDelete(event: MouseEvent<HTMLButtonElement>, list: Watchlist): void {
    if (inFlight.current) return;
    if (confirmDeleteId !== list.id) {
      setConfirmDeleteId(list.id);
      setNotice(`Select Confirm to delete ${list.name}, or Cancel to keep it`);
      return;
    }
    const button = event.currentTarget;
    runMutation(
      { kind: "delete", list },
      async (options) => {
        await apiSend<void>("DELETE", "/watchlists/" + pathSegment(list.id), undefined, options);
        return lists.filter((item) => item.id !== list.id);
      },
      () => {
        setAddTicker((current) => withoutKey(current, list.id));
        // The confirm button leaves with its list, so keyboard focus moves to the name input.
        if (hasKeyboardFocus(button)) nameInputRef.current?.focus();
        return `Deleted watchlist ${list.name}`;
      },
    );
  }

  function cancelDelete(): void {
    if (inFlight.current) return;
    setConfirmDeleteId(null);
    setNotice("");
  }

  function handleCancelClick(event: MouseEvent<HTMLButtonElement>): void {
    if (inFlight.current) return;
    const button = event.currentTarget;
    // Cancel leaves once the confirmation ends, so keyboard focus returns to the Delete button.
    if (hasKeyboardFocus(button)) {
      button.parentElement
        ?.querySelector<HTMLButtonElement>('[data-testid="watchlist-delete-button"]')
        ?.focus();
    }
    cancelDelete();
  }

  function handleDeleteKeyDown(event: KeyboardEvent<HTMLButtonElement>, list: Watchlist): void {
    if (event.key === "Escape" && confirmDeleteId === list.id) {
      event.preventDefault();
      cancelDelete();
    }
  }

  /** Drop the error shown for `key` once its input is edited; other errors stay. */
  function clearError(key: string): void {
    setErrors((current) => (current[key] === undefined ? current : withoutKey(current, key)));
  }

  function handleAdd(event: FormEvent<HTMLFormElement>, list: Watchlist): void {
    event.preventDefault();
    if (inFlight.current) return;
    const submittedTicker = addTicker[list.id] ?? "";
    const ticker = submittedTicker.trim().toUpperCase();
    const problem = tickerProblem(ticker);
    if (problem !== null) {
      setErrors({ [tickerErrorKey(list.id)]: problem });
      setNotice("");
      return;
    }
    runMutation(
      { kind: "add", list },
      async (options) => {
        const updated = await apiSend<Watchlist>(
          "PUT",
          "/watchlists/" + pathSegment(list.id) + "/items/" + pathSegment(ticker),
          undefined,
          options,
        );
        return withList(lists, updated);
      },
      () => {
        // As for creation: a ticker typed while the request was pending survives its success.
        setAddTicker((current) =>
          current[list.id] === submittedTicker ? { ...current, [list.id]: "" } : current,
        );
        return `Added ${ticker} to ${list.name}`;
      },
    );
  }

  function handleRemove(
    event: MouseEvent<HTMLButtonElement>,
    list: Watchlist,
    ticker: string,
  ): void {
    if (inFlight.current) return;
    const button = event.currentTarget;
    const item = button.closest('[data-testid="watchlist-item"]');
    runMutation(
      { kind: "remove", list },
      async (options) => {
        const updated = await apiSend<Watchlist>(
          "DELETE",
          "/watchlists/" + pathSegment(list.id) + "/items/" + pathSegment(ticker),
          undefined,
          options,
        );
        return withList(lists, updated);
      },
      () => {
        // The removed chip's button leaves with it, so keyboard focus moves to the add input.
        if (hasKeyboardFocus(button)) {
          item?.querySelector<HTMLInputElement>('[data-testid="watchlist-add-input"]')?.focus();
        }
        return `Removed ${ticker} from ${list.name}`;
      },
    );
  }

  const buttonDisabled = busy ? true : undefined;
  const nameId = `${baseId}-name`;
  const nameErrorId = `${baseId}-name-error`;
  const tickersId = `${baseId}-tickers`;
  const tickersErrorId = `${baseId}-tickers-error`;
  const nameError = errors.name;
  const tickersError = errors.tickers;
  const generalError = errors[GENERAL_ERROR_KEY];

  function renderList(list: Watchlist): ReactElement {
    const addId = `${baseId}-add-${list.id}`;
    const addErrorKey = tickerErrorKey(list.id);
    const addErrorId = `${addId}-error`;
    const addError = errors[addErrorKey];
    const confirming = confirmDeleteId === list.id;
    const count = list.tickers.length;

    return (
      <li
        key={list.id}
        data-testid="watchlist-item"
        data-watchlist-name={list.name}
        data-watchlist-id={list.id}
        className={LIST_ITEM_CLASS}
      >
        <div className={LIST_HEADER_CLASS}>
          <div className={LIST_TITLE_CLASS}>
            <h3 className={LIST_NAME_CLASS}>{list.name}</h3>
            <p className={LIST_META_CLASS}>
              {count} {count === 1 ? "ticker" : "tickers"} · updated{" "}
              {formatDate(list.updated_at.slice(0, 10))}
            </p>
          </div>
          <div className={LIST_ACTIONS_CLASS}>
            <button
              type="button"
              data-testid="watchlist-delete-button"
              aria-disabled={buttonDisabled}
              aria-label={confirming ? `Confirm deleting ${list.name}` : `Delete ${list.name}`}
              className={confirming ? CONFIRM_BUTTON_CLASS : DELETE_BUTTON_CLASS}
              onClick={(event) => handleDelete(event, list)}
              onKeyDown={(event) => handleDeleteKeyDown(event, list)}
            >
              {confirming ? "Confirm" : "Delete"}
            </button>
            {confirming ? (
              <button
                type="button"
                data-testid="watchlist-delete-cancel"
                aria-disabled={buttonDisabled}
                aria-label={`Cancel deleting ${list.name}`}
                className={SECONDARY_BUTTON_CLASS}
                onClick={handleCancelClick}
              >
                Cancel
              </button>
            ) : null}
          </div>
        </div>

        {count === 0 ? (
          <p className={NO_TICKERS_CLASS}>No tickers yet</p>
        ) : (
          <ul className={CHIPS_CLASS} aria-label={`Tickers in ${list.name}`}>
            {list.tickers.map((ticker) => (
              <li key={ticker}>
                <span
                  data-testid="watchlist-ticker-chip"
                  data-ticker={ticker}
                  className={CHIP_CLASS}
                >
                  {ticker}
                  <button
                    type="button"
                    data-testid="watchlist-ticker-remove"
                    aria-disabled={buttonDisabled}
                    aria-label={`Remove ${ticker} from ${list.name}`}
                    className={CHIP_REMOVE_CLASS}
                    onClick={(event) => handleRemove(event, list, ticker)}
                  >
                    <RemoveIcon />
                  </button>
                </span>
              </li>
            ))}
          </ul>
        )}

        <form
          className={ADD_FORM_CLASS}
          onSubmit={(event) => handleAdd(event, list)}
          noValidate
          aria-label={`Add a ticker to ${list.name}`}
        >
          <div className={ADD_ROW_CLASS}>
            <div className={ADD_FIELD_CLASS}>
              <label htmlFor={addId} className={LABEL_CLASS}>
                Add ticker
              </label>
              <input
                id={addId}
                type="text"
                data-testid="watchlist-add-input"
                value={addTicker[list.id] ?? ""}
                onChange={(event) => {
                  const value = event.target.value;
                  setAddTicker((current) => ({ ...current, [list.id]: value }));
                  clearError(addErrorKey);
                }}
                placeholder="e.g. KCB"
                autoComplete="off"
                autoCapitalize="characters"
                spellCheck={false}
                aria-invalid={addError ? true : undefined}
                aria-describedby={addError ? addErrorId : undefined}
                className={INPUT_CLASS}
              />
            </div>
            <button
              type="submit"
              data-testid="watchlist-add-button"
              aria-disabled={buttonDisabled}
              aria-label={`Add ticker to ${list.name}`}
              className={SECONDARY_BUTTON_CLASS}
            >
              Add
            </button>
          </div>
          <FieldErrorText field={addErrorKey} message={addError} id={addErrorId} />
        </form>
      </li>
    );
  }

  return (
    <WidgetFrame
      testId="terminal-watchlist-manager"
      title="Watchlists"
      state={query.state}
      error={query.error}
      onRetry={query.refetch}
      className={className}
      skeletonClassName="h-48"
    >
      <div className={BODY_CLASS}>
        <form
          className={FORM_CLASS}
          onSubmit={handleCreate}
          noValidate
          aria-label="Create a watchlist"
        >
          <div className={FIELDS_CLASS}>
            <div className={FIELD_CLASS}>
              <label htmlFor={nameId} className={LABEL_CLASS}>
                New watchlist name
              </label>
              <input
                id={nameId}
                ref={nameInputRef}
                type="text"
                data-testid="watchlist-name-input"
                value={name}
                onChange={(event) => {
                  setName(event.target.value);
                  clearError("name");
                }}
                autoComplete="off"
                aria-invalid={nameError ? true : undefined}
                aria-describedby={nameError ? nameErrorId : undefined}
                className={INPUT_CLASS}
              />
              <FieldErrorText field="name" message={nameError} id={nameErrorId} />
            </div>
            <div className={FIELD_CLASS}>
              <label htmlFor={tickersId} className={LABEL_CLASS}>
                Tickers (optional)
              </label>
              <input
                id={tickersId}
                type="text"
                data-testid="watchlist-tickers-input"
                value={tickersText}
                onChange={(event) => {
                  setTickersText(event.target.value);
                  clearError("tickers");
                }}
                placeholder="e.g. SCOM, KCB"
                autoComplete="off"
                autoCapitalize="characters"
                spellCheck={false}
                aria-invalid={tickersError ? true : undefined}
                aria-describedby={tickersError ? tickersErrorId : undefined}
                className={INPUT_CLASS}
              />
              <FieldErrorText field="tickers" message={tickersError} id={tickersErrorId} />
            </div>
          </div>
          <button
            type="submit"
            data-testid="watchlist-create-button"
            aria-disabled={buttonDisabled}
            className={PRIMARY_BUTTON_CLASS}
          >
            Create
          </button>
        </form>

        {generalError ? (
          <p role="alert" data-testid="watchlist-error" className={GENERAL_ERROR_CLASS}>
            {generalError}
          </p>
        ) : null}

        {lists.length === 0 ? (
          <p data-testid="watchlist-empty" className={EMPTY_LISTS_CLASS}>
            No watchlists yet
          </p>
        ) : (
          <ul className={LISTS_CLASS} aria-label="Saved watchlists">
            {lists.map(renderList)}
          </ul>
        )}

        <p role="status" className={STATUS_CLASS}>
          {busy ? "Saving…" : notice}
        </p>
      </div>
    </WidgetFrame>
  );
}
