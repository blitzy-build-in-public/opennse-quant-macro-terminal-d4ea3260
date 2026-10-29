/**
 * Data hook of the terminal widgets: one GET against the terminal API, exposed as the render
 * state a widget writes to its root `data-state` attribute.
 *
 * The hook holds no cache and no shared state. Widgets that mount together with the same URL
 * each call {@link apiGet}, which merges them into one in-flight fetch, so the overview page's
 * three macro widgets issue a single `/macro/overview?history_months=24` request. No context
 * provider or global store is involved, so a host renders the widgets without any setup.
 *
 * It uses only `useState`, `useEffect`, `useRef` and `useCallback`, so it runs on React 18.2 and
 * later, and it never suspends. It is internal to the widget folder and not re-exported from its
 * `index.ts`.
 */
import { useCallback, useEffect, useRef, useState } from "react";

import type { WidgetState } from "../types";
import { apiGet, buildUrl, isAbortError, TerminalApiError } from "./apiClient";

/** Options of {@link useTerminalQuery}. */
export interface UseTerminalQueryOptions<T> {
  /**
   * Endpoint path relative to the API base, for example `"/market/screener"`. `null` means there
   * is nothing to fetch, for example a chart with no subject selected: no request is sent and the
   * state is `empty`.
   */
  path: string | null;
  /**
   * Query parameters, serialized by `buildUrl` (`null`, `undefined` and `""` omitted, arrays
   * repeated, booleans as `"true"`/`"false"`). Only the serialized URL matters, so passing a new
   * object with the same content on every render never refetches.
   */
  query?: object;
  /** Base URL of the terminal API. Defaults to `"/api/v1/terminal"`. */
  apiBaseUrl?: string;
  /**
   * Host auth hook, awaited before each request. A new function identity never refetches; the
   * latest one is used by the next request.
   */
  getHeaders?: () => HeadersInit | Promise<HeadersInit>;
  /**
   * Host-supplied data. While it is defined the hook sends no request and reports `ready`, or
   * `empty` when {@link isEmpty} says so.
   */
  data?: T;
  /**
   * Decides whether a payload counts as `empty`. Defaults to "an empty array"; every other
   * payload is `ready`. It is evaluated during render, so its identity may change freely.
   */
  isEmpty?: (data: T) => boolean;
}

/** Result of {@link useTerminalQuery}. */
export interface TerminalQueryResult<T> {
  /** Render state for the widget root's `data-state` attribute. */
  state: WidgetState;
  /**
   * The host-supplied data, or the last successful payload. It survives path or query changes,
   * refetches and errors, so a widget keeps showing its previous result while recalculating.
   */
  data: T | undefined;
  /** The failure of the current request; `null` unless `state` is `error`. */
  error: TerminalApiError | null;
  /** True while the request for the current URL has not settled. */
  isFetching: boolean;
  /**
   * Sends the current request again. Safe to call from event handlers, such as a Retry button
   * or after a mutation. A no-op while host-supplied data is in use.
   */
  refetch: () => void;
}

/** The outcome of the most recent settled request. */
interface SettledResult<T> {
  /** `<url>#<nonce>` of the request that settled, or `null` before any has. */
  id: string | null;
  /** Last successful payload, boxed so a falsy payload still counts as data. */
  last: { value: T } | null;
  /** The settled request's failure, or `null` when it succeeded. */
  error: TerminalApiError | null;
}

/** Request inputs the fetch effect reads without depending on their identity. */
interface LatestInputs {
  path: string | null;
  query: object | undefined;
  apiBaseUrl: string | undefined;
  getHeaders: (() => HeadersInit | Promise<HeadersInit>) | undefined;
}

/** `refetch` while host-supplied data is in use: there is no request to repeat. */
const NOOP = (): void => {};

function defaultIsEmpty(data: unknown): boolean {
  return Array.isArray(data) ? data.length === 0 : false;
}

function toApiError(error: unknown): TerminalApiError {
  if (error instanceof TerminalApiError) {
    return error;
  }
  return new TerminalApiError({
    status: 0,
    code: "client_error",
    message: error instanceof Error ? error.message : "Unexpected error",
  });
}

function requestIdOf(key: string, nonce: number): string {
  return `${key}#${nonce}`;
}

/**
 * Fetch `options.path` from the terminal API and report it as a widget render state.
 *
 * State machine, for the current URL (`apiBaseUrl + path + query`):
 * - `data` defined: no request; `ready`, or `empty` when `isEmpty(data)`; `refetch` is a no-op.
 * - `path` null: no request; `empty` with no data.
 * - Request pending with no earlier success: `loading`, `isFetching` true.
 * - Request pending after an earlier success (a new URL, or `refetch`): `ready` or `empty` from
 *   the previous payload, `isFetching` true, so the previous result stays on screen.
 * - Request succeeded: `ready` or `empty` from the new payload.
 * - Request failed: `error` with {@link TerminalQueryResult.error} set, and `data` still holding
 *   the last success, so a widget can show its rows beside field errors. `refetch` then shows
 *   `loading`, or the previous payload with `isFetching`, until the retry settles.
 *
 * A new URL or a `refetch` aborts the request it supersedes, and unmounting aborts the pending
 * request; an aborted request never updates state. Changing the identity of `query`,
 * `getHeaders` or `isEmpty` without changing the URL never refetches.
 *
 * The first render is `loading`, or derived from `data`, on both server and client, so
 * server-rendered markup hydrates without a mismatch.
 */
export function useTerminalQuery<T>(options: UseTerminalQueryOptions<T>): TerminalQueryResult<T> {
  const providedData = options.data;
  const dataProvided = providedData !== undefined;
  const isEmptyFn: (data: T) => boolean = options.isEmpty ?? defaultIsEmpty;
  const key =
    options.path === null ? null : buildUrl(options.apiBaseUrl, options.path, options.query);

  const [nonce, setNonce] = useState(0);
  const [settled, setSettled] = useState<SettledResult<T>>({ id: null, last: null, error: null });

  const latest = useRef<LatestInputs>({
    path: options.path,
    query: options.query,
    apiBaseUrl: options.apiBaseUrl,
    getHeaders: options.getHeaders,
  });
  // Declared before the fetch effect so it runs first in the same commit, and the request below
  // always uses this render's inputs.
  useEffect(() => {
    latest.current = {
      path: options.path,
      query: options.query,
      apiBaseUrl: options.apiBaseUrl,
      getHeaders: options.getHeaders,
    };
  });

  useEffect(() => {
    if (dataProvided || key === null) {
      return undefined;
    }
    const { path, query, apiBaseUrl, getHeaders } = latest.current;
    if (path === null) {
      return undefined;
    }
    const controller = new AbortController();
    const requestId = requestIdOf(key, nonce);

    apiGet<T>(path, query, { apiBaseUrl, getHeaders, signal: controller.signal }).then(
      (value) => {
        if (controller.signal.aborted) {
          return;
        }
        setSettled({ id: requestId, last: { value }, error: null });
      },
      (error: unknown) => {
        if (controller.signal.aborted || isAbortError(error)) {
          return;
        }
        setSettled((previous) => ({
          id: requestId,
          last: previous.last,
          error: toApiError(error),
        }));
      },
    );

    return () => {
      controller.abort();
    };
  }, [key, nonce, dataProvided]);

  const refetch = useCallback(() => {
    setNonce((current) => current + 1);
  }, []);

  if (providedData !== undefined) {
    return {
      state: isEmptyFn(providedData) ? "empty" : "ready",
      data: providedData,
      error: null,
      isFetching: false,
      refetch: NOOP,
    };
  }

  if (key === null) {
    return { state: "empty", data: undefined, error: null, isFetching: false, refetch };
  }

  const last = settled.last;
  const lastState: WidgetState = last !== null && isEmptyFn(last.value) ? "empty" : "ready";

  if (settled.id === requestIdOf(key, nonce)) {
    if (settled.error !== null) {
      return {
        state: "error",
        data: last?.value,
        error: settled.error,
        isFetching: false,
        refetch,
      };
    }
    return { state: lastState, data: last?.value, error: null, isFetching: false, refetch };
  }

  if (last === null) {
    return { state: "loading", data: undefined, error: null, isFetching: true, refetch };
  }
  return { state: lastState, data: last.value, error: null, isFetching: true, refetch };
}
