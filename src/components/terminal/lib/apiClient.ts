/**
 * Fetch client of the terminal widgets: request URLs, host headers, aborts, request deadlines,
 * de-duplication of concurrent identical GETs, and normalization of error responses into
 * {@link TerminalApiError}.
 *
 * Request target. Every request goes to `apiBaseUrl + path`, where `apiBaseUrl` defaults to
 * {@link DEFAULT_API_BASE_URL}. The client holds no other URL: the widgets talk only to the
 * terminal's own API and never to an exchange or third-party data endpoint.
 *
 * Authentication. Auth is entirely host-supplied through {@link RequestOptions.getHeaders}, which
 * is awaited before every request so a token can refresh. The client stores no token, cookie or
 * session and has no login logic. `credentials` is left at fetch's default (`same-origin`), so
 * cookies reach only the page's own origin: the standalone rewrite or a host proxy.
 *
 * Deadline. Every call is bounded by {@link REQUEST_TIMEOUT_MS}, counted from the call itself and
 * covering header resolution, the fetch and the full body read, so a stalled host hook, API or
 * proxy cannot leave a widget loading or a mutation busy. When it passes, the request is aborted
 * and the call rejects with `timeout_error`, which is a {@link TerminalApiError} and not an abort,
 * so callers show it with their Retry path. Nothing is retried automatically: a retry is a new
 * call by the caller.
 *
 * De-duplication. Concurrent {@link apiGet} calls whose URL and resolved headers are identical
 * share one fetch. Each caller keeps its own `signal` and deadline: aborting one caller, or its
 * deadline passing, rejects only that caller, and the shared fetch is aborted when every caller
 * has left or when its own deadline, counted from its start, passes. A caller whose signal is
 * aborted while its headers are being resolved never joins, so React StrictMode's mount, cleanup
 * and mount sequence issues one fetch. Merging stops when the response settles or the shared
 * fetch is aborted; the next identical GET fetches again, so this is not a cache.
 * {@link apiSend} never merges.
 *
 * Responses. JSON bodies are returned exactly as parsed. Decimal strings stay strings and no
 * numeric conversion happens here.
 *
 * The module imports only a type. Its single piece of state is a private map of in-flight GETs;
 * nothing is placed on `window` or `globalThis` and nothing mutable is exported. It uses only the
 * standard `fetch`, `Headers`, `AbortController`, `URLSearchParams`, `setTimeout` and
 * `clearTimeout` globals, so it runs in any React host, server or browser. It is internal to the
 * widget folder and not re-exported from its `index.ts`.
 */
import type { TerminalFieldError } from "../types";

/** Base URL used when a widget receives no `apiBaseUrl`: the terminal router's own prefix. */
export const DEFAULT_API_BASE_URL = "/api/v1/terminal";

/**
 * Deadline of one request in milliseconds, covering header resolution, the fetch and the full body
 * read. A call still unsettled when it passes is aborted and rejects with `timeout_error`.
 *
 * 20 s is far above the API's design targets (500 ms at p95 for analytic GETs, 100 ms for a
 * valuation), so only a stalled request reaches it. It is also below the 30 s upstream timeout of
 * the Next.js rewrite proxy, so in standalone mode a widget receives this client's own
 * `timeout_error` rather than the proxy's 500 or a reset connection.
 */
export const REQUEST_TIMEOUT_MS = 20_000;

/**
 * Query parameter values {@link buildUrl} serializes. `null`, `undefined` and `""` are omitted,
 * an array repeats its key once per item, and booleans become `"true"` or `"false"`.
 */
export type QueryValue =
  | string
  | number
  | boolean
  | null
  | undefined
  | ReadonlyArray<string | number>;

/** Per-call options shared by {@link apiGet} and {@link apiSend}. */
export interface RequestOptions {
  /**
   * Base URL of the terminal API, relative (`/api/v1/terminal`) or absolute
   * (`https://api.example.com/api/v1/terminal`). Trailing slashes are ignored. Defaults to
   * {@link DEFAULT_API_BASE_URL}.
   */
  apiBaseUrl?: string;
  /**
   * Host auth hook: returns the headers to send, for example
   * `{ Authorization: "Bearer <token>" }`. Awaited before every request so a token can refresh.
   * A throw or rejection fails the request with code `headers_error`. The wait counts towards
   * {@link REQUEST_TIMEOUT_MS}: a hook still pending at the deadline fails it with
   * `timeout_error`, and its late result is ignored.
   */
  getHeaders?: () => HeadersInit | Promise<HeadersInit>;
  /**
   * Aborts this caller's request; the call then rejects with an `AbortError`. Without a signal,
   * or while it stays unaborted, the call still ends at {@link REQUEST_TIMEOUT_MS} with
   * `timeout_error`.
   */
  signal?: AbortSignal;
}

/**
 * Error thrown for every failed terminal request except an abort.
 *
 * `code` is the server's `detail.code` when the response carries one, for example
 * `primary_exceeds_offer_size` or `unknown_sector`, passed through unchanged. Otherwise the client
 * sets one of:
 * - `network_error` (status 0): the API could not be reached;
 * - `timeout_error` (status 0): the request did not settle within {@link REQUEST_TIMEOUT_MS};
 * - `headers_error` (status 0): `getHeaders` threw or rejected, or returned invalid headers;
 * - `invalid_response`: a success response whose body is not JSON;
 * - `validation_error`: FastAPI's standard 422 list, one {@link fieldErrors} entry per item;
 * - `http_<status>`: any other error response.
 *
 * `fieldErrors` holds the errors tied to a request field, for display beneath that field's input.
 */
export class TerminalApiError extends Error {
  /** HTTP status of the response, or 0 when no complete response was received. */
  readonly status: number;
  /** Stable snake_case error code. */
  readonly code: string;
  /** Errors attached to individual request fields, in response order; empty when none. */
  readonly fieldErrors: TerminalFieldError[];

  constructor(init: {
    status: number;
    code: string;
    message: string;
    fieldErrors?: TerminalFieldError[];
  }) {
    super(init.message);
    this.name = "TerminalApiError";
    this.status = init.status;
    this.code = init.code;
    this.fieldErrors = init.fieldErrors ?? [];
    // Keeps `instanceof TerminalApiError` true when compiled to a target without native classes.
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

// ---------------------------------------------------------------------------------------------
// URL building
// ---------------------------------------------------------------------------------------------

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isArray(value: unknown): value is readonly unknown[] {
  return Array.isArray(value);
}

function appendQueryScalar(params: URLSearchParams, key: string, value: unknown): void {
  if (typeof value === "string") {
    if (value !== "") {
      params.append(key, value);
    }
  } else if (typeof value === "number") {
    if (Number.isFinite(value)) {
      params.append(key, String(value));
    }
  } else if (typeof value === "boolean") {
    params.append(key, value ? "true" : "false");
  }
}

/**
 * Build the request URL `base + path + "?" + query`.
 *
 * - `base` is `apiBaseUrl` when it is a non-empty string, otherwise
 *   {@link DEFAULT_API_BASE_URL}, with trailing slashes removed. An absolute base is concatenated
 *   as given; no `URL` object or `window.location` is consulted, so this works during server
 *   rendering and in any host.
 * - `path` gains a leading `/` when it lacks one.
 * - `query` entries are appended in insertion order (see {@link QueryValue}): `undefined`,
 *   `null`, `""` and non-finite numbers are omitted; an array appends its key once per item,
 *   skipping empty items (`event_dates=a&event_dates=b`); booleans become `"true"` or `"false"`;
 *   numbers are written with `String`. Values of any other type, such as objects or functions,
 *   are skipped silently.
 *
 * The same inputs always produce a byte-identical string, which is what lets concurrent widgets
 * share one GET. `query` is typed `object` so filter interfaces without an index signature can
 * be passed directly.
 */
export function buildUrl(apiBaseUrl: string | undefined, path: string, query?: object): string {
  let base =
    typeof apiBaseUrl === "string" && apiBaseUrl !== "" ? apiBaseUrl : DEFAULT_API_BASE_URL;
  while (base.endsWith("/")) {
    base = base.slice(0, -1);
  }
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;

  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(query ?? {})) {
    if (isArray(value)) {
      for (const item of value) {
        appendQueryScalar(params, key, item);
      }
    } else {
      appendQueryScalar(params, key, value);
    }
  }
  const queryString = params.toString();
  return base + normalizedPath + (queryString ? `?${queryString}` : "");
}

/**
 * Percent-encode one path segment, such as a watchlist id or ticker in
 * `/watchlists/{watchlist_id}/items/{ticker}`.
 */
export function pathSegment(value: string): string {
  return encodeURIComponent(value);
}

// ---------------------------------------------------------------------------------------------
// Request internals
// ---------------------------------------------------------------------------------------------

const ABORT_MESSAGE = "The operation was aborted.";

function abortError(): Error {
  if (typeof DOMException === "function") {
    return new DOMException(ABORT_MESSAGE, "AbortError");
  }
  const error = new Error(ABORT_MESSAGE);
  error.name = "AbortError";
  return error;
}

/**
 * True when `error` is an abort: any object whose `name` is `"AbortError"`, which covers the
 * `DOMException` of native fetch and `AbortController`, plain `Error`s and fetch polyfills.
 * Callers treat it as a cancellation, not a failure to display.
 */
export function isAbortError(error: unknown): boolean {
  return (
    typeof error === "object" && error !== null && "name" in error && error.name === "AbortError"
  );
}

function timeoutError(): TerminalApiError {
  return new TerminalApiError({
    status: 0,
    code: "timeout_error",
    message: "The terminal API did not respond in time",
  });
}

/**
 * The bounded lifetime of one call, or of one shared GET: an internal `AbortController` that
 * aborts when the caller's signal aborts, when its owner aborts it, or when
 * {@link REQUEST_TIMEOUT_MS} passes, whichever comes first. Its signal, never the caller's, is
 * what fetch and every wait observe.
 */
interface Lifetime {
  /** Ends the lifetime: aborted by the caller's signal, by its owner, or by the deadline. */
  controller: AbortController;
  /** True when the deadline, and nothing before it, aborted `controller`. */
  timedOut: boolean;
  /** Clears the deadline and detaches from the caller's signal; safe to call more than once. */
  dispose: () => void;
}

/** Start a lifetime now, composed with `callerSignal` when one is given. */
function startLifetime(callerSignal: AbortSignal | undefined): Lifetime {
  const controller = new AbortController();
  const onCallerAbort = (): void => {
    controller.abort();
  };
  const timer = setTimeout(() => {
    // A lifetime the caller or its owner already ended stays an abort.
    if (!controller.signal.aborted) {
      lifetime.timedOut = true;
      controller.abort();
    }
  }, REQUEST_TIMEOUT_MS);
  const lifetime: Lifetime = {
    controller,
    timedOut: false,
    dispose: () => {
      clearTimeout(timer);
      callerSignal?.removeEventListener("abort", onCallerAbort);
    },
  };
  if (callerSignal?.aborted === true) {
    controller.abort();
  } else {
    callerSignal?.addEventListener("abort", onCallerAbort, { once: true });
  }
  return lifetime;
}

/** The rejection of an ended lifetime: `timeout_error` after its deadline, else an `AbortError`. */
function endError(lifetime: Lifetime): Error {
  return lifetime.timedOut ? timeoutError() : abortError();
}

/**
 * Settle as `promise` does, or reject with {@link endError} as soon as `lifetime` ends, whichever
 * comes first. An outrun `promise` keeps its rejection handler, so a late rejection, such as a
 * host `getHeaders` failing after the deadline, never becomes an unhandled rejection.
 */
function untilEnd<T>(promise: Promise<T>, lifetime: Lifetime): Promise<T> {
  const { signal } = lifetime.controller;
  return new Promise<T>((resolve, reject) => {
    const onEnd = (): void => {
      reject(endError(lifetime));
    };
    if (signal.aborted) {
      onEnd();
    } else {
      signal.addEventListener("abort", onEnd, { once: true });
    }
    promise.then(
      (value) => {
        signal.removeEventListener("abort", onEnd);
        resolve(value);
      },
      (error: unknown) => {
        signal.removeEventListener("abort", onEnd);
        reject(error);
      },
    );
  });
}

/**
 * The rejection of `fetch` or of reading its body: `timeout_error` when the lifetime's deadline
 * aborted it, an `AbortError` for any other abort, and otherwise a network failure.
 */
function transportError(error: unknown, lifetime: Lifetime): Error {
  // Checked first: fetch rejects with the signal's reason, which need not be an AbortError.
  if (lifetime.controller.signal.aborted) {
    return endError(lifetime);
  }
  if (isAbortError(error)) {
    return abortError();
  }
  return new TerminalApiError({
    status: 0,
    code: "network_error",
    message: "Could not reach the terminal API",
  });
}

async function resolveHeaders(options: RequestOptions, hasBody: boolean): Promise<Headers> {
  let headers: Headers;
  try {
    const resolved: HeadersInit | undefined = await options.getHeaders?.();
    headers = new Headers(resolved ?? {});
  } catch (error) {
    if (isAbortError(error)) {
      throw abortError();
    }
    throw new TerminalApiError({
      status: 0,
      code: "headers_error",
      message: "Could not prepare request headers",
    });
  }
  if (!headers.has("Accept")) {
    headers.set("Accept", "application/json");
  }
  if (hasBody) {
    headers.set("Content-Type", "application/json");
  }
  return headers;
}

function compareText(a: string, b: string): number {
  if (a < b) {
    return -1;
  }
  return a > b ? 1 : 0;
}

/** Order-independent identity of a header set, part of the de-duplication key. */
function headersKey(headers: Headers): string {
  const pairs: [string, string][] = [];
  // forEach rather than iteration, which would need the DOM.Iterable lib.
  headers.forEach((value, name) => {
    pairs.push([name, value]);
  });
  pairs.sort((a, b) => compareText(a[0], b[0]) || compareText(a[1], b[1]));
  return JSON.stringify(pairs);
}

function parseJsonOrNull(text: string): unknown {
  try {
    const parsed: unknown = JSON.parse(text);
    return parsed;
  } catch {
    return null;
  }
}

/**
 * Normalize an error response into a {@link TerminalApiError}, covering both 422 shapes:
 * - a coded body, `detail: {code, message, field}` at any status, keeps its code and message and
 *   gives one field error when `field` is a non-empty string;
 * - FastAPI's standard list, `detail: [{type, loc, msg}]`, gives `validation_error` with one field
 *   error per item, named by the last element of its `loc`;
 * - a string `detail` gives `http_<status>` with that string as the message;
 * - anything else gives `http_<status>` with a generic message.
 */
function normalizeError(status: number, body: unknown): TerminalApiError {
  const genericMessage = `Request failed (${status})`;
  const detail = isRecord(body) ? body.detail : undefined;

  if (isRecord(detail) && typeof detail.code === "string") {
    const code = detail.code;
    const message = typeof detail.message === "string" ? detail.message : genericMessage;
    const field = detail.field;
    const fieldErrors: TerminalFieldError[] =
      typeof field === "string" && field !== "" ? [{ field, code, message }] : [];
    return new TerminalApiError({ status, code, message, fieldErrors });
  }

  if (isArray(detail)) {
    const first = detail[0];
    const message =
      isRecord(first) && typeof first.msg === "string" ? first.msg : "Invalid request";
    const fieldErrors: TerminalFieldError[] = [];
    for (const item of detail) {
      if (!isRecord(item)) {
        continue;
      }
      const loc = item.loc;
      if (!isArray(loc) || loc.length === 0) {
        continue;
      }
      fieldErrors.push({
        field: String(loc[loc.length - 1]),
        code: typeof item.type === "string" ? item.type : "validation_error",
        message: typeof item.msg === "string" ? item.msg : "Invalid value",
      });
    }
    return new TerminalApiError({ status, code: "validation_error", message, fieldErrors });
  }

  if (typeof detail === "string" && detail !== "") {
    return new TerminalApiError({ status, code: `http_${status}`, message: detail });
  }

  return new TerminalApiError({ status, code: `http_${status}`, message: genericMessage });
}

/**
 * Run one fetch under `lifetime`'s signal and parse its body: 204 or an empty 2xx body resolves
 * `undefined`, any other 2xx resolves the parsed JSON, and every other status rejects with
 * {@link normalizeError}'s result. Ending `lifetime` aborts the fetch or the body read, which
 * rejects as {@link transportError} classifies it.
 */
async function performRequest<T>(url: string, init: RequestInit, lifetime: Lifetime): Promise<T> {
  let response: Response;
  try {
    response = await fetch(url, { ...init, signal: lifetime.controller.signal });
  } catch (error) {
    throw transportError(error, lifetime);
  }
  if (response.status === 204) {
    return undefined as T;
  }

  let text: string;
  try {
    text = await response.text();
  } catch (error) {
    throw transportError(error, lifetime);
  }

  if (response.ok) {
    if (text === "") {
      return undefined as T;
    }
    try {
      const parsed: unknown = JSON.parse(text);
      return parsed as T;
    } catch {
      throw new TerminalApiError({
        status: response.status,
        code: "invalid_response",
        message: "The terminal API returned an invalid response",
      });
    }
  }
  throw normalizeError(response.status, parseJsonOrNull(text));
}


// ---------------------------------------------------------------------------------------------
// In-flight GET de-duplication
// ---------------------------------------------------------------------------------------------

/** One shared GET and the callers currently waiting on it. */
interface InflightEntry {
  /** The shared fetch; a no-op handler keeps it from ever being an unhandled rejection. */
  promise: Promise<unknown>;
  /**
   * The shared fetch's own lifetime, started with it: aborted once no caller is left, or by its
   * deadline, which rejects every remaining caller with `timeout_error`.
   */
  lifetime: Lifetime;
  /** Callers that have neither settled nor left, by an abort or their own deadline. */
  subscribers: number;
  /** True once the shared fetch has resolved or rejected. */
  settled: boolean;
}

/** In-flight GETs keyed by method, URL and resolved headers. */
const inflight = new Map<string, InflightEntry>();

function startInflight(key: string, url: string, headers: Headers): InflightEntry {
  const lifetime = startLifetime(undefined);
  // Listeners and promise callbacks run later, so `entry` is initialised by the time they run.
  const release = (): void => {
    if (inflight.get(key) === entry) {
      inflight.delete(key);
    }
  };
  // Leaves the map the moment it is aborted, so an identical GET arriving before the rejection
  // reaches the callers starts a fresh fetch instead of joining this one.
  lifetime.controller.signal.addEventListener("abort", release, { once: true });
  const promise = performRequest<unknown>(url, { method: "GET", headers }, lifetime).finally(
    () => {
      entry.settled = true;
      lifetime.dispose();
      release();
    },
  );
  promise.catch(() => undefined);
  const entry: InflightEntry = { promise, lifetime, subscribers: 0, settled: false };
  inflight.set(key, entry);
  return entry;
}

/**
 * Wait on `entry` for one caller. When the caller's own `lifetime` ends first, the caller leaves
 * with {@link endError}, and the shared fetch is aborted if it was the last caller.
 */
function subscribe<T>(entry: InflightEntry, lifetime: Lifetime): Promise<T> {
  const { signal } = lifetime.controller;
  entry.subscribers += 1;
  return new Promise<T>((resolve, reject) => {
    let done = false;

    const onEnd = (): void => {
      if (done) {
        return;
      }
      done = true;
      reject(endError(lifetime));
      entry.subscribers -= 1;
      if (entry.subscribers === 0 && !entry.settled) {
        entry.lifetime.controller.abort();
      }
    };

    entry.promise.then(
      (value) => {
        if (done) {
          return;
        }
        done = true;
        signal.removeEventListener("abort", onEnd);
        entry.subscribers -= 1;
        resolve(value as T);
      },
      (error: unknown) => {
        if (done) {
          return;
        }
        done = true;
        signal.removeEventListener("abort", onEnd);
        entry.subscribers -= 1;
        reject(error);
      },
    );

    signal.addEventListener("abort", onEnd, { once: true });
  });
}

// ---------------------------------------------------------------------------------------------
// Public request functions
// ---------------------------------------------------------------------------------------------

/**
 * GET `path` from the terminal API and resolve its parsed JSON body (`undefined` for an empty
 * body). Rejects with a {@link TerminalApiError}, or with an `AbortError` when `options.signal`
 * aborts (see {@link isAbortError}). A call unsettled {@link REQUEST_TIMEOUT_MS} after it was made,
 * counting header resolution and the wait for the shared fetch, rejects with `timeout_error`.
 *
 * Concurrent calls whose URL and resolved headers are identical share one fetch:
 * - three widgets calling `apiGet("/macro/overview", { history_months: 24 }, {})` in the same
 *   render issue one request and all receive its body;
 * - aborting one of two callers, or its deadline passing, rejects only that caller, and the
 *   other still resolves;
 * - the shared fetch is aborted once every caller has left, or when its own deadline, counted
 *   from its start, passes, which rejects every remaining caller with `timeout_error`;
 * - a caller aborted while its headers are being resolved never joins, so StrictMode's mount,
 *   cleanup and mount sequence issues one fetch.
 *
 * Every caller receives the same parsed object, so callers must treat it as immutable. Once the
 * response settles or the shared fetch is aborted, the next identical call fetches again:
 * concurrent requests are merged, but nothing is cached.
 */
export async function apiGet<T>(
  path: string,
  query: object | undefined,
  options: RequestOptions,
): Promise<T> {
  const url = buildUrl(options.apiBaseUrl, path, query);
  const { signal } = options;
  if (signal?.aborted) {
    throw abortError();
  }
  const lifetime = startLifetime(signal);
  try {
    const headers = await untilEnd(resolveHeaders(options, false), lifetime);
    if (lifetime.controller.signal.aborted) {
      throw endError(lifetime);
    }
    const key = `GET ${url} ${headersKey(headers)}`;
    const entry = inflight.get(key) ?? startInflight(key, url, headers);
    return await subscribe<T>(entry, lifetime);
  } finally {
    lifetime.dispose();
  }
}

/**
 * Send a `POST`, `PUT` or `DELETE` to `path` with `body` encoded as JSON (no body when `body` is
 * `undefined`), and resolve the parsed JSON response, or `undefined` for 204 and empty bodies
 * (`apiSend<void>("DELETE", ...)`). Rejects as {@link apiGet} does; its deadline covers header
 * resolution, the fetch and the full body read.
 *
 * Never de-duplicated: every call is its own request, aborted by `options.signal` or by its
 * deadline, so a caller can cancel a superseded request, such as an outdated valuation, with its
 * own controller. A request that times out is never resent: the server may already have applied
 * it, and `POST /watchlists` is not idempotent, so resubmitting is left to the caller.
 */
export async function apiSend<T>(
  method: "POST" | "PUT" | "DELETE",
  path: string,
  body: unknown,
  options: RequestOptions,
): Promise<T> {
  const url = buildUrl(options.apiBaseUrl, path);
  if (options.signal?.aborted) {
    throw abortError();
  }
  const lifetime = startLifetime(options.signal);
  try {
    const headers = await untilEnd(resolveHeaders(options, body !== undefined), lifetime);
    return await performRequest<T>(
      url,
      { method, headers, body: body === undefined ? undefined : JSON.stringify(body) },
      lifetime,
    );
  } finally {
    lifetime.dispose();
  }
}

/**
 * Map each field of a {@link TerminalApiError}'s `fieldErrors` to its first message, for display
 * beneath the matching input. Returns `{}` for any other error, or for none.
 */
export function fieldErrorMap(error: unknown): Record<string, string> {
  const messages: Record<string, string> = {};
  if (!(error instanceof TerminalApiError)) {
    return messages;
  }
  for (const { field, message } of error.fieldErrors) {
    if (!Object.prototype.hasOwnProperty.call(messages, field)) {
      messages[field] = message;
    }
  }
  return messages;
}

