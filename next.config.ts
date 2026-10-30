import type { NextConfig } from "next";

// Standalone mode: the browser calls /api/v1/terminal/* on this origin, exactly as it would inside
// a host, and Next.js forwards those calls to the terminal's own FastAPI app.
//
// TERMINAL_API_ORIGIN is read and validated inside rewrites(), which Next.js evaluates at
// `next build` and `next dev`. The result is baked into the build's routes manifest: `next start`
// serves that origin and neither reads nor validates the start-time value, so set it before
// building and rebuild after changing it. TERMINAL_* variables are not NEXT_PUBLIC_, so none reach
// the browser bundle.
const DEFAULT_API_ORIGIN = "http://localhost:8000";

function resolveApiOrigin(raw: string | undefined): string {
  const value = raw ?? DEFAULT_API_ORIGIN;
  const url = URL.canParse(value) ? new URL(value) : null;
  // Userinfo, a query or a fragment would corrupt every rewrite destination. The raw text is
  // searched because a bare "?" or "#" leaves url.search and url.hash empty.
  if (url?.username || url?.password || /[?#]/.test(value)) {
    // The value is not echoed: it may hold a password or a token.
    throw new Error("TERMINAL_API_ORIGIN must not carry userinfo, a query string or a fragment");
  }
  // "localhost:8000" parses as a URL with the scheme "localhost:", so the scheme is checked too.
  // Text with an "@" is not echoed: "user:pw@host" parses without userinfo yet holds a password.
  if (url?.protocol !== "http:" && url?.protocol !== "https:") {
    const shown = value.includes("@")
      ? " (value not shown: it may hold credentials)"
      : `: "${value}"`;
    throw new Error(`TERMINAL_API_ORIGIN must be an absolute http(s) URL${shown}`);
  }
  // Rebuilt from the parsed URL, so the manifest holds only what was validated. A path prefix is
  // kept without its trailing slashes, since "/api/v1/terminal" is appended to it.
  return `${url.origin}${url.pathname.replace(/\/+$/, "")}`;
}

// The API's default TERMINAL_PDF_MAX_BYTES (20 MiB), its multipart allowance (1 MiB), and the
// largest chunk in which Node.js delivers a request body (one 64 KiB socket read).
const DEFAULT_PDF_MAX_BYTES = 20_971_520;
const MIB = 1_048_576;
const BODY_CHUNK_BYTES = 65_536;

/**
 * The rewrite's request-body limit: the API's whole-request bound for POST /ingest/pdf
 * (TERMINAL_PDF_MAX_BYTES plus its 1 MiB multipart allowance) plus one body chunk.
 *
 * The API answers a request over that bound with its own coded 413. Next.js forwards only the
 * first 10 MB of a rewritten body by default, so an upload under the bound would arrive cut short
 * and fail as a plain-text 500. Past its limit Next.js drops whole chunks, so one chunk of headroom
 * carries a longer chunked body past the API's bound, and the API answers 413 rather than parsing a
 * truncated form. The API reads and discards up to 1 MiB past its bound before it answers, so the
 * headroom (one chunk) must stay within that allowance for the whole forwarded body to be consumed
 * and the 413 to arrive intact.
 * Next.js reads the root .env, so this is the TERMINAL_PDF_MAX_BYTES the API reads.
 */
function resolveProxyBodyLimit(raw: string | undefined): number {
  const value = raw?.trim() ?? "";
  const bytes = value === "" ? DEFAULT_PDF_MAX_BYTES : /^\d+$/.test(value) ? Number(value) : NaN;
  const limit = bytes + MIB + BODY_CHUNK_BYTES;
  if (!(bytes > 0) || !Number.isSafeInteger(limit)) {
    throw new Error(
      `TERMINAL_PDF_MAX_BYTES must be a positive integer number of bytes: "${value}"`,
    );
  }
  return limit;
}

/**
 * How long the rewrite waits on an idle API connection before giving up, in milliseconds: 10
 * minutes.
 *
 * By default Next.js destroys a proxied request after 30 s without traffic from the API and
 * answers a plain-text 500, while POST /ingest/pdf sends nothing until its parse ends. A dense
 * 400-page upload (TERMINAL_PDF_MAX_PAGES, about 1,190 words per page) took 45 to 52 s, alone and
 * as one of two concurrent uploads (TERMINAL_PDF_MAX_CONCURRENCY), since each parse runs in its
 * own process. The API stops a parse at 20 s plus 0.4 s per page of TERMINAL_PDF_MAX_PAGES, 180 s
 * at 400 pages, and answers 422 pdf_parse_timeout. Ten minutes is a fixed bound over three times
 * that limit, so the database writes around a parse, on a loaded host or a host's own database,
 * still reach the client as the API's own answer.
 *
 * A client that disconnects still ends the proxied request at once: Next.js destroys the upstream
 * request as soon as the client's response closes. Widget GETs, like every call of the browser
 * client, stay bounded by that client's own 20 s REQUEST_TIMEOUT_MS
 * (src/components/terminal/lib/apiClient.ts), whose abort closes them long before this bound.
 */
const PROXY_TIMEOUT_MS = 600_000;

const nextConfig: NextConfig = {
  reactStrictMode: true,
  experimental: {
    // Applied by the router at run time, so it is read at every config load, `next start` included.
    proxyClientMaxBodySize: resolveProxyBodyLimit(process.env.TERMINAL_PDF_MAX_BYTES),
    // Also applied by the router at run time; Next.js's default is 30 s.
    proxyTimeout: PROXY_TIMEOUT_MS,
  },
  // A plain array is applied after filesystem routes; the app defines no routes under /api/.
  async rewrites() {
    const apiOrigin = resolveApiOrigin(process.env.TERMINAL_API_ORIGIN);
    return [
      {
        source: "/api/v1/terminal/:path*",
        destination: `${apiOrigin}/api/v1/terminal/:path*`,
      },
    ];
  },
};

export default nextConfig;
