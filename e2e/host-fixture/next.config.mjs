/**
 * Next.js config of the host fixture: a foreign App Router app that imports a widget from
 * `src/components/terminal/` and carries only what a real host adds to embed it, namely compiling
 * that source folder outside its own project directory and proxying `/api/v1/terminal/*` to the
 * FastAPI app that mounts `terminal_router`. None of the standalone app's settings belong here.
 *
 * ESM `.mjs`, because the fixture has no package.json whose `"type"` would decide the format.
 * Next.js loads this file with a native `import()`, so `import.meta.url` is this file's URL.
 */
import path from "node:path";
import { fileURLToPath } from "node:url";

// <repo>, from <repo>/e2e/host-fixture.
const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");

const DEFAULT_API_ORIGIN = "http://localhost:8000";

/**
 * The fixture's upstream: TERMINAL_API_ORIGIN accepted and normalized exactly as the standalone
 * app accepts it, so an accepted value gives the same rewrite destination in both apps, and in
 * addition required to be a loopback host (any port), so a test run never proxies to a remote API.
 * No message echoes the value: it may hold a password or a token.
 *
 * @param {string | undefined} raw
 * @returns {string} The origin plus any path prefix, without trailing slashes.
 */
function resolveApiOrigin(raw) {
  const value = raw ?? DEFAULT_API_ORIGIN;
  const url = URL.canParse(value) ? new URL(value) : null;
  // Userinfo, a query or a fragment would corrupt the destination. The raw text is searched
  // because a bare "?" or "#" leaves url.search and url.hash empty.
  if (url?.username || url?.password || /[?#]/.test(value)) {
    throw new Error("TERMINAL_API_ORIGIN must not carry userinfo, a query string or a fragment");
  }
  // An empty or relative value would turn the proxy into a rewrite onto this app itself, and
  // "localhost:8000" parses with the scheme "localhost:", so the scheme is checked too.
  if (url?.protocol !== "http:" && url?.protocol !== "https:") {
    throw new Error("TERMINAL_API_ORIGIN must be an absolute http(s) URL");
  }
  // The parser lowercases hosts and rewrites 127.1 or 2130706433 as dotted decimal, so these
  // tests see canonical forms. foo.localhost, "localhost.", 0.0.0.0 and IPv4-mapped IPv6 are
  // refused: whether each reaches the loopback interface depends on the resolver or the OS.
  const host = url.hostname;
  if (host !== "localhost" && host !== "[::1]" && !/^127(?:\.\d{1,3}){3}$/.test(host)) {
    throw new Error(
      "TERMINAL_API_ORIGIN must be a loopback origin: localhost, 127.0.0.0/8 or [::1]",
    );
  }
  // Rebuilt from the parsed URL, so the destination holds only what was validated. A path prefix is
  // kept without its trailing slashes, since "/api/v1/terminal" is appended to it.
  return `${url.origin}${url.pathname.replace(/\/+$/, "")}`;
}

// Next.js reads .env* files from the project directory (e2e/host-fixture), never from the
// repository root, so this value comes from the process environment (playwright.config.ts
// forwards it) or the default.
const apiOrigin = resolveApiOrigin(process.env.TERMINAL_API_ORIGIN);

// The API's default TERMINAL_PDF_MAX_BYTES (20 MiB), its multipart allowance (1 MiB), and the
// largest chunk in which Node.js delivers a request body (one 64 KiB socket read).
const DEFAULT_PDF_MAX_BYTES = 20_971_520;
const MIB = 1_048_576;
const BODY_CHUNK_BYTES = 65_536;

/**
 * The proxy's request-body limit: the API's whole-request bound for POST /ingest/pdf
 * (TERMINAL_PDF_MAX_BYTES plus 1 MiB) plus one body chunk, so uploads under the bound are
 * forwarded whole and a longer chunked body still passes the bound and gets the API's own 413.
 * The API reads and discards up to 1 MiB past its bound before it answers, so the headroom (one
 * chunk) must stay within that allowance for the whole forwarded body to be consumed and the 413 to
 * arrive intact. Read, like the origin, from the process environment (Playwright passes its own to
 * the servers it starts) or the API's default.
 *
 * @param {string | undefined} raw
 * @returns {number} Bytes.
 */
function resolveProxyBodyLimit(raw) {
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
 * The proxy's wait on an idle API connection, in milliseconds: 10 minutes, as in the standalone
 * app. By default Next.js destroys a proxied request after 30 s without traffic from the API,
 * while POST /ingest/pdf sends nothing until its parse ends: a valid 400-page upload took under
 * 40 s, alone and as one of two concurrent uploads, and the API stops any parse at 50 s with its
 * own 422 pdf_unreadable. Ten minutes is twelve times that limit. A client that disconnects still
 * ends the proxied request at once, and the widget's calls stay bounded by the browser client's
 * own 20 s REQUEST_TIMEOUT_MS (src/components/terminal/lib/apiClient.ts).
 */
const PROXY_TIMEOUT_MS = 600_000;

/** @type {import("next").NextConfig} */
const nextConfig = {
  experimental: {
    // Lets the webpack bundler (`next dev --webpack`, older hosts) compile TypeScript outside the
    // project directory: ../../src/components/terminal/**.
    externalDir: true,
    // Part of proxying, so a real host adds it too: Next.js forwards only the first 10 MB of a
    // rewritten body by default, which turns a larger upload into a plain-text 500.
    proxyClientMaxBodySize: resolveProxyBodyLimit(process.env.TERMINAL_PDF_MAX_BYTES),
    // Part of proxying too: with Next.js's 30 s default, a PDF upload whose parse takes longer
    // fails as a plain-text 500 although the API answers it.
    proxyTimeout: PROXY_TIMEOUT_MS,
  },
  turbopack: {
    // Turbopack resolves and compiles only files inside its root, and the widget folder lies
    // outside e2e/host-fixture. react, react-dom, recharts and next then resolve from the root
    // node_modules for both the fixture and the widgets, which keeps a single React instance.
    // If outputFileTracingRoot is ever added it must equal this value: Next.js warns when the two
    // differ and uses outputFileTracingRoot.
    root: repoRoot,
  },
  // The widgets' default apiBaseUrl is /api/v1/terminal, so the browser calls this app's own origin
  // and needs no CORS.
  async rewrites() {
    return [
      {
        source: "/api/v1/terminal/:path*",
        destination: `${apiOrigin}/api/v1/terminal/:path*`,
      },
    ];
  },
};

export default nextConfig;
