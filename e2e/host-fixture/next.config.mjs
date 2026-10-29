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

/** @type {import("next").NextConfig} */
const nextConfig = {
  experimental: {
    // Lets the webpack bundler (`next dev --webpack`, older hosts) compile TypeScript outside the
    // project directory: ../../src/components/terminal/**.
    externalDir: true,
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
