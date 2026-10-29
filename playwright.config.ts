/**
 * End-to-end configuration for the standalone terminal and the host-embed proof.
 *
 * Preconditions, none of which this config provides:
 * - PostgreSQL is up, migrated (`opennse-terminal migrate upgrade head`) and seeded
 *   (`python -m opennse_terminal.seed.seed_terminal_db`).
 * - The API runs on :8000 against that database (`uvicorn opennse_terminal.main:app --port 8000`).
 *   Playwright never starts the API or the database.
 * - `npm ci` is done, because the host fixture resolves `next`, `react`, `react-dom`, `recharts`
 *   and `tailwindcss` from the root `node_modules`.
 * - `npm run build` is done for the standalone app, with the same `TERMINAL_API_ORIGIN` as this run.
 *
 * Playwright starts two separate Next.js apps on separate ports: the standalone pages on :3000
 * (`terminal-views.spec.ts`) and the foreign host in `e2e/host-fixture` on :3100
 * (`host-embed.spec.ts`), which imports a widget from `src/components/terminal/`.
 *
 * TERMINAL_API_ORIGIN is resolved once, the way `next build` resolves it for the standalone app:
 * a value defined in the process environment wins, even an empty one; otherwise the first of the
 * repository-root files .env.production.local, .env.local, .env.production and .env (with
 * NODE_ENV=test: .env.test.local, .env.test and .env) that sets it, with `$VAR` references
 * expanded; otherwise http://localhost:8000. It must be a loopback origin (localhost, 127.0.0.0/8
 * or [::1], any port) without userinfo, a query or a fragment; any other value stops this config
 * from loading, before either web server starts, so an e2e run never proxies to a remote API.
 * next.config.ts itself still accepts remote origins. Both web servers receive the same
 * normalized value. The host fixture needs it forwarded, because Next.js reads env files only from
 * e2e/host-fixture for that app.
 */
import { readFileSync, statSync } from "node:fs";
import { join } from "node:path";

import { defineConfig, devices } from "@playwright/test";

const API_ORIGIN_KEY = "TERMINAL_API_ORIGIN";
const DEFAULT_API_ORIGIN = "http://localhost:8000";

// The reader below mirrors @next/env 16.4.3, which bundles dotenv 16.3.1 and dotenv-expand
// 10.0.0, step for step. @next/env is not a declared dependency, and any difference from it would
// let the host fixture target another API than the one baked into the standalone build.
type Env = Record<string, string | undefined>;

// dotenv 16.3.1's line pattern. Only matchAll uses it, and matchAll iterates a copy, so its
// lastIndex never carries over from one call to the next.
const DOTENV_LINE =
  /(?:^|^)\s*(?:export\s+)?([\w.-]+)(?:\s*=\s*?|:\s+?)(\s*'(?:\\'|[^'])*'|\s*"(?:\\"|[^"])*"|\s*`(?:\\`|[^`])*`|[^#\r\n]+)?\s*(?:#.*)?(?:$|$)/gm;

/** dotenv 16.3.1 `parse`: `KEY=value` and `KEY: value` lines, quotes stripped, `#` comments. */
function parseDotenv(contents: string): Record<string, string> {
  const parsed: Record<string, string> = {};
  // The key group always participates in a match; the value group may not.
  for (const [, key = "", raw = ""] of contents.replace(/\r\n?/gm, "\n").matchAll(DOTENV_LINE)) {
    let value = raw.trim();
    const quote = value[0];
    value = value.replace(/^(['"`])([\s\S]*)\1$/gm, "$2");
    if (quote === '"') value = value.replace(/\\n/g, "\n").replace(/\\r/g, "\r");
    parsed[key] = value;
  }
  return parsed;
}

/**
 * dotenv-expand 10.0.0 `_interpolate`: substitutes the right-most unescaped `$NAME`, `${NAME}` or
 * `${NAME:-default}` (environment value, else default, else this file's value, else "") and
 * repeats. Its first-occurrence string `replace` is kept as is, quirks included.
 */
function interpolate(value: string, env: Env, parsed: Record<string, string>): string {
  const last = [...value.matchAll(/(?!(?<=\\))\$/g)].at(-1);
  if (last === undefined) return value;
  const match = value.slice(last.index).match(/((?!(?<=\\))\${?([\w]+)(?::-([^}\\]*))?}?)/);
  if (match === null) return value;
  const [, group = "", name = "", fallback] = match;
  const replacement = env[name] || fallback || parsed[name] || "";
  return interpolate(value.replace(group, replacement), env, parsed);
}

/** dotenv-expand 10.0.0 `expand`, with `env` standing in for process.env. */
function expand(parsed: Record<string, string>, env: Env): void {
  for (const key of Object.keys(parsed)) {
    // An environment value replaces the file's value before expansion.
    const value = (Object.hasOwn(env, key) ? env[key] : parsed[key]) as string;
    parsed[key] = interpolate(value, env, parsed).replace(/\\\$/g, "$");
  }
  Object.assign(env, parsed);
}

/**
 * TERMINAL_API_ORIGIN as `next build` sees it: @next/env 16.4.3 `loadEnvConfig(dir, false)`,
 * computed on copies so process.env is never changed. Like Next.js, it logs and skips an env file
 * it cannot read or expand, so the forwarded value stays the build's.
 */
function resolveApiOrigin(): string {
  const mode = process.env.NODE_ENV === "test" ? "test" : "production";
  const names = [
    `.env.${mode}.local`,
    ...(mode === "test" ? [] : [".env.local"]),
    `.env.${mode}`,
    ".env",
  ];
  const files: { name: string; contents: string }[] = [];
  for (const name of names) {
    // Playwright loads this config as CommonJS, so __dirname is the repository root whatever the
    // working directory is.
    const file = join(__dirname, name);
    try {
      const stats = statSync(file);
      if (!stats.isFile() && !stats.isFIFO()) continue;
      files.push({ name, contents: readFileSync(file, "utf8") });
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
        console.error(`Failed to load env from ${name}`, error);
      }
    }
  }
  const initial: Env = { ...process.env };
  // A parent Next.js process that has already loaded the env files sets __NEXT_PROCESSED_ENV.
  if (process.env.__NEXT_PROCESSED_ENV || files.length === 0) {
    return initial[API_ORIGIN_KEY] ?? DEFAULT_API_ORIGIN;
  }
  // process.env as `next build` expands into it: same prototype, the next CLI's NODE_ENV and
  // NEXT_RUNTIME defaults, and the __NEXT_PROCESSED_ENV marker set before the first file.
  const env: Env = Object.assign(
    Object.create(Object.getPrototypeOf(process.env) as object | null) as Env,
    initial,
    {
      NODE_ENV: process.env.NODE_ENV || "production",
      NEXT_RUNTIME: "nodejs",
      __NEXT_PROCESSED_ENV: "true",
    },
  );
  let fromFiles: string | undefined;
  for (const { name, contents } of files) {
    try {
      const parsed = parseDotenv(contents);
      expand(parsed, env);
      fromFiles ??= parsed[API_ORIGIN_KEY];
    } catch (error) {
      console.error(`Failed to load env from ${join(__dirname, name)}`, error);
    }
  }
  // A file supplies the key only when the process environment leaves it undefined; a defined
  // value is kept, expanded if a file also assigns the key.
  const resolved = initial[API_ORIGIN_KEY] === undefined ? fromFiles : env[API_ORIGIN_KEY];
  return resolved ?? DEFAULT_API_ORIGIN;
}

/**
 * The e2e upstream policy: `value` accepted and normalized as next.config.ts accepts it (an
 * absolute http(s) URL without userinfo, a query or a fragment; the origin plus any path prefix,
 * trailing slashes removed), and in addition required to name a loopback host on any port, so
 * neither test server can proxy to a remote API. It throws while this config loads, before either
 * web server starts. No message echoes the value: it may hold a password or a token.
 */
function requireLoopbackApiOrigin(value: string): string {
  const url = URL.canParse(value) ? new URL(value) : null;
  // The raw text is searched because a bare "?" or "#" leaves url.search and url.hash empty.
  if (url?.username || url?.password || /[?#]/.test(value)) {
    throw new Error(`${API_ORIGIN_KEY} must not carry userinfo, a query string or a fragment`);
  }
  // "localhost:8000" parses as a URL with the scheme "localhost:", so the scheme is checked too.
  if (url?.protocol !== "http:" && url?.protocol !== "https:") {
    throw new Error(`${API_ORIGIN_KEY} must be an absolute http(s) URL`);
  }
  // The parser lowercases hosts and rewrites 127.1 or 2130706433 as dotted decimal, so these
  // tests see canonical forms. foo.localhost, "localhost.", 0.0.0.0 and IPv4-mapped IPv6 are
  // refused: whether each reaches the loopback interface depends on the resolver or the OS.
  const host = url.hostname;
  if (host !== "localhost" && host !== "[::1]" && !/^127(?:\.\d{1,3}){3}$/.test(host)) {
    throw new Error(
      `${API_ORIGIN_KEY} must be a loopback origin for e2e runs: localhost, 127.0.0.0/8 or [::1]`,
    );
  }
  return `${url.origin}${url.pathname.replace(/\/+$/, "")}`;
}

const apiOrigin = requireLoopbackApiOrigin(resolveApiOrigin());

export default defineConfig({
  testDir: "./e2e",
  // The host fixture is an app under test, not a test folder.
  testIgnore: ["**/host-fixture/**"],
  testMatch: "**/*.spec.ts",
  timeout: 60_000,
  expect: { timeout: 10_000 },
  // Both projects read the same seeded database; one worker keeps the run order deterministic.
  fullyParallel: false,
  workers: 1,
  retries: process.env.CI ? 2 : 0,
  forbidOnly: !!process.env.CI,
  reporter: [["list"], ["html", { open: "never" }]],
  // The default testIdAttribute (data-testid) matches the widget roots' `terminal-<widget>` ids.
  use: {
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
  },
  projects: [
    {
      name: "standalone",
      testMatch: /terminal-views\.spec\.ts$/,
      use: { ...devices["Desktop Chrome"], baseURL: "http://localhost:3000" },
    },
    {
      name: "host-embed",
      testMatch: /host-embed\.spec\.ts$/,
      use: { ...devices["Desktop Chrome"], baseURL: "http://localhost:3100" },
    },
  ],
  webServer: [
    {
      // `next start` serves the existing production build. Its rewrites to the API were baked in
      // when `npm run build` evaluated next.config.ts, so that build must have used the same
      // TERMINAL_API_ORIGIN; passing it here does not re-point an older build. apiOrigin follows
      // the build's own precedence and normalization, so while the environment and env files are
      // unchanged since the build it equals the baked origin, which then passed the same loopback
      // check.
      command: "npm run start -- --port 3000",
      url: "http://localhost:3000",
      reuseExistingServer: !process.env.CI,
      timeout: 120_000,
      env: { TERMINAL_API_ORIGIN: apiOrigin },
    },
    {
      // Launched from the repository root; the fixture's own next.config.mjs compiles the widget
      // folder outside its project directory and rewrites /api/v1/terminal/* to TERMINAL_API_ORIGIN.
      // It never reads the root env files, so the same apiOrigin as the standalone server is its
      // only source, and its config enforces the same loopback rule when launched on its own.
      // `next dev` compiles on the first request, hence the longer timeout.
      command: "npx next dev e2e/host-fixture --port 3100",
      url: "http://localhost:3100",
      reuseExistingServer: !process.env.CI,
      timeout: 180_000,
      env: { TERMINAL_API_ORIGIN: apiOrigin },
    },
  ],
});
