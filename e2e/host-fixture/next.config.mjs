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

// Next.js reads .env* files from the project directory (e2e/host-fixture), never from the
// repository root, so this value comes from the process environment (playwright.config.ts
// forwards it) or the default. Trailing slashes are trimmed so the destination never holds "//api".
const apiOrigin = (process.env.TERMINAL_API_ORIGIN ?? "http://localhost:8000").replace(/\/+$/, "");

// An empty or relative value would turn the proxy into a rewrite onto this app itself. The value is
// not echoed, since an origin can carry credentials.
if (!/^https?:\/\/[^/?#]+/i.test(apiOrigin)) {
  throw new Error("TERMINAL_API_ORIGIN must be an absolute http(s) URL");
}

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
