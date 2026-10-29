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
 */
import { defineConfig, devices } from "@playwright/test";

const apiOrigin = process.env.TERMINAL_API_ORIGIN ?? "http://localhost:8000";

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
      // TERMINAL_API_ORIGIN; passing it here does not re-point an older build.
      command: "npm run start -- --port 3000",
      url: "http://localhost:3000",
      reuseExistingServer: !process.env.CI,
      timeout: 120_000,
      env: { TERMINAL_API_ORIGIN: apiOrigin },
    },
    {
      // Launched from the repository root; the fixture's own next.config.mjs compiles the widget
      // folder outside its project directory and rewrites /api/v1/terminal/* to TERMINAL_API_ORIGIN.
      // `next dev` compiles on the first request, hence the longer timeout.
      command: "npx next dev e2e/host-fixture --port 3100",
      url: "http://localhost:3100",
      reuseExistingServer: !process.env.CI,
      timeout: 180_000,
      env: { TERMINAL_API_ORIGIN: apiOrigin },
    },
  ],
});
