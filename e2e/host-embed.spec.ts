/**
 * Host-embed proof: a foreign Next.js App Router app (`e2e/host-fixture`, port 3100) renders a
 * widget imported from `src/components/terminal/` with only the steps a host adds: its own layout
 * and CSS, a rewrite of `/api/v1/terminal/*` to the terminal API, and Tailwind 4's `@source` line.
 * No standalone layout, global CSS, provider or context is involved.
 *
 * Preconditions, none of which this spec provides:
 * 1. PostgreSQL is migrated (`opennse-terminal migrate upgrade head`) and seeded
 *    (`python -m opennse_terminal.seed.seed_terminal_db`), and the terminal API runs on :8000
 *    against it (`uvicorn opennse_terminal.main:app --port 8000`).
 * 2. `npm ci` is done: the host fixture resolves `next`, `react`, `react-dom`, `recharts` and
 *    `tailwindcss` from the repository root's `node_modules`.
 * 3. `npm run build` is done for the standalone app, with the same `TERMINAL_API_ORIGIN` as this
 *    run: playwright.config.ts starts both web servers even for `--project host-embed`, including
 *    `npm run start -- --port 3000`, which serves that build.
 * 4. Then `npm run test:e2e -- --project host-embed`. Playwright starts the fixture itself
 *    (`npx next dev e2e/host-fixture --port 3100`, see playwright.config.ts).
 *
 * The suite is read-only: its only API call is the widget's own `GET /macro/overview`, sent to
 * the host's origin. The host passes no `getHeaders` and the spec performs no login, because the
 * terminal's authentication is a host-injected dependency that is off by default.
 */
import { expect, test } from "@playwright/test";

const MACRO_OVERVIEW_PATH = "/api/v1/terminal/macro/overview";

/** The host-embed project's `baseURL` in playwright.config.ts. */
const HOST_ORIGIN = "http://localhost:3100";

/** Whole-test budget: `next dev` compiles the page and the out-of-root widget on first request. */
const TEST_TIMEOUT = 120_000;
/** Time for the first readiness signal, which absorbs that cold compile. */
const COLD_READY_TIMEOUT = 90_000;

/** Seeded current curve: 3 T-bill tenors plus 6 bond buckets. */
const MIN_CURRENT_POINTS = 9;
/** Default `compareMonthsAgo` of [6, 12], plus the current curve. */
const EXPECTED_SERIES = ["current", "m6", "m12"] as const;

// Uncaught page errors (hydration or runtime exceptions) fail the test. Console errors do not:
// `next dev` logs development noise that is not part of the embedding contract.
let pageErrors: Error[] = [];

test.beforeEach(async ({ page }) => {
  pageErrors = [];
  page.on("pageerror", (error) => {
    pageErrors.push(error);
  });
});

test.afterEach(() => {
  expect(pageErrors.map((error) => error.message)).toEqual([]);
});

test.describe("Host embed (e2e/host-fixture)", () => {
  test("renders YieldCurveChart inside a foreign host's own shell", async ({ page }) => {
    test.setTimeout(TEST_TIMEOUT);

    // Registered before navigation, so the widget's first request cannot be missed.
    const overviewResponse = page.waitForResponse(
      (response) => new URL(response.url()).pathname === MACRO_OVERVIEW_PATH,
      { timeout: COLD_READY_TIMEOUT },
    );

    await page.goto("/");

    // The shell is the host's own.
    await expect(page).toHaveTitle("Host Fixture");
    await expect(page.getByTestId("host-shell")).toBeVisible();
    await expect(page.getByTestId("host-header")).toBeVisible();
    await expect(page.getByTestId("host-header")).toContainText("Host Platform");
    const hostPage = page.getByTestId("host-page");
    await expect(hostPage).toBeVisible();

    // The widget renders from the imported folder, inside the host's page.
    const chart = hostPage.getByTestId("terminal-yield-curve-chart");
    await expect(chart).toHaveAttribute("data-state", "ready", { timeout: COLD_READY_TIMEOUT });

    // Tailwind reached the widget only through `@source`. The fixture's postcss.config.mjs pins
    // the `@tailwindcss/postcss` `base` to e2e/host-fixture, so automatic source detection cannot
    // see src/components/terminal/; the root's `bg-slate-900` is generated only because
    // app/globals.css declares `@source "../../../src/components/terminal";`. Without that line
    // the root has no background. This runs before the curve checks because a missing `@source`
    // also drops the chart's `h-64`, which collapses the chart and would otherwise surface as a
    // missing curve. The exact colour is not compared: Tailwind 4 emits `oklch(...)`.
    const background = await chart.evaluate((element) => getComputedStyle(element).backgroundColor);
    const missingSource =
      "the widget root has no Tailwind background: is the host's @source line in place?";
    // The two values getComputedStyle reports for an element with no background of its own.
    expect(background, missingSource).not.toBe("rgba(0, 0, 0, 0)");
    expect(background, missingSource).not.toBe("transparent");

    // Checked once the widget is ready, so nothing rendered late can slip past: the standalone
    // layout's navigation and title are absent.
    await expect(page.getByTestId(/^nav-/)).toHaveCount(0);
    await expect(page.getByText("OpenNSE Quant & Macro Terminal")).toHaveCount(0);

    // A rendered curve: the current curve and both comparison curves, drawn by Recharts.
    const series = chart.getByTestId("yield-curve-series");
    await expect(series).toHaveCount(EXPECTED_SERIES.length);
    for (const id of EXPECTED_SERIES) {
      await expect(series.and(page.locator(`[data-series="${id}"]`))).toHaveCount(1);
    }
    const current = series.and(page.locator('[data-series="current"]'));
    const currentPoints = Number.parseInt(
      (await current.getAttribute("data-point-count")) ?? "",
      10,
    );
    // NaN, from a missing or malformed attribute, fails this comparison too.
    expect(currentPoints).toBeGreaterThanOrEqual(MIN_CURRENT_POINTS);
    await expect(chart.locator("path.recharts-line-curve").first()).toBeVisible();

    // Same-origin proxy: the browser called the host, and the host's next.config.mjs rewrite
    // forwarded the request to the terminal API.
    const response = await overviewResponse;
    expect(response.status()).toBe(200);
    expect(new URL(response.url()).origin).toBe(HOST_ORIGIN);
  });
});
