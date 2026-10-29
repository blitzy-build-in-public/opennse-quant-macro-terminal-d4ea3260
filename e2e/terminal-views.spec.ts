/**
 * Standalone view rendering proof: every App Router page renders its `src/components/terminal/`
 * widgets from seeded data alone, before any scraper has run.
 *
 * Preconditions, none of which this spec provides:
 * 1. PostgreSQL is up: `docker compose up -d postgres`.
 * 2. The schema is migrated: `opennse-terminal migrate upgrade head`.
 * 3. The database is seeded: `python -m opennse_terminal.seed.seed_terminal_db`.
 * 4. The API runs against it: `uvicorn opennse_terminal.main:app --port 8000`.
 * 5. The standalone app is built: `npm run build`. `next.config.ts` rewrites
 *    `/api/v1/terminal/*` to `TERMINAL_API_ORIGIN` when it is evaluated at build time, so set that
 *    variable for the build if the API is not on the default `http://localhost:8000`.
 * 6. Then `npm run test:e2e`; playwright.config.ts serves the build on :3000 (project
 *    `standalone`).
 *
 * The suite is read-only against the database: it never creates, renames or deletes a watchlist
 * and never uploads a PDF. Its only POST is `/ipo/valuation`, which is stateless, so repeated
 * runs see the same seeded state. Every request goes to the app's own origin, which proxies the
 * terminal API; no external URL is involved.
 */
import { expect, test } from "@playwright/test";
import type { Locator, Page } from "@playwright/test";

/** Time for a widget fed by a light endpoint to reach `ready`. */
const READY_TIMEOUT = 20_000;
/** Time for the heavier cycle backtest and screener queries on a cold API. */
const SLOW_READY_TIMEOUT = 30_000;

const MACRO_OVERVIEW_PATH = "/api/v1/terminal/macro/overview";

/** U+2212, the minus sign `lib/format.ts` renders for negative values. */
const MINUS_SIGN = "\u2212";
/** U+2014, the placeholder `lib/format.ts` renders for a null value. */
const EMPTY_VALUE = "\u2014";

const ELECTION_DATES = ["2013-03-04", "2017-08-08", "2022-08-09", "2027-08-10"] as const;
const COMPLETED_ELECTION_DATES = ["2013-03-04", "2017-08-08", "2022-08-09"] as const;
const COMPLETED_CYCLE_LABELS = ["2013 Cycle", "2017 Cycle", "2022 Cycle"] as const;
const FUTURE_ELECTION_DATE = "2027-08-10";

const BANKING_TICKERS = ["ABSA", "COOP", "EQTY", "KCB", "SBIC"] as const;

/** The volume spikes the seeder plants and guarantees at z ≥ 4.00. */
const PLANTED_SPIKES = [
  { ticker: "SCOM", tradeDate: "2026-09-15" },
  { ticker: "KCB", tradeDate: "2026-09-24" },
  { ticker: "EABL", tradeDate: "2026-09-29" },
] as const;

async function expectReady(root: Locator, timeout: number = READY_TIMEOUT): Promise<void> {
  await expect(root).toHaveAttribute("data-state", "ready", { timeout });
}

/**
 * The first 2-decimal amount in a displayed string as an integer count of hundredths, for
 * example `"KES 6.82"` → 682 and `"−9.11%"` → −911. The digits are read as an integer string,
 * never parsed as a float, so the comparison is exact (Rule 4); displayed values are far below
 * 2^53 hundredths.
 */
function toCents(text: string | null): number {
  const match = /[\u2212-]?\d[\d,]*\.\d{2}/.exec(text ?? "");
  if (match === null) {
    throw new Error(`No 2-decimal amount in ${JSON.stringify(text)}`);
  }
  const digits = match[0].replace(MINUS_SIGN, "-").replace(/[,.]/g, "");
  return Number.parseInt(digits, 10);
}

/** An integer attribute such as `data-point-count`; NaN when it is absent, failing comparisons. */
async function readIntAttr(locator: Locator, name: string): Promise<number> {
  return Number.parseInt((await locator.getAttribute(name)) ?? "", 10);
}

/** Elements of `testId` inside `root` that also carry every given attribute value. */
function withAttributes(
  root: Locator,
  testId: string,
  attributes: Readonly<Record<string, string>>,
): Locator {
  const selector = Object.entries(attributes)
    .map(([name, value]) => `[${name}="${value}"]`)
    .join("");
  return root.getByTestId(testId).and(root.page().locator(selector));
}

// Uncaught page errors fail the test. Console errors do not: the cross-field IPO test receives a
// legitimate 422, which the browser logs as a failed resource.
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

test.describe("Macro and market overview (/)", () => {
  test("renders the four overview widgets from one shared macro request", async ({ page }) => {
    const overviewRequests: string[] = [];
    page.on("request", (request) => {
      if (new URL(request.url()).pathname === MACRO_OVERVIEW_PATH) {
        overviewRequests.push(request.url());
      }
    });

    await page.goto("/");

    const snapshot = page.getByTestId("terminal-macro-snapshot");
    const yieldCurve = page.getByTestId("terminal-yield-curve-chart");
    const inflation = page.getByTestId("terminal-inflation-trajectory-chart");
    const heatmap = page.getByTestId("terminal-sector-rotation-heatmap");
    for (const root of [snapshot, yieldCurve, inflation, heatmap]) {
      await expectReady(root);
    }

    // The seeded 2026-09-30 12-month inflation benchmark.
    await expect(snapshot.getByTestId("macro-tile-inflation_yoy_pct")).toContainText("6.80");

    // Current curve against 6 and 12 months ago; the current one holds the 3 T-bill tenors and
    // the 6 FXD bond buckets.
    const curves = yieldCurve.getByTestId("yield-curve-series");
    await expect(curves).toHaveCount(3);
    for (const id of ["current", "m6", "m12"]) {
      await expect(withAttributes(yieldCurve, "yield-curve-series", { "data-series": id })).toHaveCount(
        1,
      );
    }
    const currentCurve = withAttributes(yieldCurve, "yield-curve-series", {
      "data-series": "current",
    });
    await expect.poll(() => readIntAttr(currentCurve, "data-point-count")).toBeGreaterThanOrEqual(9);
    await expect
      .poll(() => yieldCurve.locator("path.recharts-line-curve").count())
      .toBeGreaterThanOrEqual(3);

    // The inflation line and the shaded 2.5–7.5% target band; 24 months are fully covered.
    const inflationLine = withAttributes(inflation, "inflation-legend-item", {
      "data-series": "inflation",
    });
    await expect.poll(() => readIntAttr(inflationLine, "data-point-count")).toBeGreaterThanOrEqual(1);
    await expect(
      withAttributes(inflation, "inflation-legend-item", { "data-series": "target_band" }),
    ).toHaveCount(1);
    await expect
      .poll(() => inflation.locator("path.recharts-line-curve").count())
      .toBeGreaterThanOrEqual(1);
    await expect
      .poll(() => inflation.locator(".recharts-reference-area").count())
      .toBeGreaterThanOrEqual(1);
    await expect(inflation.getByTestId("inflation-partial-coverage")).toHaveCount(0);

    // Four seeded sectors by 12 monthly periods, plus the market row.
    await expect(heatmap.getByTestId("heatmap-period")).toHaveCount(12);
    const sectorRows = heatmap.getByTestId("heatmap-row");
    await expect(sectorRows).toHaveCount(4);
    for (const row of await sectorRows.all()) {
      await expect(row.getByTestId("heatmap-cell")).toHaveCount(12);
    }
    await expect(heatmap.getByTestId("heatmap-market-row")).toHaveCount(1);

    // The three macro widgets request the identical URL, which the API client merges into one
    // in-flight GET. `next start` is a production server, so no StrictMode effect runs twice.
    expect(overviewRequests).toHaveLength(1);
    const overviewUrl = new URL(overviewRequests[0] ?? "", page.url());
    expect(overviewUrl.searchParams.get("history_months")).toBe("24");
  });
});

test.describe("Election and cycle backtesting (/cycles)", () => {
  test("overlays four cycles with markers, then projects the 2027 cycle at ±12 months", async ({
    page,
  }) => {
    // Two cold backtests can each take up to SLOW_READY_TIMEOUT.
    test.slow();
    await page.goto("/cycles");

    const backtester = page.getByTestId("terminal-cycle-backtester");
    await expectReady(backtester, SLOW_READY_TIMEOUT);

    // Default ±180-day window over the four seeded election dates.
    const legendItems = backtester.getByTestId("cycle-legend-item");
    await expect(legendItems).toHaveCount(4);
    for (const date of ELECTION_DATES) {
      await expect(
        withAttributes(backtester, "cycle-legend-item", { "data-event-date": date }),
      ).toHaveCount(1);
    }

    // 2027 has no session inside its window yet: it is drawn only as the projection, the mean of
    // the three completed cycles' normalized paths.
    const futureLegend = withAttributes(backtester, "cycle-legend-item", {
      "data-event-date": FUTURE_ELECTION_DATE,
    });
    await expect(futureLegend).toContainText("no observed sessions");
    const projectionLegend = withAttributes(backtester, "cycle-projection-legend", {
      "data-event-date": FUTURE_ELECTION_DATE,
    });
    await expect(projectionLegend).toBeVisible();
    await expect(projectionLegend).toContainText("Projection");
    await expect(projectionLegend).toContainText("mean of");
    for (const label of COMPLETED_CYCLE_LABELS) {
      await expect(projectionLegend).toContainText(label);
    }
    const mainChart = backtester.getByTestId("cycle-main-chart");
    await expect
      .poll(() => mainChart.locator("path.recharts-line-curve[stroke-dasharray]").count())
      .toBeGreaterThanOrEqual(1);

    // One valuation floor and one volume exhaustion per completed cycle; the awaiting 2027
    // cycle has no markers.
    await expect(backtester.getByTestId("marker-valuation-floor")).toHaveCount(3);
    await expect(backtester.getByTestId("marker-volume-exhaustion")).toHaveCount(3);
    for (const date of COMPLETED_ELECTION_DATES) {
      await expect(backtester.getByTestId(`cycle-status-${date}`)).toContainText(/complete/i);
    }
    await expect(backtester.getByTestId(`cycle-status-${FUTURE_ELECTION_DATE}`)).toContainText(
      /awaiting data/i,
    );

    // ±12 months opens the 2027 window on 2026-08-10, inside the seeded history, so the cycle
    // turns partial with an observed segment and keeps its projection. Marker counts are not
    // asserted here: a partial cycle may carry markers of its own.
    const twelveMonths = backtester.getByTestId("cycle-window-12m");
    await twelveMonths.click();
    await expect(twelveMonths).toHaveAttribute("aria-pressed", "true");
    await expect(backtester.getByTestId(`cycle-status-${FUTURE_ELECTION_DATE}`)).toContainText(
      /partial/i,
      { timeout: SLOW_READY_TIMEOUT },
    );
    await expectReady(backtester, SLOW_READY_TIMEOUT);
    await expect(legendItems).toHaveCount(4);
    await expect(futureLegend).toBeVisible();
    await expect(futureLegend).not.toContainText("no observed sessions");
    await expect(projectionLegend).toBeVisible();
    await expect(projectionLegend).toContainText("Projection");
  });
});

test.describe("IPO valuation sandbox (/ipo)", () => {
  /** Opens the sandbox and waits for the Quickmart preset's settled valuation. */
  async function openSandbox(page: Page): Promise<{
    sandbox: Locator;
    blended: Locator;
  }> {
    await page.goto("/ipo");
    const sandbox = page.getByTestId("terminal-ipo-valuation-sandbox");
    await expectReady(sandbox);
    const blended = sandbox.getByTestId("ipo-blended-fair-value");
    await expect(blended).toContainText("6.82");
    await expect(sandbox.getByTestId("ipo-recalculating")).toHaveCount(0);
    return { sandbox, blended };
  }

  test("values the Quickmart preset at KES 6.82 with a 5 × 7 sensitivity matrix", async ({
    page,
  }) => {
    const { sandbox, blended } = await openSandbox(page);

    expect(toCents(await blended.textContent())).toBe(682);
    await expect(sandbox.getByTestId("ipo-upside")).toContainText(`${MINUS_SIGN}9.11%`);

    // 5 WACC rows (16–20%) × 7 base-margin columns (3.0–6.0%); the preset's 18% × 4.5% cell is
    // the one outlined.
    const matrix = sandbox.getByTestId("terminal-sensitivity-matrix");
    await expect(matrix.getByTestId("sensitivity-cell")).toHaveCount(35);
    const selectedCell = withAttributes(matrix, "sensitivity-cell", { "data-selected": "true" });
    await expect(selectedCell).toHaveCount(1);
    expect(toCents(await selectedCell.getAttribute("data-wacc"))).toBe(1800);
    expect(toCents(await selectedCell.getAttribute("data-margin"))).toBe(450);

    await expect(sandbox.getByTestId("ipo-risk-scenario")).toHaveCount(3);
  });

  test("moving the WACC slider to 16.00% raises the blended fair value within 2 s", async ({
    page,
  }) => {
    const { sandbox, blended } = await openSandbox(page);

    // The slider holds hundredths of a percentage point with a 25-unit step, so eight
    // ArrowLeft presses move it from 1800 (18.00%) to 1600 (16.00%).
    const wacc = sandbox.getByTestId("ipo-input-wacc_pct");
    await expect(wacc).toHaveValue("1800");
    await wacc.focus();
    for (let press = 0; press < 8; press += 1) {
      await wacc.press("ArrowLeft");
    }
    await expect(wacc).toHaveValue("1600");
    await expect(sandbox.getByTestId("ipo-value-wacc_pct")).toContainText("16.00");

    await expect
      .poll(async () => toCents(await blended.textContent()), { timeout: 2_000 })
      .toBeGreaterThan(682);

    // The settled result is the server's valuation at 16.00%: the outlined matrix cell moves to
    // the 16% row and carries the same blended value.
    await expect(sandbox.getByTestId("ipo-recalculating")).toHaveCount(0);
    const selectedCell = withAttributes(sandbox, "sensitivity-cell", { "data-selected": "true" });
    await expect(selectedCell).toHaveCount(1);
    expect(toCents(await selectedCell.getAttribute("data-wacc"))).toBe(1600);
    expect(toCents(await selectedCell.textContent())).toBe(toCents(await blended.textContent()));

    await expect(sandbox).toHaveAttribute("data-state", "ready");
    await expect(sandbox.getByTestId("field-error-wacc_pct")).toHaveCount(0);
    await expect(sandbox.getByTestId("ipo-general-error")).toHaveCount(0);
  });

  test("primary proceeds above the offer size show the server's field error beside the input", async ({
    page,
  }) => {
    const { sandbox, blended } = await openSandbox(page);

    // KES 7.50 × 500,000,000 shares offered is 3,750,000,000.00; one cent more passes the
    // single-field check and is rejected by the server as primary_exceeds_offer_size.
    const primaryProceeds = sandbox.getByTestId("ipo-input-primary_proceeds_kes");
    const fieldError = sandbox.getByTestId("field-error-primary_proceeds_kes");
    await primaryProceeds.fill("3750000000.01");
    await expect(fieldError).toBeVisible();
    await expect(fieldError).toHaveText(/\S/);
    await expect(blended).toContainText("6.82");
    await expect(sandbox).toHaveAttribute("data-state", "ready");
    await expect(sandbox.getByTestId("ipo-general-error")).toHaveCount(0);

    await primaryProceeds.fill("0");
    await expect(fieldError).toHaveCount(0);
    await expect(sandbox.getByTestId("ipo-recalculating")).toHaveCount(0);
    await expect(blended).toContainText("6.82");
    await expect(sandbox).toHaveAttribute("data-state", "ready");
  });
});

test.describe("Custom watchlists and screener (/screener)", () => {
  test("lists eight counters, the sector summary and the seeded watchlist", async ({ page }) => {
    await page.goto("/screener");

    const screener = page.getByTestId("terminal-equity-screener");
    await expectReady(screener, SLOW_READY_TIMEOUT);
    await expect(screener.getByTestId("screener-row")).toHaveCount(8);

    // Banks have no meaningful enterprise value, so the Banking EV/EBITDA aggregate is null
    // while its P/E is computed.
    await expect(screener.getByTestId("sector-aggregate-row")).toHaveCount(4);
    const banking = withAttributes(screener, "sector-aggregate-row", { "data-sector": "Banking" });
    await expect(banking).toHaveCount(1);
    await expect(banking.locator('[data-column="ev_ebitda"] [data-part="value"]')).toHaveText(
      EMPTY_VALUE,
    );
    await expect(banking.locator('[data-column="pe"] [data-part="value"]')).not.toHaveText(
      EMPTY_VALUE,
    );

    await expect(
      screener.getByTestId("screener-filter-watchlist").locator("option", { hasText: "NSE Core 8" }),
    ).toHaveCount(1);

    const watchlists = page.getByTestId("terminal-watchlist-manager");
    await expectReady(watchlists);
    const coreWatchlist = withAttributes(watchlists, "watchlist-item", {
      "data-watchlist-name": "NSE Core 8",
    });
    await expect(coreWatchlist).toHaveCount(1);
    await expect(coreWatchlist.getByTestId("watchlist-ticker-chip")).toHaveCount(8);
  });

  test("the Banking sector filter leaves the five banks", async ({ page }) => {
    await page.goto("/screener");

    const screener = page.getByTestId("terminal-equity-screener");
    await expectReady(screener, SLOW_READY_TIMEOUT);
    const rows = screener.getByTestId("screener-row");
    await expect(rows).toHaveCount(8);

    // The select applies at once; there is no submit control.
    await screener.getByTestId("screener-filter-sector").selectOption({ label: "Banking" });
    await expect(rows).toHaveCount(5, { timeout: SLOW_READY_TIMEOUT });
    await expectReady(screener);
    const tickers = await rows.evaluateAll((elements) =>
      elements.map((element) => element.getAttribute("data-ticker") ?? ""),
    );
    expect(tickers.sort()).toEqual([...BANKING_TICKERS]);
  });

  test("the anomaly feed includes the three planted volume spikes", async ({ page }) => {
    await page.goto("/screener");

    const feed = page.getByTestId("terminal-anomaly-feed");
    await expectReady(feed, SLOW_READY_TIMEOUT);
    // An inclusion check, not an exact count: the seeder guarantees these three spikes but not
    // the absence of other background crossings of the threshold.
    for (const spike of PLANTED_SPIKES) {
      await expect(
        withAttributes(feed, "anomaly-item", {
          "data-ticker": spike.ticker,
          "data-trade-date": spike.tradeDate,
          "data-metric": "volume",
        }).first(),
      ).toBeVisible();
    }
  });

  test("selecting SCOM loads its valuation bands and net flows", async ({ page }) => {
    await page.goto("/screener");

    const bands = page.getByTestId("terminal-valuation-bands-chart");
    const flows = page.getByTestId("terminal-net-flow-chart");
    await expect(bands).toHaveAttribute("data-state", "empty");

    const screener = page.getByTestId("terminal-equity-screener");
    await expectReady(screener, SLOW_READY_TIMEOUT);
    // Scoped to the screener row: the anomaly feed lists SCOM as well.
    await withAttributes(screener, "screener-row", { "data-ticker": "SCOM" })
      .getByRole("button", { name: "SCOM", exact: true })
      .click();

    await expect(page.getByTestId("screener-selected-ticker")).toContainText("SCOM");
    await expectReady(bands, SLOW_READY_TIMEOUT);
    await expect(bands.getByTestId("bands-meta")).toContainText("SCOM");
    await expect(bands.getByTestId("bands-chart")).toBeVisible();

    // The market-wide flows stay on screen, marked "Updating…", until SCOM's arrive.
    await expectReady(flows, SLOW_READY_TIMEOUT);
    await expect(flows.getByText("Updating…")).toHaveCount(0, { timeout: SLOW_READY_TIMEOUT });
    await expect(flows.getByTestId("flows-coverage")).toContainText("100.00");
  });
});
