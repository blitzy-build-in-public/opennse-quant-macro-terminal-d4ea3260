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
 * terminal API; no external URL is involved. Some tests answer API requests themselves through
 * `page.route` with in-memory bodies, such as a valuation-bands multiple no seeded counter has,
 * or the empty results and injected outages of the widget-state tests. Those tests are read-only
 * against the database as well: a handler answers a request from memory or passes a GET request
 * through to the real API, and the widget-state handlers block every other method.
 */
import { expect, test } from "@playwright/test";
import type { Locator, Page } from "@playwright/test";

import { CHART_COLORS } from "../src/components/terminal/lib/chartTheme";
import type {
  AnomaliesResponse,
  BacktestResponse,
  FlowsResponse,
  IpoScenario,
  MacroOverviewResponse,
  ScreenerResponse,
  SectorHeatmapResponse,
  ValuationBandsResponse,
  Watchlist,
} from "../src/components/terminal/types";

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

    // The inflation line with the CBR line, and the shaded 2.5–7.5% target band; the 24 seeded
    // month-ends are fully covered. Each line is located by its own stroke colour, because any
    // `path.recharts-line-curve` alone would be satisfied by the CBR line with the inflation line
    // gone. The HTML legend's swatches are `<line>` elements, so they never match. A plotted
    // curve's `d` starts with a moveto and its coordinates; a multi-point one then draws on.
    const plottedCurve = /^M[^A-Za-z]+/;
    const multiPointCurve = /^M[^A-Za-z]+[LHVCSQTA]/;
    const inflationLegend = withAttributes(inflation, "inflation-legend-item", {
      "data-series": "inflation",
    });
    await expect.poll(() => readIntAttr(inflationLegend, "data-point-count")).toBe(24);
    const inflationCurve = inflation.locator(
      `path.recharts-line-curve[stroke="${CHART_COLORS.warning}"]`,
    );
    await expect(inflationCurve).toHaveCount(1);
    await expect(inflationCurve).toBeVisible();
    await expect(inflationCurve).toHaveAttribute("d", multiPointCurve);

    const cbrLegend = withAttributes(inflation, "inflation-legend-item", { "data-series": "cbr" });
    await expect.poll(() => readIntAttr(cbrLegend, "data-point-count")).toBeGreaterThanOrEqual(1);
    const cbrCurve = inflation.locator(`path.recharts-line-curve[stroke="${CHART_COLORS.accent}"]`);
    await expect(cbrCurve).toHaveCount(1);
    await expect(cbrCurve).toBeVisible();
    await expect(cbrCurve).toHaveAttribute("d", plottedCurve);

    await expect(
      withAttributes(inflation, "inflation-legend-item", { "data-series": "target_band" }),
    ).toHaveCount(1);
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

  /**
   * Three sectors by three months in which every displayed value can be told apart: each non-null
   * sector cell's relative return differs from its absolute return, and no sector value repeats a
   * market value. The values are chosen for that, not to satisfy relative = return − market, which
   * the zero relative return could not do without repeating the market's value. Banking holds
   * a positive, a negative and a zero relative return. Telecommunications lists its cells out of
   * order and has no cell for August. Manufacturing & Allied opens with a cell that has no
   * qualifying constituent, so both of its returns are null.
   */
  function distinguishableHeatmap(): SectorHeatmapResponse {
    const cell = (
      period_end: string,
      return_pct: string | null,
      relative_return_pct: string | null,
      constituents: number,
    ): SectorHeatmapResponse["market"][number] => ({
      period_end,
      return_pct,
      relative_return_pct,
      constituents,
    });
    return {
      as_of: "2026-09-30",
      interval: "month",
      period_ends: ["2026-07-31", "2026-08-31", "2026-09-30"],
      // The market's relative return is null, as the API sends it.
      market: [
        cell("2026-07-31", "1.10", null, 8),
        cell("2026-08-31", "-0.40", null, 8),
        cell("2026-09-30", "0.75", null, 8),
      ],
      sectors: [
        {
          sector: "Banking",
          cells: [
            cell("2026-07-31", "3.60", "2.50", 5),
            cell("2026-08-31", "-1.65", "-1.25", 5),
            cell("2026-09-30", "0.40", "0.00", 5),
          ],
        },
        {
          sector: "Telecommunications",
          cells: [cell("2026-09-30", "-5.20", "-6.10", 1), cell("2026-07-31", "6.05", "4.95", 1)],
        },
        {
          sector: "Manufacturing & Allied",
          cells: [
            cell("2026-07-31", null, null, 0),
            cell("2026-08-31", "-2.75", "-2.35", 2),
            cell("2026-09-30", "12.40", "11.65", 2),
          ],
        },
      ],
    };
  }

  test("toggles sector cells between relative and absolute returns without refetching", async ({
    page,
  }) => {
    const heatmapRequests: URL[] = [];
    await page.route("**/api/v1/terminal/market/sector-heatmap**", async (route) => {
      heatmapRequests.push(new URL(route.request().url()));
      await route.fulfill({ json: distinguishableHeatmap() });
    });

    await page.goto("/");
    const heatmap = page.getByTestId("terminal-sector-rotation-heatmap");
    await expectReady(heatmap);

    // One request, with the widget's default interval and period count.
    expect(heatmapRequests).toHaveLength(1);
    expect(heatmapRequests[0]?.searchParams.get("interval")).toBe("month");
    expect(heatmapRequests[0]?.searchParams.get("periods")).toBe("12");

    // One column per `period_ends` entry, in its order.
    const periods = heatmap.getByTestId("heatmap-period");
    await expect(periods).toHaveText(["Jul 2026", "Aug 2026", "Sep 2026"]);
    expect(
      await periods
        .locator("time")
        .evaluateAll((elements) => elements.map((element) => element.getAttribute("datetime"))),
    ).toEqual(["2026-07-31", "2026-08-31", "2026-09-30"]);

    // The expected display of every sector cell in column order, written out rather than derived
    // from the widget's formatter. A null value shows a hidden dash and the words "No data".
    const noData = `${EMPTY_VALUE}No data`;
    const expectedRows = [
      {
        sector: "Banking",
        relative: ["+2.50%", `${MINUS_SIGN}1.25%`, "0.00%"],
        absolute: ["+3.60%", `${MINUS_SIGN}1.65%`, "+0.40%"],
      },
      {
        sector: "Telecommunications",
        relative: ["+4.95%", noData, `${MINUS_SIGN}6.10%`],
        absolute: ["+6.05%", noData, `${MINUS_SIGN}5.20%`],
      },
      {
        sector: "Manufacturing & Allied",
        relative: [noData, `${MINUS_SIGN}2.35%`, "+11.65%"],
        absolute: [noData, `${MINUS_SIGN}2.75%`, "+12.40%"],
      },
    ] as const;
    const marketValues = ["+1.10%", `${MINUS_SIGN}0.40%`, "+0.75%"];
    const rowCells = (sector: string): Locator =>
      withAttributes(heatmap, "heatmap-row", { "data-sector": sector }).getByTestId("heatmap-cell");
    const expectSectorCells = async (mode: "relative" | "absolute"): Promise<void> => {
      for (const row of expectedRows) {
        await expect(rowCells(row.sector)).toHaveText([...row[mode]]);
      }
    };
    const marketCells = heatmap.getByTestId("heatmap-market-cell");
    const toggle = heatmap.getByTestId("heatmap-mode-toggle");
    const description = heatmap.getByTestId("heatmap-mode-description");

    const sectorRows = heatmap.getByTestId("heatmap-row");
    await expect(sectorRows).toHaveCount(3);
    expect(
      await sectorRows.evaluateAll((elements) =>
        elements.map((element) => element.getAttribute("data-sector")),
      ),
    ).toEqual(expectedRows.map((row) => row.sector));

    // Relative to the market by default.
    await expect(toggle).toHaveText("Absolute return");
    await expect(toggle).toHaveAttribute("aria-pressed", "false");
    await expect(description).toContainText("Showing: Relative to market");
    await expectSectorCells("relative");
    await expect(marketCells).toHaveText(marketValues);

    // Each cell is coloured by the value it shows: emerald above zero, rose below, slate at zero,
    // and a dashed outline on the card's surface where there is no value.
    const banking = rowCells("Banking");
    await expect(banking.nth(0)).toHaveClass(/(^|\s)bg-emerald-/);
    await expect(banking.nth(1)).toHaveClass(/(^|\s)bg-rose-/);
    await expect(banking.nth(2)).toHaveClass(/(^|\s)bg-slate-800(\s|$)/);
    const noConstituents = rowCells("Manufacturing & Allied").nth(0);
    await expect(noConstituents.locator('[aria-hidden="true"]')).toHaveText(EMPTY_VALUE);
    await expect(noConstituents.locator(".sr-only")).toHaveText("No data");
    await expect(noConstituents).toHaveClass(/(^|\s)outline-dashed(\s|$)/);
    await expect(noConstituents).toHaveAttribute(
      "title",
      `Return ${EMPTY_VALUE} · vs market ${EMPTY_VALUE} · 0 constituents`,
    );
    const missingPeriod = rowCells("Telecommunications").nth(1);
    await expect(missingPeriod).toHaveClass(/(^|\s)outline-dashed(\s|$)/);
    await expect(missingPeriod).toHaveAttribute("title", "No data for this period");

    // Absolute: every sector cell shows its own return, recoloured; the market row is unchanged.
    await toggle.click();
    await expect(toggle).toHaveAttribute("aria-pressed", "true");
    await expect(description).toContainText("Showing: Absolute return");
    await expectSectorCells("absolute");
    await expect(banking.nth(2)).toHaveClass(/(^|\s)bg-emerald-/);
    await expect(marketCells).toHaveText(marketValues);

    // And back to the relative returns.
    await toggle.click();
    await expect(toggle).toHaveAttribute("aria-pressed", "false");
    await expect(description).toContainText("Showing: Relative to market");
    await expectSectorCells("relative");
    await expect(banking.nth(2)).toHaveClass(/(^|\s)bg-slate-800(\s|$)/);
    await expect(marketCells).toHaveText(marketValues);

    // The mode is switched on the response already held, without another request.
    expect(heatmapRequests).toHaveLength(1);
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
  /**
   * An `ok` SCOM P/E body of 60 weekly values alternating between 100,000,000,000,000.00 and
   * 100,000,000,000,000.01 from 2025-08-01, with the statistics the API computes for them. Such a
   * near-constant 10^14 multiple once gave the multiple axis a 0.01 step whose tick indices passed
   * 2^53, where adding 1 no longer changes a float, so its tick loop never ended.
   */
  function nearConstantHighRatioBands(): ValuationBandsResponse {
    const weeks = 60;
    const firstFriday = Date.UTC(2025, 7, 1);
    const weekMs = 7 * 24 * 60 * 60 * 1000;
    const weekDate = (week: number): string =>
      new Date(firstFriday + week * weekMs).toISOString().slice(0, 10);
    const series = Array.from({ length: weeks }, (_, week) => ({
      date: weekDate(week),
      value: week % 2 === 0 ? "100000000000000.00" : "100000000000000.01",
    }));
    return {
      subject: "SCOM",
      subject_type: "ticker",
      metric: "pe",
      as_of: weekDate(weeks - 1),
      coverage: {
        requested_start: "2021-09-18",
        coverage_start: weekDate(0),
        is_partial_coverage: true,
      },
      status: "ok",
      flags: [],
      series,
      stats: {
        observations: weeks,
        mean: "100000000000000.01",
        stdev: "0.01",
        minus_2sd: "99999999999999.99",
        minus_1sd: "100000000000000.00",
        plus_1sd: "100000000000000.01",
        plus_2sd: "100000000000000.02",
        min: "100000000000000.00",
        p25: "100000000000000.00",
        median: "100000000000000.01",
        p75: "100000000000000.01",
        max: "100000000000000.01",
        current: "100000000000000.01",
        current_percentile: "100.00",
      },
    };
  }

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

  test("a near-constant 10^14 P/E still draws a few distinct, finite axis ticks", async ({
    page,
  }) => {
    await page.route("**/api/v1/terminal/market/valuation-bands**", async (route) => {
      await route.fulfill({ json: nearConstantHighRatioBands() });
    });
    await page.goto("/screener");

    const screener = page.getByTestId("terminal-equity-screener");
    await expectReady(screener, SLOW_READY_TIMEOUT);
    await withAttributes(screener, "screener-row", { "data-ticker": "SCOM" })
      .getByRole("button", { name: "SCOM", exact: true })
      .click();

    const bands = page.getByTestId("terminal-valuation-bands-chart");
    await expectReady(bands);
    await expect(bands.getByTestId("bands-meta")).toContainText("SCOM");
    const chart = bands.getByTestId("bands-chart");
    await expect(chart).toBeVisible();

    // Recharts 3 draws the tick labels in a z-index layer of their own, outside `.recharts-yAxis`.
    // Each is a grouped 2 dp decimal such as "100,000,000,000,000.00", read back without its
    // grouping to prove it is a finite number.
    const tickLabels = chart.locator(
      ".recharts-yAxis-tick-labels .recharts-cartesian-axis-tick-value",
    );
    await expect.poll(() => tickLabels.count()).toBeGreaterThanOrEqual(2);
    const labels = await tickLabels.allTextContents();
    expect(labels.length).toBeLessThanOrEqual(12);
    expect(new Set(labels).size).toBe(labels.length);
    for (const label of labels) {
      expect(label).toMatch(/^\u2212?\d{1,3}(?:,\d{3})*\.\d{2,}$/);
      expect(Number.isFinite(Number(label.replace(MINUS_SIGN, "-").replace(/,/g, "")))).toBe(true);
    }
  });
});

test.describe("Widget empty, error and Retry states", () => {
  /** Path prefix of the terminal API on the app's own origin. */
  const API_PREFIX = "/api/v1/terminal";
  /** WidgetFrame's empty-state copy for a widget that sets no message of its own. */
  const DEFAULT_EMPTY_COPY = "No data yet — run the seeder or an ingestion job";
  /**
   * Code of the injected outage. Any code but `no_event_dates`, which the backtester shows as its
   * empty state without a Retry.
   */
  const OUTAGE_CODE = "injected_outage";

  /** A widget root and its WidgetFrame title; the error state's Retry is named "Retry <title>". */
  interface WidgetRef {
    rootTestId: string;
    title: string;
  }

  /** One fetching widget, the GET its state follows, and what each of its states shows. */
  interface StateCase extends WidgetRef {
    /** Names the widget in the test titles and in its outage message. */
    label: string;
    /** The page the widget is on. */
    pagePath: string;
    /** Pathname of the widget's GET, compared exactly with each request URL's pathname. */
    endpoint: string;
    /** Time for each state to settle, and for the Retry request to be sent. */
    readyTimeout: number;
    /** Marks a case whose cold page needs more than the default test timeout. */
    slow?: boolean;
    /** Brings the loaded page to the point where the widget sends its GET. */
    prepare?: (page: Page) => Promise<void>;
    /** A body that satisfies the widget's own empty predicate, derived from the real body. */
    emptyBody: (real: unknown) => unknown;
    /** The explanation the empty state shows. */
    emptyCopy: string;
    /** Controls that stay on screen in the empty and error states. */
    retainedControls?: readonly string[];
    /** Controls that stay on screen but are `aria-disabled` until the widget shows data. */
    inertControls?: readonly string[];
    /** The other widgets fed by the same shared GET, whose states must stay their own. */
    sharedRequestWith?: readonly WidgetRef[];
    /** Asserts that the recovered widget shows real seeded data again. */
    expectRecovered: (root: Locator) => Promise<void>;
  }

  /** How the routed endpoint answers a GET. */
  type Mode = "empty" | "outage" | "pass";

  /** Types a case's empty transform by the endpoint's response body `T`. */
  function stateCase<T>({
    emptyBody,
    ...rest
  }: Omit<StateCase, "emptyBody"> & { emptyBody: (real: T) => T }): StateCase {
    // The real body is the endpoint's JSON, which `T` describes.
    return { ...rest, emptyBody: (real) => emptyBody(real as T) };
  }

  const MACRO_SNAPSHOT: WidgetRef = {
    rootTestId: "terminal-macro-snapshot",
    title: "Macro snapshot",
  };
  const YIELD_CURVE: WidgetRef = { rootTestId: "terminal-yield-curve-chart", title: "Yield curve" };
  const INFLATION_TRAJECTORY: WidgetRef = {
    rootTestId: "terminal-inflation-trajectory-chart",
    title: "Inflation trajectory",
  };

  /** Selects SCOM in the screener, which gives the valuation-bands chart a subject to request. */
  async function selectScom(page: Page): Promise<void> {
    const screener = page.getByTestId("terminal-equity-screener");
    await expectReady(screener, SLOW_READY_TIMEOUT);
    // Scoped to the screener row: the anomaly feed lists SCOM as well.
    await withAttributes(screener, "screener-row", { "data-ticker": "SCOM" })
      .getByRole("button", { name: "SCOM", exact: true })
      .click();
  }

  // The three macro widgets share one `/macro/overview` GET. Each empty body blanks only the
  // field the widget under test reads, so the other two must stay ready.
  const CASES: readonly StateCase[] = [
    stateCase<MacroOverviewResponse>({
      label: "MacroSnapshot",
      ...MACRO_SNAPSHOT,
      pagePath: "/",
      endpoint: MACRO_OVERVIEW_PATH,
      readyTimeout: READY_TIMEOUT,
      emptyBody: (real) => ({ ...real, latest: [] }),
      emptyCopy: DEFAULT_EMPTY_COPY,
      sharedRequestWith: [YIELD_CURVE, INFLATION_TRAJECTORY],
      expectRecovered: async (root) => {
        await expect(root.getByTestId("macro-tile-inflation_yoy_pct")).toContainText("6.80");
      },
    }),
    stateCase<MacroOverviewResponse>({
      label: "YieldCurveChart",
      ...YIELD_CURVE,
      pagePath: "/",
      endpoint: MACRO_OVERVIEW_PATH,
      readyTimeout: READY_TIMEOUT,
      emptyBody: (real) => ({ ...real, yield_curve: { ...real.yield_curve, current: [] } }),
      emptyCopy: DEFAULT_EMPTY_COPY,
      sharedRequestWith: [MACRO_SNAPSHOT, INFLATION_TRAJECTORY],
      expectRecovered: async (root) => {
        const current = withAttributes(root, "yield-curve-series", { "data-series": "current" });
        await expect.poll(() => readIntAttr(current, "data-point-count")).toBeGreaterThanOrEqual(9);
      },
    }),
    stateCase<MacroOverviewResponse>({
      label: "InflationTrajectoryChart",
      ...INFLATION_TRAJECTORY,
      pagePath: "/",
      endpoint: MACRO_OVERVIEW_PATH,
      readyTimeout: READY_TIMEOUT,
      // Empty only when neither the inflation line nor the CBR line has a point.
      emptyBody: (real) => ({ ...real, inflation_series: [], rates_series: [] }),
      emptyCopy: DEFAULT_EMPTY_COPY,
      sharedRequestWith: [MACRO_SNAPSHOT, YIELD_CURVE],
      expectRecovered: async (root) => {
        const line = withAttributes(root, "inflation-legend-item", { "data-series": "inflation" });
        await expect.poll(() => readIntAttr(line, "data-point-count")).toBe(24);
      },
    }),
    stateCase<SectorHeatmapResponse>({
      label: "SectorRotationHeatmap",
      rootTestId: "terminal-sector-rotation-heatmap",
      title: "Sector rotation",
      pagePath: "/",
      endpoint: `${API_PREFIX}/market/sector-heatmap`,
      readyTimeout: READY_TIMEOUT,
      emptyBody: (real) => ({ ...real, period_ends: [], sectors: [], market: [] }),
      emptyCopy: DEFAULT_EMPTY_COPY,
      inertControls: ["heatmap-mode-toggle"],
      expectRecovered: async (root) => {
        await expect(root.getByTestId("heatmap-period")).toHaveCount(12);
        await expect(root.getByTestId("heatmap-row")).toHaveCount(4);
      },
    }),
    stateCase<BacktestResponse>({
      label: "CycleBacktester",
      rootTestId: "terminal-cycle-backtester",
      title: "Election & cycle backtester",
      pagePath: "/cycles",
      endpoint: `${API_PREFIX}/cycles/backtest`,
      readyTimeout: SLOW_READY_TIMEOUT,
      slow: true,
      emptyBody: (real) => ({ ...real, cycles: [] }),
      emptyCopy: DEFAULT_EMPTY_COPY,
      retainedControls: ["cycle-window-180d", "cycle-window-12m", "cycle-metric-price_index"],
      expectRecovered: async (root) => {
        await expect(root.getByTestId("cycle-legend-item")).toHaveCount(4);
        for (const date of COMPLETED_ELECTION_DATES) {
          await expect(root.getByTestId(`cycle-status-${date}`)).toContainText(/complete/i);
        }
      },
    }),
    stateCase<IpoScenario[]>({
      label: "IpoValuationSandbox",
      rootTestId: "terminal-ipo-valuation-sandbox",
      title: "IPO valuation sandbox",
      pagePath: "/ipo",
      endpoint: `${API_PREFIX}/ipo/scenarios`,
      readyTimeout: READY_TIMEOUT,
      emptyBody: () => [],
      emptyCopy: DEFAULT_EMPTY_COPY,
      // The recovered preset is valued by `POST /ipo/valuation`, which reaches the stateless
      // API unrouted.
      expectRecovered: async (root) => {
        await expect(root.getByTestId("ipo-blended-fair-value")).toContainText("6.82");
      },
    }),
    stateCase<ScreenerResponse>({
      label: "EquityScreener",
      rootTestId: "terminal-equity-screener",
      title: "Screener",
      pagePath: "/screener",
      endpoint: `${API_PREFIX}/market/screener`,
      readyTimeout: SLOW_READY_TIMEOUT,
      // No sector data either, so the copy is the default rather than the no-match message.
      emptyBody: (real) => ({ ...real, count: 0, rows: [], sector_aggregates: [] }),
      emptyCopy: DEFAULT_EMPTY_COPY,
      retainedControls: [
        "screener-filter-sector",
        "screener-filter-watchlist",
        "screener-filter-anomalies_only",
      ],
      expectRecovered: async (root) => {
        await expect(root.getByTestId("screener-row")).toHaveCount(8);
      },
    }),
    stateCase<Watchlist[]>({
      label: "WatchlistManager",
      rootTestId: "terminal-watchlist-manager",
      title: "Watchlists",
      pagePath: "/screener",
      // The screener's own GET of the same URL may be merged with the manager's.
      endpoint: `${API_PREFIX}/watchlists`,
      readyTimeout: SLOW_READY_TIMEOUT,
      emptyBody: () => [],
      emptyCopy: "No watchlists yet — create one above",
      retainedControls: [
        "watchlist-name-input",
        "watchlist-tickers-input",
        "watchlist-create-button",
      ],
      expectRecovered: async (root) => {
        const core = withAttributes(root, "watchlist-item", {
          "data-watchlist-name": "NSE Core 8",
        });
        await expect(core).toHaveCount(1);
        await expect(core.getByTestId("watchlist-ticker-chip")).toHaveCount(8);
      },
    }),
    stateCase<AnomaliesResponse>({
      label: "AnomalyFeed",
      rootTestId: "terminal-anomaly-feed",
      title: "Volume & turnover spikes",
      pagePath: "/screener",
      endpoint: `${API_PREFIX}/market/anomalies`,
      readyTimeout: SLOW_READY_TIMEOUT,
      emptyBody: (real) => ({ ...real, items: [] }),
      emptyCopy:
        "No spikes in the latest sessions. With no data loaded yet, run the seeder or an ingestion job.",
      expectRecovered: async (root) => {
        for (const spike of PLANTED_SPIKES) {
          await expect(
            withAttributes(root, "anomaly-item", {
              "data-ticker": spike.ticker,
              "data-trade-date": spike.tradeDate,
              "data-metric": "volume",
            }).first(),
          ).toBeVisible();
        }
      },
    }),
    stateCase<ValuationBandsResponse>({
      label: "ValuationBandsChart",
      rootTestId: "terminal-valuation-bands-chart",
      // The title names the requested subject, so it reads the same before any response.
      title: "P/E bands — SCOM",
      pagePath: "/screener",
      endpoint: `${API_PREFIX}/market/valuation-bands`,
      readyTimeout: SLOW_READY_TIMEOUT,
      // The cold screener loads before the bands request is sent.
      slow: true,
      prepare: selectScom,
      // Only an `ok` result without values is empty; any other status is explained inline.
      emptyBody: (real) => ({ ...real, status: "ok", series: [] }),
      // With a subject selected, the default copy replaces the "select a counter" prompt.
      emptyCopy: DEFAULT_EMPTY_COPY,
      expectRecovered: async (root) => {
        await expect(root.getByTestId("bands-meta")).toContainText("SCOM");
        await expect(root.getByTestId("bands-chart")).toBeVisible();
      },
    }),
    stateCase<FlowsResponse>({
      label: "NetFlowChart",
      rootTestId: "terminal-net-flow-chart",
      title: "Foreign vs local net flow — Market",
      pagePath: "/screener",
      endpoint: `${API_PREFIX}/market/flows`,
      readyTimeout: SLOW_READY_TIMEOUT,
      emptyBody: (real) => ({ ...real, series: [] }),
      emptyCopy: DEFAULT_EMPTY_COPY,
      expectRecovered: async (root) => {
        await expect(root.getByTestId("flows-coverage")).toContainText("100.00");
        await expect(root.getByTestId("flows-chart")).toBeVisible();
      },
    }),
  ];

  /** The outage message unique to `c`, which its alert must show verbatim. */
  function outageMessage(c: StateCase): string {
    return `Injected outage: ${c.label}`;
  }

  /**
   * WidgetFrame's own status and alert regions. They are direct children of the root, because
   * some widgets keep further status regions in their header or controls.
   */
  function statusRegion(root: Locator): Locator {
    return root.locator(":scope > [role='status']");
  }

  function errorAlert(root: Locator): Locator {
    return root.locator(":scope > [role='alert']");
  }

  /**
   * Routes every request whose pathname is `c.endpoint`. A GET is answered as `mode()` says at
   * that moment: from the real body through the empty transform, as a coded 503, or by the real
   * API. Any other method is blocked, so the database is never written. Returns the number of
   * GETs routed so far.
   */
  async function routeEndpoint(page: Page, c: StateCase, mode: () => Mode): Promise<() => number> {
    let gets = 0;
    await page.route(
      (url) => url.pathname === c.endpoint,
      async (route) => {
        if (route.request().method() !== "GET") {
          await route.abort("blockedbyclient");
          return;
        }
        gets += 1;
        switch (mode()) {
          case "empty": {
            const response = await route.fetch();
            await route.fulfill({ response, json: c.emptyBody(await response.json()) });
            return;
          }
          case "outage":
            await route.fulfill({
              status: 503,
              json: { detail: { code: OUTAGE_CODE, message: outageMessage(c) } },
            });
            return;
          case "pass":
            await route.fallback();
            return;
        }
      },
    );
    return () => gets;
  }

  /**
   * The retained controls are on screen; so are the inert ones, `aria-disabled` exactly while
   * the widget has no data to act on.
   */
  async function expectControls(root: Locator, c: StateCase, showsData: boolean): Promise<void> {
    for (const testId of c.retainedControls ?? []) {
      await expect(root.getByTestId(testId)).toBeVisible();
    }
    for (const testId of c.inertControls ?? []) {
      const control = root.getByTestId(testId);
      await expect(control).toBeVisible();
      if (showsData) {
        await expect(control).not.toHaveAttribute("aria-disabled", "true");
      } else {
        await expect(control).toHaveAttribute("aria-disabled", "true");
      }
    }
  }

  /** `widget` shows the outage message and a Retry named after its own title. */
  async function expectOutage(page: Page, widget: WidgetRef, message: string): Promise<void> {
    const root = page.getByTestId(widget.rootTestId);
    await expect(root).toHaveAttribute("data-state", "error");
    const alert = errorAlert(root);
    await expect(alert.locator("p")).toHaveText(message);
    const retry = alert.getByTestId("widget-retry");
    await expect(retry).toBeVisible();
    await expect(retry).toHaveAccessibleName(`Retry ${widget.title}`);
  }

  // A handler still fetching a real body when its test ends must not fail the test.
  test.afterEach(async ({ page }) => {
    await page.unrouteAll({ behavior: "ignoreErrors" });
  });

  for (const c of CASES) {
    test(`${c.label} — empty result`, async ({ page }) => {
      if (c.slow) test.slow();
      const gets = await routeEndpoint(page, c, () => "empty");
      await page.goto(c.pagePath);
      await c.prepare?.(page);

      const root = page.getByTestId(c.rootTestId);
      // The empty state comes from the transformed body, not from a request never sent.
      await expect.poll(gets, { timeout: c.readyTimeout }).toBeGreaterThanOrEqual(1);
      await expect(root).toHaveAttribute("data-state", "empty", { timeout: c.readyTimeout });
      const copy = statusRegion(root).locator("p");
      await expect(copy).toHaveText(c.emptyCopy);
      await expect(copy).toBeVisible();
      await expect(root.getByTestId("widget-retry")).toHaveCount(0);
      await expect(errorAlert(root)).toHaveCount(0);
      await expectControls(root, c, false);

      // Each widget fed by the shared GET judges emptiness by its own field.
      for (const other of c.sharedRequestWith ?? []) {
        await expectReady(page.getByTestId(other.rootTestId), c.readyTimeout);
      }
    });

    test(`${c.label} — error, then Retry recovers`, async ({ page }) => {
      if (c.slow) test.slow();
      const message = outageMessage(c);
      let mode: Mode = "outage";
      const gets = await routeEndpoint(page, c, () => mode);
      await page.goto(c.pagePath);
      await c.prepare?.(page);

      const root = page.getByTestId(c.rootTestId);
      await expect(root).toHaveAttribute("data-state", "error", { timeout: c.readyTimeout });
      // The coded body's message, as the API client normalizes it.
      await expectOutage(page, c, message);
      await expectControls(root, c, false);
      // One failed shared response fails every widget it feeds.
      for (const other of c.sharedRequestWith ?? []) {
        await expectOutage(page, other, message);
      }

      mode = "pass";
      const before = gets();
      await errorAlert(root).getByTestId("widget-retry").click();
      // Retry sends a new GET rather than replaying the failed result.
      await expect
        .poll(() => gets() - before, { timeout: c.readyTimeout })
        .toBeGreaterThanOrEqual(1);
      await expectReady(root, c.readyTimeout);
      await expect(errorAlert(root)).toHaveCount(0);
      await expect(root.getByTestId("widget-retry")).toHaveCount(0);
      await c.expectRecovered(root);
      await expectControls(root, c, true);

      // Retry refetches its own widget only: the shared GET is sent once more, and the widgets
      // it did not refetch keep their error and their own Retry.
      if (c.sharedRequestWith !== undefined) {
        for (const other of c.sharedRequestWith) {
          await expectOutage(page, other, message);
        }
        expect(gets() - before).toBe(1);
      }
    });
  }
});
