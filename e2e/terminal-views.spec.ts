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
 * The suite is read-only against the database: no watchlist write reaches the API and no PDF is
 * uploaded. The only POST the API receives is `/ipo/valuation`, which is stateless, so repeated
 * runs see the same seeded state. Every request goes to the app's own origin, which proxies the
 * terminal API; no external URL is involved. Some tests answer API requests themselves through
 * `page.route`, and every such handler is read-only against the database as well: it answers a
 * request from memory, passes a GET request through to the real API, or passes the stateless
 * `/ipo/valuation` POST through.
 * - The watchlist tests answer every `/watchlists` request from an in-memory store, which reads
 *   the seeded lists once through `GET /watchlists` and never forwards a request.
 * - A screener request filtered by one of those in-memory lists is sent to the API without its
 *   `watchlist_id` and answered with that list's rows only.
 * - The anomalies-only request is answered by the API for 2026-09-29, the planted EABL spike's
 *   session, because the latest seeded session has no spike.
 * - One test answers the valuation-bands request with a multiple no seeded counter has.
 * - The widget-state tests answer with empty results and injected outages, and their handlers
 *   block every other method.
 * - One cycle test blanks overlay rates in a fetched backtest response before the page receives
 *   it.
 * - The IPO tests hold or answer valuation requests, some with responses derived from the
 *   server's own, let the sandbox abort the held ones it no longer needs, and drive its debounce
 *   with Playwright's browser clock.
 */
import { expect, test } from "@playwright/test";
import type { Locator, Page, Request, Response, Route } from "@playwright/test";

import { CHART_COLORS } from "../src/components/terminal/lib/chartTheme";
import type {
  AnomaliesResponse,
  BacktestResponse,
  CyclePoint,
  CycleResult,
  DecimalString,
  FlowsResponse,
  IpoScenario,
  IpoValuationResponse,
  MacroOverviewResponse,
  ScreenerResponse,
  ScreenerRow,
  SectorHeatmapResponse,
  ValuationBandsResponse,
  Watchlist,
} from "../src/components/terminal/types";

/** Time for a widget fed by a light endpoint to reach `ready`. */
const READY_TIMEOUT = 20_000;
/** Time for the heavier cycle backtest and screener queries on a cold API. */
const SLOW_READY_TIMEOUT = 30_000;

const MACRO_OVERVIEW_PATH = "/api/v1/terminal/macro/overview";
const FLOWS_PATH = "/api/v1/terminal/market/flows";
/** Path of `POST /ipo/valuation`, the only POST the sandbox sends. */
const IPO_VALUATION_PATH = "/api/v1/terminal/ipo/valuation";
/** The valuation endpoint as a `page.route` pattern. */
const IPO_VALUATION_ROUTE = `**${IPO_VALUATION_PATH}`;

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

/**
 * A decimal string reduced to its sign and digits, the form in which two decimals are compared:
 * leading zeros of the integer part dropped, the fraction kept as written and no sign on zero,
 * so `"-0240111043.36"` becomes `"-240111043.36"`. Throws on text that is not a plain decimal.
 */
function canonicalDecimal(value: string): string {
  const match = /^([+-]?)(\d+)(?:\.(\d+))?$/.exec(value);
  if (match === null) {
    throw new Error(`Not a decimal string: ${JSON.stringify(value)}`);
  }
  const integer = (match[2] ?? "").replace(/^0+(?=\d)/, "");
  const digits = match[3] === undefined ? integer : `${integer}.${match[3]}`;
  return match[1] === "-" && /[1-9]/.test(digits) ? `-${digits}` : digits;
}

/**
 * The first number of a displayed text, such as `"KES −1,234.50"` or `"+6.78×"`, in
 * {@link canonicalDecimal} form: grouping commas dropped, U+2212 read as the minus sign and `+`
 * dropped. An ASCII hyphen is not the displayed minus, so it is not read as one. `null` when the
 * text holds no number, as for the em dash of a null value.
 */
function displayedDecimal(text: string | null): string | null {
  const match = /([+\u2212]?)(\d{1,3}(?:,\d{3})+|\d+)(\.\d+)?/.exec(text ?? "");
  if (match === null) {
    return null;
  }
  const sign = match[1] === MINUS_SIGN ? "-" : "";
  return canonicalDecimal(sign + (match[2] ?? "").replace(/,/g, "") + (match[3] ?? ""));
}

/** A decimal string as an integer count of 10^-scale units, exact at any magnitude. */
function scaledDecimal(value: string, scale: number): bigint {
  const canonical = canonicalDecimal(value);
  const [integer = "0", fraction = ""] = canonical.replace("-", "").split(".");
  const magnitude = BigInt(integer + fraction.padEnd(scale, "0"));
  return canonical.startsWith("-") ? -magnitude : magnitude;
}

/** −1, 0 or 1 as decimal string `left` is below, equal to or above `right`, compared exactly. */
function compareDecimals(left: string, right: string): number {
  const fractionLength = (value: string): number => value.split(".")[1]?.length ?? 0;
  const scale = Math.max(fractionLength(left), fractionLength(right));
  const a = scaledDecimal(left, scale);
  const b = scaledDecimal(right, scale);
  if (a === b) {
    return 0;
  }
  return a < b ? -1 : 1;
}

const MONTH_ABBREVIATIONS = [
  "Jan",
  "Feb",
  "Mar",
  "Apr",
  "May",
  "Jun",
  "Jul",
  "Aug",
  "Sep",
  "Oct",
  "Nov",
  "Dec",
] as const;

/** An ISO date as the widgets display it: `"2025-09-30"` → `"30 Sep 2025"`. */
function displayedDate(iso: string): string {
  const [year = "", month = "", day = ""] = iso.split("-");
  const monthName = MONTH_ABBREVIATIONS[Number.parseInt(month, 10) - 1];
  if (monthName === undefined) {
    throw new Error(`Not an ISO date: ${JSON.stringify(iso)}`);
  }
  return `${Number.parseInt(day, 10)} ${monthName} ${year}`;
}

/** Whether `url` is a `GET /market/flows` for `ticker`, or for the whole market when `null`. */
function isFlowsUrl(url: string, ticker: string | null): boolean {
  const parsed = new URL(url);
  return (
    parsed.pathname === FLOWS_PATH &&
    parsed.searchParams.get("ticker") === ticker &&
    !parsed.searchParams.has("sector")
  );
}

/**
 * Asserts that the net-flow widget shows `body`, read back from the page: the subject the chart
 * surface names, the plotted bars and cumulative line, the range, the coverage, both totals and
 * the session counts. Every number is compared exactly as a decimal string (Rule 4), never as a
 * float; each read is retried until it matches, since the previous subject's flows stay on screen
 * while a new one loads.
 */
async function expectFlowsReadouts(flows: Locator, body: FlowsResponse): Promise<void> {
  const totals = body.totals;
  if (totals === null) {
    throw new Error(`The ${body.subject} flows have no totals to compare`);
  }
  const range = `${displayedDate(body.start)} – ${displayedDate(body.end)}`;
  const subject = body.subject === "market" ? "Market" : body.subject;
  const chart = flows.getByTestId("flows-chart");
  await expect(chart.locator("svg.recharts-surface > title")).toHaveText(
    `Foreign net flow — ${subject}, ${range}`,
  );
  await expect(flows.getByTestId("flows-range")).toContainText(range);
  const readDecimal = (testId: string) => async (): Promise<string | null> =>
    displayedDecimal(await flows.getByTestId(testId).textContent());
  await expect.poll(readDecimal("flows-coverage")).toBe(canonicalDecimal(body.coverage_pct));
  await expect
    .poll(readDecimal("flows-total-foreign"))
    .toBe(canonicalDecimal(totals.foreign_net_kes));
  await expect.poll(readDecimal("flows-total-local")).toBe(canonicalDecimal(totals.local_net_kes));
  await expect
    .poll(async () => {
      const text = (await flows.getByTestId("flows-sessions").textContent()) ?? "";
      const match = /^([\d,]+) \/ ([\d,]+)$/.exec(text.trim());
      return match === null ? text : [match[1], match[2]].map((n) => n?.replace(/,/g, ""));
    })
    .toEqual([String(totals.sessions_with_data), String(totals.sessions_in_range)]);

  // Plotted data: at least one drawn daily bar and a cumulative line through several sessions.
  await expect(chart.locator(".recharts-bar-rectangle path").first()).toHaveAttribute("d", /\d/);
  const line = chart.locator("path.recharts-line-curve");
  await expect(line).toHaveCount(1);
  await expect(line).toHaveAttribute("d", /^M[^L]+L[^L]+L/);
}

// Uncaught page errors fail the test. Console errors do not: several tests involve legitimate 4xx
// and 5xx responses and aborted requests, such as the IPO field-error and lifecycle tests, which the
// browser logs as failed resources.
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
  const BACKTEST_PATH = "/api/v1/terminal/cycles/backtest";
  /**
   * px within which two path coordinates count as equal. Recharts writes them with 3 decimals, so
   * the difference of two rounded coordinates is off by at most 0.001.
   */
  const PIXEL_TOLERANCE = 0.01;
  /** Fraction digits kept when a decimal string is read as a scaled integer. */
  const DECIMAL_SCALE = 6;

  type Metric = "price_index" | "pe_index" | "turnover_index" | "cumulative_foreign_net_kes";
  /** The metrics a projection carries: it has no cumulative foreign flow. */
  type IndexMetric = Exclude<Metric, "cumulative_foreign_net_kes">;
  type OverlayField = "inflation_yoy_pct" | "tbill_91d_pct" | "tbill_364d_pct";
  type LineSeries = "observed" | "projection";

  /** One value a line plots, at its calendar offset from the event; null draws no vertex. */
  interface SeriesValue {
    offset: number;
    value: DecimalString | null;
  }

  /** A vertex of a drawn path, in the chart's SVG pixel coordinates. */
  interface Vertex {
    x: number;
    y: number;
  }

  /** Every backtest request the page sends, in order. */
  function recordBacktestRequests(page: Page): URL[] {
    const requests: URL[] = [];
    page.on("request", (request) => {
      const url = new URL(request.url());
      if (url.pathname === BACKTEST_PATH) requests.push(url);
    });
    return requests;
  }

  /**
   * The next successful backtest response whose query passes `matches`, with that query and its
   * body. Call it before the action that sends the request.
   */
  async function nextBacktest(
    page: Page,
    matches: (query: URLSearchParams) => boolean,
  ): Promise<{ query: URLSearchParams; body: BacktestResponse }> {
    const response = await page.waitForResponse(
      (candidate) => {
        const url = new URL(candidate.url());
        return url.pathname === BACKTEST_PATH && matches(url.searchParams);
      },
      { timeout: SLOW_READY_TIMEOUT },
    );
    expect(response.status()).toBe(200);
    return {
      query: new URL(response.url()).searchParams,
      body: (await response.json()) as BacktestResponse,
    };
  }

  /** Asserts that `query` sends the window once, in `unit` only, `size` on both sides. */
  function expectWindowQuery(query: URLSearchParams, unit: "days" | "months", size: string): void {
    const other = unit === "days" ? "months" : "days";
    expect(query.getAll(`pre_${unit}`)).toEqual([size]);
    expect(query.getAll(`post_${unit}`)).toEqual([size]);
    expect(query.has(`pre_${other}`)).toBe(false);
    expect(query.has(`post_${other}`)).toBe(false);
  }

  function cycleOf(body: BacktestResponse, eventDate: string): CycleResult {
    const cycle = body.cycles.find((candidate) => candidate.event_date === eventDate);
    if (cycle === undefined) throw new Error(`The backtest response has no ${eventDate} cycle`);
    return cycle;
  }

  /** The `field` value of each of the cycle's sessions. */
  function observedSeries(cycle: CycleResult, field: Metric | OverlayField): SeriesValue[] {
    return cycle.points.map((point: CyclePoint) => ({
      offset: point.offset_days,
      value: point[field],
    }));
  }

  /** The `metric` value at each offset of the cycle's projection; [] without one. */
  function projectionSeries(cycle: CycleResult, metric: IndexMetric): SeriesValue[] {
    return (cycle.projection?.points ?? []).map((point) => ({
      offset: point.offset_days,
      value: point[metric],
    }));
  }

  /** The main-chart path drawn for `eventDate`'s observed sessions or its projection. */
  function cycleLine(chart: Locator, eventDate: string, series: LineSeries): Locator {
    return withAttributes(chart, "cycle-line", {
      "data-event-date": eventDate,
      "data-series": series,
    });
  }

  /**
   * The vertices of a path Recharts draws with its linear curve, `M x,y` then `L x,y` per value;
   * [] for a path without geometry. They are pixel coordinates, never displayed values.
   */
  function pathVertices(d: string | null): Vertex[] {
    return Array.from((d ?? "").matchAll(/[ML](-?[\d.]+),(-?[\d.]+)/g), (match) => ({
      x: Number(match[1]),
      y: Number(match[2]),
    }));
  }

  /**
   * A decimal string such as `"-1234.5"` as an exact integer count of 10^-DECIMAL_SCALE units,
   * read from its digits and never through a float (Rule 4).
   */
  function scaledDecimal(value: DecimalString): bigint {
    const match = /^(-?)(\d+)(?:\.(\d+))?$/.exec(value);
    const fraction = match?.[3] ?? "";
    if (match === null || fraction.length > DECIMAL_SCALE) {
      throw new Error(`Not a decimal string of at most ${DECIMAL_SCALE} places: ${value}`);
    }
    const magnitude = BigInt((match[2] ?? "") + fraction.padEnd(DECIMAL_SCALE, "0"));
    return match[1] === "-" ? -magnitude : magnitude;
  }

  /**
   * How `vertices` depart from the values they should draw, one message per departure. Each value
   * needs a vertex placed linearly in its offset, which rises above the previous vertex where the
   * value rises, falls where it falls and stays level where it holds; the highest value must be
   * drawn more than 1 px above the lowest, so a flat line never passes.
   */
  function pathMismatches(
    vertices: readonly Vertex[],
    drawn: ReadonlyArray<{ offset: number; value: bigint }>,
  ): string[] {
    if (vertices.length !== drawn.length) {
      return [`${vertices.length} vertices for ${drawn.length} values`];
    }
    const firstVertex = vertices[0];
    const lastVertex = vertices[vertices.length - 1];
    const first = drawn[0];
    const last = drawn[drawn.length - 1];
    if (!firstVertex || !lastVertex || !first || !last || last.offset <= first.offset) {
      return ["fewer than two values at distinct offsets"];
    }
    const pxPerDay = (lastVertex.x - firstVertex.x) / (last.offset - first.offset);
    const mismatches: string[] = pxPerDay > 0 ? [] : [`x moves ${pxPerDay} px per day`];
    let highest = 0;
    let lowest = 0;
    drawn.forEach((item, index) => {
      const vertex = vertices[index];
      if (vertex === undefined) return;
      const expectedX = firstVertex.x + (item.offset - first.offset) * pxPerDay;
      if (Math.abs(vertex.x - expectedX) > PIXEL_TOLERANCE) {
        mismatches.push(`offset ${item.offset}: x ${vertex.x}, expected ${expectedX}`);
      }
      if (item.value > (drawn[highest]?.value ?? item.value)) highest = index;
      if (item.value < (drawn[lowest]?.value ?? item.value)) lowest = index;
      const previous = drawn[index - 1];
      const previousVertex = vertices[index - 1];
      if (previous === undefined || previousVertex === undefined) return;
      // SVG y grows downwards, so a rising value moves the line up.
      const dy = vertex.y - previousVertex.y;
      let follows = Math.abs(dy) <= PIXEL_TOLERANCE;
      if (item.value > previous.value) follows = dy < PIXEL_TOLERANCE;
      if (item.value < previous.value) follows = dy > -PIXEL_TOLERANCE;
      if (!follows) mismatches.push(`offset ${item.offset}: y moves ${dy} against its value`);
    });
    const top = vertices[highest]?.y ?? Number.NaN;
    const bottom = vertices[lowest]?.y ?? Number.NaN;
    const hasRange = (drawn[highest]?.value ?? 0) !== (drawn[lowest]?.value ?? 0);
    if (hasRange && !(bottom - top > 1)) {
      mismatches.push(`the highest value is drawn at y ${top}, the lowest at y ${bottom}`);
    }
    return mismatches;
  }

  /**
   * Asserts that `path` is one drawn line through exactly the non-null values of `series`, in
   * order (see {@link pathMismatches}), retrying while the chart re-renders; returns its vertices.
   */
  async function expectPathDraws(path: Locator, series: readonly SeriesValue[]): Promise<Vertex[]> {
    const drawn = series.flatMap((item) =>
      item.value === null ? [] : [{ offset: item.offset, value: scaledDecimal(item.value) }],
    );
    expect(drawn.length).toBeGreaterThanOrEqual(2);
    let vertices: Vertex[] = [];
    await expect(async () => {
      expect(await path.count(), "paths drawn for the series").toBe(1);
      vertices = pathVertices(await path.getAttribute("d", { timeout: 1_000 }));
      expect(pathMismatches(vertices, drawn)).toEqual([]);
    }).toPass({ timeout: READY_TIMEOUT });
    return vertices;
  }

  /**
   * Asserts that `cycle` is drawn as one visible, solid line through every session's `metric`
   * value; returns its path data.
   */
  async function expectObservedLine(
    chart: Locator,
    cycle: CycleResult,
    metric: Metric,
  ): Promise<string> {
    const line = cycleLine(chart, cycle.event_date, "observed");
    await expectPathDraws(line, observedSeries(cycle, metric));
    await expect(line).toBeVisible();
    await expect(line).not.toHaveAttribute("stroke-dasharray", /\S/);
    expect((await line.boundingBox())?.width ?? 0).toBeGreaterThan(0);
    return (await line.getAttribute("d")) ?? "";
  }

  const OVERLAY_FIELDS: readonly OverlayField[] = [
    "inflation_yoy_pct",
    "tbill_91d_pct",
    "tbill_364d_pct",
  ];

  /**
   * The metric toggles in the order the metric test presses them, each a change from the one
   * before, with the heading, value-axis title and tooltip prefix each shows.
   */
  const METRIC_TOGGLES: ReadonlyArray<{
    metric: Metric;
    label: string;
    axisTitle: string;
    prefix: string;
  }> = [
    { metric: "pe_index", label: "P/E index", axisTitle: "P/E index (anchor = 100)", prefix: "" },
    {
      metric: "turnover_index",
      label: "Turnover index",
      axisTitle: "Turnover index (anchor = 100)",
      prefix: "",
    },
    {
      metric: "cumulative_foreign_net_kes",
      label: "Cumulative foreign flow",
      axisTitle: "Cumulative foreign net flow (KES)",
      prefix: "KES ",
    },
    {
      metric: "price_index",
      label: "Price index",
      axisTitle: "Price index (anchor = 100)",
      prefix: "",
    },
  ];

  /** A date with no stored cycle, inside the seeded history, so it backtests as complete. */
  const CUSTOM_EVENT_DATE = "2020-03-16";
  /** Days on each side of the event within which the null-overlay fixture removes inflation. */
  const INFLATION_GAP_DAYS = 30;
  const MONTH_ABBREVIATIONS = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
  ] as const;

  function isIndexMetric(metric: Metric): metric is IndexMetric {
    return metric !== "cumulative_foreign_net_kes";
  }

  /** The overlay-chart path drawn for `field`. */
  function overlayLine(chart: Locator, field: OverlayField): Locator {
    return withAttributes(chart, "cycle-overlay-line", { "data-series": field });
  }

  function eventChip(backtester: Locator, eventDate: string): Locator {
    return withAttributes(backtester, "cycle-event-chip", { "data-event-date": eventDate });
  }

  /** An ISO date as the widgets display it: `4 Mar 2013`. */
  function displayedDate(iso: string): string {
    const [year = "", month = "", day = ""] = iso.split("-");
    const monthName = MONTH_ABBREVIATIONS[Number.parseInt(month, 10) - 1] ?? "";
    return `${Number.parseInt(day, 10)} ${monthName} ${year}`;
  }

  /**
   * A cycle label as the widget displays it: the server's generic `Event 2020-03-16` names its
   * date in short form, `Event 16 Mar 2020`; a stored label such as `2013 Cycle` is unchanged.
   */
  function displayedLabel(label: string): string {
    const date = /^Event (\d{4}-\d{2}-\d{2})$/.exec(label)?.[1];
    return date === undefined ? label : `Event ${displayedDate(date)}`;
  }

  /**
   * `value` as `formatDecimal` displays it, built from the string alone: integer digits grouped
   * with ",", U+2212 before a non-zero negative, `prefix` before the sign and `suffix` after; "—"
   * for null.
   */
  function displayedDecimal(value: DecimalString | null, prefix = "", suffix = ""): string {
    if (value === null) return EMPTY_VALUE;
    const match = /^(-?)(\d+)(\.\d+)?$/.exec(value);
    if (match === null) throw new Error(`Not a decimal string: ${value}`);
    const integer = match[2] ?? "";
    const fraction = match[3] ?? "";
    const sign = match[1] === "-" && /[1-9]/.test(integer + fraction) ? MINUS_SIGN : "";
    const grouped = integer.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
    return `${prefix}${sign}${grouped}${fraction}${suffix}`;
  }

  /** A whole-day offset as the tooltips show it: `+12`, `0`, `−5`. */
  function displayedOffset(offset: number): string {
    if (offset > 0) return `+${offset}`;
    return offset < 0 ? `${MINUS_SIGN}${-offset}` : "0";
  }

  /** The offset a tooltip shows as `+12`, `0` or `−5`. */
  function parseDisplayedOffset(text: string): number {
    const match = /^([+\u2212]?)(\d+)$/.exec(text);
    if (match === null) throw new Error(`Not a displayed offset: ${JSON.stringify(text)}`);
    const days = Number.parseInt(match[2] ?? "", 10);
    return match[1] === MINUS_SIGN ? -days : days;
  }

  /**
   * The last of `rows` at or before `day`, or undefined when `day` is past every row: the session
   * or projected offset a tooltip pairs with a hovered day.
   */
  function lastAtOrBefore<R extends { offset_days: number }>(
    rows: readonly R[],
    day: number,
  ): R | undefined {
    let found: R | undefined;
    let latest = Number.NEGATIVE_INFINITY;
    for (const row of rows) {
      latest = Math.max(latest, row.offset_days);
      if (row.offset_days <= day && (found === undefined || row.offset_days > found.offset_days)) {
        found = row;
      }
    }
    return day > latest ? undefined : found;
  }

  /** Points at the middle of `chart`'s event line, which opens its tooltip at about day 0. */
  async function hoverEventLine(page: Page, chart: Locator): Promise<void> {
    await chart.scrollIntoViewIfNeeded();
    const centre = await chart.locator(".recharts-reference-line-line").evaluate((element) => {
      const box = element.getBoundingClientRect();
      return { x: box.left + box.width / 2, y: box.top + box.height / 2 };
    });
    // From outside the chart, so the pointer enters it afresh.
    await page.mouse.move(0, 0);
    await page.mouse.move(centre.x, centre.y, { steps: 4 });
  }

  /**
   * Hovers the main chart's event line and checks its tooltip against `body`: each cycle's row
   * names its last session on or before the hovered day and shows that session's `metric` value,
   * each projection's row its value at the last projected offset on or before that day, both as
   * `formatDecimal` displays them. A line with nothing on or before that day, or a projection
   * under cumulative flow, has no row.
   */
  async function expectMainTooltip(
    page: Page,
    chart: Locator,
    body: BacktestResponse,
    metric: Metric,
    prefix: string,
  ): Promise<void> {
    await hoverEventLine(page, chart);
    const title = chart.getByText(/^On or before day \S+ from event$/);
    await expect(title).toBeVisible();
    const dayText = /day (\S+) from/.exec((await title.textContent()) ?? "")?.[1] ?? "";
    const day = parseDisplayedOffset(dayText);
    for (const cycle of body.cycles) {
      const rowOf = (series: LineSeries): Locator =>
        withAttributes(chart, "cycle-tooltip-row", {
          "data-event-date": cycle.event_date,
          "data-series": series,
        });
      const observedRow = rowOf("observed");
      const session = lastAtOrBefore(cycle.points, day);
      if (session === undefined) {
        await expect(observedRow).toHaveCount(0);
      } else {
        const meta =
          `${displayedDate(session.trade_date)} \u00b7 ` +
          `${displayedOffset(session.offset_days)} d`;
        await expect(observedRow).toContainText(meta);
        await expect(observedRow.locator('[data-part="value"]')).toHaveText(
          displayedDecimal(session[metric], prefix),
        );
      }
      const projectionRow = rowOf("projection");
      const projected = isIndexMetric(metric)
        ? lastAtOrBefore(cycle.projection?.points ?? [], day)
        : undefined;
      if (projected === undefined || !isIndexMetric(metric)) {
        await expect(projectionRow).toHaveCount(0);
      } else {
        const name = `Projection \u00b7 ${displayedLabel(cycle.label)}`;
        await expect(projectionRow).toContainText(
          `${name}${displayedOffset(projected.offset_days)} d`,
        );
        await expect(projectionRow.locator('[data-part="value"]')).toHaveText(
          displayedDecimal(projected[metric], prefix),
        );
      }
    }
  }

  /**
   * Hovers the overlay chart's event line and checks its tooltip against `cycle`: the session it
   * names, and each rate of that session as `formatDecimal` displays it, "—" where it is null.
   * Returns that session.
   */
  async function expectOverlayTooltip(
    page: Page,
    chart: Locator,
    cycle: CycleResult,
  ): Promise<CyclePoint> {
    await hoverEventLine(page, chart);
    const offsetLabel = chart.getByText(/^\(\S+ d\)$/);
    await expect(offsetLabel).toBeVisible();
    const offsetText = /^\((\S+) d\)$/.exec((await offsetLabel.textContent()) ?? "")?.[1] ?? "";
    const offset = parseDisplayedOffset(offsetText);
    const session = cycle.points.find((point) => point.offset_days === offset);
    if (session === undefined) {
      throw new Error(`The ${cycle.event_date} cycle has no session at ${offset} d`);
    }
    await expect(offsetLabel.locator("xpath=..")).toHaveText(
      `${displayedDate(session.trade_date)} (${displayedOffset(offset)} d)`,
    );
    for (const field of OVERLAY_FIELDS) {
      const row = withAttributes(chart, "cycle-overlay-tooltip-row", { "data-series": field });
      await expect(row.locator('[data-part="value"]')).toHaveText(
        displayedDecimal(session[field], "", "%"),
      );
    }
    return session;
  }

  /**
   * `body` with its first cycle's 364-day T-bill null on every session, as the API reports a
   * yield more than 62 days stale, and its inflation null within {@link INFLATION_GAP_DAYS} of
   * the event.
   */
  function withBlankedOverlay(body: BacktestResponse): BacktestResponse {
    return {
      ...body,
      cycles: body.cycles.map((cycle, index) =>
        index === 0
          ? {
              ...cycle,
              points: cycle.points.map((point) => ({
                ...point,
                inflation_yoy_pct:
                  Math.abs(point.offset_days) <= INFLATION_GAP_DAYS
                    ? null
                    : point.inflation_yoy_pct,
                tbill_364d_pct: null,
              })),
            }
          : cycle,
      ),
    };
  }

  test("overlays four cycles with markers, then projects the 2027 cycle at ±12 months", async ({
    page,
  }) => {
    // Two cold backtests can each take up to SLOW_READY_TIMEOUT.
    test.slow();
    const requests = recordBacktestRequests(page);
    const defaultBacktest = nextBacktest(page, () => true);
    await page.goto("/cycles");

    const backtester = page.getByTestId("terminal-cycle-backtester");
    await expectReady(backtester, SLOW_READY_TIMEOUT);

    // The first request names no event, so the server backtests its stored cycles, and sends the
    // ±180-day window in days only.
    const { query: daysQuery, body: daysResult } = await defaultBacktest;
    expect(requests).toHaveLength(1);
    expectWindowQuery(daysQuery, "days", "180");
    expect(daysQuery.has("event_dates")).toBe(false);
    expect(daysResult.cycles.map((cycle) => cycle.event_date)).toEqual([...ELECTION_DATES]);

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

    // Each completed cycle is one solid line through every session's price index. The awaiting
    // 2027 cycle has no observed line, only its dashed projection through the projected offsets.
    for (const date of COMPLETED_ELECTION_DATES) {
      await expectObservedLine(mainChart, cycleOf(daysResult, date), "price_index");
    }
    const awaiting = cycleOf(daysResult, FUTURE_ELECTION_DATE);
    expect(awaiting.points).toEqual([]);
    await expect(cycleLine(mainChart, FUTURE_ELECTION_DATE, "observed")).toHaveCount(0);
    const futureProjection = cycleLine(mainChart, FUTURE_ELECTION_DATE, "projection");
    await expectPathDraws(futureProjection, projectionSeries(awaiting, "price_index"));
    await expect(futureProjection).toHaveAttribute("stroke-dasharray", /\S/);

    // One valuation floor and one volume exhaustion per completed cycle; the awaiting 2027
    // cycle has no markers.
    await expect(backtester.getByTestId("marker-valuation-floor")).toHaveCount(3);
    await expect(backtester.getByTestId("marker-volume-exhaustion")).toHaveCount(3);
    for (const date of COMPLETED_ELECTION_DATES) {
      await expect(backtester.getByTestId(`cycle-status-${date}`)).toContainText(/complete/i);
    }
    const futureStatus = backtester.getByTestId(`cycle-status-${FUTURE_ELECTION_DATE}`);
    await expect(futureStatus).toContainText(/awaiting data/i);
    expect(awaiting.days_until_window_start).not.toBeNull();
    await expect(futureStatus).toContainText(`opens in ${awaiting.days_until_window_start} days`);

    // ±12 months opens the 2027 window on 2026-08-10, inside the seeded history, so the cycle
    // turns partial with an observed segment and keeps its projection. Marker counts are not
    // asserted here: a partial cycle may carry markers of its own.
    const monthsBacktest = nextBacktest(page, (query) => query.has("pre_months"));
    const twelveMonths = backtester.getByTestId("cycle-window-12m");
    await twelveMonths.click();
    await expect(twelveMonths).toHaveAttribute("aria-pressed", "true");

    // The preset sends the window in months only, still for the stored cycles.
    const { query: monthsQuery, body: monthsResult } = await monthsBacktest;
    expect(requests).toHaveLength(2);
    expectWindowQuery(monthsQuery, "months", "12");
    expect(monthsQuery.has("event_dates")).toBe(false);
    const partial = cycleOf(monthsResult, FUTURE_ELECTION_DATE);
    expect(partial).toMatchObject({
      status: "partial",
      window_start: "2026-08-10",
      window_end: "2028-08-10",
    });
    const firstSession = partial.points[0];
    const lastSession = partial.points[partial.points.length - 1];
    expect(firstSession?.trade_date).toBe("2026-08-10");
    expect(lastSession?.trade_date).toBe("2026-09-30");
    expect(partial.projection).not.toBeNull();

    await expect(futureStatus).toContainText(/partial/i, { timeout: SLOW_READY_TIMEOUT });
    await expectReady(backtester, SLOW_READY_TIMEOUT);
    await expect(legendItems).toHaveCount(4);
    await expect(futureLegend).toBeVisible();
    await expect(futureLegend).not.toContainText("no observed sessions");
    await expect(projectionLegend).toBeVisible();
    await expect(projectionLegend).toContainText("Projection");

    // Every cycle is now redrawn over its ±12-month sessions, 2027's observed segment included.
    for (const date of COMPLETED_ELECTION_DATES) {
      await expectObservedLine(mainChart, cycleOf(monthsResult, date), "price_index");
    }
    const segment = pathVertices(await expectObservedLine(mainChart, partial, "price_index"));
    await expectPathDraws(futureProjection, projectionSeries(partial, "price_index"));
    await expect(futureProjection).toHaveAttribute("stroke-dasharray", /\S/);

    // The segment's sessions all precede the event, so it lies wholly left of the event line,
    // and its own offsets, laid out at its own pixels per day, put day 0 on that line.
    const firstVertex = segment[0];
    const lastVertex = segment[segment.length - 1];
    if (!firstSession || !lastSession || !firstVertex || !lastVertex) {
      throw new Error("The partial 2027 cycle drew no observed segment");
    }
    expect(lastSession.offset_days).toBeLessThan(0);
    const eventLine = mainChart.locator(".recharts-reference-line-line");
    await expect(eventLine).toHaveCount(1);
    const eventX = Number(await eventLine.getAttribute("x1"));
    const pxPerDay =
      (lastVertex.x - firstVertex.x) / (lastSession.offset_days - firstSession.offset_days);
    expect(Math.abs(firstVertex.x - firstSession.offset_days * pxPerDay - eventX)).toBeLessThan(
      0.5,
    );
    const segmentRight = await cycleLine(mainChart, FUTURE_ELECTION_DATE, "observed").evaluate(
      (element) => element.getBoundingClientRect().right,
    );
    const eventLeft = await eventLine.evaluate((element) => element.getBoundingClientRect().left);
    expect(segmentRight).toBeLessThan(eventLeft);
  });

  test("editing event dates re-queries the backtest, and invalid dates send no request", async ({
    page,
  }) => {
    // Each of the many chip changes waits on a backtest of its own.
    test.slow();
    const requests = recordBacktestRequests(page);
    const initial = nextBacktest(page, () => true);
    await page.goto("/cycles");

    const backtester = page.getByTestId("terminal-cycle-backtester");
    await expectReady(backtester, SLOW_READY_TIMEOUT);
    await initial;
    const mainChart = backtester.getByTestId("cycle-main-chart");
    const chips = backtester.getByTestId("cycle-event-chip");
    const legendItems = backtester.getByTestId("cycle-legend-item");
    const summaryRows = backtester.getByTestId("cycle-summary-row");
    const removeButton = (date: string): Locator =>
      eventChip(backtester, date).getByRole("button", { name: /^Remove / });
    await expect(chips).toHaveCount(ELECTION_DATES.length);

    // Removing the 2013 chip asks for exactly the three remaining dates, in the same window, and
    // the 2013 cycle leaves the chips, the legend, the summary and the chart.
    const [removed, ...kept] = ELECTION_DATES;
    const removal = nextBacktest(page, (query) => query.has("event_dates"));
    await removeButton(removed).click();
    const { query: removalQuery, body: afterRemoval } = await removal;
    expect(removalQuery.getAll("event_dates")).toEqual(kept);
    expectWindowQuery(removalQuery, "days", "180");
    expect(afterRemoval.cycles.map((cycle) => cycle.event_date)).toEqual(kept);
    await expect(chips).toHaveCount(kept.length);
    await expect(eventChip(backtester, removed)).toHaveCount(0);
    for (const testId of ["cycle-legend-item", "cycle-summary-row"]) {
      await expect(
        withAttributes(backtester, testId, { "data-event-date": removed }),
      ).toHaveCount(0);
    }
    await expect(legendItems).toHaveCount(kept.length);
    await expect(summaryRows).toHaveCount(kept.length);
    await expect(cycleLine(mainChart, removed, "observed")).toHaveCount(0);
    for (const date of kept.filter((date) => date !== FUTURE_ELECTION_DATE)) {
      await expectObservedLine(mainChart, cycleOf(afterRemoval, date), "price_index");
    }

    // A custom date joins the chips in date order and returns under the server's generic label,
    // shown with its date in short form, as a complete cycle drawn with its own line.
    const customDate = backtester.getByTestId("cycle-custom-date");
    const addDate = backtester.getByTestId("cycle-add-date");
    const withCustom = [...kept, CUSTOM_EVENT_DATE].sort();
    const addition = nextBacktest(page, (query) =>
      query.getAll("event_dates").includes(CUSTOM_EVENT_DATE),
    );
    await customDate.fill(CUSTOM_EVENT_DATE);
    await addDate.click();
    const { query: additionQuery, body: afterAddition } = await addition;
    expect(additionQuery.getAll("event_dates")).toEqual(withCustom);
    expectWindowQuery(additionQuery, "days", "180");
    const custom = cycleOf(afterAddition, CUSTOM_EVENT_DATE);
    expect(custom.label).toBe(`Event ${CUSTOM_EVENT_DATE}`);
    expect(custom.status).toBe("complete");
    const customLabel = displayedLabel(custom.label);
    expect(customLabel).toBe("Event 16 Mar 2020");
    await expect(customDate).toHaveValue("");
    await expect(chips).toHaveCount(withCustom.length);
    await expect(eventChip(backtester, CUSTOM_EVENT_DATE)).toHaveText(customLabel);
    await expect(
      withAttributes(backtester, "cycle-legend-item", { "data-event-date": CUSTOM_EVENT_DATE }),
    ).toHaveText(customLabel);
    await expect(
      withAttributes(backtester, "cycle-summary-row", { "data-event-date": CUSTOM_EVENT_DATE }),
    ).toContainText(customLabel);
    await expect(backtester.getByTestId(`cycle-status-${CUSTOM_EVENT_DATE}`)).toHaveText(
      "complete",
    );
    await expectObservedLine(mainChart, custom, "price_index");

    // Invalid additions are refused in place without a request: a date already selected, no
    // date, an unfinished date, and a date whose ±180-day window would leave the API's calendar.
    const chipError = backtester.getByTestId("cycle-chip-error");
    const rangePhrase = "between 30 Jun 0001 and 4 Jul 9999 for the ±180 days window";
    const settledRequests = requests.length;
    await customDate.fill(kept[0]);
    await addDate.click();
    await expect(chipError).toHaveText(`${displayedDate(kept[0])} is already selected.`);
    await customDate.fill("");
    await addDate.click();
    await expect(chipError).toHaveText("Enter a complete date.");
    // Typing only the month leaves the input's value empty, with bad input.
    await customDate.focus();
    await page.keyboard.type("03");
    await addDate.click();
    await expect(chipError).toHaveText(`Enter a complete date ${rangePhrase}.`);
    await customDate.fill("0001-01-01");
    await addDate.click();
    await expect(chipError).toHaveText(`Enter a date ${rangePhrase}.`);
    await expect(chips).toHaveCount(withCustom.length);
    expect(requests).toHaveLength(settledRequests);

    // Four more dates reach the limit of eight; a ninth is refused without a request.
    const extraDates = ["2013-03-04", "2015-06-01", "2019-01-15", "2024-05-20"];
    for (const date of extraDates) {
      const added = nextBacktest(page, (query) => query.getAll("event_dates").includes(date));
      await customDate.fill(date);
      await addDate.click();
      await added;
    }
    const eightDates = [...withCustom, ...extraDates].sort();
    await expect(chips).toHaveCount(8);
    await expect(legendItems).toHaveCount(8);
    expect(
      await chips.evaluateAll((elements) =>
        elements.map((element) => element.getAttribute("data-event-date")),
      ),
    ).toEqual(eightDates);
    const requestsAtLimit = requests.length;
    await customDate.fill("2025-01-15");
    await addDate.click();
    await expect(chipError).toHaveText("At most 8 event dates can be compared. Remove one first.");
    await expect(chips).toHaveCount(8);
    expect(requests).toHaveLength(requestsAtLimit);

    // Removing all dates but one: the last chip's Remove is disabled, so no empty list is sent.
    const lastDate = "2022-08-09";
    const lastRemoval = nextBacktest(
      page,
      (query) => query.getAll("event_dates").join() === lastDate,
    );
    for (const date of eightDates.filter((date) => date !== lastDate)) {
      await removeButton(date).click();
      await expect(eventChip(backtester, date)).toHaveCount(0);
    }
    const { body: single } = await lastRemoval;
    expect(single.cycles.map((cycle) => cycle.event_date)).toEqual([lastDate]);
    await expect(chips).toHaveCount(1);
    await expect(removeButton(lastDate)).toBeDisabled();
    await expect(legendItems).toHaveCount(1);
    await expect(summaryRows).toHaveCount(1);
    await expectObservedLine(mainChart, cycleOf(single, lastDate), "price_index");
  });

  test("each metric redraws every cycle from that metric's values without a new request", async ({
    page,
  }) => {
    test.slow();
    const requests = recordBacktestRequests(page);
    const initial = nextBacktest(page, () => true);
    await page.goto("/cycles");

    const backtester = page.getByTestId("terminal-cycle-backtester");
    await expectReady(backtester, SLOW_READY_TIMEOUT);
    const { body } = await initial;
    const mainChart = backtester.getByTestId("cycle-main-chart");
    const awaiting = cycleOf(body, FUTURE_ELECTION_DATE);
    const projection = cycleLine(mainChart, FUTURE_ELECTION_DATE, "projection");
    const projectionLegend = withAttributes(backtester, "cycle-projection-legend", {
      "data-event-date": FUTURE_ELECTION_DATE,
    });
    const projectionNote = backtester.getByText(/^Projections are descriptive averages/);
    // Every metric's value-axis title ends "(anchor = 100)" or "(KES)".
    const valueAxisTitle = mainChart
      .locator("text.recharts-label")
      .filter({ hasText: /\((?:anchor = 100|KES)\)$/ });

    // The price index is plotted first.
    await expect(backtester.getByTestId("cycle-metric-price_index")).toHaveAttribute(
      "aria-pressed",
      "true",
    );
    const drawn = new Map<string, string>();
    for (const date of COMPLETED_ELECTION_DATES) {
      drawn.set(date, await expectObservedLine(mainChart, cycleOf(body, date), "price_index"));
    }

    for (const toggle of METRIC_TOGGLES) {
      await backtester.getByTestId(`cycle-metric-${toggle.metric}`).click();
      for (const other of METRIC_TOGGLES) {
        await expect(backtester.getByTestId(`cycle-metric-${other.metric}`)).toHaveAttribute(
          "aria-pressed",
          String(other === toggle),
        );
      }
      const heading = `${toggle.label} by days from event`;
      await expect(backtester.getByRole("heading", { name: heading, exact: true })).toBeVisible();
      await expect(valueAxisTitle).toHaveText([toggle.axisTitle]);

      // Every completed cycle's line changes, to one through this metric's values.
      for (const date of COMPLETED_ELECTION_DATES) {
        await expect(cycleLine(mainChart, date, "observed")).not.toHaveAttribute(
          "d",
          drawn.get(date) ?? "",
        );
        drawn.set(date, await expectObservedLine(mainChart, cycleOf(body, date), toggle.metric));
      }

      // A projection carries the three index levels only.
      if (isIndexMetric(toggle.metric)) {
        await expectPathDraws(projection, projectionSeries(awaiting, toggle.metric));
        await expect(projection).toHaveAttribute("stroke-dasharray", /\S/);
        await expect(projectionLegend).toBeVisible();
        await expect(projectionNote).toHaveText(
          "Projections are descriptive averages of completed cycles, not forecasts.",
        );
      } else {
        await expect(projection).toHaveCount(0);
        await expect(projectionLegend).toHaveCount(0);
        await expect(projectionNote).toHaveText(
          "Projections are descriptive averages of completed cycles, not forecasts; none is drawn" +
            " for cumulative foreign flow.",
        );
      }

      await expectMainTooltip(page, mainChart, body, toggle.metric, toggle.prefix);
    }

    // A metric change redraws the loaded result; it never asks the server again.
    expect(requests).toHaveLength(1);
  });

  test("the macro overlay plots inflation and both T-bill yields of the selected cycle", async ({
    page,
  }) => {
    test.slow();
    const requests = recordBacktestRequests(page);
    const initial = nextBacktest(page, () => true);
    await page.goto("/cycles");

    const backtester = page.getByTestId("terminal-cycle-backtester");
    await expectReady(backtester, SLOW_READY_TIMEOUT);
    const { body } = await initial;
    const select = backtester.getByTestId("cycle-overlay-select");
    const overlayChart = backtester.getByTestId("cycle-overlay-chart");

    // Only cycles with sessions can be overlaid, so the awaiting 2027 cycle is not offered, and
    // the first of them is selected.
    const candidates = body.cycles.filter((cycle) => cycle.points.length > 0);
    expect(candidates.map((cycle) => cycle.event_date)).toEqual([...COMPLETED_ELECTION_DATES]);
    const options = select.locator("option");
    await expect(options).toHaveText(candidates.map((cycle) => displayedLabel(cycle.label)));
    expect(
      await options.evaluateAll((elements) =>
        elements.map((element) => element.getAttribute("value")),
      ),
    ).toEqual(candidates.map((cycle) => cycle.event_date));
    const [first, second] = candidates;
    if (first === undefined || second === undefined) {
      throw new Error("The backtest response has fewer than two cycles with sessions");
    }
    await expect(select).toHaveValue(first.event_date);

    await expect(overlayChart.getByTestId("cycle-overlay-line")).toHaveCount(OVERLAY_FIELDS.length);
    const drawn = new Map<OverlayField, string>();
    for (const field of OVERLAY_FIELDS) {
      const line = overlayLine(overlayChart, field);
      await expectPathDraws(line, observedSeries(first, field));
      await expect(line).toBeVisible();
      drawn.set(field, (await line.getAttribute("d")) ?? "");
    }
    await expectOverlayTooltip(page, overlayChart, first);

    // Choosing another cycle redraws every rate from its sessions, out of the loaded result.
    await select.selectOption(second.event_date);
    await expect(select).toHaveValue(second.event_date);
    for (const field of OVERLAY_FIELDS) {
      const line = overlayLine(overlayChart, field);
      await expect(line).not.toHaveAttribute("d", drawn.get(field) ?? "");
      await expectPathDraws(line, observedSeries(second, field));
      await expect(line).toBeVisible();
    }
    await expectOverlayTooltip(page, overlayChart, second);
    expect(requests).toHaveLength(1);
  });

  test("stale and missing overlay rates leave gaps in the overlay and show as —", async ({
    page,
  }) => {
    // The handler fetches the real backtest response and blanks rates in that copy before the page
    // receives it; nothing is written to the database.
    const served: BacktestResponse[] = [];
    await page.route("**/api/v1/terminal/cycles/backtest**", async (route) => {
      const response = await route.fetch();
      const body = withBlankedOverlay((await response.json()) as BacktestResponse);
      served.push(body);
      await route.fulfill({ response, json: body });
    });
    await page.goto("/cycles");

    const backtester = page.getByTestId("terminal-cycle-backtester");
    await expectReady(backtester, SLOW_READY_TIMEOUT);
    expect(served).toHaveLength(1);
    const cycle = served[0]?.cycles[0];
    if (cycle === undefined) throw new Error("The backtest response has no cycle");
    expect(cycle.event_date).toBe(ELECTION_DATES[0]);
    await expect(backtester.getByTestId("cycle-overlay-select")).toHaveValue(cycle.event_date);
    const overlayChart = backtester.getByTestId("cycle-overlay-chart");

    // Inflation is drawn through its remaining readings and the 91-day yield through all of
    // them; the 364-day yield, null on every session, has no geometry at all.
    const inflation = observedSeries(cycle, "inflation_yoy_pct");
    expect(inflation.some((item) => item.value === null)).toBe(true);
    await expectPathDraws(overlayLine(overlayChart, "inflation_yoy_pct"), inflation);
    await expectPathDraws(
      overlayLine(overlayChart, "tbill_91d_pct"),
      observedSeries(cycle, "tbill_91d_pct"),
    );
    const staleGeometry = await overlayLine(overlayChart, "tbill_364d_pct").evaluateAll(
      (elements) => elements.map((element) => element.getAttribute("d")),
    );
    expect(staleGeometry.flatMap((d) => pathVertices(d))).toEqual([]);

    // A session inside the inflation gap shows both missing rates as "—" beside its 91-day yield.
    const session = await expectOverlayTooltip(page, overlayChart, cycle);
    expect(session.inflation_yoy_pct).toBeNull();
    expect(session.tbill_364d_pct).toBeNull();
    expect(session.tbill_91d_pct).not.toBeNull();
    for (const field of ["inflation_yoy_pct", "tbill_364d_pct"] as const) {
      const row = withAttributes(overlayChart, "cycle-overlay-tooltip-row", {
        "data-series": field,
      });
      await expect(row.locator('[data-part="value"]')).toHaveText(EMPTY_VALUE);
    }
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

  /** A decoded JSON object, such as a valuation request or response body. */
  type JsonObject = Readonly<Record<string, unknown>>;

  /** A valuation POST the page sent: its request, its decoded body and, once received, response. */
  interface ValuationPost {
    request: Request;
    body: unknown;
    response: Response | null;
  }

  /** `value` as a JSON object; anything else fails the test. */
  function jsonObject(value: unknown): JsonObject {
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
      throw new Error(`Expected a JSON object, got ${JSON.stringify(value)}`);
    }
    return value as JsonObject;
  }

  /** The decoded JSON object of a valuation response; no response or other JSON fails the test. */
  async function valuationJson(response: Response | null): Promise<JsonObject> {
    if (response === null) throw new Error("The valuation request received no response");
    return jsonObject(await response.json());
  }

  /** True for the sandbox's `POST /ipo/valuation`. */
  function isValuationPost(request: Request): boolean {
    return request.method() === "POST" && new URL(request.url()).pathname === IPO_VALUATION_PATH;
  }

  /**
   * Every valuation POST the page sends from now on, in send order, each with its response once
   * that arrives. Register it before the input whose requests it must see.
   */
  function recordValuationPosts(page: Page): ValuationPost[] {
    const posts: ValuationPost[] = [];
    page.on("request", (request) => {
      if (isValuationPost(request)) {
        const body: unknown = request.postDataJSON();
        posts.push({ request, body, response: null });
      }
    });
    page.on("response", (response) => {
      const post = posts.find((candidate) => candidate.request === response.request());
      if (post !== undefined) post.response = response;
    });
    return posts;
  }

  /** The preset's sensitivity axes as the API returns them: WACC rows, base-margin columns, %. */
  const OUTPUT_MATRIX_WACC = ["16.00", "17.00", "18.00", "19.00", "20.00"] as const;
  const OUTPUT_MATRIX_MARGINS = ["3.00", "3.50", "4.00", "4.50", "5.00", "5.50", "6.00"] as const;
  /** The preset's KES 7.50 offer in hundredths, the threshold of the matrix and bar colours. */
  const OUTPUT_OFFER_CENTS = 750;
  /** `SensitivityMatrix` cell tones: at or above the offer, below it, and not comparable. */
  const OUTPUT_TONE_AT_OR_ABOVE_OFFER = "bg-emerald-900/60 text-emerald-100";
  const OUTPUT_TONE_BELOW_OFFER = "bg-rose-900/60 text-rose-100";
  const OUTPUT_TONE_NOT_COMPARABLE = "bg-slate-800 text-slate-400";
  /** Method bar fills of `lib/chartTheme.ts`: emerald-500 at or above the offer, rose-500 below. */
  const OUTPUT_BAR_FILL_AT_OR_ABOVE_OFFER = "#00bc7d";
  const OUTPUT_BAR_FILL_BELOW_OFFER = "#ff2056";
  /** U+00D7, the suffix `lib/format.ts` renders after a ratio. */
  const OUTPUT_RATIO_SIGN = "\u00d7";
  /**
   * Preset matrix cells whose fair values are known without the blended result, in hundredths of
   * a KES: the four hand-calculated corners, then the cells just below (7.49) and just above
   * (7.73) the offer.
   */
  const OUTPUT_REFERENCE_CELLS = [
    { wacc: "16.00", margin: "3.00", cents: 297, tone: OUTPUT_TONE_BELOW_OFFER },
    { wacc: "16.00", margin: "6.00", cents: 1250, tone: OUTPUT_TONE_AT_OR_ABOVE_OFFER },
    { wacc: "20.00", margin: "3.00", cents: 214, tone: OUTPUT_TONE_BELOW_OFFER },
    { wacc: "20.00", margin: "6.00", cents: 1016, tone: OUTPUT_TONE_AT_OR_ABOVE_OFFER },
    { wacc: "20.00", margin: "5.00", cents: 749, tone: OUTPUT_TONE_BELOW_OFFER },
    { wacc: "16.00", margin: "4.50", cents: 773, tone: OUTPUT_TONE_AT_OR_ABOVE_OFFER },
  ] as const;
  /** The flag of a matrix cell whose WACC does not exceed terminal growth. */
  const OUTPUT_NULL_CELL_FLAG = "wacc_not_above_terminal_growth";
  /** The flag of a comparables value for a non-positive base EBITDA. */
  const OUTPUT_NULL_COMPS_FLAG = "comps_non_positive_ebitda";

  /** The matrix cell at a WACC and base margin, each as the API's 2-decimal string. */
  function outputMatrixCell(matrix: Locator, wacc: string, margin: string): Locator {
    return withAttributes(matrix, "sensitivity-cell", { "data-wacc": wacc, "data-margin": margin });
  }

  /** One computed style property of an element, as the browser resolves it. */
  async function outputComputedStyle(
    locator: Locator,
    property: "backgroundColor" | "boxShadow",
  ): Promise<string> {
    return locator.evaluate((element, name) => getComputedStyle(element)[name], property);
  }

  /** An SVG element's box in its chart's user coordinates, where y grows downward. */
  async function outputSvgBox(
    locator: Locator,
  ): Promise<{ x: number; y: number; width: number; height: number }> {
    return locator.evaluate((element) => {
      if (!(element instanceof SVGGraphicsElement)) {
        throw new Error(`<${element.tagName}> is not an SVG graphics element`);
      }
      const box = element.getBBox();
      return { x: box.x, y: box.y, width: box.width, height: box.height };
    });
  }

  test("values the Quickmart preset at KES 6.82 with a 5 × 7 sensitivity matrix", async ({
    page,
  }) => {
    const { sandbox, blended } = await openSandbox(page);

    expect(toCents(await blended.textContent())).toBe(682);
    await expect(sandbox.getByTestId("ipo-upside")).toContainText(`${MINUS_SIGN}9.11%`);

    // The per-method cards, and the implied multiple at the offer: (7.50 × 1,000,000,000 shares
    // + 4,000,000,000 net debt) ÷ (45,000,000,000 × 4.50%) = 5.68×. A blended value below the
    // offer is labelled as downside, in the rose tone.
    await expect(sandbox.getByTestId("ipo-dcf-fair-value")).toHaveText("KES 5.48");
    await expect(sandbox.getByTestId("ipo-comps-fair-value")).toHaveText("KES 8.15");
    await expect(sandbox.getByTestId("ipo-implied-ev-ebitda")).toHaveText(
      `5.68${OUTPUT_RATIO_SIGN}`,
    );
    const upside = sandbox.getByTestId("ipo-upside");
    await expect(upside.locator("xpath=preceding-sibling::dt")).toHaveText("Downside vs offer");
    await expect(upside).toContainClass("text-rose-400");
    await expect(upside).not.toContainClass("text-emerald-400");

    // The method chart: DCF, comparables and blended bars, left to right and as tall as their
    // values rank (5.48 < 6.82 < 8.15), coloured against the offer. Only the comparables bar
    // reaches above the labelled offer line. The chart's description gives every value.
    const chart = sandbox.getByTestId("ipo-fair-value-chart");
    const bars = chart.locator(".recharts-bar-rectangle path.recharts-rectangle");
    await expect(bars).toHaveCount(3);
    const dcfBar = bars.nth(0);
    const compsBar = bars.nth(1);
    const blendedBar = bars.nth(2);
    await expect(dcfBar).toHaveAttribute("fill", OUTPUT_BAR_FILL_BELOW_OFFER);
    await expect(compsBar).toHaveAttribute("fill", OUTPUT_BAR_FILL_AT_OR_ABOVE_OFFER);
    await expect(blendedBar).toHaveAttribute("fill", OUTPUT_BAR_FILL_BELOW_OFFER);
    for (const bar of [dcfBar, compsBar, blendedBar]) {
      await expect
        .poll(async () => {
          const box = await outputSvgBox(bar);
          return Math.min(box.width, box.height);
        })
        .toBeGreaterThan(0);
    }
    const dcfBox = await outputSvgBox(dcfBar);
    const compsBox = await outputSvgBox(compsBar);
    const blendedBox = await outputSvgBox(blendedBar);
    expect(dcfBox.x).toBeLessThan(compsBox.x);
    expect(compsBox.x).toBeLessThan(blendedBox.x);
    expect(dcfBox.height).toBeLessThan(blendedBox.height);
    expect(blendedBox.height).toBeLessThan(compsBox.height);
    const offerLine = chart.locator("line.recharts-reference-line-line");
    await expect(offerLine).toHaveCount(1);
    const offerY = (await outputSvgBox(offerLine)).y;
    expect(compsBox.y).toBeLessThan(offerY);
    expect(dcfBox.y).toBeGreaterThan(offerY);
    expect(blendedBox.y).toBeGreaterThan(offerY);
    await expect(chart.locator("text.recharts-label")).toHaveText(["Offer KES 7.50"]);
    await expect(chart.locator("svg.recharts-surface > desc")).toHaveText(
      "DCF KES 5.48, Comparables KES 8.15, Blended KES 6.82; offer KES 7.50",
    );

    // 5 WACC rows (16–20%) × 7 base-margin columns (3.0–6.0%); the preset's 18% × 4.5% cell is
    // the one outlined.
    const matrix = sandbox.getByTestId("terminal-sensitivity-matrix");
    await expect(matrix.getByTestId("sensitivity-cell")).toHaveCount(35);
    const selectedCell = withAttributes(matrix, "sensitivity-cell", { "data-selected": "true" });
    await expect(selectedCell).toHaveCount(1);
    expect(toCents(await selectedCell.getAttribute("data-wacc"))).toBe(1800);
    expect(toCents(await selectedCell.getAttribute("data-margin"))).toBe(450);

    // The headers: the corner, the margins left to right, the WACC values top to bottom. Each row
    // holds one cell per margin column, carrying its row's WACC and its column's margin, coloured
    // by its own fair value against the offer, compared in hundredths.
    await expect(matrix.locator("thead th")).toHaveText([
      /WACC.*Margin/,
      ...OUTPUT_MATRIX_MARGINS.map((margin) => `${margin}%`),
    ]);
    await expect(matrix.locator("tbody th")).toHaveText(
      OUTPUT_MATRIX_WACC.map((wacc) => `${wacc}%`),
    );
    const bodyRows = matrix.locator("tbody tr");
    await expect(bodyRows).toHaveCount(OUTPUT_MATRIX_WACC.length);
    for (const [rowIndex, wacc] of OUTPUT_MATRIX_WACC.entries()) {
      const rowCells = bodyRows.nth(rowIndex).getByTestId("sensitivity-cell");
      await expect(rowCells).toHaveCount(OUTPUT_MATRIX_MARGINS.length);
      for (const [columnIndex, margin] of OUTPUT_MATRIX_MARGINS.entries()) {
        const cell = rowCells.nth(columnIndex);
        await expect(cell).toHaveAttribute("data-wacc", wacc);
        await expect(cell).toHaveAttribute("data-margin", margin);
        const atOrAboveOffer = toCents(await cell.textContent()) >= OUTPUT_OFFER_CENTS;
        await expect(cell).toContainClass(
          atOrAboveOffer ? OUTPUT_TONE_AT_OR_ABOVE_OFFER : OUTPUT_TONE_BELOW_OFFER,
        );
        await expect(cell).not.toContainClass(
          atOrAboveOffer ? OUTPUT_TONE_BELOW_OFFER : OUTPUT_TONE_AT_OR_ABOVE_OFFER,
        );
      }
    }

    // Cell values independent of the blended result, each in its expected tone.
    for (const reference of OUTPUT_REFERENCE_CELLS) {
      const cell = outputMatrixCell(matrix, reference.wacc, reference.margin);
      await expect(cell).toHaveCount(1);
      expect(toCents(await cell.textContent())).toBe(reference.cents);
      await expect(cell).toContainClass(reference.tone);
    }

    // The tone classes are compiled: the emerald 7.73 and rose 7.49 cells paint different,
    // non-transparent backgrounds.
    const emeraldBackground = await outputComputedStyle(
      outputMatrixCell(matrix, "16.00", "4.50"),
      "backgroundColor",
    );
    const roseBackground = await outputComputedStyle(
      outputMatrixCell(matrix, "20.00", "5.00"),
      "backgroundColor",
    );
    expect(emeraldBackground).not.toBe(roseBackground);
    expect([emeraldBackground, roseBackground]).not.toContain("rgba(0, 0, 0, 0)");

    // The outlined cell is the only current one for assistive technology, holds the preset's own
    // valuation and draws its sky ring, which no other cell has.
    await expect(matrix.locator("[aria-current]")).toHaveCount(1);
    await expect(selectedCell).toHaveAttribute("aria-current", "true");
    await expect(
      withAttributes(matrix, "sensitivity-cell", { "data-selected": "false" }),
    ).toHaveCount(34);
    expect(toCents(await selectedCell.textContent())).toBe(682);
    await expect(selectedCell).toContainText(`${MINUS_SIGN}9.11%`);
    await expect(selectedCell).toHaveAttribute(
      "title",
      `Downside ${MINUS_SIGN}9.11% vs offer KES 7.50`,
    );
    await expect(selectedCell).toContainClass("ring-2 ring-inset ring-sky-400");
    await expect(matrix.locator(".ring-sky-400")).toHaveCount(1);
    expect(await outputComputedStyle(selectedCell, "boxShadow")).not.toBe("none");
    expect(
      await outputComputedStyle(outputMatrixCell(matrix, "18.00", "4.00"), "boxShadow"),
    ).toBe("none");

    await expect(sandbox.getByTestId("ipo-risk-scenario")).toHaveCount(3);
  });

  test("a null matrix cell and comparables value show an em dash explained by their flags", async ({
    page,
  }) => {
    // The server's own preset valuation, except that the 16% × 3.0% cell is null with the flag a
    // WACC at or below terminal growth gives it, and the comparables value is null with the flag
    // of a non-positive base EBITDA. Only the rendering of null values is under test.
    let fulfilledValuations = 0;
    await page.route(IPO_VALUATION_ROUTE, async (route) => {
      const response = await route.fetch();
      if (!response.ok()) {
        await route.fulfill({ response });
        return;
      }
      const valuation = (await response.json()) as IpoValuationResponse;
      valuation.sensitivity.cells = valuation.sensitivity.cells.map((row) =>
        row.map((cell) =>
          cell.wacc_pct === "16.00" && cell.ebitda_margin_pct === "3.00"
            ? {
                ...cell,
                fair_value_per_share_kes: null,
                upside_pct: null,
                flags: [OUTPUT_NULL_CELL_FLAG],
              }
            : cell,
        ),
      );
      valuation.comparables = {
        ...valuation.comparables,
        enterprise_value_kes: null,
        equity_value_kes: null,
        fair_value_per_share_kes: null,
        flags: [OUTPUT_NULL_COMPS_FLAG],
      };
      valuation.flags = [...valuation.flags, OUTPUT_NULL_COMPS_FLAG, OUTPUT_NULL_CELL_FLAG];
      fulfilledValuations += 1;
      await route.fulfill({ response, json: valuation });
    });

    const { sandbox } = await openSandbox(page);
    expect(fulfilledValuations).toBeGreaterThanOrEqual(1);

    // The null cell: an em dash for sighted users, its flag for screen readers and as its pointer
    // tooltip, in the neutral tone rather than either comparison colour.
    const matrix = sandbox.getByTestId("terminal-sensitivity-matrix");
    const nullCell = outputMatrixCell(matrix, "16.00", "3.00");
    await expect(nullCell).toHaveAttribute("title", OUTPUT_NULL_CELL_FLAG);
    await expect(nullCell.locator('[aria-hidden="true"]')).toHaveText(EMPTY_VALUE);
    await expect(nullCell.locator(".sr-only")).toHaveText(
      `Not computable: ${OUTPUT_NULL_CELL_FLAG}`,
    );
    await expect(nullCell).toContainClass(OUTPUT_TONE_NOT_COMPARABLE);
    await expect(nullCell).not.toContainClass(OUTPUT_TONE_AT_OR_ABOVE_OFFER);
    await expect(nullCell).not.toContainClass(OUTPUT_TONE_BELOW_OFFER);
    const neutralBackground = await outputComputedStyle(nullCell, "backgroundColor");
    for (const [wacc, margin] of [
      ["16.00", "4.50"],
      ["16.00", "3.50"],
    ] as const) {
      expect(neutralBackground).not.toBe(
        await outputComputedStyle(outputMatrixCell(matrix, wacc, margin), "backgroundColor"),
      );
    }
    // The rest of the matrix is still compared with the offer.
    const corner = outputMatrixCell(matrix, "16.00", "6.00");
    expect(toCents(await corner.textContent())).toBe(1250);
    await expect(corner).toContainClass(OUTPUT_TONE_AT_OR_ABOVE_OFFER);

    // The keyboard explanation: the matrix's only button, described by a tooltip holding the
    // flag, which keyboard focus shows and Escape hides.
    const explanation = nullCell.getByRole("button");
    await expect(matrix.getByRole("button")).toHaveCount(1);
    await expect(explanation).toHaveAccessibleDescription(OUTPUT_NULL_CELL_FLAG);
    const tooltip = nullCell.getByRole("tooltip", { includeHidden: true });
    await expect(tooltip).toHaveText(OUTPUT_NULL_CELL_FLAG);
    await expect(tooltip).toBeHidden();
    await explanation.focus();
    await expect(tooltip).toBeVisible();
    await page.keyboard.press("Escape");
    await expect(tooltip).toBeHidden();

    // The comparables card: an em dash, explained by its flag as the tooltip and to screen
    // readers; the chart's description and the valuation's flag list carry the flags too.
    const comps = sandbox.getByTestId("ipo-comps-fair-value");
    await expect(comps).toHaveAttribute("title", OUTPUT_NULL_COMPS_FLAG);
    await expect(comps).toHaveText(`${EMPTY_VALUE}: ${OUTPUT_NULL_COMPS_FLAG}`);
    await expect(comps.locator(".sr-only")).toHaveText(`: ${OUTPUT_NULL_COMPS_FLAG}`);
    await expect(
      sandbox.getByTestId("ipo-fair-value-chart").locator("svg.recharts-surface > desc"),
    ).toContainText(`Comparables ${EMPTY_VALUE}`);
    await expect(sandbox.getByTestId("ipo-flag")).toHaveText([
      OUTPUT_NULL_COMPS_FLAG,
      OUTPUT_NULL_CELL_FLAG,
    ]);
  });

  // Request lifecycle of the valuation. The browser clock and the valuation route are under the
  // tests' control, so the debounce and the held, superseded and suppressed requests are proven
  // without wall-clock waits. Expected bodies are input literals and expected fair values are
  // the server's own responses; no valuation is computed here.

  /** The sandbox's default debounce: inputs left unchanged this long are posted. */
  const LIFECYCLE_DEBOUNCE_MS = 150;
  /**
   * Fake time well past the debounce and well below the client's 20 s request deadline, so a held
   * request never times out while the clock advances.
   */
  const LIFECYCLE_IDLE_MS = 5_000;
  /** How far past the page's own time the paused clock is set; also below the 20 s deadline. */
  const LIFECYCLE_PAUSE_LEAD_MS = 5_000;
  /** The longest the settled valuation may take after the final slider input. */
  const LIFECYCLE_SETTLE_BUDGET_MS = 2_000;
  /** The output's class while a valuation is pending. */
  const LIFECYCLE_DIMMED_CLASS = /(?:^|\s)opacity-60(?:\s|$)/;
  /** U+2026, the ellipsis of the "Recalculating…" indicator. */
  const LIFECYCLE_RECALCULATING_TEXT = "Recalculating\u2026";

  /**
   * The body posted for the seeded Quickmart preset: sliders serialized from their positions at
   * each slider's scale (net debt in whole KES, DCF weight in whole percent), text inputs as the
   * preset's own strings, counts as JSON integers, and no sensitivity axes.
   */
  const LIFECYCLE_PRESET_BODY: Readonly<Record<string, string | number>> = {
    offer_price_kes: "7.50",
    revenue_growth_pct: "10.00",
    ebitda_margin_pct: "4.50",
    margin_expansion_pct: "0.50",
    wacc_pct: "18.00",
    terminal_growth_pct: "5.00",
    net_debt_kes: "4000000000",
    nwc_pct_revenue: "-3.00",
    peer_ev_ebitda_multiple: "6.00",
    dcf_weight_pct: "50",
    capex_pct_revenue: "2.00",
    da_pct_revenue: "1.50",
    tax_rate_pct: "30.00",
    forecast_years: 5,
    base_revenue_kes: "45000000000.00",
    shares_outstanding: 1_000_000_000,
    shares_offered: 500_000_000,
    primary_proceeds_kes: "0.00",
  };

  /** Each slider, and the exact string one ArrowRight step from its preset position sends. */
  const LIFECYCLE_SLIDER_STEPS = [
    { field: "offer_price_kes", sent: "7.55" },
    { field: "revenue_growth_pct", sent: "10.50" },
    { field: "ebitda_margin_pct", sent: "4.75" },
    { field: "margin_expansion_pct", sent: "0.75" },
    { field: "wacc_pct", sent: "18.25" },
    { field: "terminal_growth_pct", sent: "5.25" },
    { field: "net_debt_kes", sent: "4250000000" },
    { field: "nwc_pct_revenue", sent: "-2.50" },
    { field: "peer_ev_ebitda_multiple", sent: "6.25" },
    { field: "dcf_weight_pct", sent: "55" },
  ] as const;

  /**
   * Each text input, a valid string typed into it and what it sends: the typed string unchanged,
   * trailing zeros included, and counts as JSON integers.
   */
  const LIFECYCLE_TEXT_ENTRIES = [
    { field: "capex_pct_revenue", typed: "2.5", sent: "2.5" },
    { field: "da_pct_revenue", typed: "1.25", sent: "1.25" },
    { field: "tax_rate_pct", typed: "25.0000", sent: "25.0000" },
    { field: "forecast_years", typed: "7", sent: 7 },
    { field: "base_revenue_kes", typed: "50000000000.50", sent: "50000000000.50" },
    { field: "shares_outstanding", typed: "1200000000", sent: 1_200_000_000 },
    { field: "shares_offered", typed: "400000000", sent: 400_000_000 },
    { field: "primary_proceeds_kes", typed: "1000000.10", sent: "1000000.10" },
  ] as const;

  /**
   * Tax rates refused by the field's own check (0–50, 4 dp, no spaces around it, since it is sent
   * as typed), and the message shown for each.
   */
  const LIFECYCLE_INVALID_TAX_RATES = [
    { typed: "abc", message: "Enter a valid number" },
    { typed: "60", message: "Must be at most 50" },
    { typed: " 25 ", message: "Remove the spaces around the number" },
  ] as const;

  /** A late answer attempted for a superseded valuation request. */
  interface LifecycleStaleAnswer {
    kind: string;
    /** The answer, built from the server's response to the preset. */
    answer: (preset: JsonObject) => { status: number; json: unknown };
  }

  /**
   * A success carrying a blended value no valuation of these inputs gives, and a server error
   * that would raise the general error; neither may ever be shown.
   */
  const LIFECYCLE_STALE_ANSWERS: readonly LifecycleStaleAnswer[] = [
    {
      kind: "success",
      answer: (preset) => ({
        status: 200,
        json: { ...preset, blended_fair_value_per_share_kes: "99.99" },
      }),
    },
    {
      kind: "error",
      answer: () => ({ status: 500, json: { detail: "Stale valuation failure" } }),
    },
  ];

  /** A valuation POST held by the test's route until the test answers it. */
  interface LifecycleHeldPost {
    route: Route;
    request: Request;
    body: unknown;
  }

  /** What the sandbox has shown since {@link lifecycleWatchOutput} started watching it. */
  interface LifecycleShown {
    /** Each successive distinct text of the blended fair value card. */
    blended: string[];
    /** Whether the general error banner ever appeared. */
    generalError: boolean;
  }

  /**
   * Holds every valuation POST the page sends from now on, in send order. Each stays pending
   * until the test continues or fulfils it, or the page aborts it.
   */
  async function lifecycleHoldPosts(page: Page): Promise<LifecycleHeldPost[]> {
    const held: LifecycleHeldPost[] = [];
    await page.route(IPO_VALUATION_ROUTE, async (route, request) => {
      if (!isValuationPost(request)) {
        await route.fallback();
        return;
      }
      const body: unknown = request.postDataJSON();
      held.push({ route, request, body });
    });
    return held;
  }

  /** Held POST number `count` (1-based), once the page has sent it. */
  async function lifecycleHeld(
    held: readonly LifecycleHeldPost[],
    count: number,
  ): Promise<LifecycleHeldPost> {
    await expect
      .poll(() => held.length, { message: `valuation POST ${count} is held` })
      .toBeGreaterThanOrEqual(count);
    const post = held[count - 1];
    if (post === undefined) throw new Error(`No held valuation POST ${count}`);
    return post;
  }

  /** The blended fair value per share of a decoded valuation response, as the API's string. */
  function lifecycleBlended(served: JsonObject): string {
    const value = served.blended_fair_value_per_share_kes;
    if (typeof value !== "string") {
      throw new Error(`No blended fair value in ${JSON.stringify(served)}`);
    }
    return value;
  }

  /** Lets a held POST through to the API and returns the server's decoded 200 response. */
  async function lifecycleRelease(post: LifecycleHeldPost): Promise<JsonObject> {
    await post.route.continue();
    const response = await post.request.response();
    expect(response?.status(), "the released valuation's status").toBe(200);
    return valuationJson(response);
  }

  /**
   * Waits until valuation POST number `count` (1-based) is answered 200 and the sandbox has
   * settled on it, with no "Recalculating…" left, and returns the server's decoded response.
   */
  async function lifecycleSettle(
    sandbox: Locator,
    posts: readonly ValuationPost[],
    count: number,
  ): Promise<JsonObject> {
    await expect
      .poll(() => posts[count - 1]?.response?.status() ?? null, {
        message: `valuation POST ${count} is answered`,
      })
      .toBe(200);
    await expect(sandbox.getByTestId("ipo-recalculating")).toHaveCount(0);
    return valuationJson(posts[count - 1]?.response ?? null);
  }

  /** The page-load POST is the only one so far and carries exactly the preset's body. */
  async function lifecycleExpectPresetPost(posts: readonly ValuationPost[]): Promise<void> {
    await expect.poll(() => posts.length, { message: "page-load valuation POSTs" }).toBe(1);
    expect(posts[0]?.body).toEqual(LIFECYCLE_PRESET_BODY);
  }

  /**
   * Opens the sandbox with the browser clock installed and running, so the preset loads and
   * settles normally, then pauses the clock: from then on no timer, the debounce included, fires
   * until the test advances it.
   *
   * `started()` counts the valuation POSTs the page has handed to `fetch` since the pause. The
   * count is kept in the page and read by a fresh evaluation, which runs only once every promise
   * callback queued before it has run, so it already includes a request started by a timer the
   * last clock advance fired. Playwright's own request events carry no such ordering.
   */
  async function lifecycleOpenPaused(page: Page): Promise<{
    sandbox: Locator;
    blended: Locator;
    started: () => Promise<number>;
  }> {
    await page.addInitScript((path) => {
      const counter = { valuationPosts: 0 };
      const originalFetch = window.fetch.bind(window);
      window.fetch = (input: RequestInfo | URL, init?: RequestInit) => {
        let url: string;
        let method = init?.method;
        if (typeof input === "string") {
          url = input;
        } else if (input instanceof URL) {
          url = input.href;
        } else {
          url = input.url;
          method ??= input.method;
        }
        const pathname = new URL(url, window.location.href).pathname;
        if ((method ?? "GET").toUpperCase() === "POST" && pathname === path) {
          counter.valuationPosts += 1;
        }
        return originalFetch(input, init);
      };
      (window as unknown as { lifecycleCounter?: typeof counter }).lifecycleCounter = counter;
    }, IPO_VALUATION_PATH);
    const readCount = (): Promise<number> =>
      page.evaluate(() => {
        const store = window as unknown as { lifecycleCounter?: { valuationPosts: number } };
        return store.lifecycleCounter?.valuationPosts ?? Number.NaN;
      });
    await page.clock.install();
    const opened = await openSandbox(page);
    const pageNow = await page.evaluate(() => Date.now());
    await page.clock.pauseAt(pageNow + LIFECYCLE_PAUSE_LEAD_MS);
    const atPause = await readCount();
    return { ...opened, started: async () => (await readCount()) - atPause };
  }

  /**
   * Records in the page, with a MutationObserver on the sandbox root, every text the blended
   * fair value card shows and whether the general error banner ever appears, so a result shown
   * only briefly is caught even between two assertions. Returns a reader of the record.
   */
  async function lifecycleWatchOutput(sandbox: Locator): Promise<() => Promise<LifecycleShown>> {
    await sandbox.evaluate((root) => {
      const shown: LifecycleShown = { blended: [], generalError: false };
      const record = (): void => {
        const card = root.querySelector('[data-testid="ipo-blended-fair-value"]');
        const text = card?.textContent ?? "";
        if (shown.blended.at(-1) !== text) shown.blended.push(text);
        if (root.querySelector('[data-testid="ipo-general-error"]') !== null) {
          shown.generalError = true;
        }
      };
      record();
      new MutationObserver(record).observe(root, {
        childList: true,
        subtree: true,
        characterData: true,
      });
      (window as unknown as { lifecycleShown?: LifecycleShown }).lifecycleShown = shown;
    });
    return () =>
      sandbox.page().evaluate(() => {
        const shown = (window as unknown as { lifecycleShown?: LifecycleShown }).lifecycleShown;
        if (shown === undefined) throw new Error("The sandbox output is not being watched");
        return shown;
      });
  }

  test("moving the WACC slider to 16.00% raises the blended fair value within 2 s", async ({
    page,
  }) => {
    const { sandbox, blended } = await openSandbox(page);
    const posts = recordValuationPosts(page);

    // The slider holds hundredths of a percentage point with a 25-unit step, so eight
    // ArrowLeft presses move it from 1800 (18.00%) to 1600 (16.00%).
    const wacc = sandbox.getByTestId("ipo-input-wacc_pct");
    await expect(wacc).toHaveValue("1800");
    await wacc.focus();
    for (let press = 1; press < 8; press += 1) {
      await wacc.press("ArrowLeft");
    }
    // One 2 s budget, started just before the eighth and final input, covers every check of the
    // settled result below; each waits only for the time left of it.
    const finalInputAt = Date.now();
    await wacc.press("ArrowLeft");
    const timeLeft = (): { timeout: number } => ({
      // At least 1 ms: a zero timeout would mean no limit at all.
      timeout: Math.max(1, finalInputAt + LIFECYCLE_SETTLE_BUDGET_MS - Date.now()),
    });
    await expect(wacc).toHaveValue("1600", timeLeft());
    await expect(sandbox.getByTestId("ipo-value-wacc_pct")).toHaveText("16.00%", timeLeft());

    // The settled request is the last POST sent, for exactly "16.00", answered 200; a response
    // to an intermediate WACC never matches.
    await expect
      .poll(
        () => {
          const last = posts.at(-1);
          return last === undefined
            ? null
            : { wacc: jsonObject(last.body).wacc_pct, status: last.response?.status() };
        },
        { ...timeLeft(), message: "the last valuation POST is WACC 16.00, answered 200" },
      )
      .toEqual({ wacc: "16.00", status: 200 });
    const settled = posts.at(-1);
    const servedBlended = lifecycleBlended(await valuationJson(settled?.response ?? null));
    expect(toCents(servedBlended)).toBeGreaterThan(682);

    // The screen shows that response: no indicator, its blended value, and the outlined matrix
    // cell in the 16% row carrying the same value.
    await expect(sandbox.getByTestId("ipo-recalculating")).toHaveCount(0, timeLeft());
    await expect
      .poll(async () => toCents(await blended.textContent()), timeLeft())
      .toBe(toCents(servedBlended));
    const selectedCell = withAttributes(sandbox, "sensitivity-cell", { "data-selected": "true" });
    await expect(selectedCell).toHaveCount(1, timeLeft());
    await expect(selectedCell).toHaveAttribute("data-wacc", "16.00", timeLeft());
    await expect(selectedCell).toHaveAttribute("data-margin", "4.50", timeLeft());
    expect(toCents(await selectedCell.textContent())).toBe(toCents(servedBlended));

    await expect(sandbox).toHaveAttribute("data-state", "ready", timeLeft());
    await expect(sandbox.getByTestId("field-error-wacc_pct")).toHaveCount(0, timeLeft());
    await expect(sandbox.getByTestId("ipo-general-error")).toHaveCount(0, timeLeft());
    expect(posts.at(-1)).toBe(settled);
    expect(Date.now() - finalInputAt).toBeLessThanOrEqual(LIFECYCLE_SETTLE_BUDGET_MS);
  });

  test("slider changes show at once, and only the last is posted, 150 ms after it", async ({
    page,
  }) => {
    const { sandbox, blended, started } = await lifecycleOpenPaused(page);
    const posts = recordValuationPosts(page);

    // Four changes 100 ms apart: each shows beside its slider at once and posts nothing. The
    // burst lasts twice the debounce, but the inputs never stay unchanged for 150 ms.
    const burst = [
      { field: "wacc_pct", key: "ArrowLeft", shown: "17.75%" },
      { field: "wacc_pct", key: "ArrowLeft", shown: "17.50%" },
      { field: "terminal_growth_pct", key: "ArrowRight", shown: "5.25%" },
      { field: "offer_price_kes", key: "ArrowRight", shown: "KES 7.55" },
    ] as const;
    for (const [index, change] of burst.entries()) {
      if (index > 0) await page.clock.runFor(100);
      await sandbox.getByTestId(`ipo-input-${change.field}`).press(change.key);
      await expect(sandbox.getByTestId(`ipo-value-${change.field}`)).toHaveText(change.shown);
      expect(await started()).toBe(0);
    }

    // The debounce runs from the last change only: 149 ms after it nothing is sent, and 1 ms
    // later exactly one POST carries every change of the burst.
    await page.clock.runFor(LIFECYCLE_DEBOUNCE_MS - 1);
    expect(await started()).toBe(0);
    await page.clock.runFor(1);
    expect(await started()).toBe(1);
    await expect.poll(() => posts.length).toBe(1);
    expect(posts[0]?.body).toEqual({
      ...LIFECYCLE_PRESET_BODY,
      wacc_pct: "17.50",
      terminal_growth_pct: "5.25",
      offer_price_kes: "7.55",
    });

    const served = await lifecycleSettle(sandbox, posts, 1);
    await expect
      .poll(async () => toCents(await blended.textContent()))
      .toBe(toCents(lifecycleBlended(served)));
    expect(await started()).toBe(1);
    expect(posts).toHaveLength(1);
  });

  for (const { field, sent } of LIFECYCLE_SLIDER_STEPS) {
    test(`one step of the ${field} slider posts the preset body with ${field} "${sent}"`, async ({
      page,
    }) => {
      const posts = recordValuationPosts(page);
      const { sandbox, blended } = await openSandbox(page);
      await lifecycleExpectPresetPost(posts);

      await sandbox.getByTestId(`ipo-input-${field}`).press("ArrowRight");
      const served = await lifecycleSettle(sandbox, posts, 2);
      expect(posts[1]?.body).toEqual({ ...LIFECYCLE_PRESET_BODY, [field]: sent });
      expect(posts).toHaveLength(2);
      await expect
        .poll(async () => toCents(await blended.textContent()))
        .toBe(toCents(lifecycleBlended(served)));
    });
  }

  for (const { field, typed, sent } of LIFECYCLE_TEXT_ENTRIES) {
    test(`typing ${JSON.stringify(typed)} into ${field} posts the preset body with ${field} ${JSON.stringify(sent)}`, async ({
      page,
    }) => {
      const posts = recordValuationPosts(page);
      const { sandbox, blended } = await openSandbox(page);
      await lifecycleExpectPresetPost(posts);

      await sandbox.getByTestId(`ipo-input-${field}`).fill(typed);
      const served = await lifecycleSettle(sandbox, posts, 2);
      expect(posts[1]?.body).toEqual({ ...LIFECYCLE_PRESET_BODY, [field]: sent });
      expect(posts).toHaveLength(2);
      await expect(sandbox.getByTestId(`field-error-${field}`)).toHaveCount(0);
      await expect
        .poll(async () => toCents(await blended.textContent()))
        .toBe(toCents(lifecycleBlended(served)));
    });
  }

  test("a pending valuation keeps the previous result on screen, dimmed, until it settles", async ({
    page,
  }) => {
    const { sandbox, blended, started } = await lifecycleOpenPaused(page);
    const held = await lifecycleHoldPosts(page);
    const output = sandbox.getByTestId("ipo-valuation-output");
    const indicator = sandbox.getByTestId("ipo-recalculating");
    await expect(output).not.toHaveClass(LIFECYCLE_DIMMED_CLASS);
    await expect(output).toHaveCSS("opacity", "1");

    await sandbox.getByTestId("ipo-input-wacc_pct").press("ArrowLeft");
    await expect(sandbox.getByTestId("ipo-value-wacc_pct")).toHaveText("17.75%");
    await page.clock.runFor(LIFECYCLE_DEBOUNCE_MS);
    const pending = await lifecycleHeld(held, 1);
    expect(pending.body).toEqual({ ...LIFECYCLE_PRESET_BODY, wacc_pct: "17.75" });

    // Fake seconds pass with the request held: the preset's result stays, dimmed to 60% opacity
    // beneath the indicator, and nothing else is sent.
    await page.clock.runFor(LIFECYCLE_IDLE_MS);
    expect(await started()).toBe(1);
    expect(held).toHaveLength(1);
    await expect(blended).toHaveText("KES 6.82");
    await expect(output).toHaveClass(LIFECYCLE_DIMMED_CLASS);
    await expect(output).toHaveCSS("opacity", "0.6");
    await expect(indicator).toBeVisible();
    await expect(indicator).toHaveText(LIFECYCLE_RECALCULATING_TEXT);
    await expect(sandbox).toHaveAttribute("data-state", "ready");
    await expect(sandbox.getByTestId("ipo-general-error")).toHaveCount(0);

    // The server's answer replaces it at full opacity, and the indicator goes.
    const servedBlended = lifecycleBlended(await lifecycleRelease(pending));
    expect(toCents(servedBlended)).not.toBe(682);
    await expect
      .poll(async () => toCents(await blended.textContent()))
      .toBe(toCents(servedBlended));
    await expect(indicator).toHaveCount(0);
    await expect(output).not.toHaveClass(LIFECYCLE_DIMMED_CLASS);
    await expect(output).toHaveCSS("opacity", "1");
    expect(await started()).toBe(1);
    expect(held).toHaveLength(1);
  });

  for (const stale of LIFECYCLE_STALE_ANSWERS) {
    test(`a superseded valuation is aborted, and a late ${stale.kind} answer to it is never shown`, async ({
      page,
    }) => {
      const posts = recordValuationPosts(page);
      const { sandbox, blended, started } = await lifecycleOpenPaused(page);
      await lifecycleExpectPresetPost(posts);
      const preset = await valuationJson(posts[0]?.response ?? null);
      const held = await lifecycleHoldPosts(page);
      const shown = await lifecycleWatchOutput(sandbox);
      const wacc = sandbox.getByTestId("ipo-input-wacc_pct");
      const waccValue = sandbox.getByTestId("ipo-value-wacc_pct");

      // Request A, for WACC 17.75%, is sent and held.
      await wacc.press("ArrowLeft");
      await expect(waccValue).toHaveText("17.75%");
      await page.clock.runFor(LIFECYCLE_DEBOUNCE_MS);
      const requestA = await lifecycleHeld(held, 1);
      expect(requestA.body).toEqual({ ...LIFECYCLE_PRESET_BODY, wacc_pct: "17.75" });

      // Moving on to 17.50% aborts A at once, before B is even due.
      await wacc.press("ArrowLeft");
      await expect(waccValue).toHaveText("17.50%");
      await expect
        .poll(() => requestA.request.failure()?.errorText ?? null, { message: "request A failed" })
        .toBe("net::ERR_ABORTED");
      expect(await started()).toBe(1);
      // The late answer to A. Playwright may refuse to answer a request the page has cancelled;
      // either way, nothing of it may reach the screen.
      await requestA.route.fulfill(stale.answer(preset)).catch(() => undefined);

      // Request B is posted after the debounce and its server answer is the only one shown.
      await page.clock.runFor(LIFECYCLE_DEBOUNCE_MS);
      expect(await started()).toBe(2);
      const requestB = await lifecycleHeld(held, 2);
      expect(requestB.body).toEqual({ ...LIFECYCLE_PRESET_BODY, wacc_pct: "17.50" });
      const servedBlended = lifecycleBlended(await lifecycleRelease(requestB));
      await expect
        .poll(async () => toCents(await blended.textContent()))
        .toBe(toCents(servedBlended));
      await expect(sandbox.getByTestId("ipo-recalculating")).toHaveCount(0);
      await expect(sandbox.getByTestId("ipo-general-error")).toHaveCount(0);
      await expect(sandbox).toHaveAttribute("data-state", "ready");
      expect(await started()).toBe(2);
      expect(held).toHaveLength(2);
      expect(posts).toHaveLength(3);

      // From the preset's result straight to B's: no other value and no error ever appeared.
      const record = await shown();
      expect(record.blended.map(toCents)).toEqual([682, toCents(servedBlended)]);
      expect(record.generalError).toBe(false);
    });
  }

  for (const { typed, message } of LIFECYCLE_INVALID_TAX_RATES) {
    test(`tax rate ${JSON.stringify(typed)} is refused locally, aborting the valuation in flight and posting nothing`, async ({
      page,
    }) => {
      const { sandbox, blended, started } = await lifecycleOpenPaused(page);
      const posts = recordValuationPosts(page);
      const held = await lifecycleHoldPosts(page);
      const output = sandbox.getByTestId("ipo-valuation-output");
      const indicator = sandbox.getByTestId("ipo-recalculating");
      const taxRate = sandbox.getByTestId("ipo-input-tax_rate_pct");
      const fieldError = sandbox.getByTestId("field-error-tax_rate_pct");

      // A valid change is posted and held in flight.
      await sandbox.getByTestId("ipo-input-wacc_pct").press("ArrowLeft");
      await expect(sandbox.getByTestId("ipo-value-wacc_pct")).toHaveText("17.75%");
      await page.clock.runFor(LIFECYCLE_DEBOUNCE_MS);
      const inFlight = await lifecycleHeld(held, 1);

      // The invalid text shows its message beneath the input and aborts the request in flight.
      // Well past the debounce nothing is posted, and the preset's result stays, undimmed.
      await taxRate.fill(typed);
      await expect(fieldError).toHaveText(message);
      await expect(taxRate).toHaveAttribute("aria-invalid", "true");
      await expect
        .poll(() => inFlight.request.failure()?.errorText ?? null, {
          message: "the request in flight failed",
        })
        .toBe("net::ERR_ABORTED");
      await page.clock.runFor(LIFECYCLE_IDLE_MS);
      expect(await started()).toBe(1);
      expect(posts).toHaveLength(1);
      await expect(blended).toHaveText("KES 6.82");
      await expect(indicator).toHaveCount(0);
      await expect(output).not.toHaveClass(LIFECYCLE_DIMMED_CLASS);
      await expect(output).toHaveCSS("opacity", "1");
      await expect(sandbox).toHaveAttribute("data-state", "ready");
      await expect(sandbox.getByTestId("ipo-general-error")).toHaveCount(0);

      // Correcting the text clears the message and posts exactly once, after the debounce.
      await taxRate.fill("25");
      await expect(fieldError).toHaveCount(0);
      await page.clock.runFor(LIFECYCLE_DEBOUNCE_MS - 1);
      expect(await started()).toBe(1);
      await page.clock.runFor(1);
      expect(await started()).toBe(2);
      const corrected = await lifecycleHeld(held, 2);
      expect(corrected.body).toEqual({
        ...LIFECYCLE_PRESET_BODY,
        wacc_pct: "17.75",
        tax_rate_pct: "25",
      });
      const servedBlended = lifecycleBlended(await lifecycleRelease(corrected));
      await expect
        .poll(async () => toCents(await blended.textContent()))
        .toBe(toCents(servedBlended));
      await expect(indicator).toHaveCount(0);
      expect(await started()).toBe(2);
      expect(posts).toHaveLength(2);
    });
  }

  /** The server's coded 422 message for the preset with primary proceeds of 3,750,000,000.01. */
  const FIELD_ERRORS_PRIMARY_MESSAGE =
    "Primary proceeds (KES 3,750,000,000.01) cannot exceed the offer size (KES 3,750,000,000.00, " +
    "offer price times shares offered): the difference would be negative secondary proceeds.";
  /** The server's coded 422 message for the preset with WACC 8.00% and terminal growth 10.00%. */
  const FIELD_ERRORS_WACC_MESSAGE = "WACC (8.00%) must be above the terminal growth rate (10.00%).";
  /**
   * Fake-clock time run after a change: far past the 150 ms debounce, and far short of the API
   * client's 20 s request timeout, which the same clock drives.
   */
  const FIELD_ERRORS_PAST_DEBOUNCE_MS = 1_000;

  /** Body fields a valuation POST must carry, each with its exact JSON value. */
  type FieldErrorsBodyFields = Readonly<Record<string, string | number>>;
  /** One change to an input: a key pressed on a slider, or the text typed into a text input. */
  type FieldErrorsChange = { readonly press: string } | { readonly fill: string };

  /** The response to the next valuation POST carrying `fields`; start it before the change. */
  function fieldErrorsValuationResponse(
    page: Page,
    fields: FieldErrorsBodyFields,
  ): Promise<Response> {
    return page.waitForResponse((response) => {
      const request = response.request();
      if (!isValuationPost(request)) return false;
      const body = jsonObject(request.postDataJSON());
      return Object.entries(fields).every(([field, value]) => body[field] === value);
    });
  }

  /** Makes `change` to `input` the way a user would: a key press, or typing the whole text. */
  async function fieldErrorsChangeInput(input: Locator, change: FieldErrorsChange): Promise<void> {
    if ("press" in change) {
      await input.press(change.press);
    } else {
      await input.fill(change.fill);
    }
  }

  /**
   * Asserts `message`, exactly, beneath the input of `field`: inside that input's own field
   * container, named by the input's `aria-describedby`, with the input marked `aria-invalid`.
   */
  async function fieldErrorsExpectBesideInput(
    sandbox: Locator,
    field: string,
    message: string,
  ): Promise<void> {
    const input = sandbox.getByTestId(`ipo-input-${field}`);
    const fieldError = input.locator("xpath=..").getByTestId(`field-error-${field}`);
    await expect(fieldError).toHaveText(message);
    await expect(input).toHaveAttribute("aria-invalid", "true");
    const errorId = await fieldError.getAttribute("id");
    expect(errorId).toBeTruthy();
    expect((await input.getAttribute("aria-describedby"))?.split(" ")).toContain(errorId);
  }

  /** Asserts that the input of `field` shows no error and is no longer marked invalid. */
  async function fieldErrorsExpectNone(sandbox: Locator, field: string): Promise<void> {
    await expect(sandbox.getByTestId(`field-error-${field}`)).toHaveCount(0);
    await expect(sandbox.getByTestId(`ipo-input-${field}`)).not.toHaveAttribute("aria-invalid");
  }

  /** Asserts that the result on screen is settled: undimmed, no indicator, the root `ready`. */
  async function fieldErrorsExpectSettled(sandbox: Locator): Promise<void> {
    await expect(sandbox.getByTestId("ipo-recalculating")).toHaveCount(0);
    await expect(sandbox.getByTestId("ipo-valuation-output")).toHaveCSS("opacity", "1");
    await expect(sandbox).toHaveAttribute("data-state", "ready");
  }

  test("primary proceeds above the offer size show the server's field error beside the input", async ({
    page,
  }) => {
    const { sandbox, blended } = await openSandbox(page);
    const primaryProceeds = sandbox.getByTestId("ipo-input-primary_proceeds_kes");
    const primaryProceedsCard = sandbox.getByTestId("ipo-primary-proceeds");
    const generalError = sandbox.getByTestId("ipo-general-error");
    await expect(primaryProceeds).toHaveValue("0.00");
    await expect(primaryProceedsCard).toHaveText("KES 0.00");

    // KES 7.50 × 500,000,000 shares offered is 3,750,000,000.00; one cent more passes the
    // single-field check and is rejected by the server as primary_exceeds_offer_size, a coded
    // detail object rather than FastAPI's list.
    const rejected = fieldErrorsValuationResponse(page, { primary_proceeds_kes: "3750000000.01" });
    await primaryProceeds.fill("3750000000.01");
    const rejection = await rejected;
    expect(rejection.status()).toBe(422);
    expect(await rejection.json()).toEqual({
      detail: {
        code: "primary_exceeds_offer_size",
        field: "primary_proceeds_kes",
        message: FIELD_ERRORS_PRIMARY_MESSAGE,
      },
    });
    await fieldErrorsExpectBesideInput(
      sandbox,
      "primary_proceeds_kes",
      FIELD_ERRORS_PRIMARY_MESSAGE,
    );
    // The preset's result stays on screen, settled.
    await fieldErrorsExpectSettled(sandbox);
    expect(toCents(await blended.textContent())).toBe(682);
    await expect(primaryProceedsCard).toHaveText("KES 0.00");
    await expect(generalError).toHaveCount(0);

    // "0" is sent as typed, which is not the preset's "0.00", so restoring it is a new valuation:
    // the error clears once the server has answered it.
    const recovered = fieldErrorsValuationResponse(page, { primary_proceeds_kes: "0" });
    await primaryProceeds.fill("0");
    const recovery = await recovered;
    expect(recovery.status()).toBe(200);
    const recoveryBody = await valuationJson(recovery);
    expect(recoveryBody.blended_fair_value_per_share_kes).toBe("6.82");
    expect(recoveryBody.primary_proceeds_kes).toBe("0.00");
    await fieldErrorsExpectNone(sandbox, "primary_proceeds_kes");
    await fieldErrorsExpectSettled(sandbox);
    expect(toCents(await blended.textContent())).toBe(
      toCents(String(recoveryBody.blended_fair_value_per_share_kes)),
    );
    await expect(primaryProceedsCard).toHaveText("KES 0.00");
    await expect(generalError).toHaveCount(0);
  });

  test("WACC at or below terminal growth shows the server's coded error beneath the WACC slider", async ({
    page,
  }) => {
    const { sandbox, blended } = await openSandbox(page);
    const wacc = sandbox.getByTestId("ipo-input-wacc_pct");
    const terminalGrowth = sandbox.getByTestId("ipo-input-terminal_growth_pct");
    const generalError = sandbox.getByTestId("ipo-general-error");

    // Home takes WACC to its 8.00% minimum, still above the preset's 5.00% terminal growth.
    const lowered = fieldErrorsValuationResponse(page, {
      wacc_pct: "8.00",
      terminal_growth_pct: "5.00",
    });
    await wacc.focus();
    await wacc.press("Home");
    const lowWacc = await lowered;
    expect(lowWacc.status()).toBe(200);
    const lowWaccBlended = String((await valuationJson(lowWacc)).blended_fair_value_per_share_kes);
    await fieldErrorsExpectSettled(sandbox);
    expect(toCents(await blended.textContent())).toBe(toCents(lowWaccBlended));

    // End takes terminal growth to its 10.00% maximum, so WACC no longer exceeds it.
    const rejected = fieldErrorsValuationResponse(page, {
      wacc_pct: "8.00",
      terminal_growth_pct: "10.00",
    });
    await terminalGrowth.focus();
    await terminalGrowth.press("End");
    const rejection = await rejected;
    expect(rejection.status()).toBe(422);
    expect(await rejection.json()).toEqual({
      detail: {
        code: "wacc_not_above_terminal_growth",
        field: "wacc_pct",
        message: FIELD_ERRORS_WACC_MESSAGE,
      },
    });
    await fieldErrorsExpectBesideInput(sandbox, "wacc_pct", FIELD_ERRORS_WACC_MESSAGE);
    await fieldErrorsExpectNone(sandbox, "terminal_growth_pct");
    await expect(sandbox.getByTestId("ipo-value-terminal_growth_pct")).toHaveText("10.00%");
    // The 8.00% valuation stays on screen, settled.
    await fieldErrorsExpectSettled(sandbox);
    expect(toCents(await blended.textContent())).toBe(toCents(lowWaccBlended));
    await expect(generalError).toHaveCount(0);

    // End takes WACC up to 30.00%, above terminal growth again: valued and cleared.
    const recovered = fieldErrorsValuationResponse(page, {
      wacc_pct: "30.00",
      terminal_growth_pct: "10.00",
    });
    await wacc.focus();
    await wacc.press("End");
    const recovery = await recovered;
    expect(recovery.status()).toBe(200);
    const recoveredBlended = String(
      (await valuationJson(recovery)).blended_fair_value_per_share_kes,
    );
    expect(toCents(recoveredBlended)).not.toBe(toCents(lowWaccBlended));
    await fieldErrorsExpectNone(sandbox, "wacc_pct");
    await expect(wacc).not.toHaveAttribute("aria-describedby");
    await fieldErrorsExpectSettled(sandbox);
    expect(toCents(await blended.textContent())).toBe(toCents(recoveredBlended));
    await expect(generalError).toHaveCount(0);
  });

  test("FastAPI's standard 422 list puts each item beneath its input, or in the general error", async ({
    page,
  }) => {
    const { sandbox, blended } = await openSandbox(page);
    // Pydantic's list for two request fields that have inputs, a text input and a slider, and
    // one for a request field that has none.
    const inputItems = [
      {
        type: "decimal_max_places",
        loc: ["body", "tax_rate_pct"],
        msg: "Decimal input should have no more than 4 decimal places",
        input: "31",
      },
      {
        type: "less_than_equal",
        loc: ["body", "ebitda_margin_pct"],
        msg: "Input should be less than or equal to 15",
        input: "4.50",
      },
    ] as const;
    const unmatchedItem = {
      type: "too_long",
      loc: ["body", "sensitivity_wacc_pct"],
      msg: "List should have at most 9 items after validation, not 10",
      input: ["10", "11", "12", "13", "14", "15", "16", "17", "18", "19"],
    } as const;
    const answeredBodies: JsonObject[] = [];
    await page.route(
      IPO_VALUATION_ROUTE,
      async (route) => {
        answeredBodies.push(jsonObject(route.request().postDataJSON()));
        await route.fulfill({
          status: 422,
          contentType: "application/json",
          body: JSON.stringify({ detail: [...inputItems, unmatchedItem] }),
        });
      },
      { times: 1 },
    );

    const answered = fieldErrorsValuationResponse(page, { tax_rate_pct: "31" });
    await sandbox.getByTestId("ipo-input-tax_rate_pct").fill("31");
    expect((await answered).status()).toBe(422);
    expect(answeredBodies.map((body) => body.tax_rate_pct)).toEqual(["31"]);

    // Each item's msg sits beneath the input its last loc element names; the item that names no
    // input is shown in the general error with its field, without a Retry.
    for (const item of inputItems) {
      await fieldErrorsExpectBesideInput(sandbox, item.loc[1], item.msg);
    }
    await expect(sandbox.locator('[data-testid^="field-error-"]')).toHaveCount(inputItems.length);
    await expect(sandbox.getByTestId("ipo-general-error").locator("p")).toHaveText([
      `${unmatchedItem.msg} (${unmatchedItem.loc[1]})`,
    ]);
    await expect(sandbox.getByTestId("ipo-retry")).toHaveCount(0);
    // The preset's result stays on screen, settled.
    await fieldErrorsExpectSettled(sandbox);
    expect(toCents(await blended.textContent())).toBe(682);
  });

  /**
   * Detours away from the preset's inputs and back to its body, byte for byte, so the result on
   * screen already answers them: one reverted inside the debounce, one after the server rejected
   * it. A further change follows, whose POST must be the only one since the return.
   */
  const FIELD_ERRORS_RETURN_CASES: readonly {
    title: string;
    field: string;
    detour: FieldErrorsChange;
    /** The detour's body value and coded rejection when it outlasts the debounce, else null. */
    rejection: { bodyValue: string; code: string; message: string } | null;
    restore: FieldErrorsChange;
    next: FieldErrorsChange;
    nextBodyValue: string;
  }[] = [
    {
      title: "a WACC step reverted inside the debounce",
      field: "wacc_pct",
      detour: { press: "ArrowRight" },
      rejection: null,
      restore: { press: "ArrowLeft" },
      next: { press: "ArrowLeft" },
      nextBodyValue: "17.75",
    },
    {
      title: "rejected primary proceeds retyped as the preset's 0.00",
      field: "primary_proceeds_kes",
      detour: { fill: "3750000000.01" },
      rejection: {
        bodyValue: "3750000000.01",
        code: "primary_exceeds_offer_size",
        message: FIELD_ERRORS_PRIMARY_MESSAGE,
      },
      restore: { fill: "0.00" },
      next: { fill: "1" },
      nextBodyValue: "1",
    },
  ];

  for (const scenario of FIELD_ERRORS_RETURN_CASES) {
    test(`returning to the result on screen after ${scenario.title} sends no request`, async ({
      page,
    }) => {
      const { sandbox, blended } = await openSandbox(page);
      const input = sandbox.getByTestId(`ipo-input-${scenario.field}`);
      const output = sandbox.getByTestId("ipo-valuation-output");
      const recalculating = sandbox.getByTestId("ipo-recalculating");
      const posts = recordValuationPosts(page);
      // From here the page's timers run only when the test advances the paused clock, so the
      // debounce cannot elapse between two changes, however slowly they are made.
      const installedAt = Date.now();
      await page.clock.install({ time: installedAt });
      await page.clock.pauseAt(installedAt + FIELD_ERRORS_PAST_DEBOUNCE_MS);

      await fieldErrorsChangeInput(input, scenario.detour);
      await expect(recalculating).toBeVisible();
      await expect(output).toHaveCSS("opacity", "0.6");
      expect(posts).toHaveLength(0);
      if (scenario.rejection !== null) {
        const rejected = fieldErrorsValuationResponse(page, {
          [scenario.field]: scenario.rejection.bodyValue,
        });
        await page.clock.runFor(FIELD_ERRORS_PAST_DEBOUNCE_MS);
        const rejection = await rejected;
        expect(rejection.status()).toBe(422);
        expect(await rejection.json()).toEqual({
          detail: {
            code: scenario.rejection.code,
            field: scenario.field,
            message: scenario.rejection.message,
          },
        });
        await fieldErrorsExpectBesideInput(sandbox, scenario.field, scenario.rejection.message);
        await expect(recalculating).toHaveCount(0);
      }
      const sentBeforeReturn = posts.length;

      // Back at the preset's body: the change resolves before any timer runs, clearing the
      // server's error, and nothing is scheduled once the clock runs past the debounce.
      await fieldErrorsChangeInput(input, scenario.restore);
      await fieldErrorsExpectNone(sandbox, scenario.field);
      await fieldErrorsExpectSettled(sandbox);
      await page.clock.runFor(FIELD_ERRORS_PAST_DEBOUNCE_MS);
      await fieldErrorsExpectSettled(sandbox);
      expect(toCents(await blended.textContent())).toBe(682);
      await expect(sandbox.getByTestId("ipo-general-error")).toHaveCount(0);

      // A further change is posted as usual. Requests are reported in the order they are sent,
      // so its POST being the only one since the return proves that the return sent none.
      const nextAnswered = fieldErrorsValuationResponse(page, {
        [scenario.field]: scenario.nextBodyValue,
      });
      await fieldErrorsChangeInput(input, scenario.next);
      await page.clock.runFor(FIELD_ERRORS_PAST_DEBOUNCE_MS);
      expect((await nextAnswered).status()).toBe(200);
      expect(
        posts.slice(sentBeforeReturn).map((post) => jsonObject(post.body)[scenario.field]),
      ).toEqual([scenario.nextBodyValue]);
    });
  }
});

const WATCHLISTS_PATH = "/api/v1/terminal/watchlists";
const SCREENER_PATH = "/api/v1/terminal/market/screener";
const BANDS_PATH = "/api/v1/terminal/market/valuation-bands";

/** The eight seeded counters, the only tickers `terminal_equities` holds. */
const SEEDED_TICKERS: ReadonlySet<string> = new Set([
  "ABSA",
  "COOP",
  "EABL",
  "EQTY",
  "KCB",
  "KEGN",
  "SBIC",
  "SCOM",
]);

/**
 * Orders strings by Unicode code point, the order PostgreSQL's `"C"` collation gives watchlist
 * names in `GET /watchlists`. Each string is split into whole code points first, so a character
 * beyond U+FFFF is compared as one value rather than as two UTF-16 units.
 */
function compareCodePoints(a: string, b: string): number {
  const left = Array.from(a, (character) => character.codePointAt(0) ?? 0);
  const right = Array.from(b, (character) => character.codePointAt(0) ?? 0);
  for (let index = 0; index < Math.min(left.length, right.length); index += 1) {
    const difference = (left[index] ?? 0) - (right[index] ?? 0);
    if (difference !== 0) {
      return difference;
    }
  }
  return left.length - right.length;
}

/** One request the in-memory watchlist API answered. */
interface RecordedRequest {
  method: string;
  /** The URL path with every segment percent-decoded. */
  path: string;
  /** The parsed JSON body, or `null` for a request without one. */
  body: unknown;
}

/** The watchlists of one test, answered in memory; see {@link installWatchlistStore}. */
interface WatchlistStore {
  /** Every request answered, in arrival order. */
  readonly requests: readonly RecordedRequest[];
  /** The requests other than `GET`, in arrival order. */
  mutations(): RecordedRequest[];
  /** The stored lists in `GET /watchlists` order. */
  lists(): Watchlist[];
  /** The stored list with `id`, if any. */
  find(id: string): Watchlist | undefined;
  /** The stored list named `name`; throws when there is none. */
  byName(name: string): Watchlist;
}

/** The `POST /watchlists` body, or `null` when it is not `{name: string, tickers?: string[]}`. */
function parseWatchlistCreate(body: unknown): { name: string; tickers: string[] } | null {
  if (typeof body !== "object" || body === null || !("name" in body)) {
    return null;
  }
  const name: unknown = body.name;
  const tickers: unknown = "tickers" in body ? body.tickers : [];
  if (typeof name !== "string" || !Array.isArray(tickers)) {
    return null;
  }
  const items: unknown[] = tickers;
  const names = items.filter((item): item is string => typeof item === "string");
  return names.length === items.length ? { name: name.trim(), tickers: names } : null;
}

/**
 * Answers every `/watchlists` request of `page` from memory, so a test can create, edit and
 * delete watchlists without writing to the seeded database. The store starts from the API's own
 * `GET /watchlists`, read once before the page loads; nothing else reaches the API, whatever the
 * method or path. It follows the API's watchlist contract:
 * - `GET /watchlists`: 200 with the lists ordered by name in code-point order;
 * - `POST /watchlists`: 201 with the new list, its tickers trimmed, upper-cased, de-duplicated and
 *   sorted; 422 `unknown_ticker` (`field` "tickers") for a ticker outside the eight seeded ones;
 *   409 `watchlist_name_taken` (`field` "name") for a name already used;
 * - `DELETE /watchlists/{id}`: 204, or 404 `watchlist_not_found` (`field` "watchlist_id");
 * - `PUT` and `DELETE /watchlists/{id}/items/{ticker}`: 200 with the list, idempotent, advancing
 *   `updated_at` only on a change; 404 `watchlist_not_found`, then 404 `unknown_ticker` (`field`
 *   "ticker").
 * Ids and timestamps come from counters, so every run produces the same values.
 */
async function installWatchlistStore(page: Page): Promise<WatchlistStore> {
  const seed = await page.request.get(WATCHLISTS_PATH);
  expect(seed.status()).toBe(200);
  const stored = new Map<string, Watchlist>();
  for (const list of (await seed.json()) as Watchlist[]) {
    stored.set(list.id, list);
  }
  const requests: RecordedRequest[] = [];
  let created = 0;
  let clock = 0;
  const now = (): string => {
    clock += 1;
    return new Date(Date.UTC(2026, 9, 30, 12, 0, clock)).toISOString();
  };
  const ordered = (): Watchlist[] =>
    [...stored.values()].sort((a, b) => compareCodePoints(a.name, b.name));
  const sortedTickers = (tickers: Iterable<string>): string[] =>
    [...new Set(tickers)].sort(compareCodePoints);
  const codedError = (status: number, code: string, message: string, field: string) => ({
    status,
    json: { detail: { code, message, field } },
  });

  await page.route("**/api/v1/terminal/watchlists**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const method = request.method();
    const body: unknown = request.postData() === null ? null : request.postDataJSON();
    const decodedPath = url.pathname.split("/").map(decodeURIComponent).join("/");
    requests.push({ method, path: decodedPath, body });

    // The pattern also matches a path such as `/watchlistsX`, which is no route of the API.
    const rest = url.pathname.slice(WATCHLISTS_PATH.length);
    if (rest !== "" && !rest.startsWith("/")) {
      await route.fulfill({ status: 404, json: { detail: "Not Found" } });
      return;
    }
    const segments = rest === "" ? [] : rest.slice(1).split("/").map(decodeURIComponent);
    const [id, items, rawTicker, ...extra] = segments;

    if (id === undefined && method === "GET") {
      await route.fulfill({ json: ordered() });
      return;
    }
    if (id === undefined && method === "POST") {
      const create = parseWatchlistCreate(body);
      if (create === null || create.name === "") {
        await route.fulfill({
          status: 422,
          json: { detail: [{ type: "invalid", loc: ["body"], msg: "Invalid body", input: body }] },
        });
        return;
      }
      const tickers = [...new Set(create.tickers.map((ticker) => ticker.trim().toUpperCase()))];
      const unknown = tickers.filter((ticker) => !SEEDED_TICKERS.has(ticker));
      if (unknown.length > 0) {
        await route.fulfill(
          codedError(422, "unknown_ticker", `Unknown tickers: ${unknown.join(", ")}.`, "tickers"),
        );
        return;
      }
      if ([...stored.values()].some((list) => list.name === create.name)) {
        await route.fulfill(
          codedError(
            409,
            "watchlist_name_taken",
            `A watchlist named '${create.name}' already exists.`,
            "name",
          ),
        );
        return;
      }
      created += 1;
      const timestamp = now();
      const list: Watchlist = {
        id: `00000000-0000-4000-8000-${String(created).padStart(12, "0")}`,
        name: create.name,
        tickers: sortedTickers(tickers),
        created_at: timestamp,
        updated_at: timestamp,
      };
      stored.set(list.id, list);
      await route.fulfill({ status: 201, json: list });
      return;
    }

    const list = id === undefined ? undefined : stored.get(id);
    const isDelete = method === "DELETE" && items === undefined;
    const isItem = (method === "PUT" || method === "DELETE") && items === "items";
    if (
      id === undefined ||
      (!isDelete && !isItem) ||
      (isItem && rawTicker === undefined) ||
      extra.length > 0
    ) {
      await route.fulfill({ status: 405, json: { detail: "Method Not Allowed" } });
      return;
    }
    if (list === undefined) {
      await route.fulfill(
        codedError(404, "watchlist_not_found", `Watchlist ${id} was not found.`, "watchlist_id"),
      );
      return;
    }
    if (isDelete) {
      stored.delete(list.id);
      await route.fulfill({ status: 204 });
      return;
    }
    const ticker = (rawTicker ?? "").trim().toUpperCase();
    if (!SEEDED_TICKERS.has(ticker)) {
      await route.fulfill(
        codedError(404, "unknown_ticker", `Ticker '${ticker}' is not a known equity.`, "ticker"),
      );
      return;
    }
    const tickers =
      method === "PUT"
        ? sortedTickers([...list.tickers, ticker])
        : list.tickers.filter((item) => item !== ticker);
    const changed = tickers.length !== list.tickers.length;
    const updated: Watchlist = changed ? { ...list, tickers, updated_at: now() } : list;
    stored.set(updated.id, updated);
    await route.fulfill({ json: updated });
  });

  return {
    requests,
    mutations: () => requests.filter((request) => request.method !== "GET"),
    lists: ordered,
    find: (id) => stored.get(id),
    byName: (name) => {
      const list = [...stored.values()].find((item) => item.name === name);
      if (list === undefined) {
        throw new Error(`No stored watchlist is named ${JSON.stringify(name)}`);
      }
      return list;
    },
  };
}

/**
 * Answers `GET /market/screener` for a watchlist held by `store`, which the API cannot know: the
 * same request is sent without `watchlist_id`, and only the list's rows are returned, with
 * `count` to match. Any other screener request goes to the API unchanged, so an id the store
 * does not hold gets the API's own `watchlist_not_found`.
 */
async function routeScreenerToStore(page: Page, store: WatchlistStore): Promise<void> {
  await page.route("**/api/v1/terminal/market/screener**", async (route) => {
    const url = new URL(route.request().url());
    const id = url.searchParams.get("watchlist_id");
    const list = id === null ? undefined : store.find(id);
    if (list === undefined) {
      await route.continue();
      return;
    }
    const upstream = new URL(url);
    upstream.searchParams.delete("watchlist_id");
    const response = await route.fetch({ url: upstream.toString() });
    if (response.status() !== 200) {
      await route.fulfill({ response });
      return;
    }
    const body = (await response.json()) as ScreenerResponse;
    const rows = body.rows.filter((row) => list.tickers.includes(row.ticker));
    await route.fulfill({ response, json: { ...body, count: rows.length, rows } });
  });
}

/** The query of a request URL as an object, for comparing a whole query at once. */
function queryOf(url: string): Record<string, string> {
  return Object.fromEntries(new URL(url).searchParams);
}

/** Whether `url` is a `GET /market/screener` whose query has `key`, set to `value` if given. */
function isScreenerUrl(url: string, key?: string, value?: string): boolean {
  const parsed = new URL(url);
  if (parsed.pathname !== SCREENER_PATH) {
    return false;
  }
  if (key === undefined) {
    return true;
  }
  return value === undefined
    ? parsed.searchParams.has(key)
    : parsed.searchParams.get(key) === value;
}

/** The tickers of the shown screener rows, in table order. */
async function rowTickers(screener: Locator): Promise<string[]> {
  return screener
    .getByTestId("screener-row")
    .evaluateAll((rows) => rows.map((row) => row.getAttribute("data-ticker") ?? ""));
}

/** Each shown row's `column` cell read back as a canonical decimal, `null` for the em dash. */
async function columnValues(
  screener: Locator,
  column: string,
): Promise<{ ticker: string; value: string | null }[]> {
  const cells = await screener.getByTestId("screener-row").evaluateAll(
    (rows, name) =>
      rows.map((row) => ({
        ticker: row.getAttribute("data-ticker") ?? "",
        text: row.querySelector(`[data-column="${name}"]`)?.textContent ?? "",
      })),
    column,
  );
  return cells.map(({ ticker, text }) => ({ ticker, value: displayedDecimal(text) }));
}

/** Waits for the screener to show `count` rows from a settled request, none of them dimmed. */
async function expectSettledRows(screener: Locator, count: number): Promise<void> {
  await expectReady(screener, SLOW_READY_TIMEOUT);
  await expect(screener.locator('[aria-busy="true"]')).toHaveCount(0, {
    timeout: SLOW_READY_TIMEOUT,
  });
  await expect(screener.getByTestId("screener-row")).toHaveCount(count);
}

/** A watchlist card of the manager, by its name. */
function watchlistCard(manager: Locator, name: string): Locator {
  return withAttributes(manager, "watchlist-item", { "data-watchlist-name": name });
}

/** Creates a watchlist through the manager's form and waits for its card. */
async function createWatchlist(manager: Locator, name: string, tickers: string): Promise<Locator> {
  await manager.getByTestId("watchlist-name-input").fill(name);
  await manager.getByTestId("watchlist-tickers-input").fill(tickers);
  await manager.getByTestId("watchlist-create-button").click();
  const card = watchlistCard(manager, name);
  await expect(card).toHaveCount(1);
  return card;
}

/** The ticker chips of a watchlist card, in order. */
function chipsOf(card: Locator): Locator {
  return card.getByTestId("watchlist-ticker-chip");
}

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
    const marketFlowsResponse = page.waitForResponse((response) =>
      isFlowsUrl(response.url(), null),
    );
    await page.goto("/screener");

    const bands = page.getByTestId("terminal-valuation-bands-chart");
    const flows = page.getByTestId("terminal-net-flow-chart");
    await expect(bands).toHaveAttribute("data-state", "empty");

    // Before any selection the flows are the whole market's.
    const marketResponse = await marketFlowsResponse;
    expect(marketResponse.status()).toBe(200);
    const market = (await marketResponse.json()) as FlowsResponse;
    expect(market.subject).toBe("market");
    await expectReady(flows, SLOW_READY_TIMEOUT);
    await expectFlowsReadouts(flows, market);

    const screener = page.getByTestId("terminal-equity-screener");
    await expectReady(screener, SLOW_READY_TIMEOUT);
    // Registered before the click, so SCOM's own response is the one observed.
    const scomFlowsResponse = page.waitForResponse((response) =>
      isFlowsUrl(response.url(), "SCOM"),
    );
    // Scoped to the screener row: the anomaly feed lists SCOM as well.
    await withAttributes(screener, "screener-row", { "data-ticker": "SCOM" })
      .getByRole("button", { name: "SCOM", exact: true })
      .click();

    await expect(page.getByTestId("screener-selected-ticker")).toContainText("SCOM");
    await expectReady(bands, SLOW_READY_TIMEOUT);
    await expect(bands.getByTestId("bands-meta")).toContainText("SCOM");
    await expect(bands.getByTestId("bands-chart")).toBeVisible();

    const scomResponse = await scomFlowsResponse;
    expect(scomResponse.status()).toBe(200);
    const scom = (await scomResponse.json()) as FlowsResponse;
    expect(scom.subject).toBe("SCOM");
    expect(scom.totals).not.toBeNull();
    // The readouts can tell SCOM's flows from the market's only because their totals differ.
    expect(scom.totals?.foreign_net_kes).not.toBe(market.totals?.foreign_net_kes);
    expect(scom.totals?.local_net_kes).not.toBe(market.totals?.local_net_kes);

    // The market-wide flows stay on screen, marked "Updating…", until SCOM's arrive.
    await expectReady(flows, SLOW_READY_TIMEOUT);
    await expect(flows.getByText("Updating…")).toHaveCount(0, { timeout: SLOW_READY_TIMEOUT });
    await expectFlowsReadouts(flows, scom);
    await expect(flows.getByTestId("flows-coverage")).toContainText("100.00");
  });

  test("a near-constant 10^14 P/E still draws a few distinct, finite axis ticks", async ({
    page,
  }) => {
    const bandsRequests: URL[] = [];
    await page.route("**/api/v1/terminal/market/valuation-bands**", async (route) => {
      bandsRequests.push(new URL(route.request().url()));
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

    // The handler above answered SCOM's P/E request, and its extreme value reached the widget.
    expect(bandsRequests.length).toBeGreaterThanOrEqual(1);
    for (const url of bandsRequests) {
      expect(url.searchParams.get("ticker")).toBe("SCOM");
      expect(url.searchParams.get("metric")).toBe("pe");
    }
    await expect(bands.getByTestId("bands-stat-current").locator("dd")).toHaveText(
      "100,000,000,000,000.01\u00d7",
    );

    // Recharts 3 draws the tick labels in a z-index layer of their own, outside `.recharts-yAxis`.
    // Each is a grouped 2 dp decimal such as "100,000,000,000,000.00", read back without its
    // grouping to prove it is a finite number.
    const tickLabels = chart.locator(
      ".recharts-yAxis-tick-labels .recharts-cartesian-axis-tick-value",
    );
    await expect.poll(() => tickLabels.count()).toBeGreaterThanOrEqual(2);
    const labels = await tickLabels.allTextContents();
    // The axis spans the fixture's 10^14 values: every tick's integer part, read exactly as a
    // BigInt, is at least the band floor's 99,999,999,999,999.
    const axisFloor = BigInt("99999999999999");
    for (const label of labels) {
      const integerPart = /^(\d{1,3}(?:,\d{3})*)\.\d+$/.exec(label)?.[1];
      expect(integerPart, `tick label ${JSON.stringify(label)}`).toBeDefined();
      expect(BigInt((integerPart ?? "").replace(/,/g, "")) >= axisFloor).toBe(true);
    }
    expect(labels.length).toBeLessThanOrEqual(12);
    expect(new Set(labels).size).toBe(labels.length);
    for (const label of labels) {
      expect(label).toMatch(/^\u2212?\d{1,3}(?:,\d{3})*\.\d{2,}$/);
      expect(Number.isFinite(Number(label.replace(MINUS_SIGN, "-").replace(/,/g, "")))).toBe(true);
    }
  });

  // Watchlist mutations below are answered by the in-memory store, never by the API, so the
  // seeded database is never written.

  test("a created watchlist gets its card, chips and screener option, then is edited and deleted", async ({
    page,
  }) => {
    const store = await installWatchlistStore(page);
    await page.goto("/screener");

    const manager = page.getByTestId("terminal-watchlist-manager");
    const screener = page.getByTestId("terminal-equity-screener");
    await expectReady(manager);
    await expectReady(screener, SLOW_READY_TIMEOUT);
    const watchlistSelect = screener.getByTestId("screener-filter-watchlist");

    const name = "Banking pair";
    const card = await createWatchlist(manager, name, "KCB, EQTY");
    const list = store.byName(name);
    const listPath = `${WATCHLISTS_PATH}/${list.id}`;
    const create = {
      method: "POST",
      path: WATCHLISTS_PATH,
      body: { name, tickers: ["KCB", "EQTY"] },
    };
    expect(store.mutations()).toEqual([create]);
    await expect(card).toHaveAttribute("data-watchlist-id", list.id);
    await expect(chipsOf(card)).toHaveText(["EQTY", "KCB"]);
    // The screener offers the new list without a reload.
    await expect(watchlistSelect.locator(`option[value="${list.id}"]`)).toHaveText(name);

    await card.getByTestId("watchlist-add-input").fill("scom");
    await card.getByTestId("watchlist-add-button").click();
    await expect(chipsOf(card)).toHaveText(["EQTY", "KCB", "SCOM"]);
    const add = { method: "PUT", path: `${listPath}/items/SCOM`, body: null };
    expect(store.mutations()).toEqual([create, add]);

    await chipsOf(card)
      .and(page.locator('[data-ticker="KCB"]'))
      .getByTestId("watchlist-ticker-remove")
      .click();
    await expect(chipsOf(card)).toHaveText(["EQTY", "SCOM"]);
    const remove = { method: "DELETE", path: `${listPath}/items/KCB`, body: null };
    expect(store.mutations()).toEqual([create, add, remove]);

    // Delete asks for a confirmation first; Cancel keeps the list and sends nothing.
    const deleteButton = card.getByTestId("watchlist-delete-button");
    await deleteButton.click();
    await expect(deleteButton).toHaveText("Confirm");
    await card.getByTestId("watchlist-delete-cancel").click();
    await expect(deleteButton).toHaveText("Delete");
    await expect(card.getByTestId("watchlist-delete-cancel")).toHaveCount(0);
    await expect(chipsOf(card)).toHaveText(["EQTY", "SCOM"]);
    expect(store.mutations()).toEqual([create, add, remove]);

    await deleteButton.click();
    await expect(deleteButton).toHaveText("Confirm");
    await deleteButton.click();
    await expect(card).toHaveCount(0);
    await expect(watchlistSelect.locator(`option[value="${list.id}"]`)).toHaveCount(0);
    expect(store.mutations()).toEqual([
      create,
      add,
      remove,
      { method: "DELETE", path: listPath, body: null },
    ]);
    expect(store.find(list.id)).toBeUndefined();
  });

  test("watchlist errors appear beside their inputs, and invalid tickers send no request", async ({
    page,
  }) => {
    const store = await installWatchlistStore(page);
    await page.goto("/screener");

    const manager = page.getByTestId("terminal-watchlist-manager");
    const screener = page.getByTestId("terminal-equity-screener");
    await expectReady(manager);
    const core = store.byName("NSE Core 8");
    // Both widgets have loaded their lists, so every later request comes from the steps below.
    await expect(
      screener.getByTestId("screener-filter-watchlist").locator(`option[value="${core.id}"]`),
    ).toHaveCount(1);
    const coreCard = watchlistCard(manager, core.name);
    const nameInput = manager.getByTestId("watchlist-name-input");
    const tickersInput = manager.getByTestId("watchlist-tickers-input");
    const createButton = manager.getByTestId("watchlist-create-button");

    // A taken name: 409 watchlist_name_taken, beneath the name.
    await nameInput.fill(core.name);
    await createButton.click();
    await expect(manager.getByTestId("field-error-name")).toHaveText(
      `A watchlist named '${core.name}' already exists.`,
    );
    expect(store.mutations()).toEqual([
      { method: "POST", path: WATCHLISTS_PATH, body: { name: core.name } },
    ]);

    // A ticker the API does not hold: 422 unknown_ticker, beneath the ticker list.
    await nameInput.fill("Unknown pick");
    await tickersInput.fill("SCOM, ZZZZ");
    await createButton.click();
    await expect(manager.getByTestId("field-error-tickers")).toHaveText("Unknown tickers: ZZZZ.");
    expect(store.mutations().at(-1)).toEqual({
      method: "POST",
      path: WATCHLISTS_PATH,
      body: { name: "Unknown pick", tickers: ["SCOM", "ZZZZ"] },
    });
    await expect(watchlistCard(manager, "Unknown pick")).toHaveCount(0);

    // Adding a ticker the API does not hold: 404 unknown_ticker, beneath that list's input.
    const addInput = coreCard.getByTestId("watchlist-add-input");
    const addButton = coreCard.getByTestId("watchlist-add-button");
    const addError = coreCard.getByTestId(`field-error-ticker-${core.id}`);
    await addInput.fill("ZZZZ");
    await addButton.click();
    await expect(addError).toHaveText("Ticker 'ZZZZ' is not a known equity.");
    expect(store.mutations().at(-1)).toEqual({
      method: "PUT",
      path: `${WATCHLISTS_PATH}/${core.id}/items/ZZZZ`,
      body: null,
    });
    await expect(chipsOf(coreCard)).toHaveCount(8);

    // Tickers that cannot travel as one path segment are refused before any request is sent.
    for (const ticker of ["..", "A/B"]) {
      const sent = store.requests.length;
      await addInput.fill(ticker);
      // Editing the input clears its previous error, so the message below is a new one.
      await expect(addError).toHaveCount(0);
      await addButton.click();
      await expect(addError).toHaveText("Enter a valid ticker.");
      expect(store.requests).toHaveLength(sent);
    }
    const sent = store.requests.length;
    await nameInput.fill("Dotted");
    await tickersInput.fill("SCOM, ..");
    await createButton.click();
    await expect(manager.getByTestId("field-error-tickers")).toHaveText(
      ".. is not a valid ticker.",
    );
    expect(store.requests).toHaveLength(sent);
    await expect(watchlistCard(manager, "Dotted")).toHaveCount(0);
  });

  test("watchlists keep code-point order in the manager and the screener selector", async ({
    page,
  }) => {
    const store = await installWatchlistStore(page);
    await page.goto("/screener");

    const manager = page.getByTestId("terminal-watchlist-manager");
    const screener = page.getByTestId("terminal-equity-screener");
    await expectReady(manager);
    await expectReady(screener, SLOW_READY_TIMEOUT);
    const created = ["alpha", "Beta", "' OR '1'='1"];
    for (const name of created) {
      await createWatchlist(manager, name, "");
    }

    // Code-point order puts the apostrophe first and upper case before lower case.
    const expected = ["' OR '1'='1", "Beta", "NSE Core 8", "alpha"];
    expect([...created, "NSE Core 8"].sort(compareCodePoints)).toEqual(expected);
    expect(store.lists().map((list) => list.name)).toEqual(expected);
    // A locale-aware sort orders the same names differently, so the checks below tell them apart.
    expect([...expected].sort((a, b) => a.localeCompare(b, "en"))).not.toEqual(expected);

    const managerNames = (): Promise<string[]> =>
      manager
        .getByTestId("watchlist-item")
        .evaluateAll((items) =>
          items.map((item) => item.getAttribute("data-watchlist-name") ?? ""),
        );
    const optionNames = (): Promise<string[]> =>
      screener
        .getByTestId("screener-filter-watchlist")
        .locator('option:not([value=""])')
        .allTextContents();
    await expect.poll(managerNames).toEqual(expected);
    await expect.poll(optionNames).toEqual(expected);

    // A reload shows the order of `GET /watchlists` itself in both widgets.
    await page.reload();
    await expectReady(manager);
    await expectReady(screener, SLOW_READY_TIMEOUT);
    await expect.poll(managerNames).toEqual(expected);
    await expect.poll(optionNames).toEqual(expected);
  });

  test("the screener follows a watchlist the manager creates, extends and deletes", async ({
    page,
  }) => {
    const store = await installWatchlistStore(page);
    await routeScreenerToStore(page, store);
    await page.goto("/screener");

    const manager = page.getByTestId("terminal-watchlist-manager");
    const screener = page.getByTestId("terminal-equity-screener");
    await expectReady(manager);
    await expectSettledRows(screener, 8);

    const name = "Telco and brewer";
    const card = await createWatchlist(manager, name, "SCOM, EABL");
    const list = store.byName(name);
    const select = screener.getByTestId("screener-filter-watchlist");
    await expect(select.locator(`option[value="${list.id}"]`)).toHaveText(name);

    const filtered = page.waitForResponse((response) =>
      isScreenerUrl(response.url(), "watchlist_id", list.id),
    );
    await select.selectOption(list.id);
    const filteredResponse = await filtered;
    expect(queryOf(filteredResponse.url())).toEqual({ watchlist_id: list.id });
    await expectSettledRows(screener, 2);
    expect((await rowTickers(screener)).sort()).toEqual(["EABL", "SCOM"]);
    await expect(screener.getByTestId("screener-context-watchlist")).toHaveText(
      `Watchlist: ${name}`,
    );

    // A ticker added in the manager refetches the filtered counters.
    const refetched = page.waitForResponse((response) =>
      isScreenerUrl(response.url(), "watchlist_id", list.id),
    );
    await card.getByTestId("watchlist-add-input").fill("KCB");
    await card.getByTestId("watchlist-add-button").click();
    await expect(chipsOf(card)).toHaveText(["EABL", "KCB", "SCOM"]);
    expect(((await (await refetched).json()) as ScreenerResponse).count).toBe(3);
    await expectSettledRows(screener, 3);
    expect((await rowTickers(screener)).sort()).toEqual(["EABL", "KCB", "SCOM"]);

    // Deleting the applied list in the manager clears the filter with a notice naming it.
    const unfiltered = page.waitForResponse(
      (response) => isScreenerUrl(response.url()) && new URL(response.url()).search === "",
    );
    const deleteButton = card.getByTestId("watchlist-delete-button");
    await deleteButton.click();
    await expect(deleteButton).toHaveText("Confirm");
    await deleteButton.click();
    await expect(card).toHaveCount(0);
    expect((await unfiltered).status()).toBe(200);
    await expect(select).toHaveValue("");
    await expect(select.locator(`option[value="${list.id}"]`)).toHaveCount(0);
    await expect(screener.getByTestId("screener-watchlist-notice")).toHaveText(
      `\u201c${name}\u201d was deleted. Showing all counters.`,
    );
    await expectSettledRows(screener, 8);
    await expect(screener.getByTestId("screener-context-watchlist")).toHaveCount(0);
  });

  /** A numeric screener filter, its typed bound and the row column it bounds. */
  const NUMERIC_FILTER_CASES = [
    { key: "min_pe", value: "6", column: "pe" },
    { key: "max_pe", value: "5", column: "pe" },
    { key: "min_pb", value: "1", column: "pb" },
    { key: "max_pb", value: "0.8", column: "pb" },
    { key: "min_ev_ebitda", value: "7", column: "ev_ebitda" },
    { key: "max_ev_ebitda", value: "7", column: "ev_ebitda" },
    { key: "min_dividend_yield", value: "8", column: "dividend_yield_pct" },
    { key: "min_volume_z", value: "-0.5", column: "volume_z" },
  ] as const satisfies ReadonlyArray<{
    key: string;
    value: string;
    column: "pe" | "pb" | "ev_ebitda" | "dividend_yield_pct" | "volume_z";
  }>;

  /**
   * Whether `value` passes the inclusive bound of filter `key`, compared exactly. A null value
   * fails any bound on its metric, as the API filters.
   */
  function withinBound(value: string | null, key: string, bound: string): boolean {
    if (value === null) {
      return false;
    }
    const comparison = compareDecimals(value, bound);
    return key.startsWith("min_") ? comparison >= 0 : comparison <= 0;
  }

  /** Loads the screener and returns its first, unfiltered response. */
  async function openScreener(
    page: Page,
  ): Promise<{ screener: Locator; initial: ScreenerResponse }> {
    const initialResponse = page.waitForResponse((response) => isScreenerUrl(response.url()));
    await page.goto("/screener");
    const response = await initialResponse;
    expect(queryOf(response.url())).toEqual({});
    const screener = page.getByTestId("terminal-equity-screener");
    await expectSettledRows(screener, 8);
    return { screener, initial: (await response.json()) as ScreenerResponse };
  }

  for (const filter of NUMERIC_FILTER_CASES) {
    test(`${filter.key}=${filter.value} is sent as typed and keeps only the counters within it`, async ({
      page,
    }) => {
      const { screener, initial } = await openScreener(page);
      const expected = initial.rows
        .filter((row) => withinBound(row[filter.column], filter.key, filter.value))
        .map((row) => row.ticker)
        .sort();
      // The bound keeps some counters but not all, so the filter visibly narrows the table.
      expect(expected.length).toBeGreaterThan(0);
      expect(expected.length).toBeLessThan(initial.rows.length);

      const filtered = page.waitForResponse((response) =>
        isScreenerUrl(response.url(), filter.key),
      );
      const input = screener.getByTestId(`screener-filter-${filter.key}`);
      await input.fill(filter.value);
      await input.press("Enter");
      const response = await filtered;
      expect(response.status()).toBe(200);
      expect(queryOf(response.url())).toEqual({ [filter.key]: filter.value });

      await expectSettledRows(screener, expected.length);
      expect((await rowTickers(screener)).sort()).toEqual(expected);
      for (const { ticker, value } of await columnValues(screener, filter.column)) {
        expect(withinBound(value, filter.key, filter.value), `${ticker} ${value}`).toBe(true);
      }
    });
  }

  test("anomalies only sends anomalies_only=true and shows only flagged counters", async ({
    page,
  }) => {
    // The latest seeded session, 2026-09-30, has no spike, so the filter alone would leave no row
    // to check. Its request is answered by the API for the session of the planted EABL spike.
    const eablSpike = PLANTED_SPIKES[2];
    const anomalyRequests: string[] = [];
    const answered: ScreenerResponse[] = [];
    await page.route("**/api/v1/terminal/market/screener**", async (route) => {
      const url = new URL(route.request().url());
      if (url.searchParams.get("anomalies_only") !== "true") {
        await route.continue();
        return;
      }
      anomalyRequests.push(url.toString());
      url.searchParams.set("as_of", eablSpike.tradeDate);
      const response = await route.fetch({ url: url.toString() });
      if (response.status() === 200) {
        answered.push((await response.json()) as ScreenerResponse);
      }
      await route.fulfill({ response });
    });
    const { screener } = await openScreener(page);

    await screener.getByTestId("screener-filter-anomalies_only").check();
    await expect.poll(() => answered.length).toBe(1);
    expect(anomalyRequests.map(queryOf)).toEqual([{ anomalies_only: "true" }]);
    const body = answered[0];
    if (body === undefined) {
      throw new Error("The anomalies-only request was not answered");
    }
    expect(body.rows.map((row) => row.ticker)).toContain(eablSpike.ticker);
    for (const row of body.rows) {
      expect(row.is_volume_anomaly || row.is_turnover_anomaly, row.ticker).toBe(true);
    }

    await expectSettledRows(screener, body.rows.length);
    expect(await rowTickers(screener)).toEqual(body.rows.map((row) => row.ticker));
    const rows = screener.getByTestId("screener-row");
    for (let index = 0; index < body.rows.length; index += 1) {
      await expect(rows.nth(index)).toContainText(/Vol spike|T\/O spike/);
    }
    await expect(screener.getByTestId("screener-filter-anomalies_only")).toBeChecked();
  });

  test("sector, P/E and dividend-yield filters combine into one request", async ({ page }) => {
    const { screener, initial } = await openScreener(page);
    const query = { sector: "Banking", max_pe: "5", min_dividend_yield: "7" };
    const inSector = (row: ScreenerRow): boolean => row.sector === query.sector;
    const lowPe = (row: ScreenerRow): boolean => withinBound(row.pe, "max_pe", query.max_pe);
    const highYield = (row: ScreenerRow): boolean =>
      withinBound(row.dividend_yield_pct, "min_dividend_yield", query.min_dividend_yield);
    const expected = initial.rows
      .filter((row) => inSector(row) && lowPe(row) && highYield(row))
      .map((row) => row.ticker)
      .sort();
    // Each filter alone keeps more counters than the three together.
    expect(expected.length).toBeGreaterThan(0);
    for (const single of [inSector, lowPe, highYield]) {
      expect(expected.length).toBeLessThan(initial.rows.filter(single).length);
    }

    const combined = page.waitForResponse(
      (response) =>
        isScreenerUrl(response.url(), "sector") &&
        isScreenerUrl(response.url(), "max_pe") &&
        isScreenerUrl(response.url(), "min_dividend_yield"),
    );
    await screener.getByTestId("screener-filter-sector").selectOption({ label: query.sector });
    for (const key of ["max_pe", "min_dividend_yield"] as const) {
      const input = screener.getByTestId(`screener-filter-${key}`);
      await input.fill(query[key]);
      await input.press("Enter");
    }
    expect(queryOf((await combined).url())).toEqual(query);

    await expectSettledRows(screener, expected.length);
    expect((await rowTickers(screener)).sort()).toEqual(expected);
    const sectors = await screener
      .getByTestId("screener-row")
      .locator('[data-column="sector"]')
      .allTextContents();
    expect(new Set(sectors)).toEqual(new Set([query.sector]));
    for (const { ticker, value } of await columnValues(screener, "pe")) {
      expect(withinBound(value, "max_pe", query.max_pe), `${ticker} ${value}`).toBe(true);
    }
    for (const { ticker, value } of await columnValues(screener, "dividend_yield_pct")) {
      expect(
        withinBound(value, "min_dividend_yield", query.min_dividend_yield),
        `${ticker} ${value}`,
      ).toBe(true);
    }
  });

  /** The screener's sortable columns, ticker first so each later press starts that column. */
  const SORT_COLUMNS = [
    "ticker",
    "market_cap_kes",
    "pe",
    "pb",
    "ev_ebitda",
    "dividend_yield_pct",
    "volume_z",
  ] as const satisfies ReadonlyArray<keyof ScreenerRow>;

  /**
   * Asserts that the shown rows are ordered by `column` in `order` as the API sorts: tickers in
   * code-point order; metrics compared exactly, nulls last in either order and ties by ticker.
   */
  async function expectRowsSorted(
    screener: Locator,
    column: (typeof SORT_COLUMNS)[number],
    order: "asc" | "desc",
  ): Promise<void> {
    const direction = order === "asc" ? 1 : -1;
    const cells =
      column === "ticker"
        ? (await rowTickers(screener)).map((ticker) => ({ ticker, value: null }))
        : await columnValues(screener, column);
    for (let index = 1; index < cells.length; index += 1) {
      const previous = cells[index - 1];
      const current = cells[index];
      if (previous === undefined || current === undefined) {
        continue;
      }
      const pair = `${previous.ticker} ${previous.value} before ${current.ticker} ${current.value}`;
      if (column === "ticker") {
        const step = direction * compareCodePoints(previous.ticker, current.ticker);
        expect(step, pair).toBeLessThan(0);
      } else if (previous.value === null) {
        // Nulls come last in either order, among themselves by ticker.
        expect(current.value, pair).toBeNull();
        expect(compareCodePoints(previous.ticker, current.ticker), pair).toBeLessThan(0);
      } else if (current.value !== null) {
        const step = direction * compareDecimals(previous.value, current.value);
        expect(step, pair).toBeLessThanOrEqual(0);
        if (step === 0) {
          expect(compareCodePoints(previous.ticker, current.ticker), pair).toBeLessThan(0);
        }
      }
    }
  }

  /** Asserts that `column`'s header is the one sorted header, in `order`. */
  async function expectActiveSort(
    screener: Locator,
    column: (typeof SORT_COLUMNS)[number],
    order: "asc" | "desc",
  ): Promise<void> {
    await expect(screener.locator("thead th[aria-sort]")).toHaveCount(1);
    // A `has` locator is queried inside each header, so it is built from the page.
    const button = screener.page().getByTestId(`screener-sort-${column}`);
    await expect(screener.locator("thead th", { has: button })).toHaveAttribute(
      "aria-sort",
      order === "asc" ? "ascending" : "descending",
    );
  }

  test("each sort header requests its order, flips when pressed again and marks aria-sort", async ({
    page,
  }) => {
    const { screener, initial } = await openScreener(page);
    // Before any press the API's default, market cap descending, is the sorted header.
    expect(await rowTickers(screener)).toEqual(initial.rows.map((row) => row.ticker));
    await expectActiveSort(screener, "market_cap_kes", "desc");
    await expectRowsSorted(screener, "market_cap_kes", "desc");

    for (const column of SORT_COLUMNS) {
      // A newly pressed column starts ascending for tickers and descending for metrics.
      const first = column === "ticker" ? "asc" : "desc";
      for (const order of [first, first === "asc" ? "desc" : "asc"] as const) {
        const sorted = page.waitForResponse(
          (response) =>
            isScreenerUrl(response.url(), "sort", column) &&
            isScreenerUrl(response.url(), "order", order),
        );
        await screener.getByTestId(`screener-sort-${column}`).click();
        const response = await sorted;
        expect(queryOf(response.url())).toEqual({ sort: column, order });
        const body = (await response.json()) as ScreenerResponse;
        await expectSettledRows(screener, 8);
        await expect.poll(() => rowTickers(screener)).toEqual(body.rows.map((row) => row.ticker));
        await expectActiveSort(screener, column, order);
        await expectRowsSorted(screener, column, order);
      }
    }
  });

  test("Reset sends the default request, clears every filter and restores the eight counters", async ({
    page,
  }) => {
    const { screener, initial } = await openScreener(page);
    const sectorSelect = screener.getByTestId("screener-filter-sector");
    const minPe = screener.getByTestId("screener-filter-min_pe");
    const anomaliesOnly = screener.getByTestId("screener-filter-anomalies_only");

    await sectorSelect.selectOption({ label: "Banking" });
    await expectSettledRows(screener, 5);
    await minPe.fill("4.5");
    await minPe.press("Enter");
    const sorted = page.waitForResponse((response) =>
      isScreenerUrl(response.url(), "sort", "pe"),
    );
    await screener.getByTestId("screener-sort-pe").click();
    expect(queryOf((await sorted).url())).toEqual({
      sector: "Banking",
      min_pe: "4.5",
      sort: "pe",
      order: "desc",
    });
    await expectSettledRows(screener, 3);
    const narrowed = page.waitForResponse((response) =>
      isScreenerUrl(response.url(), "anomalies_only", "true"),
    );
    await anomaliesOnly.check();
    expect(queryOf((await narrowed).url())).toEqual({
      sector: "Banking",
      min_pe: "4.5",
      anomalies_only: "true",
      sort: "pe",
      order: "desc",
    });
    await expect(screener).toHaveAttribute("data-state", "empty", {
      timeout: SLOW_READY_TIMEOUT,
    });

    const reset = page.waitForResponse(
      (response) => isScreenerUrl(response.url()) && new URL(response.url()).search === "",
    );
    await screener.getByTestId("screener-reset").click();
    expect((await reset).status()).toBe(200);
    await expectSettledRows(screener, 8);
    expect(await rowTickers(screener)).toEqual(initial.rows.map((row) => row.ticker));
    await expectActiveSort(screener, "market_cap_kes", "desc");
    await expect(sectorSelect).toHaveValue("");
    await expect(screener.getByTestId("screener-filter-watchlist")).toHaveValue("");
    await expect(anomaliesOnly).not.toBeChecked();
    for (const { key } of NUMERIC_FILTER_CASES) {
      await expect(screener.getByTestId(`screener-filter-${key}`)).toHaveValue("");
    }
  });

  test("selecting KCB in the anomaly feed loads its valuation bands and net flows", async ({
    page,
  }) => {
    const marketFlowsResponse = page.waitForResponse((response) =>
      isFlowsUrl(response.url(), null),
    );
    await page.goto("/screener");

    const feed = page.getByTestId("terminal-anomaly-feed");
    const bands = page.getByTestId("terminal-valuation-bands-chart");
    const flows = page.getByTestId("terminal-net-flow-chart");
    await expectReady(feed, SLOW_READY_TIMEOUT);
    await expect(bands).toHaveAttribute("data-state", "empty");
    const market = (await (await marketFlowsResponse).json()) as FlowsResponse;
    await expectReady(flows, SLOW_READY_TIMEOUT);
    await expectFlowsReadouts(flows, market);

    const kcbSpike = PLANTED_SPIKES[1];
    const bandsResponse = page.waitForResponse((response) => {
      const url = new URL(response.url());
      return url.pathname === BANDS_PATH && url.searchParams.get("ticker") === kcbSpike.ticker;
    });
    const kcbFlowsResponse = page.waitForResponse((response) =>
      isFlowsUrl(response.url(), kcbSpike.ticker),
    );
    await withAttributes(feed, "anomaly-item", {
      "data-ticker": kcbSpike.ticker,
      "data-trade-date": kcbSpike.tradeDate,
      "data-metric": "volume",
    })
      .getByRole("button", { name: `Select ${kcbSpike.ticker}`, exact: true })
      .click();

    await expect(page.getByTestId("screener-selected-ticker")).toHaveText(
      `Selected counter: ${kcbSpike.ticker}`,
    );
    const bandsAnswer = await bandsResponse;
    expect(bandsAnswer.status()).toBe(200);
    const kcbBands = (await bandsAnswer.json()) as ValuationBandsResponse;
    expect(kcbBands.subject).toBe(kcbSpike.ticker);
    expect(kcbBands.stats).not.toBeNull();
    await expectReady(bands, SLOW_READY_TIMEOUT);
    await expect(bands.getByTestId("bands-meta")).toContainText(`Counter ${kcbSpike.ticker} · `);
    await expect
      .poll(async () =>
        displayedDecimal(await bands.getByTestId("bands-stat-current").locator("dd").textContent()),
      )
      .toBe(canonicalDecimal(kcbBands.stats?.current ?? ""));

    const flowsAnswer = await kcbFlowsResponse;
    expect(flowsAnswer.status()).toBe(200);
    const kcbFlows = (await flowsAnswer.json()) as FlowsResponse;
    expect(kcbFlows.subject).toBe(kcbSpike.ticker);
    expect(kcbFlows.totals?.foreign_net_kes).not.toBe(market.totals?.foreign_net_kes);
    await expectReady(flows, SLOW_READY_TIMEOUT);
    await expect(flows.getByText("Updating…")).toHaveCount(0, { timeout: SLOW_READY_TIMEOUT });
    await expectFlowsReadouts(flows, kcbFlows);

    // The selection is marked in both lists that offer it.
    await expect(
      withAttributes(page.getByTestId("terminal-equity-screener"), "screener-row", {
        "data-ticker": kcbSpike.ticker,
      }),
    ).toHaveAttribute("data-selected", "true");
    await expect(
      withAttributes(feed, "anomaly-item", {
        "data-ticker": kcbSpike.ticker,
        "data-trade-date": kcbSpike.tradeDate,
        "data-metric": "volume",
      }),
    ).toHaveAttribute("data-selected", "true");
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
