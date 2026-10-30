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
 * or the empty results and injected outages of the widget-state tests, and one cycle test blanks
 * overlay rates in a fetched backtest response before the page receives it. Those tests are
 * read-only against the database as well: a handler answers a request from memory or passes a
 * GET request through to the real API, and the widget-state handlers block every other method.
 */
import { expect, test } from "@playwright/test";
import type { Locator, Page } from "@playwright/test";

import { CHART_COLORS } from "../src/components/terminal/lib/chartTheme";
import type {
  AnomaliesResponse,
  BacktestResponse,
  CyclePoint,
  CycleResult,
  DecimalString,
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
