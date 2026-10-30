/**
 * JSON contract of the terminal API (`/api/v1/terminal`) as the widgets consume it.
 *
 * Each interface mirrors one Pydantic v2 model in `backend/opennse_terminal/schemas/*.py`, and
 * every field name is the JSON key byte for byte (snake_case, no renaming layer). Conventions:
 * - Monetary amounts, prices, ratios, percentages, Z-scores and index levels are
 *   {@link DecimalString}, never `number`, so the server's exact `Decimal` value survives.
 * - Share counts, volumes, offsets, counts and years are JSON integers bounded to 10^12, below
 *   2^53, so `number` holds them exactly.
 * - Dates are ISO `YYYY-MM-DD` strings; timestamps are ISO 8601 datetime strings.
 * - A nullable field is a key that is always present with a `null` value; only request fields
 *   the server defaults are optional keys.
 * - `flags` lists open-ended reason codes explaining null or qualified values.
 *
 * This module is type-only and imports nothing, so it stays inside the widget folder's import
 * boundary and a host can copy it with the folder.
 */

// ---------------------------------------------------------------------------------------------
// Common (schemas/common.py)
// ---------------------------------------------------------------------------------------------

/**
 * Exact decimal serialized by Pydantic v2 as a JSON string, already rounded at the API boundary.
 * Display with `formatDecimal`; convert with `toChartNumber` only for chart coordinates.
 */
export type DecimalString = string;

/** Render state of a widget, written to its root `data-state` attribute. */
export type WidgetState = "loading" | "ready" | "empty" | "error";

/** Props shared by every fetching widget. */
export interface TerminalWidgetProps {
  /** Base URL of the terminal API, relative or absolute. Default `"/api/v1/terminal"`. */
  apiBaseUrl?: string;
  /**
   * Returns the host's explicit auth headers, such as a bearer token, sent with every request.
   * Called before each request so tokens can refresh. The widgets also leave fetch's default
   * `same-origin` credentials, so a host session cookie reaches an API on the page's own origin
   * without `getHeaders`.
   */
  getHeaders?: () => HeadersInit | Promise<HeadersInit>;
  /** Extra classes merged onto the widget's root element. */
  className?: string;
}

/** The `detail` object of a coded error response, at any HTTP status. */
export interface TerminalErrorDetail {
  /** Stable snake_case error code, for example `primary_exceeds_offer_size`. */
  code: string;
  /** Human-readable explanation of the error. */
  message: string;
  /** The offending request field, for example `primary_proceeds_kes`; null when none. */
  field: string | null;
}

/** One error attached to a request field, normalized from either 422 shape by the API client. */
export interface TerminalFieldError {
  /** Request field the error belongs to, as its snake_case API name. */
  field: string;
  /** Error code: the coded `detail.code`, or the FastAPI validation `type`. */
  code: string;
  /** Human-readable message for display beneath the field's input. */
  message: string;
}

/** One entry of FastAPI's standard 422 `detail` list for a single-field validation failure. */
export interface FastApiValidationItem {
  /** Pydantic error type, for example `less_than_equal`. */
  type: string;
  /** Location of the failing value, for example `["body", "wacc_pct"]` or `["query", "min_pe"]`. */
  loc: (string | number)[];
  /** Human-readable message. */
  msg: string;
  /**
   * The rejected value; for a field missing from a supplied object, the containing object as
   * sent, for example `{"tickers": []}` for a watchlist body without `name`. Null when a whole
   * parameter or the whole body is absent.
   */
  input?: unknown;
}

/** How much of a requested history window the stored data covers. */
export interface CoverageMeta {
  /** First date of the requested window. */
  requested_start: string;
  /** First date in the window that has a value; null when there is none. */
  coverage_start: string | null;
  /** True when the available data starts materially after `requested_start`. */
  is_partial_coverage: boolean;
}

/** One dated value of a time series, in the unit of the series that holds it. */
export interface SeriesPoint {
  /** Observation or trade date. */
  date: string;
  /** The value on that date; null when undefined. */
  value: DecimalString | null;
}

/** Body of `GET /health`. A failed database check is the coded 503 `database_unavailable`. */
export interface HealthResponse {
  /** Overall status of the terminal API; `ok` when every check passed. */
  status: "ok" | "degraded";
  /** Result of the database round-trip check. */
  database: "ok" | "unavailable";
  /** Installed `opennse_terminal` package version. */
  version: string;
}

// ---------------------------------------------------------------------------------------------
// Macro (schemas/macro.py): GET /macro/overview
// ---------------------------------------------------------------------------------------------

/** The eight indicator columns of `terminal_macro_indicators`, one snapshot each. */
export type IndicatorKey =
  | "inflation_yoy_pct"
  | "cbr_rate_pct"
  | "interbank_rate_pct"
  | "tbill_91d_pct"
  | "tbill_182d_pct"
  | "tbill_364d_pct"
  | "fx_kes_usd"
  | "fx_reserves_usd_m";

/** Unit of an indicator: percentage points, KES per USD, or millions of USD. */
export type IndicatorUnit = "pct" | "kes_per_usd" | "usd_m";

/** Yield-curve tenor: the three T-bills, then the FXD bond buckets of 2 to 25 years. */
export type TenorLabel = "91D" | "182D" | "364D" | "2Y" | "5Y" | "10Y" | "15Y" | "20Y" | "25Y";

/** Instrument behind a yield-curve point: a CBK Treasury bill or an FXD Treasury bond. */
export type Instrument = "tbill" | "bond";

/** One stored row of the policy, interbank and T-bill rate history, in percentage points. */
export interface RatesPoint {
  /**
   * Date the observation is stored under, unchanged: a month-end for CBK and seeded rates, the
   * row's own date (possibly mid-month) for a CSV or Excel import.
   */
  observation_date: string;
  /** Central Bank Rate (%). */
  cbr_rate_pct: DecimalString | null;
  /** Interbank rate (%). */
  interbank_rate_pct: DecimalString | null;
  /** 91-day Treasury bill yield (%). */
  tbill_91d_pct: DecimalString | null;
  /** 182-day Treasury bill yield (%). */
  tbill_182d_pct: DecimalString | null;
  /** 364-day Treasury bill yield (%). */
  tbill_364d_pct: DecimalString | null;
}

/** Latest value of one indicator column with its previous value, for a KPI tile. */
export interface IndicatorSnapshot {
  /** Indicator column name. */
  key: IndicatorKey;
  /** Human-readable label, for example "12-month inflation". */
  label: string;
  /** Latest non-null value in `unit`; null when none is stored. */
  value: DecimalString | null;
  /** Unit of `value`, `previous_value` and `change`. */
  unit: IndicatorUnit;
  /** Observation date of `value`; null when `value` is null. */
  as_of: string | null;
  /** The previous non-null value of the same column. */
  previous_value: DecimalString | null;
  /** `value` minus `previous_value`, in the same unit; null when either is null. */
  change: DecimalString | null;
}

/** One tenor of the yield curve at a date, with its own observation date and provenance. */
export interface YieldCurvePoint {
  /** Tenor label. */
  tenor_label: TenorLabel;
  /** Tenor in years: 0.25, 0.50 or 1.00 for a T-bill, the bond's remaining tenor otherwise. */
  tenor_years: DecimalString;
  /** Yield in percentage points. */
  yield_pct: DecimalString;
  /** Treasury bill or FXD bond. */
  instrument: Instrument;
  /** CBK issue number such as `FXD1/2018/025`, or `FXD-SEED-…` when seeded; null for T-bills. */
  bond_code: string | null;
  /** Date of the observation used, which can precede the curve date. */
  observation_date: string;
  /** Provenance of the observation, for example `seed`, `cbk_rates_csv` or `cbk_bonds`. */
  source: string;
}

/** The yield curve as it stood at one month-end. */
export interface YieldCurveSnapshot {
  /** Month-end date the curve is resolved at. */
  as_of: string;
  /** Curve points in ascending tenor; tenors without an observation are omitted. */
  points: YieldCurvePoint[];
}

/** The current yield curve and its month-end history for comparison overlays. */
export interface YieldCurve {
  /** Curve at the latest observation date, in ascending tenor; empty without data. */
  current: YieldCurvePoint[];
  /** One snapshot per month-end in the history window, oldest first. */
  history: YieldCurveSnapshot[];
}

/** Body of `GET /macro/overview`; every list is empty, never an error, without data. */
export interface MacroOverviewResponse {
  /** Window coverage; partial when the first observation is over a month after the start. */
  coverage: CoverageMeta;
  /** One snapshot per indicator column. */
  latest: IndicatorSnapshot[];
  /** 12-month inflation (%) per observation in the window, oldest first. */
  inflation_series: SeriesPoint[];
  /** Policy, interbank and T-bill rates per observation in the window, oldest first. */
  rates_series: RatesPoint[];
  /** Current yield curve and its month-end history over the window. */
  yield_curve: YieldCurve;
}

// ---------------------------------------------------------------------------------------------
// Market (schemas/market.py): GET /market/screener, /sector-heatmap, /valuation-bands,
// /anomalies, /flows
// ---------------------------------------------------------------------------------------------

/** Screener columns a caller may sort by. */
export type ScreenerSort =
  | "ticker"
  | "market_cap_kes"
  | "pe"
  | "pb"
  | "ev_ebitda"
  | "dividend_yield_pct"
  | "volume_z";

/** Sort direction of the screener rows. */
export type SortOrder = "asc" | "desc";

/**
 * Query parameters of `GET /market/screener`; an omitted key takes the server default.
 * A row whose metric is null fails every filter on that metric. A `min_*` above its `max_*` is
 * the coded 422 `min_exceeds_max` with `field` set to the `min_*` key.
 */
export interface ScreenerFilters {
  /** Keep only equities in this sector, for example `Banking`; unknown is a 404. */
  sector?: string;
  /** Keep only the tickers of this watchlist (UUID); unknown is a 404. */
  watchlist_id?: string;
  /** Minimum P/E ratio (×), inclusive. */
  min_pe?: DecimalString;
  /** Maximum P/E ratio (×), inclusive. */
  max_pe?: DecimalString;
  /** Minimum P/B ratio (×), inclusive. */
  min_pb?: DecimalString;
  /** Maximum P/B ratio (×), inclusive. */
  max_pb?: DecimalString;
  /** Minimum EV/EBITDA multiple (×), inclusive. */
  min_ev_ebitda?: DecimalString;
  /** Maximum EV/EBITDA multiple (×), inclusive. */
  max_ev_ebitda?: DecimalString;
  /** Minimum dividend yield in percentage points (`"5.00"` means 5%), inclusive. */
  min_dividend_yield?: DecimalString;
  /**
   * Minimum rolling volume Z-score (standard deviations), inclusive, compared with the 2 dp
   * `volume_z` shown; the anomaly flags compare the unrounded score instead.
   */
  min_volume_z?: DecimalString;
  /**
   * Keep only rows with `is_volume_anomaly` or `is_turnover_anomaly`: an unrounded Z-score at or
   * above the threshold, positive side only. Default false.
   */
  anomalies_only?: boolean;
  /**
   * Spike threshold in standard deviations, 0.5 to 10: an unrounded Z-score at or above it is an
   * anomaly. Default `"2.0"`.
   */
  anomaly_threshold?: DecimalString;
  /** Rolling Z-score window N in the ticker's own prior rows, 5 to 250. Default 20. */
  z_window?: number;
  /** Valuation date; defaults to the latest stored trade date. */
  as_of?: string;
  /** Column to sort by. Default `market_cap_kes`. */
  sort?: ScreenerSort;
  /** Sort direction. Default `desc`. */
  order?: SortOrder;
}

/**
 * One equity in the screener, valued as of the response's `as_of`. Session-change and Z-score
 * reasons appear in `flags` prefixed with their metric, as `change_pct:<code>`,
 * `volume_z:<code>` and `turnover_z:<code>`.
 */
export interface ScreenerRow {
  /** NSE ticker symbol, for example `SCOM`. */
  ticker: string;
  /** Registered company name. */
  company_name: string;
  /** Sector name as stored, for example `Banking`. */
  sector: string;
  /**
   * ISO 4217 code of the equity's listing currency, KES by default; `close_price` and
   * `turnover_kes` are in it, never converted. The computed metrics are KES-only and flag
   * `currency_mismatch` otherwise: a non-KES equity has a null `market_cap_kes`, and a non-KES
   * equity or non-KES fundamentals give null `eps_kes`, `pe`, `pb`, `ev_ebitda`,
   * `dividend_yield_pct` and `fundamentals_period_end`.
   */
  currency: string;
  /** Date of the latest close on or before `as_of`. */
  trade_date: string;
  /** Closing price per share on `trade_date`, in `currency` (KES by default), never converted. */
  close_price: DecimalString;
  /** Change from the previous close, %; null without one (`change_pct:no_previous_close`). */
  change_pct: DecimalString | null;
  /** Close × shares outstanding, KES; null (`currency_mismatch`) for a non-KES equity. */
  market_cap_kes: DecimalString | null;
  /** Earnings per share, KES per share. */
  eps_kes: DecimalString | null;
  /** Price-to-earnings ratio (×). */
  pe: DecimalString | null;
  /** Price-to-book ratio (×). */
  pb: DecimalString | null;
  /** Enterprise value to EBITDA (×); always null for Banking-sector equities. */
  ev_ebitda: DecimalString | null;
  /** Dividend per share over close, %. */
  dividend_yield_pct: DecimalString | null;
  /** Shares traded on `trade_date`. */
  volume: number;
  /** Value traded on `trade_date`, in `currency` (KES by default) as stored, never converted. */
  turnover_kes: DecimalString;
  /** Rolling volume Z-score on `trade_date` (standard deviations). */
  volume_z: DecimalString | null;
  /** Rolling turnover Z-score on `trade_date` (standard deviations). */
  turnover_z: DecimalString | null;
  /**
   * True when the unrounded volume Z-score is at or above the threshold (positive side only).
   * Comparing the 2 dp `volume_z` instead can differ: raw 1.996 shows `"2.00"` yet is not an
   * anomaly at threshold 2.
   */
  is_volume_anomaly: boolean;
  /**
   * True when the unrounded turnover Z-score is at or above the threshold (positive side only);
   * comparing the 2 dp `turnover_z` instead can differ, as for `is_volume_anomaly`.
   */
  is_turnover_anomaly: boolean;
  /** Free float as a percentage of shares outstanding. */
  free_float_pct: DecimalString | null;
  /** Period end of the FY fundamentals used; null when none has passed its availability lag. */
  fundamentals_period_end: string | null;
  /** Reason codes for null or qualified values. */
  flags: string[];
}

/** Point-in-time valuation of one sector, each multiple over its own matched eligible set. */
export interface SectorAggregate {
  /** Sector name as stored. */
  sector: string;
  /** Equities in the sector. */
  constituent_count: number;
  /** Market capitalization summed over the KES-listed constituents, KES; null when none. */
  market_cap_kes: DecimalString | null;
  /** Sector P/E (×): Σ market cap ÷ Σ net income over the NI-eligible set. */
  pe: DecimalString | null;
  /** Sector P/B (×): Σ market cap ÷ Σ equity over the equity-eligible set. */
  pb: DecimalString | null;
  /** Sector EV/EBITDA (×) over non-Banking members with EV and EBITDA. */
  ev_ebitda: DecimalString | null;
  /** Capitalization-weighted dividend yield of the dps-eligible set, %. */
  dividend_yield_pct: DecimalString | null;
  /** Constituents used for `pe`. */
  pe_eligible_count: number;
  /** Constituents used for `pb`. */
  pb_eligible_count: number;
  /** Constituents used for `ev_ebitda`. */
  ev_ebitda_eligible_count: number;
  /** Constituents used for `dividend_yield_pct`. */
  dy_eligible_count: number;
  /** Reason codes for null values, for example `ev_not_meaningful_for_banks`. */
  flags: string[];
}

/** Body of `GET /market/screener`. */
export interface ScreenerResponse {
  /** Valuation date used. */
  as_of: string;
  /** Rolling Z-score window N used. */
  z_window: number;
  /** Spike threshold used, in standard deviations. */
  anomaly_threshold: DecimalString;
  /** Number of rows returned; equals `rows.length`. */
  count: number;
  /** Equities passing every filter, sorted. */
  rows: ScreenerRow[];
  /** One entry per sector in the universe, independent of the filters. */
  sector_aggregates: SectorAggregate[];
}

/**
 * Period boundaries of the sector heatmap: the last market session on or before each month-end
 * or each Friday, with `as_of` replacing a month-end or Friday after it. The latest period is
 * thus clamped to `as_of` and can be month-to-date or week-to-date.
 */
export type HeatmapInterval = "month" | "week";

/** Capitalization-weighted return of one sector, or of the market, over one period. */
export interface HeatmapCell {
  /** Market session closing the period. */
  period_end: string;
  /** Period return, %. */
  return_pct: DecimalString | null;
  /** Sector return minus market return, percentage points; always null on the market row. */
  relative_return_pct: DecimalString | null;
  /** Constituents with a close within 10 days of both period boundaries. */
  constituents: number;
}

/** One sector's row of the heatmap, one cell per period. */
export interface SectorHeatmapRow {
  /** Sector name as stored. */
  sector: string;
  /** One cell per entry of `period_ends`, in order. */
  cells: HeatmapCell[];
}

/** Body of `GET /market/sector-heatmap`. */
export interface SectorHeatmapResponse {
  /**
   * Cutoff of the latest period: the requested `as_of`, else the latest stored trade date, else
   * the current date in the configured timezone (`TERMINAL_TIMEZONE`, Africa/Nairobi by default)
   * when no price exists (with empty lists). It is not itself a boundary: `period_ends` holds
   * observed market sessions, so an `as_of` that is not one (a weekend, a holiday, a date past
   * the data) differs from the last `period_ends` entry.
   */
  as_of: string;
  /** Period length. */
  interval: HeatmapInterval;
  /** Closing session of each period, ascending. */
  period_ends: string[];
  /** Whole-market return per period. */
  market: HeatmapCell[];
  /** One row per sector. */
  sectors: SectorHeatmapRow[];
}

/** The multiple a valuation band is drawn for: P/E or P/B. */
export type BandMetric = "pe" | "pb";

/** Whether a valuation band describes one counter or one sector aggregate. */
export type SubjectType = "ticker" | "sector";

/** Outcome of a valuation-band request; `stats` is present only for `ok`. */
export type BandsStatus =
  | "ok"
  | "insufficient_history"
  | "no_positive_earnings"
  | "no_positive_equity";

/** Statistics of the daily multiple over the lookback; ratios in ×. */
export interface BandStats {
  /** Non-null daily values the statistics use. */
  observations: number;
  /** Mean of the daily values. */
  mean: DecimalString;
  /** Sample standard deviation (n − 1). */
  stdev: DecimalString;
  /** Mean minus two standard deviations. */
  minus_2sd: DecimalString;
  /** Mean minus one standard deviation. */
  minus_1sd: DecimalString;
  /** Mean plus one standard deviation. */
  plus_1sd: DecimalString;
  /** Mean plus two standard deviations. */
  plus_2sd: DecimalString;
  /** Lowest daily value. */
  min: DecimalString;
  /** 25th percentile (linear interpolation, inclusive method). */
  p25: DecimalString;
  /** Median. */
  median: DecimalString;
  /** 75th percentile (linear interpolation, inclusive method). */
  p75: DecimalString;
  /** Highest daily value. */
  max: DecimalString;
  /** Latest daily value. */
  current: DecimalString;
  /** Share of daily values at or below `current`, %. */
  current_percentile: DecimalString;
}

/** Body of `GET /market/valuation-bands` for one ticker or one sector. */
export interface ValuationBandsResponse {
  /** The ticker or sector name the bands describe. */
  subject: string;
  /** Whether `subject` is a ticker or a sector. */
  subject_type: SubjectType;
  /** The banded multiple. */
  metric: BandMetric;
  /** End of the lookback. */
  as_of: string;
  /** Requested lookback start against the first session that has a value. */
  coverage: CoverageMeta;
  /** `ok`, or why the statistics are unavailable. */
  status: BandsStatus;
  /** Reason codes for null or qualified values. */
  flags: string[];
  /**
   * Weekly as-of transport sample of the multiple (×), ascending: one point per ISO week with a
   * non-null daily value, dated on the week's last market session on or before `as_of` and
   * carrying the week's latest non-null daily value, which may come from an earlier session than
   * that date. Weeks without a value are omitted; `stats` uses the daily values.
   */
  series: SeriesPoint[];
  /** Band statistics; null for every status other than `ok`. */
  stats: BandStats | null;
}

/** Metrics the anomaly feed scans (query value); `both` scans volume and turnover. */
export type AnomalyMetric = "volume" | "turnover" | "both";

/** One positive volume or turnover spike: an unrounded Z-score at or above the threshold. */
export interface AnomalyItem {
  /** NSE ticker symbol. */
  ticker: string;
  /** Session of the spike. */
  trade_date: string;
  /** The metric that spiked. */
  metric: "volume" | "turnover";
  /**
   * The metric on `trade_date`: shares for volume, or for turnover the value traded in the
   * equity's listing currency (KES by default); a non-KES equity's turnover is scored and
   * returned as stored, never converted to KES.
   */
  value: DecimalString;
  /** Mean of the prior window, in `value`'s unit (shares, or the listing currency for turnover). */
  rolling_mean: DecimalString;
  /** Sample standard deviation of the prior window, in `value`'s unit. */
  rolling_stdev: DecimalString;
  /** Z-score of `value` against the window (standard deviations). */
  z_score: DecimalString;
  /** Prior rows of the ticker in the window; missing sessions are skipped, not zero-filled. */
  observations_used: number;
}

/** Body of `GET /market/anomalies`, items sorted by date then Z-score, both descending. */
export interface AnomaliesResponse {
  /** Rolling Z-score window N used. */
  window: number;
  /** Spike threshold used, in standard deviations. */
  threshold: DecimalString;
  /** Spikes whose unrounded Z-score is ≥ threshold. */
  items: AnomalyItem[];
}

/** Outcome of a net-flow request; `totals` is null for `no_flow_data`. */
export type FlowsStatus = "ok" | "no_flow_data";

/** Foreign and local net flow of the subject on one market session, KES. */
export interface FlowPoint {
  /** Market session. */
  trade_date: string;
  /** Net foreign buying, KES; null when no constituent has flow data. */
  foreign_net_kes: DecimalString | null;
  /** Estimated net local buying, KES: the negative of `foreign_net_kes`. */
  local_net_kes: DecimalString | null;
  /** Running sum of `foreign_net_kes` from the range start, KES; null days add nothing. */
  cumulative_foreign_net_kes: DecimalString;
  /** Sum of `foreign_net_kes` over the last N market sessions, KES. */
  rolling_foreign_net_kes: DecimalString | null;
  /** Turnover of the constituents with flow data, KES. */
  turnover_kes: DecimalString | null;
  /** `foreign_net_kes` over `turnover_kes`, %; null when turnover is 0. */
  foreign_net_pct_of_turnover: DecimalString | null;
}

/** Net flow totals over the whole range. */
export interface FlowTotals {
  /** Total net foreign buying, KES. */
  foreign_net_kes: DecimalString;
  /** Total estimated net local buying, KES. */
  local_net_kes: DecimalString;
  /** `foreign_net_kes` over the range's turnover with data, %; null when that turnover is 0. */
  foreign_net_pct_of_turnover: DecimalString | null;
  /** Market sessions in the range with flow data. */
  sessions_with_data: number;
  /** Market sessions in the range. */
  sessions_in_range: number;
}

/** Body of `GET /market/flows` for one ticker, one sector or the whole market. */
export interface FlowsResponse {
  /** The ticker, the sector name, or `market` when neither is given. */
  subject: string;
  /** Valuation date used. */
  as_of: string;
  /** First date of the range. */
  start: string;
  /** Last date of the range. */
  end: string;
  /** `ok`, or `no_flow_data` when no session in the range has flow data. */
  status: FlowsStatus;
  /** Reason codes for null or qualified values. */
  flags: string[];
  /** Sessions with flow data over market sessions in range, %. */
  coverage_pct: DecimalString;
  /** Range totals; null when `status` is `no_flow_data`. */
  totals: FlowTotals | null;
  /** One point per market session, ascending. */
  series: FlowPoint[];
}


// ---------------------------------------------------------------------------------------------
// Cycles (schemas/cycles.py): GET /cycles/backtest
// ---------------------------------------------------------------------------------------------

/** Data state of one cycle: fully observed, partly observed, not yet open, or no cohort. */
export type BacktestStatus = "complete" | "partial" | "awaiting_data" | "no_constituents";

/** The {@link CyclePoint} field a {@link CycleMarker} was selected on. */
export type MarkerMetric = "pe_ratio" | "price_index" | "turnover_index";

/** The resolved window, echoed in the form that produced it; the other form's fields are null. */
export interface BacktestWindow {
  /** Calendar days before the event; null when the window is given in months. */
  pre_days: number | null;
  /** Calendar days after the event; null when the window is given in months. */
  post_days: number | null;
  /** Calendar months before the event; null when the window is given in days. */
  pre_months: number | null;
  /** Calendar months after the event; null when the window is given in days. */
  post_months: number | null;
}

/** One market session of a cycle on the event-relative axis; index levels are base 100. */
export interface CyclePoint {
  /** `trade_date` minus the event date, in calendar days (negative before the event). */
  offset_days: number;
  /** Market session date. */
  trade_date: string;
  /** Cohort capitalization, base 100 at the anchor. */
  price_index: DecimalString | null;
  /** Trailing 20-session mean cohort turnover, base 100 at the anchor. */
  turnover_index: DecimalString | null;
  /** Cohort P/E (×) over the members with available earnings on the day. */
  pe_ratio: DecimalString | null;
  /** `pe_ratio`, base 100 at the anchor. */
  pe_index: DecimalString | null;
  /** Running sum of the cohort's foreign net flow from the anchor, KES. */
  cumulative_foreign_net_kes: DecimalString | null;
  /** 12-month inflation (%) as of the session; null when over 62 days stale. */
  inflation_yoy_pct: DecimalString | null;
  /** 91-day T-bill yield (%) as of the session; null when over 62 days stale. */
  tbill_91d_pct: DecimalString | null;
  /** 364-day T-bill yield (%) as of the session; null when over 62 days stale. */
  tbill_364d_pct: DecimalString | null;
}

/** A point singled out on the cycle chart, with the metric and value that selected it. */
export interface CycleMarker {
  /** Calendar days from the event to the marked session. */
  offset_days: number;
  /** The marked market session. */
  trade_date: string;
  /** The point field whose minimum selected this marker. */
  metric: MarkerMetric;
  /** That field's value at the marked session. */
  value: DecimalString;
}

/** The valuation floor and volume exhaustion points of one cycle. */
export interface CycleMarkers {
  /** Minimum `pe_ratio` point, else minimum `price_index`; earliest on a tie. */
  valuation_floor: CycleMarker | null;
  /** Minimum `turnover_index` point; earliest on a tie. */
  volume_exhaustion: CycleMarker | null;
}

/** Cycle returns in percentage points; "index at T" is the last point on or before the event. */
export interface CycleSummary {
  /** (price index at T ÷ 100 − 1) × 100. */
  pre_event_return_pct: DecimalString | null;
  /** (last price index ÷ price index at T − 1) × 100; null when no point lies after T. */
  post_event_return_pct: DecimalString | null;
  /** Minimum of (price index ÷ its running maximum − 1) × 100. */
  max_drawdown_pct: DecimalString | null;
  /** (P/E at the valuation floor ÷ anchor P/E − 1) × 100. */
  pe_compression_pct: DecimalString | null;
}

/** The mean normalized levels of the complete cycles at one calendar offset. */
export interface CycleProjectionPoint {
  /** Calendar days from the projected cycle's event. */
  offset_days: number;
  /** Mean price index of the basis cycles. */
  price_index: DecimalString | null;
  /** Mean turnover index of the basis cycles. */
  turnover_index: DecimalString | null;
  /** Mean P/E index of the basis cycles. */
  pe_index: DecimalString | null;
}

/** A labelled descriptive average of the complete cycles' paths: never a forecast. */
export interface CycleProjection {
  /** Always true, so a projection is never mistaken for observed data. */
  is_projection: true;
  /** Labels of the complete cycles averaged. */
  basis: string[];
  /** One point per calendar offset of the target window with a defined mean. */
  points: CycleProjectionPoint[];
}

/**
 * One cycle of a backtest. For `awaiting_data` and `no_constituents`, `points` is empty and
 * `markers` and `summary` are null.
 */
export interface CycleResult {
  /** Label from `terminal_cycles`, or `"Event YYYY-MM-DD"` for an ad hoc date. */
  label: string;
  /** The event day, T. */
  event_date: string;
  /** First calendar day of the window. */
  window_start: string;
  /** Last calendar day of the window. */
  window_end: string;
  /** Data state of the cycle. */
  status: BacktestStatus;
  /**
   * First market session in the window; null when the window holds no session. Each defined
   * normalized index (`price_index`, `turnover_index`, `pe_index`) is 100 at the anchor. An index
   * that cannot be normalized (zero anchor capitalization, insufficient or zero anchor turnover,
   * unavailable anchor P/E) stays null throughout, with the matching code in `flags`.
   */
  anchor_date: string | null;
  /** Calendar days until the window opens, for an `awaiting_data` cycle; null otherwise. */
  days_until_window_start: number | null;
  /** Sorted tickers of the cohort, fixed at the anchor. */
  constituents: string[];
  /**
   * Sorted candidates without an actual close within 10 sessions on or before the anchor, or
   * listed in a currency other than KES (flag `currency_mismatch`).
   */
  excluded_constituents: string[];
  /** Market sessions from the anchor to the last observed session in the window. */
  points: CyclePoint[];
  /** Valuation floor and volume exhaustion markers. */
  markers: CycleMarkers | null;
  /** Pre-event, post-event, drawdown and P/E compression figures. */
  summary: CycleSummary | null;
  /** Mean path of the complete cycles, for `partial` and `awaiting_data` cycles. */
  projection: CycleProjection | null;
  /** Reason codes, for example `zero_anchor_capitalization` or `insufficient_projection_basis`. */
  flags: string[];
}

/**
 * Body of `GET /cycles/backtest`. `cycles` holds one result per distinct event date, ascending by
 * `event_date`: a date requested more than once is returned once, and the stored
 * `terminal_cycles` defaults are likewise in date order.
 */
export interface BacktestResponse {
  /** The resolved window applied to every cycle. */
  window: BacktestWindow;
  /** Tickers considered for every cohort. */
  candidates: string[];
  /** Latest trade date among the candidates' prices; null when none has prices. */
  data_available_through: string | null;
  /**
   * One result per distinct event date, ascending by `event_date`; a date requested more than
   * once appears once.
   */
  cycles: CycleResult[];
}

// ---------------------------------------------------------------------------------------------
// IPO sandbox (schemas/ipo.py): GET /ipo/scenarios, POST /ipo/valuation
// ---------------------------------------------------------------------------------------------

/** One qualitative risk note of a preset. */
export interface RiskScenario {
  /** Short heading, for example "Valuation Floor". */
  title: string;
  /** The risk note, verbatim. */
  description: string;
}

/**
 * Body of `POST /ipo/valuation`: every sandbox assumption, all required. Rates are percentage
 * points (`"18.00"` means 18%) and `*_kes` amounts are whole shillings. Defaults below are the
 * Quickmart preset. Cross-field rules are coded 422s: `wacc_not_above_terminal_growth`,
 * `shares_offered_exceeds_outstanding` and `primary_exceeds_offer_size`.
 */
export interface IpoValuationRequest {
  /** Offer price, KES per share. Default 7.50, range 0.50–50.00. */
  offer_price_kes: DecimalString;
  /** Annual revenue growth, % per year. Default 10.00, range −20.00–40.00. */
  revenue_growth_pct: DecimalString;
  /** Base-year EBITDA margin, % of revenue. Default 4.50, range 0.50–15.00. */
  ebitda_margin_pct: DecimalString;
  /** Margin change reached by year N, percentage points. Default 0.50, range −3.00–3.00. */
  margin_expansion_pct: DecimalString;
  /** Weighted average cost of capital, %. Default 18.00, range 8.00–30.00. */
  wacc_pct: DecimalString;
  /** Perpetual growth after year N, %. Default 5.00, range 0.00–10.00. */
  terminal_growth_pct: DecimalString;
  /** Net debt before the offer, KES. Default 4,000,000,000, range −2×10^10–4×10^10. */
  net_debt_kes: DecimalString;
  /** Working capital per shilling of incremental revenue, %. Default −3.00, range −15.00–15.00. */
  nwc_pct_revenue: DecimalString;
  /** Peer EV/EBITDA multiple (×). Default 6.00, range 1.00–20.00. */
  peer_ev_ebitda_multiple: DecimalString;
  /** Weight of the DCF value in the blend, %. Default 50, range 0–100. */
  dcf_weight_pct: DecimalString;
  /** Capital expenditure, % of revenue. Default 2.00, range 0.00–10.00. */
  capex_pct_revenue: DecimalString;
  /** Depreciation and amortisation, % of revenue. Default 1.50, range 0.00–10.00. */
  da_pct_revenue: DecimalString;
  /** Corporate tax rate on positive EBIT, %. Default 30.00, range 0.00–50.00. */
  tax_rate_pct: DecimalString;
  /** Explicit forecast horizon N, whole years. Default 5, range 3–10. */
  forecast_years: number;
  /** Base-year revenue, KES. Default 45,000,000,000, range > 0 and ≤ 10^14. */
  base_revenue_kes: DecimalString;
  /** Shares in issue after the offer. Default 1,000,000,000, range 1–10^12. */
  shares_outstanding: number;
  /** Shares sold in the offer. Default 500,000,000, range 0 to `shares_outstanding`. */
  shares_offered: number;
  /** New cash raised by the issuer, KES. Default 0.00, range 0 to offer price × shares offered. */
  primary_proceeds_kes: DecimalString;
  /** Sensitivity rows: 1–9 WACC values (%), each 8.00–30.00; omitted or null uses 16–20. */
  sensitivity_wacc_pct?: DecimalString[] | null;
  /** Sensitivity columns: 1–9 base margins (%), each 0.50–15.00; omitted or null uses 3.0–6.0. */
  sensitivity_margin_pct?: DecimalString[] | null;
}

/** One read-only sandbox preset, an item of `GET /ipo/scenarios`. */
export interface IpoScenario {
  /** Identifier of the preset row (UUID). */
  id: string;
  /** Issuer name, for example "Quickmart Supermarket". */
  company_name: string;
  /** Issuer sector; null when not recorded. */
  sector: string | null;
  /**
   * ISO 4217 currency of every amount in the preset; always KES, because a preset stored in
   * another currency is left out of `GET /ipo/scenarios`, never converted.
   */
  currency: string;
  /** Qualitative risk notes, in stored order. */
  risk_scenarios: RiskScenario[];
  /** The preset's assumptions, ready to post to `POST /ipo/valuation` unchanged. */
  assumptions: IpoValuationRequest;
}

/** One explicit forecast year of the DCF; amounts in KES. */
export interface ProjectionYear {
  /** Forecast year index t, 1 to `forecast_years`. */
  year: number;
  /** Revenue, KES. */
  revenue_kes: DecimalString;
  /** EBITDA margin on the linear expansion path, %. */
  ebitda_margin_pct: DecimalString;
  /** EBITDA, KES. */
  ebitda_kes: DecimalString;
  /** Depreciation and amortisation, KES. */
  da_kes: DecimalString;
  /** EBIT = EBITDA − D&A, KES. */
  ebit_kes: DecimalString;
  /** Tax on positive EBIT, KES. */
  tax_kes: DecimalString;
  /** NOPAT = EBIT − tax, KES. */
  nopat_kes: DecimalString;
  /** Capital expenditure, KES. */
  capex_kes: DecimalString;
  /** Working-capital investment, KES. */
  delta_nwc_kes: DecimalString;
  /** Free cash flow = NOPAT + D&A − capex − ΔNWC, KES. */
  fcf_kes: DecimalString;
  /** End-of-year discount factor 1 ÷ (1 + WACC)^t, 6 dp. */
  discount_factor: DecimalString;
  /** Present value of the year's free cash flow, KES. */
  pv_fcf_kes: DecimalString;
}

/** Discounted cash flow valuation at the request's WACC and base margin. */
export interface DcfResult {
  /** One entry per forecast year, 1 to N. */
  projections: ProjectionYear[];
  /** Sum of the explicit free cash flows' present values, KES. */
  pv_explicit_fcf_kes: DecimalString;
  /** Gordon terminal value FCF_N × (1 + g_T) ÷ (WACC − g_T), KES. */
  terminal_value_kes: DecimalString;
  /** Present value of the terminal value, KES. */
  pv_terminal_value_kes: DecimalString;
  /** Enterprise value, KES. */
  enterprise_value_kes: DecimalString;
  /** Equity value = EV − net debt + primary proceeds, KES. */
  equity_value_kes: DecimalString;
  /** DCF fair value, KES per share. */
  fair_value_per_share_kes: DecimalString;
  /** Reason codes, for example `negative_terminal_fcf`. */
  flags: string[];
}

/** Peer EV/EBITDA multiple valuation on base-year EBITDA. */
export interface ComparablesResult {
  /** Peer EV/EBITDA multiple applied (×). */
  ev_ebitda_multiple: DecimalString;
  /** Base-year EBITDA, KES. */
  base_ebitda_kes: DecimalString;
  /** Enterprise value, KES; null with `comps_non_positive_ebitda`. */
  enterprise_value_kes: DecimalString | null;
  /** Equity value, KES; null with `comps_non_positive_ebitda`. */
  equity_value_kes: DecimalString | null;
  /** Comparables fair value, KES per share; null with `comps_non_positive_ebitda`. */
  fair_value_per_share_kes: DecimalString | null;
  /** Reason codes. */
  flags: string[];
}

/** The blended valuation recomputed at one WACC and one base EBITDA margin. */
export interface SensitivityCell {
  /** WACC of this row, %. */
  wacc_pct: DecimalString;
  /** Base EBITDA margin of this column, %. */
  ebitda_margin_pct: DecimalString;
  /** Blended fair value, KES per share; null when the WACC does not exceed terminal growth. */
  fair_value_per_share_kes: DecimalString | null;
  /** Upside against the offer price, %; null when the fair value is null. */
  upside_pct: DecimalString | null;
  /** Reason codes, for example `wacc_not_above_terminal_growth`. */
  flags: string[];
}

/** WACC × base EBITDA margin grid of blended fair values; the `SensitivityMatrix` data prop. */
export interface SensitivityMatrixData {
  /** Row axis: WACC values, %. */
  rows: DecimalString[];
  /** Column axis: base EBITDA margins, %. */
  cols: DecimalString[];
  /** One list per row, each holding one cell per column. */
  cells: SensitivityCell[][];
}

/** Body of a successful `POST /ipo/valuation`. */
export interface IpoValuationResponse {
  /** Discounted cash flow valuation. */
  dcf: DcfResult;
  /** Peer multiple valuation. */
  comparables: ComparablesResult;
  /** Weighted blend of the DCF and comparables values, KES per share. */
  blended_fair_value_per_share_kes: DecimalString;
  /** Offer price of the request, KES per share. */
  offer_price_kes: DecimalString;
  /** (blended fair value − offer price) ÷ offer price × 100, %. */
  upside_pct: DecimalString;
  /** Offer price × shares outstanding, KES. */
  implied_market_cap_at_offer_kes: DecimalString;
  /** Implied EV ÷ base-year EBITDA at the offer (×); null when EBITDA is not positive. */
  implied_ev_ebitda_at_offer: DecimalString | null;
  /** Offer price × shares offered, KES. */
  offer_size_kes: DecimalString;
  /** New cash raised by the issuer, KES. */
  primary_proceeds_kes: DecimalString;
  /** Offer size − primary proceeds, paid to selling shareholders, KES. */
  secondary_proceeds_kes: DecimalString;
  /** Blended fair value across the WACC and base EBITDA margin axes. */
  sensitivity: SensitivityMatrixData;
  /** Union of the DCF, comparables and sensitivity-cell reason codes. */
  flags: string[];
}

// ---------------------------------------------------------------------------------------------
// PDF ingestion (schemas/ingest.py): POST /ingest/pdf
// ---------------------------------------------------------------------------------------------

/**
 * The six line items the PDF parser can extract, each committed to the `terminal_fundamentals`
 * column `<item>_kes` (`shares_outstanding` to `shares_outstanding`).
 */
export type LineItemName =
  | "revenue"
  | "ebitda"
  | "net_income"
  | "total_debt"
  | "operating_cash_flow"
  | "shares_outstanding";

/**
 * Outcome for one line item: `found` on a matched label, `derived` from component lines,
 * `ambiguous` when the column or candidate evidence is insufficient, `not_found` otherwise.
 */
export type ExtractedItemStatus = "found" | "derived" | "ambiguous" | "not_found";

/**
 * Unit of an extracted value: the filing's currency for monetary items, `shares` for
 * `shares_outstanding`.
 */
export type ItemUnit = "KES" | "USD" | "shares";

/**
 * Overall extraction outcome: `extracted` when no requested item is `not_found`, `partial` when
 * some are, `no_items` when all are.
 */
export type FilingStatus = "extracted" | "partial" | "no_items";

/**
 * One statement line a `derived` item was summed from: operating profit and depreciation and
 * amortisation for EBITDA, or one current and one non-current borrowings component for total
 * debt.
 */
export interface DerivationEntry {
  /** Statement label of the component line, as printed. */
  label: string;
  /** 1-based PDF page the component line was read from. */
  page_number: number;
  /**
   * Component value after scale normalization (printed token × page scale), in the filing's
   * currency.
   */
  value: DecimalString;
}

/**
 * One candidate of an `ambiguous` item: the token selected on one of its matched lines, or one of
 * the value tokens offered by a matched line whose column evidence selected no single readable
 * token. At most five are returned per item.
 */
export interface Candidate {
  /** Statement label of the matching line, as printed. */
  label: string;
  /** 1-based PDF page of the matching line. */
  page_number: number;
  /** The numeric token exactly as printed in the PDF text layer, for example `"(1,234)"`. */
  raw_value: string;
  /**
   * The token after sign handling and scale normalization; null when it is not a well-formed
   * number or its scaled amount is not a valid value for the item: a monetary amount whose
   * magnitude reaches 10^22 once rounded to 2 places (beyond the NUMERIC(24,2) fundamentals
   * columns), or a share count that is fractional, negative or above 10^12.
   */
  value: DecimalString | null;
}

/**
 * Extraction result for one requested line item. Only `found` and `derived` items are ever
 * committed to `terminal_fundamentals`; an `ambiguous` item is reported with its candidates but
 * never written.
 */
export interface ExtractedItem {
  /** Requested line item. */
  item: LineItemName;
  /** `found`, `derived`, `ambiguous` (insufficient evidence, never committed) or `not_found`. */
  status: ExtractedItemStatus;
  /**
   * Value after scale normalization: whole units of the filing's currency for monetary items, a
   * share count for `shares_outstanding`. For `ambiguous`, the first match on the primary
   * statement page (else the first match) when equal-priority matches conflict, but null when no
   * valid amount could be selected or derived: column evidence selected no single readable token,
   * the amount is not a valid value for the item (see {@link Candidate}), or an EBITDA derivation
   * has an ambiguous part or a sum beyond the NUMERIC(24,2) range. Null for `not_found`.
   */
  value: DecimalString | null;
  /**
   * `KES` or `USD` for monetary items (the filing's currency, never converted); `shares` for
   * `shares_outstanding`.
   */
  unit: ItemUnit;
  /**
   * The selected numeric token exactly as printed, before sign and scale handling; null when no
   * single token was selected, as for `not_found` items or a derived sum.
   */
  raw_value: string | null;
  /**
   * Multiplier applied to the printed figures: 1, 1000, 1000000 or 1000000000; null for
   * `not_found` items.
   */
  scale_applied: number | null;
  /** 1-based PDF page of the selected line; null for `not_found`. */
  page_number: number | null;
  /** Statement label that matched, as printed; null for `not_found`. */
  matched_label: string | null;
  /** Component lines summed into a `derived` value; empty for other statuses. */
  derivation: DerivationEntry[];
  /** Up to five candidates of an `ambiguous` item (see {@link Candidate}); empty otherwise. */
  candidates: Candidate[];
}

/**
 * Body of a successful (HTTP 200) multipart `POST /ingest/pdf`, with or without `commit`. A
 * `commit=false` preview always returns the full extraction, even for a USD statement; with
 * `commit=true` the `found` and `derived` items are merged into the `terminal_fundamentals` row
 * of the ticker and period. No error body carries an extraction. The endpoint's coded and
 * field-validation rejections are told apart by the JSON type of `detail`:
 * - an object, the coded {@link TerminalErrorDetail}: 413 `request_too_large` or
 *   `file_too_large`, 415 `not_a_pdf`, 422 `pdf_encrypted`, `pdf_unreadable`,
 *   `pdf_parse_timeout` (the parse reached its wall-clock limit), `too_many_pages`,
 *   `commit_requires_ticker_and_period` or `commit_requires_kes`, 409 `currency_conflict`,
 *   404 `unknown_ticker`, 429 `ingest_busy` (with `Retry-After: 5`) and 500 `internal_error`;
 * - a list of {@link FastApiValidationItem} entries with no stable code: FastAPI's standard 422
 *   for a missing `file` or an invalid form field, such as an unknown `document_type` or `items`
 *   value, an over-long `ticker` or a malformed `period_end_date`.
 *
 * A body FastAPI cannot parse as multipart gets FastAPI's own 400 with a string `detail`, and a
 * host's auth override answers in the host's own shape. Ingestion is API-only: this folder ships
 * no upload widget.
 */
export interface IngestPdfResponse {
  /** The `terminal_corporate_filings` row (UUID), upserted on `file_sha256` by every upload. */
  filing_id: string;
  /** SHA-256 of the uploaded bytes as 64 lowercase hex characters; the file is never stored. */
  file_sha256: string;
  /** Number of pages in the uploaded PDF. */
  page_count: number;
  /**
   * Statement scale detected from the page headers (1, 1000, 1000000 or 1000000000), the most
   * frequent across pages; 1 with the `scale_not_detected` warning when no page carries a
   * marker; null when the PDF has no text layer.
   */
  detected_scale: number | null;
  /**
   * ISO 4217 currency of the statements: KES unless a USD or US$ header was detected. Values are
   * never converted.
   */
  currency: string;
  /** `extracted` if no requested item is `not_found`, `partial` if some are, `no_items` if all. */
  status: FilingStatus;
  /** One result per requested line item. */
  items: ExtractedItem[];
  /** The requested items whose status is `not_found`. */
  missing_items: LineItemName[];
  /** True when the `found` and `derived` items were written to `terminal_fundamentals`. */
  committed: boolean;
  /** The `terminal_fundamentals` row (UUID) written by the commit; null when not committed. */
  fundamentals_id: string | null;
  /**
   * Extraction warning codes, open-ended like `flags`: `no_text_layer` (the PDF has no text layer
   * and no OCR is attempted), `scale_not_detected` and `non_kes_currency`.
   */
  warnings: string[];
}

// ---------------------------------------------------------------------------------------------
// Watchlists (schemas/watchlists.py): /watchlists endpoints
// ---------------------------------------------------------------------------------------------

/** One watchlist of the calling principal. Ownership is server-side, so no owner field exists. */
export interface Watchlist {
  /** Watchlist identifier (UUID). */
  id: string;
  /** Watchlist name, unique per owner. */
  name: string;
  /** NSE tickers on the watchlist, sorted ascending. */
  tickers: string[];
  /** Creation timestamp (ISO 8601 with UTC offset). */
  created_at: string;
  /**
   * Creation time, or the time of the latest tracked change (ISO 8601 with UTC offset): an
   * update of the watchlist row through the ORM, or a ticker actually added or removed by `PUT`
   * or `DELETE /watchlists/{watchlist_id}/items/{ticker}` (an idempotent no-op is not one). Item
   * rows changed directly in the database, such as by the cascade when an equity's ticker is
   * renamed or the equity is deleted, change `tickers` without advancing it.
   */
  updated_at: string;
}

/** Body of `POST /watchlists`; the owner is always the calling principal. */
export interface WatchlistCreate {
  /** Watchlist name, 1–100 characters after trimming; unique per owner. */
  name: string;
  /** Initial NSE tickers, at most 100; omitted means an empty watchlist. */
  tickers?: string[];
}
