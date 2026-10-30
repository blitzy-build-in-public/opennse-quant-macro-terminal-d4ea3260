"use client";

import type { ReactElement } from "react";

import { EMPTY_VALUE, compareDecimal, formatDate, formatDecimal } from "./lib/format";
import type { DecimalKind } from "./lib/format";
import { useTerminalQuery } from "./lib/useTerminalQuery";
import { WidgetFrame } from "./shared/WidgetFrame";
import { KPI_LABEL_CLASS, KPI_TILE_CLASS, KPI_VALUE_CLASS } from "./shared/headingAndTileClasses";
import type {
  DecimalString,
  IndicatorSnapshot,
  IndicatorUnit,
  MacroOverviewResponse,
  TerminalWidgetProps,
} from "./types";

/** Props of {@link MacroSnapshot}. */
export interface MacroSnapshotProps extends TerminalWidgetProps {
  /**
   * Month-ends in the `GET /macro/overview` history window, from 1 to 240; the server validates
   * the range. Default 24. Keep it equal to the `historyMonths` of a `YieldCurveChart` or
   * `InflationTrajectoryChart` on the same page, so the widgets share a single request.
   */
  historyMonths?: number;
  /**
   * A `GET /macro/overview` body supplied by the host. While it is defined the widget sends no
   * request and renders it as given.
   */
  data?: MacroOverviewResponse;
}

const MACRO_OVERVIEW_PATH = "/macro/overview";
const DEFAULT_HISTORY_MONTHS = 24;

/** Title of a change that cannot be computed, because the column has no earlier observation. */
const NO_PREVIOUS_VALUE = "previous value unavailable";

/**
 * Title and screen-reader text of a value shown as "—", because the indicator column holds no
 * non-null observation.
 */
const NO_STORED_VALUE = "No value stored for this indicator";

// Every class below is a complete static string, so Tailwind's source scan and a host's `@source`
// directive or `content` glob find each one. `m-0` on the list elements cancels the user-agent
// `dl`/`dd` margins in a host that ships no CSS reset.
const GRID_CLASS = "m-0 grid grid-cols-2 gap-3 md:grid-cols-4";
// Tiles, labels and values build on the KPI classes shared with the other widgets.
// In two-column tiles on narrow screens, `text-balance` wraps "USD 7,850.00 m" as "USD" /
// "7,850.00 m" rather than leaving the unit alone on its own line.
const LABEL_CLASS = `${KPI_LABEL_CLASS} text-balance break-words`;
const VALUE_CLASS = `${KPI_VALUE_CLASS} text-slate-100 text-balance`;
// Neutral colour for every direction: a rise is good news for reserves but not for inflation, so
// the direction is carried by the arrow and the sign, never by a colour.
const CHANGE_CLASS = "m-0 flex flex-wrap items-baseline gap-1 text-xs tabular-nums text-slate-300";
const AS_OF_CLASS = "m-0 text-xs text-slate-400";
// The loading skeleton's heights are the loaded tile grid, measured on the standalone page with the
// seeded data at each viewport width where it reflows, so the card keeps its height when the data
// arrives.
const SKELETON_HEIGHT_CLASS =
  "h-[38.75rem] min-[325px]:h-[37.75rem] min-[326px]:h-[33.75rem] min-[327px]:h-[33.5rem] min-[392px]:h-[32.5rem] min-[401px]:h-[31.75rem] min-[402px]:h-[30.75rem] min-[768px]:h-[15rem]";

/** Direction of an indicator's change against its previous value. */
type ChangeDirection = "up" | "down" | "flat";

const DIRECTION_MARK: Record<ChangeDirection, string> = {
  up: "\u25b2",
  down: "\u25bc",
  // As wide as the arrows (the same Geometric Shapes block), so a zero change stays visibly marked.
  flat: "\u25ac",
};

/** The tiles are empty when no indicator column holds a value. */
function isSnapshotEmpty(data: MacroOverviewResponse): boolean {
  return data.latest.length === 0 || data.latest.every((item) => item.value === null);
}

/** Formatting kind of an indicator unit. */
function kindFor(unit: IndicatorUnit): DecimalKind {
  switch (unit) {
    case "pct":
      return "pct";
    case "kes_per_usd":
      return "kes_per_usd";
    case "usd_m":
      return "usd_m";
    default: {
      const unreachable: never = unit;
      return unreachable;
    }
  }
}

/**
 * Direction of a change, read exactly from its decimal string: a leading `-` is down, all-zero
 * digits are flat. `null` when the text is not a decimal, so no arrow claims a direction.
 */
function changeDirection(change: DecimalString): ChangeDirection | null {
  const sign = compareDecimal(change, "0");
  if (sign === null) return null;
  if (sign > 0) return "up";
  if (sign < 0) return "down";
  return "flat";
}

/**
 * A change as displayed: rates move in percentage points (`+0.20 pp`, never `+0.20%`, which would
 * read as a relative change); FX and reserves keep their own unit (`+0.50 KES/USD`).
 */
function formatChange(change: DecimalString, unit: IndicatorUnit): string {
  if (unit === "pct") {
    return formatDecimal(change, "pct", { unit: false, signed: true }) + " pp";
  }
  return formatDecimal(change, kindFor(unit), { signed: true });
}

function ValueText({ item }: { item: IndicatorSnapshot }): ReactElement {
  if (item.value === null) {
    return (
      <dd className={VALUE_CLASS} title={NO_STORED_VALUE}>
        <span aria-hidden="true">{EMPTY_VALUE}</span>
        <span className="sr-only">{NO_STORED_VALUE}</span>
      </dd>
    );
  }
  return <dd className={VALUE_CLASS}>{formatDecimal(item.value, kindFor(item.unit))}</dd>;
}

function ChangeText({ item }: { item: IndicatorSnapshot }): ReactElement {
  if (item.change === null) {
    return (
      <dd className={CHANGE_CLASS} title={NO_PREVIOUS_VALUE}>
        <span aria-hidden="true">{EMPTY_VALUE}</span>
        <span className="sr-only">{"Change: " + NO_PREVIOUS_VALUE}</span>
      </dd>
    );
  }
  const direction = changeDirection(item.change);
  return (
    <dd
      className={CHANGE_CLASS}
      title={"Previous value " + formatDecimal(item.previous_value, kindFor(item.unit))}
    >
      {direction === null ? null : <span aria-hidden="true">{DIRECTION_MARK[direction]}</span>}
      <span className="sr-only">{"Change from previous value: "}</span>
      <span>{formatChange(item.change, item.unit)}</span>
    </dd>
  );
}

function AsOfText({ asOf }: { asOf: string | null }): ReactElement {
  if (asOf === null) {
    return <dd className={AS_OF_CLASS}>No observation yet</dd>;
  }
  return (
    <dd className={AS_OF_CLASS}>
      As of <time dateTime={asOf}>{formatDate(asOf)}</time>
    </dd>
  );
}

/** One KPI tile: label, latest value, change against the previous value, and its date. */
function IndicatorTile({ item }: { item: IndicatorSnapshot }): ReactElement {
  return (
    <div data-testid={"macro-tile-" + item.key} className={KPI_TILE_CLASS}>
      <dt className={LABEL_CLASS}>{item.label}</dt>
      <ValueText item={item} />
      <ChangeText item={item} />
      <AsOfText asOf={item.as_of} />
    </div>
  );
}

/**
 * KPI tiles of the latest CBK indicators from `GET /macro/overview`: 12-month inflation, the
 * Central Bank Rate, the interbank rate, the 91/182/364-day T-bills, KES per USD and FX reserves,
 * each with its change against the previous observation and the date it was observed.
 *
 * Tiles follow the server's order. Values are formatted from the API's decimal strings, never
 * converted to numbers. The request is `/macro/overview?history_months=<historyMonths>`, the
 * same URL a `YieldCurveChart` or `InflationTrajectoryChart` with the same `historyMonths` sends,
 * so the API client merges them into one request. With `data` supplied nothing is fetched.
 *
 * The root carries `data-testid="terminal-macro-snapshot"` and `data-state`; each tile carries
 * `data-testid="macro-tile-<indicator key>"`.
 */
export function MacroSnapshot({
  apiBaseUrl,
  getHeaders,
  className,
  historyMonths = DEFAULT_HISTORY_MONTHS,
  data,
}: MacroSnapshotProps): ReactElement {
  const { state, data: overview, error, refetch } = useTerminalQuery<MacroOverviewResponse>({
    path: MACRO_OVERVIEW_PATH,
    // Exactly this key and nothing else, so the URL is byte-identical to the other macro widgets'.
    query: { history_months: historyMonths },
    apiBaseUrl,
    getHeaders,
    data,
    isEmpty: isSnapshotEmpty,
  });
  const latest = overview?.latest ?? [];

  return (
    <WidgetFrame
      testId="terminal-macro-snapshot"
      title="Macro snapshot"
      state={state}
      error={error}
      onRetry={refetch}
      className={className}
      skeletonClassName={SKELETON_HEIGHT_CLASS}
    >
      <dl className={GRID_CLASS}>
        {latest.map((item) => (
          <IndicatorTile key={item.key} item={item} />
        ))}
      </dl>
    </WidgetFrame>
  );
}
