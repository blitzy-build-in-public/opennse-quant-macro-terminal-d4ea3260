import type { Metadata } from "next";

import {
  InflationTrajectoryChart,
  MacroSnapshot,
  SectorRotationHeatmap,
  YieldCurveChart,
} from "@/components/terminal";

// `absolute` because the root layout's title template applies only to child segments, and this
// page shares the root segment; it spells out the same "<view> — <product>" form the template
// gives the other views.
export const metadata: Metadata = {
  title: { absolute: "Macro & Market Overview — OpenNSE Quant & Macro Terminal" },
};

// Server component: it holds no state and passes no function props, so it prerenders as static
// HTML and each widget fetches on the client. The widgets keep their default props on purpose:
// the three macro widgets then request the identical `/macro/overview?history_months=24` URL,
// which the terminal API client de-duplicates into one request. Grid spans live on wrapper divs,
// which carry no data-testid or data-state, so each widget root keeps its own test hooks. The
// page stack uses flex gap rather than space-y sibling margins, with the same 1rem spacing.
export default function OverviewPage() {
  return (
    <div className="flex flex-col gap-4">
      <h1 className="text-xl font-semibold text-slate-100">Macro &amp; Market Overview</h1>
      <p className="text-sm text-slate-400">
        Kenyan rates, inflation and the yield curve from Central Bank of Kenya releases, with sector
        rotation across NSE equities.
      </p>
      <div className="grid gap-4 lg:grid-cols-2">
        <div className="lg:col-span-2">
          <MacroSnapshot />
        </div>
        <YieldCurveChart />
        <InflationTrajectoryChart />
        <div className="lg:col-span-2">
          <SectorRotationHeatmap />
        </div>
      </div>
    </div>
  );
}
