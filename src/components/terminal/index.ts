/**
 * Public entry point of the OpenNSE terminal widget library: the eleven data widgets, the
 * presentational {@link SensitivityMatrix}, their props, and every API type from `./types`.
 * `lib/*` and `shared/*` are internal and not exported.
 *
 * Embedding in a host application (React 18.2+):
 * - Copy this folder into the host, or consume it in place. Tailwind 4 detects a copy inside the
 *   host project automatically; from outside the project root or `node_modules`, add
 *   `@source "<relative-path>/src/components/terminal";` to the host's CSS entry. Tailwind 3 adds
 *   `"./src/components/terminal/**\/*.{ts,tsx}"` to `content`. The widgets use the default
 *   slate, emerald, rose, amber and sky palettes, so a host theme must keep those families.
 * - Install `recharts@3.10.1` and the `react-is` version matching the host's React.
 * - Route `/api/v1/terminal/*` to a FastAPI app that mounts `terminal_router` (for example a
 *   Next.js rewrite), or pass an absolute `apiBaseUrl` and allow that origin in the API's CORS.
 * - Pass `getHeaders` to attach host credentials, such as a bearer token, to every request.
 */

// No "use client" here: each widget module carries its own directive, so a server component
// importing from this barrel keeps the client boundary at the widget. Widgets are re-exported by
// name, never with `export *`, which breaks client-reference resolution for server importers.

export { MacroSnapshot, type MacroSnapshotProps } from "./MacroSnapshot";
export { YieldCurveChart, type YieldCurveChartProps } from "./YieldCurveChart";
export {
  InflationTrajectoryChart,
  type InflationTrajectoryChartProps,
} from "./InflationTrajectoryChart";
export { SectorRotationHeatmap, type SectorRotationHeatmapProps } from "./SectorRotationHeatmap";
export { CycleBacktester, type CycleBacktesterProps } from "./CycleBacktester";
export { IpoValuationSandbox, type IpoValuationSandboxProps } from "./IpoValuationSandbox";
export { SensitivityMatrix, type SensitivityMatrixProps } from "./SensitivityMatrix";
export { EquityScreener, type EquityScreenerProps } from "./EquityScreener";
export { WatchlistManager, type WatchlistManagerProps } from "./WatchlistManager";
export { ValuationBandsChart, type ValuationBandsChartProps } from "./ValuationBandsChart";
export { NetFlowChart, type NetFlowChartProps } from "./NetFlowChart";
export { AnomalyFeed, type AnomalyFeedProps } from "./AnomalyFeed";

// `./types` is type-only, so a wildcard type re-export emits nothing at runtime.
export type * from "./types";
