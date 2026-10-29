// Host-embed proof page (AAP 0.16.4): a foreign host renders a terminal widget with nothing but an
// import statement. YieldCurveChart comes by relative path from the widget folder's public barrel,
// the same entry point a host that copies the folder imports, and takes default props only, so it
// fetches /api/v1/terminal/macro/overview from this app's own origin, which ../next.config.mjs
// rewrites to the terminal API. This is a server component; the widget module carries its own
// "use client" directive.
import { YieldCurveChart } from "../../../src/components/terminal";

export default function HostPage() {
  return (
    <main data-testid="host-page" className="mx-auto max-w-4xl space-y-4">
      <h1 className="text-lg font-semibold">Host dashboard</h1>
      <YieldCurveChart />
    </main>
  );
}
