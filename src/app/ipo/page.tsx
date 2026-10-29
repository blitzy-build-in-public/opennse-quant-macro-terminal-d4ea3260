import { IpoValuationSandbox } from "@/components/terminal";

// A server component that prerenders statically: the widget is a client component that owns its
// slider state, debounced valuation requests and loading/error states, and fetches on the client.
export default function IpoPage() {
  return (
    <div className="space-y-4">
      <h1 className="text-xl font-semibold text-slate-100">IPO Valuation Sandbox</h1>
      <p className="text-sm text-slate-400">
        Test offer price, growth, margin, WACC and debt assumptions with DCF and comparable
        multiples; values are in KES.
      </p>
      <IpoValuationSandbox />
    </div>
  );
}
