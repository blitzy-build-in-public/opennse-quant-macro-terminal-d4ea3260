import type { Metadata } from "next";
import Link from "next/link";
import type { ReactNode } from "react";
import HeaderNav from "./HeaderNav";
import "./globals.css";

// Standalone page shell only. The widgets in src/components/terminal/ never depend on anything
// here (no providers, context, fonts or global state), so they render the same inside a host.
// next/font is deliberately not used: Google fonts are fetched at build time, which would make
// `next build` depend on the network. Borders and offsets use Tailwind's logical utilities
// (border-be, border-bs, inset-s, inset-bs), which render as bottom/top/left in this LTR layout.
// The body's surface colours carry Tailwind's `!` (important) modifier: Next's built-in 404 page
// injects unlayered `body` colours (white, or black under prefers-color-scheme: dark), and those
// beat layered utilities, so `!` keeps the slate shell surface on every Not Found route.

export const metadata: Metadata = {
  title: {
    default: "OpenNSE Quant & Macro Terminal",
    template: "%s — OpenNSE Quant & Macro Terminal",
  },
  description:
    "Self-hosted analytics terminal for Nairobi Securities Exchange equities and Kenyan macroeconomic data, built on public sources.",
};

const FOCUS_RING_CLASS =
  "focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400";

const HOME_LINK_CLASS = `rounded text-base font-semibold tracking-tight text-slate-100 hover:text-sky-300 active:text-sky-400 motion-safe:transition-colors motion-safe:duration-150 motion-safe:ease-out ${FOCUS_RING_CLASS}`;

export default function RootLayout({ children }: Readonly<{ children: ReactNode }>) {
  return (
    <html lang="en">
      <body className="min-h-screen bg-slate-950! font-sans text-slate-100! antialiased">
        <a
          href="#main-content"
          className={`sr-only focus-visible:not-sr-only focus-visible:absolute focus-visible:inset-s-4 focus-visible:inset-bs-4 focus-visible:z-50 focus-visible:rounded focus-visible:bg-slate-800 focus-visible:px-3 focus-visible:py-1.5 focus-visible:text-sm focus-visible:text-slate-100 ${FOCUS_RING_CLASS}`}
        >
          Skip to content
        </a>
        <header className="border-be border-slate-800">
          <div className="mx-auto flex w-full max-w-7xl flex-wrap items-center justify-between gap-x-6 gap-y-2 px-4 py-3">
            <Link href="/" className={HOME_LINK_CLASS}>
              OpenNSE Quant &amp; Macro Terminal
            </Link>
            <HeaderNav />
          </div>
        </header>
        <main id="main-content" className="mx-auto w-full max-w-7xl px-4 py-6">
          {children}
        </main>
        <footer className="border-bs border-slate-800">
          <p className="mx-auto w-full max-w-7xl px-4 py-4 text-xs text-slate-400">
            Data from the public NSE market-statistics page and Central Bank of Kenya statistical
            releases. Seeded data is synthetic.
          </p>
        </footer>
      </body>
    </html>
  );
}
