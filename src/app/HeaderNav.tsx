"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

// The header's view links. A client component because only the client router knows the current
// path; the root layout stays a server component and renders this beside the home link. It lives
// in src/app rather than src/components/terminal/ because it imports next/*, which the embeddable
// widget folder forbids. The class strings are defined here, not imported from the layout, so the
// server layout never imports a value from this client module.

const NAV_ITEMS = [
  { href: "/", label: "Overview", testId: "nav-overview" },
  { href: "/cycles", label: "Cycle Backtester", testId: "nav-cycles" },
  { href: "/ipo", label: "IPO Sandbox", testId: "nav-ipo" },
  { href: "/screener", label: "Screener", testId: "nav-screener" },
] as const;

// The same focus ring the layout gives its skip link and home link.
const FOCUS_RING_CLASS =
  "focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400";

/* BLITZY [A11Y]: nav links use the specified px-3 py-1.5 text-sm, a target about 32px tall,
   below the 44x44px recommendation (above WCAG 2.2 AA's 24px). Implemented as specified;
   flagged for designer review. */
const NAV_LINK_CLASS = `block rounded px-3 py-1.5 text-sm text-slate-300 hover:bg-slate-800 hover:text-slate-100 active:bg-slate-700 active:text-slate-100 motion-safe:transition-colors motion-safe:duration-150 motion-safe:ease-out ${FOCUS_RING_CLASS}`;

// The current view: a filled slate surface, brighter text and medium weight. Padding, radius,
// text size and transitions match the other links, so the header keeps its height on every view.
const NAV_LINK_ACTIVE_CLASS = `block rounded bg-slate-800 px-3 py-1.5 text-sm font-medium text-slate-100 hover:bg-slate-700 active:bg-slate-700 motion-safe:transition-colors motion-safe:duration-150 motion-safe:ease-out ${FOCUS_RING_CLASS}`;

/** True when `href` names the view at `pathname`: "/" only on the root, others on their subtree. */
function isActiveHref(pathname: string | null, href: string): boolean {
  if (pathname === null) {
    return false;
  }
  if (href === "/") {
    return pathname === "/";
  }
  return pathname === href || pathname.startsWith(`${href}/`);
}

export default function HeaderNav() {
  const pathname: string | null = usePathname();

  return (
    <nav aria-label="Terminal views">
      <ul className="flex flex-wrap items-center gap-1">
        {NAV_ITEMS.map((item) => {
          const isActive = isActiveHref(pathname, item.href);
          return (
            <li key={item.href}>
              <Link
                href={item.href}
                data-testid={item.testId}
                aria-current={isActive ? "page" : undefined}
                className={isActive ? NAV_LINK_ACTIVE_CLASS : NAV_LINK_CLASS}
              >
                {item.label}
              </Link>
            </li>
          );
        })}
      </ul>
    </nav>
  );
}
