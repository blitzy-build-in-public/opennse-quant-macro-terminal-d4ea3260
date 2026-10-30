import type { Metadata } from "next";
import type { ReactNode } from "react";

// A server segment layout that exists only to give /screener its document title: the page is a
// client component (it owns the selected ticker), and a client module cannot export metadata. It
// returns its children unchanged, with no wrapper element, so the page's DOM is unaffected.
export const metadata: Metadata = {
  title: "Custom Watchlists & Screener",
};

export default function ScreenerLayout({ children }: Readonly<{ children: ReactNode }>) {
  return children;
}
