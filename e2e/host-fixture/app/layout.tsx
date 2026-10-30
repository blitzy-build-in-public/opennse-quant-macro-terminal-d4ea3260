// Root layout of the foreign host app used by the host-embed proof (AAP 0.16.4). It is the host's
// own shell and gives the widget nothing to lean on: no providers, context, fonts or navigation.
// The light theme is deliberate: a widget that relied on the terminal's dark styles being inherited
// would render unreadably here, so the widget root must set its own surface.
import type { Metadata } from "next";
import type { ReactNode } from "react";
import "./globals.css";

export const metadata: Metadata = { title: "Host Fixture" };

export default function RootLayout({ children }: Readonly<{ children: ReactNode }>) {
  return (
    <html lang="en">
      <body data-testid="host-shell" className="min-h-screen bg-white text-gray-900">
        <header
          data-testid="host-header"
          className="border-b border-gray-200 px-6 py-3 font-semibold"
        >
          Host Platform
        </header>
        <div className="p-6">{children}</div>
      </body>
    </html>
  );
}
