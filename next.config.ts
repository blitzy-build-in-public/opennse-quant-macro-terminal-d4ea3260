import type { NextConfig } from "next";

// Standalone mode: the browser calls /api/v1/terminal/* on this origin, exactly as it would inside
// a host, and Next.js forwards those calls to the terminal's own FastAPI app.
//
// TERMINAL_API_ORIGIN is read when Next.js evaluates rewrites() during `next build` (and
// `next dev`). Rewrites are baked into the build's routes manifest, so set it before building and
// rebuild after changing it. TERMINAL_* variables are not NEXT_PUBLIC_, so none reach the browser
// bundle.
const DEFAULT_API_ORIGIN = "http://localhost:8000";

function resolveApiOrigin(raw: string | undefined): string {
  const origin = (raw ?? DEFAULT_API_ORIGIN).replace(/\/+$/, "");
  // "localhost:8000" parses as a URL with the scheme "localhost:", so the scheme is checked too.
  const protocol = URL.canParse(origin) ? new URL(origin).protocol : null;
  if (protocol !== "http:" && protocol !== "https:") {
    throw new Error(`TERMINAL_API_ORIGIN must be an absolute http(s) URL: "${origin}"`);
  }
  return origin;
}

const apiOrigin = resolveApiOrigin(process.env.TERMINAL_API_ORIGIN);

const nextConfig: NextConfig = {
  reactStrictMode: true,
  // A plain array is applied after filesystem routes; the app defines no routes under /api/.
  async rewrites() {
    return [
      {
        source: "/api/v1/terminal/:path*",
        destination: `${apiOrigin}/api/v1/terminal/:path*`,
      },
    ];
  },
};

export default nextConfig;
