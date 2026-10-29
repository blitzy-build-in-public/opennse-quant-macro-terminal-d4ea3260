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
  const value = raw ?? DEFAULT_API_ORIGIN;
  const url = URL.canParse(value) ? new URL(value) : null;
  // Userinfo, a query or a fragment would corrupt every rewrite destination. The raw text is
  // searched because a bare "?" or "#" leaves url.search and url.hash empty.
  if (url?.username || url?.password || /[?#]/.test(value)) {
    // The value is not echoed: it may hold a password or a token.
    throw new Error("TERMINAL_API_ORIGIN must not carry userinfo, a query string or a fragment");
  }
  // "localhost:8000" parses as a URL with the scheme "localhost:", so the scheme is checked too.
  // Text with an "@" is not echoed: "user:pw@host" parses without userinfo yet holds a password.
  if (url?.protocol !== "http:" && url?.protocol !== "https:") {
    const shown = value.includes("@")
      ? " (value not shown: it may hold credentials)"
      : `: "${value}"`;
    throw new Error(`TERMINAL_API_ORIGIN must be an absolute http(s) URL${shown}`);
  }
  // Rebuilt from the parsed URL, so the manifest holds only what was validated. A path prefix is
  // kept without its trailing slashes, since "/api/v1/terminal" is appended to it.
  return `${url.origin}${url.pathname.replace(/\/+$/, "")}`;
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
