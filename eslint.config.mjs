import { defineConfig, globalIgnores } from "eslint/config";
import nextVitals from "eslint-config-next/core-web-vitals";
import nextTs from "eslint-config-next/typescript";

// The import boundary of src/components/terminal/** is checked on resolved paths by
// scripts/check-terminal-boundary.mjs, which `npm run lint` runs after ESLint. It is not
// expressed as no-restricted-imports here, because specifier patterns cannot tell whether a
// relative import resolves outside the widget folder.
export default defineConfig([
  ...nextVitals,
  ...nextTs,
  {
    rules: {
      "@typescript-eslint/no-explicit-any": "error",
    },
  },
  globalIgnores([
    ".next/**",
    "out/**",
    "build/**",
    "next-env.d.ts",
    "node_modules/**",
    "backend/**",
    ".venv/**",
    "dist/**",
    // Root-anchored patterns: the host fixture's own build output needs its own entry.
    "e2e/host-fixture/.next/**",
    "test-results/**",
    "playwright-report/**",
    "blob-report/**",
  ]),
]);
