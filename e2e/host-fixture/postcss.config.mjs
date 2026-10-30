/**
 * PostCSS config of the host fixture: Tailwind CSS 4 through `@tailwindcss/postcss` and nothing
 * else (Tailwind 4 needs no autoprefixer). It is the host's own config and neither imports nor
 * extends the standalone app's root `postcss.config.mjs`.
 *
 * `base` restricts Tailwind's automatic source detection to this fixture directory. Left unset,
 * detection starts at the cwd of the process evaluating this config: the repository root under
 * webpack (`next dev --webpack e2e/host-fixture`) or plain Node.js run from there, where it would
 * find the widget classes in `src/components/terminal/` on its own. (Turbopack's PostCSS worker
 * happens to run in the fixture directory.) With `base` set, under either bundler, those classes
 * reach the stylesheet only through the `@source` line in `app/globals.css`, the one step a
 * Tailwind 4 host adds, so the host-embed test proves that step rather than passing by accident.
 * `@source` paths resolve relative to the CSS file that declares them, not relative to `base`.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const CONFIG_FILE_NAME = "postcss.config.mjs";

/** Directory of this module according to `import.meta.url`, or null when that is not usable. */
function moduleDir() {
  const url = import.meta.url;
  if (typeof url !== "string" || !url.startsWith("file:")) {
    return null;
  }
  try {
    return path.dirname(fileURLToPath(url));
  } catch {
    return null;
  }
}

/**
 * Absolute path of this directory.
 *
 * Plain Node.js gives a correct `import.meta.url`. Turbopack does not: it evaluates PostCSS configs
 * from its own bundle in a Node.js worker whose cwd is the Next.js project directory (this
 * fixture), and with `turbopack.root` at the repository root the `import.meta.url` it supplies
 * names a directory that does not exist (`<repo>/e2e/e2e/host-fixture` under Next.js 16.4.3).
 * A candidate therefore counts only when it holds this config file, which is why `node:fs` is
 * imported. `<cwd>/e2e/host-fixture` is tried before the cwd itself, so a launch from the
 * repository root never selects the root's own `postcss.config.mjs`. Finding no candidate is an
 * error: building without `base` would let the widget classes in through automatic detection.
 */
function resolveFixtureDir() {
  const cwd = process.cwd();
  const candidates = [moduleDir(), path.resolve(cwd, "e2e/host-fixture"), cwd].filter(
    (dir) => dir !== null,
  );
  const fixtureDir = candidates.find((dir) => fs.existsSync(path.join(dir, CONFIG_FILE_NAME)));
  if (fixtureDir === undefined) {
    throw new Error(
      `Host fixture PostCSS config: no candidate directory (${candidates.join(", ")}) holds ` +
        `${CONFIG_FILE_NAME}, so Tailwind's \`base\` cannot be pinned to the fixture.`,
    );
  }
  return fixtureDir;
}

const fixtureDir = resolveFixtureDir();

const config = {
  plugins: {
    "@tailwindcss/postcss": { base: fixtureDir },
  },
};

export default config;
