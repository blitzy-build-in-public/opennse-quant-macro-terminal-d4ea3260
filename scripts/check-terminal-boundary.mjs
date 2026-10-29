/**
 * check-terminal-boundary: import boundary of the embeddable widget folder.
 *
 * `src/components/terminal/` must stay self-contained so a host application can copy or import
 * it (Rule 1, Tech Stack & Modularity Constraints; AAP 0.4.4). The rule is checked on resolved
 * paths, not on specifier patterns:
 *   - a relative specifier passes only if it resolves inside `src/components/terminal/`, at any
 *     nesting depth;
 *   - a non-relative specifier passes only if it is `react`, `react-dom` or `recharts`, or a
 *     subpath of one of them (`react/jsx-runtime`). `next`, `next/*`, `@/…` aliases, host
 *     packages and data libraries are rejected.
 *
 * Usage:
 *   node scripts/check-terminal-boundary.mjs              check every .ts/.tsx file in the folder
 *   node scripts/check-terminal-boundary.mjs --self-test  run the in-memory checker cases
 *
 * Exit codes:
 *   0  no violation (or every self-test case passed)
 *   1  at least one violation (or a self-test case failed)
 *   2  usage error, or the folder is missing, holds no source files or cannot be read
 *
 * `npm run lint` runs ESLint, then this script, then this script with `--self-test`.
 */
import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import ts from "typescript";

// Resolved from this file rather than process.cwd(), so the result is the same from any directory.
const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const TERMINAL_REL = "src/components/terminal";
const TERMINAL_ROOT = path.join(REPO_ROOT, "src", "components", "terminal");

/** Packages a widget may import. `react-is` is a Recharts peer that widgets never import. */
const ALLOWED_PACKAGES = ["react", "react-dom", "recharts"];
/** Checked file extensions; `.d.ts` files end in `.ts` and are included. */
const SOURCE_EXTENSIONS = [".ts", ".tsx"];

const REASON_OUTSIDE = "outside-terminal-folder";
const REASON_PACKAGE = "disallowed-package";
const NON_RELATIVE_NOTE =
  `(non-relative specifier; allowed: ${ALLOWED_PACKAGES.join(", ")} and their subpaths)`;

/**
 * @typedef {"import" | "reference-path" | "reference-types"} SpecifierKind
 * @typedef {{ specifier: string, pos: number, kind: SpecifierKind }} SpecifierRef
 * @typedef {{
 *   file: string,
 *   line: number,
 *   column: number,
 *   specifier: string,
 *   kind: SpecifierKind,
 *   resolved: string,
 *   reason: string,
 * }} Violation
 */

/**
 * Converts platform separators to `/`, so printed paths are identical on every OS.
 * @param {string} p
 * @returns {string}
 */
function toPosix(p) {
  return p.split(path.sep).join("/");
}

/**
 * @param {string} spec
 * @returns {boolean} true for `.`, `..`, `./…` and `../…`
 */
function isRelativeSpecifier(spec) {
  return spec === "." || spec === ".." || spec.startsWith("./") || spec.startsWith("../");
}

/**
 * Exact name or `name/` subpath match: a bare prefix test would accept `recharts-extra`.
 * @param {string} spec
 * @returns {boolean}
 */
function isAllowedPackage(spec) {
  return ALLOWED_PACKAGES.some((name) => spec === name || spec.startsWith(`${name}/`));
}

/**
 * @param {string} root absolute directory
 * @param {string} target absolute path
 * @returns {boolean} true when `target` is `root` itself or lies beneath it
 */
function isWithin(root, target) {
  const rel = path.relative(root, target);
  // An absolute result means another drive (Windows). `..foo` is a legitimate name inside the
  // root, so only `..` itself or a `../` prefix means the path escapes.
  if (path.isAbsolute(rel)) return false;
  return rel !== ".." && !rel.startsWith(`..${path.sep}`);
}

/**
 * The folder itself (`rel === ""`, i.e. its `index.ts`) counts as inside.
 * @param {string} resolvedAbs
 * @param {string} terminalRoot
 * @returns {boolean}
 */
function isInsideTerminal(resolvedAbs, terminalRoot) {
  return isWithin(terminalRoot, resolvedAbs);
}

/**
 * Module specifiers found by parsing the source into an AST. This backs up `ts.preProcessFile`,
 * whose token scanner does not report `export * as ns from "x"`, `export type * as ns from "x"`
 * or `import defer * as ns from "x"` in TypeScript 5.9; without it those forms would bypass the
 * boundary.
 * @param {string} sourceText
 * @param {string} fileName decides between TS and TSX parsing
 * @returns {SpecifierRef[]} positions of the first character of each specifier's text
 */
function listAstSpecifiers(sourceText, fileName) {
  const scriptKind = fileName.endsWith(".tsx") ? ts.ScriptKind.TSX : ts.ScriptKind.TS;
  const sourceFile = ts.createSourceFile(
    fileName,
    sourceText,
    ts.ScriptTarget.Latest,
    false,
    scriptKind,
  );
  /** @type {SpecifierRef[]} */
  const found = [];
  /** @param {import("typescript").Node | undefined} literal */
  const record = (literal) => {
    if (literal && (ts.isStringLiteral(literal) || ts.isNoSubstitutionTemplateLiteral(literal))) {
      // +1 skips the opening quote, so `pos` is the specifier's first character.
      const pos = literal.getStart(sourceFile) + 1;
      found.push({ specifier: literal.text, pos, kind: "import" });
    }
  };
  /** @param {import("typescript").Node} node */
  const visit = (node) => {
    if (ts.isImportDeclaration(node) || ts.isExportDeclaration(node)) {
      record(node.moduleSpecifier);
    } else if (
      ts.isImportEqualsDeclaration(node) &&
      ts.isExternalModuleReference(node.moduleReference)
    ) {
      record(node.moduleReference.expression);
    } else if (
      ts.isCallExpression(node) &&
      (node.expression.kind === ts.SyntaxKind.ImportKeyword ||
        (ts.isIdentifier(node.expression) && node.expression.text === "require"))
    ) {
      record(node.arguments[0]);
    } else if (ts.isImportTypeNode(node) && ts.isLiteralTypeNode(node.argument)) {
      record(node.argument.literal);
    }
    // forEachChild stops at the first truthy callback result, so visit must return nothing.
    ts.forEachChild(node, visit);
  };
  visit(sourceFile);
  return found;
}

/**
 * Every specifier the file pulls in, from `ts.preProcessFile` (AAP 0.4.4) merged with the AST
 * backstop. `importedFiles` covers import/export-from, side-effect and type-only imports, dynamic
 * `import()` anywhere (type positions included), `import x = require()`, `require()` and module
 * augmentations. `/// <reference path>` and `/// <reference types>` are kept as their own kinds.
 * Ambient `declare module` names in a script file are declarations, not imports, and are skipped.
 * @param {string} sourceText
 * @param {string} [fileName]
 * @returns {SpecifierRef[]} sorted by position; `pos` is the first character of the specifier
 */
function listSpecifiers(sourceText, fileName = "module.tsx") {
  const info = ts.preProcessFile(
    sourceText,
    /* readImportFiles */ true,
    /* detectJavaScriptImports */ true,
  );
  /**
   * @param {import("typescript").FileReference[]} files
   * @param {SpecifierKind} kind
   * @param {number} offset added to `pos` so every kind points at the specifier's first character
   * @returns {SpecifierRef[]}
   */
  const fromReferences = (files, kind, offset) =>
    files.map((f) => ({ specifier: f.fileName, pos: f.pos + offset, kind }));
  /** @type {SpecifierRef[]} */
  const refs = [
    // preProcessFile reports an import at its opening quote, a reference at its path text.
    ...fromReferences(info.importedFiles, "import", 1),
    ...listAstSpecifiers(sourceText, fileName),
    ...fromReferences(info.referencedFiles, "reference-path", 0),
    ...fromReferences(info.typeReferenceDirectives, "reference-types", 0),
  ];
  const seen = new Set();
  return refs
    .filter((ref) => {
      const key = `${ref.kind}\u0000${ref.pos}\u0000${ref.specifier}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    })
    .sort((a, b) => a.pos - b.pos);
}

/**
 * @param {string} sourceText
 * @param {number} pos zero-based character offset
 * @returns {{ line: number, column: number }} 1-based
 */
function lineCol(sourceText, pos) {
  let line = 1;
  let lineStart = 0;
  for (let i = 0; i < pos && i < sourceText.length; i += 1) {
    if (sourceText[i] === "\n") {
      line += 1;
      lineStart = i + 1;
    }
  }
  return { line, column: pos - lineStart + 1 };
}

/**
 * Checks one file's specifiers against the boundary. Performs no filesystem access, so the
 * self-test can call it with virtual paths.
 * @param {string} absFile absolute path of the file (real or virtual)
 * @param {string} sourceText
 * @param {string} terminalRoot absolute path of the widget folder
 * @returns {Violation[]}
 */
function checkSource(absFile, sourceText, terminalRoot) {
  /** @type {Violation[]} */
  const violations = [];
  for (const ref of listSpecifiers(sourceText, absFile)) {
    const { line, column } = lineCol(sourceText, ref.pos);
    const base = { file: absFile, line, column, specifier: ref.specifier, kind: ref.kind };
    // A reference path is file-relative even without a `./` prefix, so it resolves like one.
    if (ref.kind === "reference-path" || isRelativeSpecifier(ref.specifier)) {
      const resolved = path.resolve(path.dirname(absFile), ref.specifier);
      if (!isInsideTerminal(resolved, terminalRoot)) {
        violations.push({ ...base, resolved, reason: REASON_OUTSIDE });
      }
    } else if (!isAllowedPackage(ref.specifier)) {
      // Aliases such as `@/…` need no tsconfig resolution: every alias is rejected here.
      violations.push({ ...base, resolved: NON_RELATIVE_NOTE, reason: REASON_PACKAGE });
    }
  }
  return violations;
}

/**
 * Recursive walk, sorted by name at every level so output order is deterministic.
 * @param {string} dir
 * @returns {string[]} absolute paths of regular `.ts`/`.tsx` files
 */
function collectSourceFiles(dir) {
  const entries = fs
    .readdirSync(dir, { withFileTypes: true })
    .sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
  /** @type {string[]} */
  const files = [];
  for (const entry of entries) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...collectSourceFiles(full));
    } else if (entry.isFile() && SOURCE_EXTENSIONS.some((ext) => entry.name.endsWith(ext))) {
      files.push(full);
    }
  }
  return files;
}

/**
 * Repository-relative POSIX path, or the absolute path when it lies outside the repository.
 * @param {string} absPath
 * @returns {string}
 */
function displayPath(absPath) {
  if (!isWithin(REPO_ROOT, absPath)) return toPosix(absPath);
  return toPosix(path.relative(REPO_ROOT, absPath)) || ".";
}

/**
 * @param {Violation} v
 * @returns {string} `<file>:<line>:<col>  import "<spec>" -> <resolved>  [<reason>]`
 */
function formatViolation(v) {
  const label = v.kind === "import" ? "import" : v.kind.replace("-", " ");
  const resolved = v.reason === REASON_OUTSIDE ? displayPath(v.resolved) : v.resolved;
  const location = `${displayPath(v.file)}:${v.line}:${v.column}`;
  return `${location}  ${label} "${v.specifier}" -> ${resolved}  [${v.reason}]`;
}

/**
 * Checks every source file in the widget folder.
 * @returns {number} exit code
 */
function runCheck() {
  let isDirectory = false;
  try {
    isDirectory = fs.statSync(TERMINAL_ROOT).isDirectory();
  } catch {
    isDirectory = false;
  }
  if (!isDirectory) {
    console.error(
      `check-terminal-boundary: ${TERMINAL_REL} not found at ${toPosix(TERMINAL_ROOT)}`,
    );
    return 2;
  }

  const files = collectSourceFiles(TERMINAL_ROOT);
  // A vacuous pass would hide a moved or renamed widget folder.
  if (files.length === 0) {
    console.error(
      `check-terminal-boundary: no ${SOURCE_EXTENSIONS.join("/")} files found in ${TERMINAL_REL} ` +
        `(${toPosix(TERMINAL_ROOT)})`,
    );
    return 2;
  }

  /** @type {Violation[]} */
  const violations = [];
  for (const file of files) {
    violations.push(...checkSource(file, fs.readFileSync(file, "utf8"), TERMINAL_ROOT));
  }

  if (violations.length > 0) {
    for (const v of violations) console.error(formatViolation(v));
    const fileCount = new Set(violations.map((v) => v.file)).size;
    console.error(
      `check-terminal-boundary: ${violations.length} violation(s) in ${fileCount} file(s)`,
    );
    return 1;
  }

  console.log(`check-terminal-boundary: OK (${files.length} files in ${TERMINAL_REL} checked)`);
  return 0;
}


/**
 * @typedef {{
 *   name: string,
 *   file: string,
 *   source: string,
 *   expect: "reject" | "accept",
 *   specifier?: string,
 *   reason?: string,
 * }} SelfTestCase
 * `file` is relative to the widget folder and becomes a virtual path under it; a reject case
 * names the specifier (and reason) that must be reported.
 */

/** @type {SelfTestCase[]} */
const SELF_TEST_CASES = [
  // The cases AAP 0.4.4 requires.
  {
    name: "top-level widget importing a host component from the parent folder",
    file: "X.tsx",
    source: 'import SomeHostComponent from "../SomeHostComponent";\n',
    expect: "reject",
    specifier: "../SomeHostComponent",
    reason: REASON_OUTSIDE,
  },
  {
    name: "lib module importing a standalone page",
    file: "lib/x.ts",
    source: 'import Page from "../../app/page";\n',
    expect: "reject",
    specifier: "../../app/page",
    reason: REASON_OUTSIDE,
  },
  {
    name: "path alias @/",
    file: "X.tsx",
    source: 'import { x } from "@/lib/x";\n',
    expect: "reject",
    specifier: "@/lib/x",
    reason: REASON_PACKAGE,
  },
  {
    name: "next/link",
    file: "X.tsx",
    source: 'import Link from "next/link";\n',
    expect: "reject",
    specifier: "next/link",
    reason: REASON_PACKAGE,
  },
  {
    name: "data library @tanstack/react-query",
    file: "X.tsx",
    source: 'import { useQuery } from "@tanstack/react-query";\n',
    expect: "reject",
    specifier: "@tanstack/react-query",
    reason: REASON_PACKAGE,
  },
  {
    name: "widget importing ./lib/format",
    file: "X.tsx",
    source: 'import { formatDecimal } from "./lib/format";\n',
    expect: "accept",
  },
  {
    name: "lib module importing ../types",
    file: "lib/x.ts",
    source: 'import type { DecimalString } from "../types";\n',
    expect: "accept",
  },
  {
    name: "recharts",
    file: "X.tsx",
    source: 'import { LineChart } from "recharts";\n',
    expect: "accept",
  },

  // Every other specifier form the checker must see.
  {
    name: "export-from of next/navigation",
    file: "X.tsx",
    source: 'export * from "next/navigation";\n',
    expect: "reject",
    specifier: "next/navigation",
    reason: REASON_PACKAGE,
  },
  {
    name: "namespace re-export (missed by preProcessFile, caught by the AST backstop)",
    file: "X.tsx",
    source: 'export * as navigation from "next/navigation";\n',
    expect: "reject",
    specifier: "next/navigation",
    reason: REASON_PACKAGE,
  },
  {
    name: "type-only namespace re-export escaping the folder",
    file: "lib/x.ts",
    source: 'export type * as HostTypes from "../../host-types";\n',
    expect: "reject",
    specifier: "../../host-types",
    reason: REASON_OUTSIDE,
  },
  {
    name: "deferred namespace import",
    file: "X.tsx",
    source: 'import defer * as heavy from "@tanstack/react-query";\n',
    expect: "reject",
    specifier: "@tanstack/react-query",
    reason: REASON_PACKAGE,
  },
  {
    name: "dynamic import inside a function body",
    file: "X.tsx",
    source:
      "export function Widget() {\n" +
      '  const load = () => import("../../../app/page");\n' +
      "  return load;\n" +
      "}\n",
    expect: "reject",
    specifier: "../../../app/page",
    reason: REASON_OUTSIDE,
  },
  {
    name: "import() in a type position",
    file: "X.tsx",
    source: 'export type Page = typeof import("../../app/page");\n',
    expect: "reject",
    specifier: "../../app/page",
    reason: REASON_OUTSIDE,
  },
  {
    name: "side-effect import of host CSS",
    file: "X.tsx",
    source: 'import "../Host.css";\n',
    expect: "reject",
    specifier: "../Host.css",
    reason: REASON_OUTSIDE,
  },
  {
    name: "import = require of a host package",
    file: "X.tsx",
    source: 'import legacy = require("next/router");\n',
    expect: "reject",
    specifier: "next/router",
    reason: REASON_PACKAGE,
  },
  {
    name: "require() call",
    file: "lib/x.ts",
    source: 'export const dynamic = require("next/dynamic");\n',
    expect: "reject",
    specifier: "next/dynamic",
    reason: REASON_PACKAGE,
  },
  {
    name: "module augmentation of a host package",
    file: "X.tsx",
    source:
      'import { useState } from "react";\n' +
      'declare module "next/link" {\n' +
      "  export const prefetch: boolean;\n" +
      "}\n" +
      "export const hook = useState;\n",
    expect: "reject",
    specifier: "next/link",
    reason: REASON_PACKAGE,
  },
  {
    name: "/// <reference path> escaping the folder",
    file: "X.tsx",
    source: '/// <reference path="../../../next-env.d.ts" />\nexport {};\n',
    expect: "reject",
    specifier: "../../../next-env.d.ts",
    reason: REASON_OUTSIDE,
  },
  {
    name: "/// <reference types> of a non-allowed package",
    file: "types.ts",
    source: '/// <reference types="node" />\nexport {};\n',
    expect: "reject",
    specifier: "node",
    reason: REASON_PACKAGE,
  },
  {
    name: "prefix guard: recharts-extra",
    file: "X.tsx",
    source: 'import x from "recharts-extra";\n',
    expect: "reject",
    specifier: "recharts-extra",
    reason: REASON_PACKAGE,
  },
  {
    name: "react-is (not in the allowlist)",
    file: "X.tsx",
    source: 'import isMemo from "react-is";\n',
    expect: "reject",
    specifier: "react-is",
    reason: REASON_PACKAGE,
  },
  {
    name: "react/jsx-runtime subpath",
    file: "X.tsx",
    source: 'import { jsx } from "react/jsx-runtime";\n',
    expect: "accept",
  },
  {
    name: "react-dom",
    file: "X.tsx",
    source: 'import { createPortal } from "react-dom";\n',
    expect: "accept",
  },
  {
    name: "nested file climbing back into lib/",
    file: "shared/a/b/Deep.tsx",
    source: 'import { formatDecimal } from "../../../lib/format";\n',
    expect: "accept",
  },
  {
    name: "lib module importing the folder index",
    file: "lib/x.ts",
    source: 'import { MacroSnapshot } from "..";\n',
    expect: "accept",
  },
  {
    name: "sibling whose name starts with two dots",
    file: "X.tsx",
    source: 'import { helper } from "./..helpers";\n',
    expect: "accept",
  },
  {
    name: "/// <reference types> of react",
    file: "types.ts",
    source: '/// <reference types="react" />\nexport {};\n',
    expect: "accept",
  },
  {
    name: "client widget with comments and strings that only mention host imports",
    file: "Widget.tsx",
    source:
      '"use client";\n' +
      "// import Link from \"next/link\";\n" +
      'import { useEffect, useState } from "react";\n' +
      'import { Line, LineChart } from "recharts";\n' +
      'import type { MacroOverviewResponse } from "./types";\n' +
      'import { WidgetFrame } from "./shared/WidgetFrame";\n' +
      "const hint = \"import x from 'next/router'\";\n" +
      "export function Widget({ data }: { data?: MacroOverviewResponse }) {\n" +
      "  const [ready, setReady] = useState(false);\n" +
      "  useEffect(() => setReady(true), []);\n" +
      "  return (\n" +
      '    <WidgetFrame title={hint} state={ready ? "ready" : "loading"}>\n' +
      "      <LineChart data={data ? [data] : []}>\n" +
      '        <Line dataKey="value" />\n' +
      "      </LineChart>\n" +
      "    </WidgetFrame>\n" +
      "  );\n" +
      "}\n",
    expect: "accept",
  },
];

/**
 * Runs every in-memory case through `checkSource`, the same function the real run uses. Reads
 * no files, so it passes before the widgets exist.
 * @returns {number} exit code
 */
function runSelfTest() {
  let passed = 0;
  for (const testCase of SELF_TEST_CASES) {
    const virtualFile = path.join(TERMINAL_ROOT, ...testCase.file.split("/"));
    const violations = checkSource(virtualFile, testCase.source, TERMINAL_ROOT);
    const ok =
      testCase.expect === "accept"
        ? violations.length === 0
        : violations.some(
            (v) =>
              v.specifier === testCase.specifier &&
              (testCase.reason === undefined || v.reason === testCase.reason),
          );
    if (ok) {
      passed += 1;
      console.log(`PASS ${testCase.name}`);
    } else {
      const found =
        violations.map((v) => `"${v.specifier}" [${v.reason}]`).join(", ") || "none";
      const got = `got ${violations.length} violations: ${found}`;
      console.log(`FAIL ${testCase.name} (expected ${testCase.expect}, ${got})`);
    }
  }
  const total = SELF_TEST_CASES.length;
  console.log(`check-terminal-boundary --self-test: ${passed}/${total} passed`);
  return passed === total ? 0 : 1;
}

const USAGE = [
  "usage: node scripts/check-terminal-boundary.mjs              check src/components/terminal",
  "       node scripts/check-terminal-boundary.mjs --self-test  run the checker's self-test",
].join("\n");

/**
 * @param {string[]} args
 * @returns {number} exit code
 */
function main(args) {
  if (args.length === 0) return runCheck();
  if (args.length === 1 && args[0] === "--self-test") return runSelfTest();
  console.error(USAGE);
  return 2;
}

try {
  // exitCode rather than process.exit(), so buffered output is flushed.
  process.exitCode = main(process.argv.slice(2));
} catch (error) {
  console.error(
    `check-terminal-boundary: ${error instanceof Error ? error.message : String(error)}`,
  );
  process.exitCode = 2;
}
