/**
 * check-terminal-boundary: import boundary of the embeddable widget folder.
 *
 * `src/components/terminal/` must stay self-contained so a host application can copy or import
 * it (Rule 1, Tech Stack & Modularity Constraints; AAP 0.4.4). The rule is checked on resolved
 * paths, not on specifier patterns, in every .ts, .tsx, .mts, .cts, .js, .jsx, .mjs and .cjs file
 * in the folder, at any depth:
 *   - a relative specifier passes only if it resolves inside `src/components/terminal/`, at any
 *     nesting depth;
 *   - a non-relative specifier passes only if it is `react`, `react-dom` or `recharts`, or a
 *     subpath of one of them (`react/jsx-runtime`) with no `.`, `..` or empty segment. `next`,
 *     `next/*`, `@/…` aliases, host packages and data libraries are rejected;
 *   - `import()` and `require()` must take a string literal, since any other argument can name
 *     any module.
 * A violation prints the file, the specifier, the resolved path and its reason
 * (`outside-terminal-folder`, `disallowed-package` or `non-literal-specifier`). A file TypeScript
 * cannot parse prints `cannot parse` with the diagnostic, because it may hide imports.
 *
 * Usage:
 *   node scripts/check-terminal-boundary.mjs              check every source file in the folder
 *   node scripts/check-terminal-boundary.mjs --self-test  run the in-memory checker cases
 *   node scripts/check-terminal-boundary.mjs --help       print this help (or -h)
 *
 * Exit codes:
 *   0  no violation (or every self-test case passed, or help was printed)
 *   1  at least one violation (or a self-test case failed)
 *   2  usage error; the folder is missing or holds no source files; a file cannot be read or
 *      cannot be parsed (2 outranks 1, because the check could not run completely)
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
/**
 * Checked file extensions: every TypeScript and JavaScript module kind, because `allowJs` lets a
 * `.js` file in the folder re-export host code to a widget. `.d.ts`, `.d.mts` and `.d.cts` files
 * end in a listed extension and are included.
 */
const SOURCE_EXTENSIONS = [".ts", ".tsx", ".mts", ".cts", ".js", ".jsx", ".mjs", ".cjs"];

const REASON_OUTSIDE = "outside-terminal-folder";
const REASON_PACKAGE = "disallowed-package";
const REASON_NON_LITERAL = "non-literal-specifier";
const NON_RELATIVE_NOTE =
  `(non-relative specifier; allowed: ${ALLOWED_PACKAGES.join(", ")} and their subpaths)`;
const NON_LITERAL_NOTE = "(non-literal specifier; the resolved path cannot be determined)";
const MISSING_ARGUMENT_NOTE = "(no specifier argument; the resolved path cannot be determined)";

/**
 * `call` is set only on an `import()` or `require()` whose first argument is not a string
 * literal; `specifier` then holds that argument's source text, or "" when there is no argument.
 * @typedef {"import" | "reference-path" | "reference-types"} SpecifierKind
 * @typedef {"import" | "require"} SpecifierCall
 * @typedef {{
 *   specifier: string,
 *   pos: number,
 *   kind: SpecifierKind,
 *   call?: SpecifierCall,
 * }} SpecifierRef
 * @typedef {{
 *   file: string,
 *   line: number,
 *   column: number,
 *   specifier: string,
 *   kind: SpecifierKind,
 *   call?: SpecifierCall,
 *   resolved: string,
 *   reason: string,
 * }} Violation
 * @typedef {{
 *   file: string,
 *   line: number,
 *   column: number,
 *   code: number,
 *   message: string,
 * }} ParseError
 *   `message` is `TS<code> <text>`, for example `TS1185 Merge conflict marker encountered.`
 * @typedef {{ violations: Violation[], parseErrors: ParseError[] }} CheckResult
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
 * The one test of which files the walk checks, shared with the self-test so its cases use only
 * names a real run would collect.
 * @param {string} name file name or path
 * @returns {boolean}
 */
function isSourceFileName(name) {
  return SOURCE_EXTENSIONS.some((ext) => name.endsWith(ext));
}

/**
 * The script kind tsc gives each extension: `.ts`, `.mts` and `.cts` are TypeScript without JSX,
 * and `.js`, `.mjs` and `.cjs` are JavaScript, where type syntax is an error.
 * @param {string} fileName
 * @returns {import("typescript").ScriptKind}
 */
function scriptKindFor(fileName) {
  if (fileName.endsWith(".tsx")) return ts.ScriptKind.TSX;
  if (fileName.endsWith(".jsx")) return ts.ScriptKind.JSX;
  if ([".js", ".mjs", ".cjs"].some((ext) => fileName.endsWith(ext))) return ts.ScriptKind.JS;
  return ts.ScriptKind.TS;
}

/**
 * @param {string} spec
 * @returns {boolean} true for `.`, `..`, `./…` and `../…`
 */
function isRelativeSpecifier(spec) {
  return spec === "." || spec === ".." || spec.startsWith("./") || spec.startsWith("../");
}

/**
 * True for a character some resolver turns into a separator or a dot segment, so the specifier's
 * path cannot be judged from its text: `\` (a separator on Windows), `%` (Node's ESM resolver
 * decodes `%2e` to `.`), and ASCII controls and space (URL parsing drops tab and newline, so
 * `.<tab>.` becomes `..`, and trims trailing spaces).
 * @param {string} spec
 * @returns {boolean}
 */
function hasAmbiguousPathCharacter(spec) {
  for (const ch of spec) {
    if (ch === "\\" || ch === "%" || ch <= " " || ch === "\u007f") return true;
  }
  return false;
}

/**
 * Exact name or `name/` subpath match (a bare prefix test would accept `recharts-extra`), on a
 * specifier that is already in normal form. A `.` or `..` segment or an empty segment
 * (`recharts/../../src/app/page`, `react/./x`, `react//x`) changes under
 * `path.posix.normalize` and is rejected, because bundlers resolve it out of the package and into
 * host code. A trailing slash (`react/`) is kept by normalization and still passes.
 * @param {string} spec
 * @returns {boolean}
 */
function isAllowedPackage(spec) {
  if (hasAmbiguousPathCharacter(spec) || path.posix.normalize(spec) !== spec) return false;
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
 * Parses one file, once, for both the AST backstop and the parse check.
 * @param {string} fileName its extension decides the script kind (see scriptKindFor)
 * @param {string} sourceText
 * @returns {import("typescript").SourceFile} named with `/` separators, as TypeScript names
 *   program files, so `listParseErrors` can find it again by name
 */
function parseSource(fileName, sourceText) {
  return ts.createSourceFile(
    toPosix(fileName),
    sourceText,
    ts.ScriptTarget.Latest,
    false,
    scriptKindFor(fileName),
  );
}

/**
 * Syntax errors in a parsed file, from a one-file program over a host that serves only that
 * file, so nothing is read from or written to disk. A file with any of these may hide imports
 * (a merge-conflict marker or a binary-looking file ends the parse early), so its check is
 * incomplete.
 * @param {import("typescript").SourceFile} sourceFile
 * @returns {{ pos: number, code: number, message: string }[]}
 */
function listParseErrors(sourceFile) {
  /** @type {import("typescript").CompilerHost} */
  const host = {
    getSourceFile: (name) => (name === sourceFile.fileName ? sourceFile : undefined),
    getDefaultLibFileName: () => "lib.d.ts",
    writeFile: () => {
      throw new Error("the parse check never emits");
    },
    getCurrentDirectory: () => path.posix.dirname(sourceFile.fileName),
    getCanonicalFileName: (name) => name,
    useCaseSensitiveFileNames: () => true,
    getNewLine: () => "\n",
    fileExists: (name) => name === sourceFile.fileName,
    readFile: () => undefined,
  };
  const program = ts.createProgram({
    rootNames: [sourceFile.fileName],
    options: { noLib: true, noResolve: true, types: [], allowJs: true, noEmit: true },
    host,
  });
  return program.getSyntacticDiagnostics(sourceFile).map((d) => ({
    pos: d.start ?? 0,
    code: d.code,
    message: `TS${d.code} ${ts.flattenDiagnosticMessageText(d.messageText, " ")}`,
  }));
}

/**
 * Module specifiers found by parsing the source into an AST. This backs up `ts.preProcessFile`,
 * whose token scanner does not report `export * as ns from "x"`, `export type * as ns from "x"`
 * or `import defer * as ns from "x"` in TypeScript 5.9; without it those forms would bypass the
 * boundary.
 * @param {import("typescript").SourceFile} sourceFile
 * @returns {SpecifierRef[]} positions of the first character of each specifier's text
 */
function listAstSpecifiers(sourceFile) {
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
      const argument = node.arguments[0];
      const isLiteral =
        argument !== undefined &&
        (ts.isStringLiteral(argument) || ts.isNoSubstitutionTemplateLiteral(argument));
      if (isLiteral) {
        record(argument);
      } else {
        // A computed or missing specifier can name any module, so it is reported rather than
        // skipped. preProcessFile does not list these calls.
        found.push({
          // Whitespace is collapsed so a multi-line argument still prints on one line.
          specifier: argument ? argument.getText(sourceFile).replace(/\s+/g, " ") : "",
          // `arguments.pos` is the offset just after `(`.
          pos: argument ? argument.getStart(sourceFile) : node.arguments.pos,
          kind: "import",
          call: node.expression.kind === ts.SyntaxKind.ImportKeyword ? "import" : "require",
        });
      }
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
 * Start offsets (the opening quote or backtick) of every string literal and no-substitution
 * template literal in the AST, whatever its role. `listSpecifiers` keeps a `ts.preProcessFile`
 * hit only where one of these starts. preProcessFile's token scanner has no regex or JSX
 * context, so a hit it reads from a regex literal's body or from JSX text sits where the AST has
 * no string token, and is dropped. Every hit on a real string token is kept, in any role and
 * behind any wrapper (`as`, `satisfies`, `!`, a `<T>` assertion, parentheses), so no form
 * preProcessFile lists can bypass the boundary by falling outside a list of roles.
 * @param {import("typescript").SourceFile} sourceFile
 * @returns {Set<number>}
 */
function listStringLiteralStarts(sourceFile) {
  /** @type {Set<number>} */
  const starts = new Set();
  /** @param {import("typescript").Node} node */
  const visit = (node) => {
    if (ts.isStringLiteral(node) || ts.isNoSubstitutionTemplateLiteral(node)) {
      starts.add(node.getStart(sourceFile));
    }
    // forEachChild stops at the first truthy callback result, so visit must return nothing.
    ts.forEachChild(node, visit);
  };
  visit(sourceFile);
  return starts;
}

/**
 * Every specifier the file pulls in, from `ts.preProcessFile` (AAP 0.4.4) merged with the AST
 * backstop. `importedFiles` covers import/export-from, side-effect and type-only imports, dynamic
 * `import()` anywhere (type positions included), `import x = require()`, `require()` and module
 * augmentations. `/// <reference path>` and `/// <reference types>` are kept as their own kinds.
 * Ambient `declare module` names in a script file are declarations, not imports, and are skipped.
 * preProcessFile's token scanner has no regex or JSX context, so it reads a regex literal's body
 * and JSX text as code and lists an `import "x"` or `require("x")` written there. preProcessFile
 * stays the lister; the AST only confirms that an `importedFiles` hit starts a real string token
 * (`listStringLiteralStarts`), and a hit that does not is dropped.
 * @param {import("typescript").SourceFile} sourceFile
 * @returns {SpecifierRef[]} sorted by position; `pos` is the first character of the specifier
 */
function listSpecifiers(sourceFile) {
  const info = ts.preProcessFile(
    sourceFile.text,
    /* readImportFiles */ true,
    /* detectJavaScriptImports */ true,
  );
  // preProcessFile reports an import at its opening quote, where the AST literal starts.
  const stringStarts = listStringLiteralStarts(sourceFile);
  const importedFiles = info.importedFiles.filter((f) => stringStarts.has(f.pos));
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
    ...fromReferences(importedFiles, "import", 1),
    ...listAstSpecifiers(sourceFile),
    ...fromReferences(info.referencedFiles, "reference-path", 0),
    ...fromReferences(info.typeReferenceDirectives, "reference-types", 0),
  ];
  const seen = new Set();
  return refs
    .filter((ref) => {
      const key = `${ref.kind}\u0000${ref.call ?? ""}\u0000${ref.pos}\u0000${ref.specifier}`;
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
 * Checks one file's specifiers against the boundary and reports any syntax error that may hide
 * one. Performs no filesystem access, so the self-test can call it with virtual paths.
 * @param {string} absFile absolute path of the file (real or virtual)
 * @param {string} sourceText
 * @param {string} terminalRoot absolute path of the widget folder
 * @returns {CheckResult}
 */
function checkSource(absFile, sourceText, terminalRoot) {
  const sourceFile = parseSource(absFile, sourceText);
  /** @type {ParseError[]} */
  const parseErrors = listParseErrors(sourceFile).map(({ pos, code, message }) => ({
    file: absFile,
    ...lineCol(sourceText, pos),
    code,
    message,
  }));
  /** @type {Violation[]} */
  const violations = [];
  for (const ref of listSpecifiers(sourceFile)) {
    const { line, column } = lineCol(sourceText, ref.pos);
    const base = { file: absFile, line, column, specifier: ref.specifier, kind: ref.kind };
    if (ref.call !== undefined) {
      const resolved = ref.specifier === "" ? MISSING_ARGUMENT_NOTE : NON_LITERAL_NOTE;
      violations.push({ ...base, call: ref.call, resolved, reason: REASON_NON_LITERAL });
    } else if (ref.kind === "reference-path" || isRelativeSpecifier(ref.specifier)) {
      // A reference path is file-relative even without a `./` prefix, so it resolves like one.
      const resolved = path.resolve(path.dirname(absFile), ref.specifier);
      if (!isInsideTerminal(resolved, terminalRoot)) {
        violations.push({ ...base, resolved, reason: REASON_OUTSIDE });
      }
    } else if (!isAllowedPackage(ref.specifier)) {
      // Aliases such as `@/…` need no tsconfig resolution: every alias is rejected here.
      violations.push({ ...base, resolved: NON_RELATIVE_NOTE, reason: REASON_PACKAGE });
    }
  }
  return { violations, parseErrors };
}

/**
 * Recursive walk, sorted by name at every level so output order is deterministic.
 * @param {string} dir
 * @returns {string[]} absolute paths of regular files with a SOURCE_EXTENSIONS extension
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
    } else if (entry.isFile() && isSourceFileName(entry.name)) {
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
 * Writes ASCII control characters as `\u00XX` (a tab as `\u0009`, a newline as `\u000a`), so a
 * specifier holding one still prints as one line and cannot be mistaken for a backslash
 * sequence. Every other character prints as written.
 * @param {string} text
 * @returns {string}
 */
function escapeControlCharacters(text) {
  let out = "";
  for (const ch of text) {
    const code = ch.charCodeAt(0);
    out += code < 0x20 || code === 0x7f ? `\\u${code.toString(16).padStart(4, "0")}` : ch;
  }
  return out;
}

/**
 * @param {Violation} v
 * @returns {string} `<file>:<line>:<col>  import "<spec>" -> <resolved>  [<reason>]`; a
 *   non-literal specifier prints as its call, `<file>:<line>:<col>  import(<expr>) -> …`
 */
function formatViolation(v) {
  const label = v.kind === "import" ? "import" : v.kind.replace("-", " ");
  const resolved = v.reason === REASON_OUTSIDE ? displayPath(v.resolved) : v.resolved;
  const location = `${displayPath(v.file)}:${v.line}:${v.column}`;
  const specifier = escapeControlCharacters(v.specifier);
  // The argument is source text, not a string, so it is shown inside its call instead of quotes.
  if (v.call !== undefined) {
    return `${location}  ${v.call}(${specifier}) -> ${resolved}  [${v.reason}]`;
  }
  return `${location}  ${label} "${specifier}" -> ${resolved}  [${v.reason}]`;
}

/**
 * @param {ParseError} e
 * @returns {string} `<file>:<line>:<col>  cannot parse: TS<code> <message>`
 */
function formatParseError(e) {
  return `${displayPath(e.file)}:${e.line}:${e.column}  cannot parse: ${e.message}`;
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
  /** @type {ParseError[]} */
  const parseErrors = [];
  for (const file of files) {
    const result = checkSource(file, fs.readFileSync(file, "utf8"), TERMINAL_ROOT);
    violations.push(...result.violations);
    parseErrors.push(...result.parseErrors);
  }

  for (const v of violations) console.error(formatViolation(v));
  for (const e of parseErrors) console.error(formatParseError(e));
  if (violations.length > 0) {
    const fileCount = new Set(violations.map((v) => v.file)).size;
    console.error(
      `check-terminal-boundary: ${violations.length} violation(s) in ${fileCount} file(s)`,
    );
  }
  if (parseErrors.length > 0) {
    // Code 2 (the check could not run completely) outranks 1: an unparsed file may hide imports.
    const fileCount = new Set(parseErrors.map((e) => e.file)).size;
    console.error(
      `check-terminal-boundary: ${fileCount} file(s) cannot be parsed ` +
        `(${parseErrors.length} error(s)); their imports may be incomplete`,
    );
    return 2;
  }
  if (violations.length > 0) return 1;

  console.log(`check-terminal-boundary: OK (${files.length} files in ${TERMINAL_REL} checked)`);
  return 0;
}


/**
 * @typedef {{
 *   name: string,
 *   file: string,
 *   source: string,
 *   expect: "reject" | "accept" | "unparseable",
 *   specifier?: string,
 *   reason?: string,
 *   diagnostic?: number,
 * }} SelfTestCase
 * `file` is relative to the widget folder and becomes a virtual path under it. An accept case
 * must give no violation and no parse error; a reject case names the specifier (and reason) that
 * must be reported from a source that parses; an unparseable case names the TypeScript
 * diagnostic code that must be reported.
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
    name: "dynamic import() of a variable",
    file: "X.tsx",
    source: "export async function load(m: string) {\n  return import(m);\n}\n",
    expect: "reject",
    specifier: "m",
    reason: REASON_NON_LITERAL,
  },
  {
    name: "dynamic import() of a template with a substitution",
    file: "X.tsx",
    source: "export async function load(x: string) {\n  return import(`next/${x}`);\n}\n",
    expect: "reject",
    specifier: "`next/${x}`",
    reason: REASON_NON_LITERAL,
  },
  {
    name: "require() of a variable",
    file: "lib/x.ts",
    source: "export function load(x: string) {\n  return require(x);\n}\n",
    expect: "reject",
    specifier: "x",
    reason: REASON_NON_LITERAL,
  },
  {
    name: "import() with no argument",
    file: "X.tsx",
    source: "export const load = () => import();\n",
    expect: "reject",
    specifier: "",
    reason: REASON_NON_LITERAL,
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
    name: "allowlisted package climbing into host pages with ..",
    file: "X.tsx",
    source: 'import Page from "recharts/../../src/app/page";\n',
    expect: "reject",
    specifier: "recharts/../../src/app/page",
    reason: REASON_PACKAGE,
  },
  {
    name: "allowlisted package climbing into a sibling package with ..",
    file: "X.tsx",
    source: 'import Link from "react/../next/link";\n',
    expect: "reject",
    specifier: "react/../next/link",
    reason: REASON_PACKAGE,
  },
  {
    name: "allowlisted package with a . segment",
    file: "X.tsx",
    source: 'import debounce from "react/./../lodash";\n',
    expect: "reject",
    specifier: "react/./../lodash",
    reason: REASON_PACKAGE,
  },
  {
    name: "allowlisted package with an empty segment",
    file: "X.tsx",
    source: 'import { jsx } from "react//jsx-runtime";\n',
    expect: "reject",
    specifier: "react//jsx-runtime",
    reason: REASON_PACKAGE,
  },
  {
    name: "allowlisted package with a backslash separator",
    file: "X.tsx",
    source: 'import Link from "react/..\\\\next";\n',
    expect: "reject",
    specifier: "react/..\\next",
    reason: REASON_PACKAGE,
  },
  {
    name: "allowlisted package with percent-encoded dot segments",
    file: "X.tsx",
    source: 'import Page from "recharts/%2e%2e/%2e%2e/src/app/page";\n',
    expect: "reject",
    specifier: "recharts/%2e%2e/%2e%2e/src/app/page",
    reason: REASON_PACKAGE,
  },
  {
    name: "allowlisted package with dot segments split by tabs",
    file: "X.tsx",
    source: 'import Page from "recharts/.\\t./.\\t./src/app/page";\n',
    expect: "reject",
    specifier: "recharts/.\t./.\t./src/app/page",
    reason: REASON_PACKAGE,
  },
  {
    name: "/// <reference types> climbing out of react",
    file: "types.ts",
    source: '/// <reference types="react/../node" />\nexport {};\n',
    expect: "reject",
    specifier: "react/../node",
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
    name: "react/ with a trailing slash",
    file: "X.tsx",
    source: 'import React from "react/";\n',
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
  {
    name: "regex literal that only mentions a host import",
    file: "lib/x.ts",
    source: 'export const importPattern = /import "next\\/link"/;\n',
    expect: "accept",
  },
  {
    name: "JSX text that only mentions host imports",
    file: "X.tsx",
    source:
      'export const ImportSample = () => <p>import Link from "next/link"; require("next")</p>;\n',
    expect: "accept",
  },

  // Every module extension beyond .ts and .tsx; the walk collects each, so each must be checked.
  ...[".js", ".jsx", ".mts", ".cts", ".mjs", ".cjs"].map(
    (ext) =>
      /** @type {SelfTestCase} */ ({
        name: `next/link in a ${ext} file`,
        file: `X${ext}`,
        source: 'import "next/link";\n',
        expect: "reject",
        specifier: "next/link",
        reason: REASON_PACKAGE,
      }),
  ),
  {
    name: "JavaScript helper re-exporting host code",
    file: "lib/helper.js",
    source: 'export { HOST_SECRET } from "../../app/hostSecret";\n',
    expect: "reject",
    specifier: "../../app/hostSecret",
    reason: REASON_OUTSIDE,
  },
  {
    name: "module.require() in a CommonJS file (listed by preProcessFile only)",
    file: "lib/x.cjs",
    source: 'module.exports = module.require("next/router");\n',
    expect: "reject",
    specifier: "next/router",
    reason: REASON_PACKAGE,
  },
  {
    name: "AMD define() dependency (listed by preProcessFile only)",
    file: "lib/x.js",
    source: 'define(["next/link"], function () {});\n',
    expect: "reject",
    specifier: "next/link",
    reason: REASON_PACKAGE,
  },
  {
    name: "AMD define() dependency after a module name (listed by preProcessFile only)",
    file: "lib/x.js",
    source: 'define("widget", ["next/link"], function () {});\n',
    expect: "reject",
    specifier: "next/link",
    reason: REASON_PACKAGE,
  },
  {
    name: "new require() (listed by preProcessFile only)",
    file: "lib/x.js",
    source: 'export const head = new require("next/head");\n',
    expect: "reject",
    specifier: "next/head",
    reason: REASON_PACKAGE,
  },
  // Wrapped specifiers: preProcessFile lists the string token, whatever expression wraps it.
  {
    name: "module.require() of a host package behind an as assertion",
    file: "lib/x.ts",
    source: 'export const link = module.require("next/link" as string);\n',
    expect: "reject",
    specifier: "next/link",
    reason: REASON_PACKAGE,
  },
  {
    name: "module.require() of a standalone page behind an as assertion",
    file: "lib/x.ts",
    source: 'export const page = module.require("../../app/page" as string);\n',
    expect: "reject",
    specifier: "../../app/page",
    reason: REASON_OUTSIDE,
  },
  {
    name: "new require() of a host package behind an as assertion",
    file: "lib/x.ts",
    source: 'export const link = new require("next/link" as string);\n',
    expect: "reject",
    specifier: "next/link",
    reason: REASON_PACKAGE,
  },
  {
    name: "module.require() of a host package behind a non-null assertion",
    file: "lib/x.ts",
    source: 'export const link = module.require("next/link"!);\n',
    expect: "reject",
    specifier: "next/link",
    reason: REASON_PACKAGE,
  },
  {
    name: "AMD define() dependency array behind an as assertion",
    file: "lib/x.ts",
    source: 'define(["next/link"] as string[], function () {});\n',
    expect: "reject",
    specifier: "next/link",
    reason: REASON_PACKAGE,
  },
  {
    name: "AMD define() dependency array behind satisfies",
    file: "lib/x.ts",
    source: 'define(["next/link"] satisfies string[], function () {});\n',
    expect: "reject",
    specifier: "next/link",
    reason: REASON_PACKAGE,
  },
  {
    name: "JSX component in a .jsx file",
    file: "X.jsx",
    source:
      'import { useState } from "react";\n' +
      "export const Counter = () => <div>{String(useState)}</div>;\n",
    expect: "accept",
  },

  // Sources TypeScript cannot parse. Each may hide imports, so a run must not report them clean.
  {
    name: "merge-conflict markers hiding a host import",
    file: "X.ts",
    source:
      "<<<<<<< HEAD\n" +
      'import { useState } from "react";\n' +
      "=======\n" +
      'import Link from "next/link";\n' +
      ">>>>>>> branch\n" +
      "export {};\n",
    expect: "unparseable",
    diagnostic: 1185,
  },
  {
    name: "binary-looking file hiding a host import",
    file: "X.ts",
    source: '\uFFFD\uFFFD\u0000garbage\u0000\nimport Link from "next/link";\n',
    expect: "unparseable",
    diagnostic: 1490,
  },
  {
    name: "malformed object literal",
    file: "X.ts",
    source: "export const x = {;\n",
    expect: "unparseable",
    diagnostic: 1136,
  },
  {
    name: "unterminated template literal",
    file: "X.ts",
    source: "export const x = `abc\n",
    expect: "unparseable",
    diagnostic: 1160,
  },
  {
    name: "unclosed block comment hiding a host import",
    file: "X.ts",
    source: '/* unclosed\nimport Link from "next/link";\n',
    expect: "unparseable",
    diagnostic: 1010,
  },
  {
    name: "type annotation in a .js file",
    file: "X.js",
    source: "export const x: number = 1;\n",
    expect: "unparseable",
    diagnostic: 8010,
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
    // A case under a name the walk skips would pass here while real files like it go unchecked.
    const collected = isSourceFileName(virtualFile);
    const { violations, parseErrors } = checkSource(virtualFile, testCase.source, TERMINAL_ROOT);
    let ok = false;
    if (testCase.expect === "accept") {
      ok = violations.length === 0 && parseErrors.length === 0;
    } else if (testCase.expect === "reject") {
      ok =
        parseErrors.length === 0 &&
        violations.some(
          (v) =>
            v.specifier === testCase.specifier &&
            (testCase.reason === undefined || v.reason === testCase.reason),
        );
    } else {
      ok = parseErrors.some((e) => e.code === testCase.diagnostic);
    }
    ok = ok && collected;
    if (ok) {
      passed += 1;
      console.log(`PASS ${testCase.name}`);
    } else if (!collected) {
      console.log(`FAIL ${testCase.name} (${testCase.file} is not a file the check collects)`);
    } else {
      const found =
        violations.map((v) => `"${v.specifier}" [${v.reason}]`).join(", ") || "none";
      const errors = parseErrors.map((e) => `TS${e.code}`).join(", ") || "none";
      const got =
        `got ${violations.length} violations: ${found}; ` +
        `${parseErrors.length} parse errors: ${errors}`;
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
  "       node scripts/check-terminal-boundary.mjs --help       print this help (or -h)",
].join("\n");

/** The header comment's account of the check, printed by `--help` and `-h`. */
const HELP = [
  "check-terminal-boundary: import boundary of the embeddable widget folder",
  "",
  USAGE,
  "",
  `Checks every ${SOURCE_EXTENSIONS.join(", ")} file in ${TERMINAL_REL}/,`,
  "at any depth, on resolved paths rather than specifier patterns:",
  `  - a relative specifier passes only if it resolves inside ${TERMINAL_REL}/;`,
  `  - a non-relative specifier passes only if it is ${ALLOWED_PACKAGES.join(", ")} or a subpath`,
  "    of one of them (react/jsx-runtime) with no '.', '..' or empty segment;",
  "  - import() and require() must take a string literal.",
  "A violation prints <file>:<line>:<col>, the specifier, the resolved path and its reason",
  `(${REASON_OUTSIDE}, ${REASON_PACKAGE} or ${REASON_NON_LITERAL}). A file TypeScript`,
  "cannot parse prints <file>:<line>:<col>  cannot parse: <diagnostic>, as it may hide imports.",
  "",
  "Exit codes:",
  "  0  no violation (or every self-test case passed, or help was printed)",
  "  1  at least one violation (or a self-test case failed)",
  "  2  usage error; the folder is missing or holds no source files; a file cannot be read or",
  "     cannot be parsed (2 outranks 1, because the check could not run completely)",
  "",
  "`npm run lint` runs `eslint .`, then this script, then this script with --self-test.",
].join("\n");

/**
 * @param {string[]} args
 * @returns {number} exit code
 */
function main(args) {
  if (args.length === 0) return runCheck();
  if (args.length === 1 && args[0] === "--self-test") return runSelfTest();
  if (args.length === 1 && (args[0] === "--help" || args[0] === "-h")) {
    console.log(HELP);
    return 0;
  }
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
