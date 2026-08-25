import { readdir, readFile } from "node:fs/promises";
import { join, resolve } from "node:path";

const command = "node scripts/check-public-error-debug-redaction-gate.mjs";
const workflows = ["ci.yml", "release-qualification.yml"];
const publicErrorDeclaration =
  /#\s*\[\s*derive\s*\((?:(?!\)\s*\]).)*\bthiserror::Error\b(?:(?!\)\s*\]).)*\)\s*\]\s*(?:#\s*\[[^\]]*\]\s*)*pub\s+(?:enum|struct)\s+([A-Za-z_][A-Za-z0-9_]*)/gs;
const sourceCarryingMember =
  /#\s*\[\s*(?:source|from)\b|#\s*\[\s*error\s*\(\s*transparent\s*\)\s*\]/;

function parseCommandArguments(commandArguments) {
  if (commandArguments.length === 0) {
    return { selfTest: false, workspaceRoot: "." };
  }
  if (commandArguments.length === 1 && commandArguments[0] === "--self-test") {
    return { selfTest: true, workspaceRoot: null };
  }
  if (commandArguments.length === 1 && !commandArguments[0].startsWith("-")) {
    return { selfTest: false, workspaceRoot: commandArguments[0] };
  }
  throw new Error(
    "usage: node scripts/check-public-error-debug-redaction-gate.mjs [workspace-root] | --self-test",
  );
}

function publicSourceErrorViolations(source) {
  const violations = [];
  for (const match of source.matchAll(publicErrorDeclaration)) {
    const name = match[1];
    const body = itemBody(source, match.index + match[0].length);
    if (body === null || !sourceCarryingMember.test(body)) {
      continue;
    }
    if (!hasExplicitDebugImplementation(source, name)) {
      violations.push({
        line: source.slice(0, match.index).split("\n").length,
        name,
      });
    }
  }
  return violations;
}

function itemBody(source, start) {
  let genericDepth = 0;
  let hasWhereClause = false;
  for (let index = start; index < source.length; index += 1) {
    const character = source[index];
    const next = source[index + 1];
    if (character === '"' || (character === "'" && isCharacterLiteralStart(source, index))) {
      index = skipQuotedLiteral(source, index, character);
    } else if (character === "/" && next === "/") {
      const lineEnd = source.indexOf("\n", index + 2);
      index = lineEnd === -1 ? source.length : lineEnd;
    } else if (character === "/" && next === "*") {
      const commentEnd = source.indexOf("*/", index + 2);
      index = commentEnd === -1 ? source.length : commentEnd + 1;
    } else if (character === "<") {
      genericDepth += 1;
    } else if (character === ">" && genericDepth > 0) {
      genericDepth -= 1;
    } else if (genericDepth === 0 && isWhereClauseAt(source, index)) {
      hasWhereClause = true;
      index += "where".length - 1;
    } else if (genericDepth === 0 && character === "{") {
      return delimitedBody(source, index, "{", "}");
    } else if (genericDepth === 0 && character === "(" && !hasWhereClause) {
      return delimitedBody(source, index, "(", ")");
    } else if (genericDepth === 0 && character === ";") {
      return null;
    }
  }
  return null;
}

function isWhereClauseAt(source, index) {
  return (
    source.startsWith("where", index) &&
    !/[A-Za-z0-9_]/.test(source[index - 1] ?? "") &&
    !/[A-Za-z0-9_]/.test(source[index + "where".length] ?? "")
  );
}

function isCharacterLiteralStart(source, index) {
  const next = source[index + 1];
  if (next === "\\") {
    return true;
  }
  const codePoint = source.codePointAt(index + 1);
  if (codePoint === undefined) {
    return false;
  }
  return source[index + 1 + String.fromCodePoint(codePoint).length] === "'";
}

function delimitedBody(source, opening, openingDelimiter, closingDelimiter) {
  let depth = 1;
  for (let index = opening + 1; index < source.length; index += 1) {
    const character = source[index];
    const next = source[index + 1];
    if (character === '"' || (character === "'" && isCharacterLiteralStart(source, index))) {
      index = skipQuotedLiteral(source, index, character);
    } else if (character === "/" && next === "/") {
      const lineEnd = source.indexOf("\n", index + 2);
      index = lineEnd === -1 ? source.length : lineEnd;
    } else if (character === "/" && next === "*") {
      const commentEnd = source.indexOf("*/", index + 2);
      index = commentEnd === -1 ? source.length : commentEnd + 1;
    } else if (character === openingDelimiter) {
      depth += 1;
    } else if (character === closingDelimiter) {
      depth -= 1;
      if (depth === 0) {
        return source.slice(opening + 1, index);
      }
    }
  }
  return null;
}

function skipQuotedLiteral(source, start, quote) {
  for (let index = start + 1; index < source.length; index += 1) {
    if (source[index] === "\\") {
      index += 1;
    } else if (source[index] === quote) {
      return index;
    }
  }
  return source.length;
}

function hasExplicitDebugImplementation(source, name) {
  const escapedName = name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const implementation = new RegExp(
    `impl(?:\\s*<[^>{}]*>)?\\s+(?:(?:std::)?fmt::)?Debug\\s+for\\s+${escapedName}(?:\\s*<[^>{}]*>)?`,
    "s",
  );
  return implementation.test(source);
}

function workflowViolations(workflowSources) {
  const violations = [];
  for (const workflow of workflows) {
    if (!workflowSources.get(workflow)?.includes(command)) {
      violations.push(`${workflow}: missing public source-error Debug quality gate`);
    }
  }
  return violations;
}

function runSelfTest() {
  const allowed = [
    "#[derive(thiserror::Error)]\npub enum LocalError { #[error(\"invalid input\")] InvalidInput }",
    "#[derive(thiserror::Error)]\npub enum SafeError<E> { #[error(\"adapter failed\")] Adapter(#[source] E) }\nimpl<E> std::fmt::Debug for SafeError<E> { fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { formatter.finish_non_exhaustive() } }",
  ];
  for (const source of allowed) {
    if (publicSourceErrorViolations(source).length > 0) {
      throw new Error(`safe declaration was rejected: ${source}`);
    }
  }

  const rejected = [
    "#[derive(thiserror::Error)]\npub enum LeakyError<E> { #[error(\"adapter failed\")] Adapter(#[source] E) }",
    "#[derive(thiserror::Error)]\npub struct LeakyError(#[from] std::io::Error);",
    "#[derive(thiserror::Error)]\npub struct LeakyError<T> where T: Fn() { #[source] source: std::io::Error, marker: T }",
    "#[derive(thiserror::Error)]\npub struct LeakyError<T> where T: std::error::Error + 'static { #[source] source: T }",
    "#[derive(thiserror::Error)]\npub enum LeakyError { #[error(transparent)] Adapter(std::io::Error) }",
  ];
  for (const source of rejected) {
    if (publicSourceErrorViolations(source).length !== 1) {
      throw new Error(`public source-carrying error was accepted: ${source}`);
    }
  }

  const validWorkflows = new Map(workflows.map((workflow) => [workflow, `- run: ${command}`]));
  if (workflowViolations(validWorkflows).length > 0) {
    throw new Error("valid workflow gates were rejected");
  }
  validWorkflows.delete("ci.yml");
  if (
    !workflowViolations(validWorkflows).includes(
      "ci.yml: missing public source-error Debug quality gate",
    )
  ) {
    throw new Error("missing workflow gate was accepted");
  }

  for (const invalidArguments of [["--unknown"], ["one", "two"]]) {
    try {
      parseCommandArguments(invalidArguments);
      throw new Error(`${invalidArguments.join(" ")}: accepted invalid command-line arguments`);
    } catch (error) {
      if (!String(error).includes("usage:")) {
        throw error;
      }
    }
  }

  console.log("public source-error Debug gate self-test OK");
}

const { selfTest, workspaceRoot: workspaceRootArgument } = parseCommandArguments(
  process.argv.slice(2),
);

if (selfTest) {
  runSelfTest();
  process.exit(0);
}

const workspaceRoot = resolve(workspaceRootArgument);
const sourceFiles = [];
await collectRustFiles(join(workspaceRoot, "crates"), sourceFiles);

const violations = [];
for (const file of sourceFiles) {
  const source = await readFile(file, "utf8");
  for (const { line, name } of publicSourceErrorViolations(source)) {
    violations.push(
      `${file}:${line}: public source-carrying error ${name} must implement content-safe Debug explicitly`,
    );
  }
}

const workflowSources = new Map(
  await Promise.all(
    workflows.map(async (workflow) => [
      workflow,
      await readFile(join(workspaceRoot, ".github", "workflows", workflow), "utf8"),
    ]),
  ),
);
violations.push(...workflowViolations(workflowSources));

if (violations.length > 0) {
  throw new Error(`public source-error Debug quality gate failed:\n${violations.join("\n")}`);
}

console.log(
  `checked ${sourceFiles.length} Rust sources and ${workflows.length} workflow gates: public source-carrying errors use explicit Debug`,
);

async function collectRustFiles(directory, files) {
  const entries = await readdir(directory, { withFileTypes: true });
  for (const entry of entries) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      await collectRustFiles(path, files);
    } else if (entry.isFile() && entry.name.endsWith(".rs")) {
      files.push(path);
    }
  }
}
