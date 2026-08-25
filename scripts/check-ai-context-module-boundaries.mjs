import { readFile, stat } from "node:fs/promises";
import { basename, dirname, join, resolve } from "node:path";

const workspaceRoot = resolve(process.argv[2] ?? ".");
const contextPath = join(workspaceRoot, "docs", "ai-context.html");
const qualityGateCommand = "node scripts/check-ai-context-module-boundaries.mjs";
const qualityGateWorkflows = [
  join(workspaceRoot, ".github", "workflows", "ci.yml"),
  join(workspaceRoot, ".github", "workflows", "release-qualification.yml"),
];
const modulePattern =
  /^(?:\s*#\[path\s*=\s*"([^"]+)"\]\s*\r?\n)?\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+([A-Za-z_][A-Za-z0-9_]*)\s*(;|\{)/gm;
const documentedBoundaryPattern =
  /<code>(rustee-[a-z0-9-]+(?:::[a-z_][a-z0-9_]*)*)<\/code>/g;

async function exists(path) {
  try {
    await stat(path);
    return true;
  } catch {
    return false;
  }
}

function childModuleCandidates(parentPath, name) {
  const fileName = basename(parentPath);
  const sourceDirectory =
    fileName === "lib.rs" || fileName === "main.rs" || fileName === "mod.rs"
      ? dirname(parentPath)
      : join(dirname(parentPath), fileName.slice(0, -3));

  return [join(sourceDirectory, `${name}.rs`), join(sourceDirectory, name, "mod.rs")];
}

async function childModulePath(parentPath, name) {
  for (const candidate of childModuleCandidates(parentPath, name)) {
    if (await exists(candidate)) {
      return candidate;
    }
  }
  return undefined;
}

async function explicitChildModulePath(parentPath, configuredPath) {
  const path = resolve(dirname(parentPath), configuredPath);
  return (await exists(path)) ? path : undefined;
}

async function collectDeclaredModules(crateName) {
  const rootPath = join(workspaceRoot, "crates", crateName, "src", "lib.rs");
  const declared = new Set([crateName]);
  const visited = new Set();

  async function visit(path, parentSegments) {
    if (visited.has(path)) {
      return;
    }
    visited.add(path);

    const source = await readFile(path, "utf8");
    for (const match of source.matchAll(modulePattern)) {
      const configuredPath = match[1];
      const name = match[2];
      const terminator = match[3];
      const segments = [...parentSegments, name];
      declared.add(`${crateName}::${segments.join("::")}`);

      if (terminator === ";") {
        const childPath = configuredPath
          ? await explicitChildModulePath(path, configuredPath)
          : await childModulePath(path, name);
        if (childPath) {
          await visit(childPath, segments);
        }
      }
    }
  }

  await visit(rootPath, []);
  return declared;
}

const html = await readFile(contextPath, "utf8");
const documentedBoundaries = [
  ...new Set([...html.matchAll(documentedBoundaryPattern)].map((match) => match[1])),
].sort();
const documentedBoundarySet = new Set(documentedBoundaries);
const documentedCrates = [...new Set(documentedBoundaries.map((boundary) => boundary.split("::")[0]))];
const declaredByCrate = new Map();
const missing = [];

for (const workflowPath of qualityGateWorkflows) {
  let workflow;
  try {
    workflow = await readFile(workflowPath, "utf8");
  } catch {
    missing.push(`${workflowPath}: AI context quality-gate workflow is missing`);
    continue;
  }
  if (!workflow.includes(qualityGateCommand)) {
    missing.push(`${workflowPath}: missing ${qualityGateCommand}`);
  }
}

for (const crateName of documentedCrates) {
  const rootPath = join(workspaceRoot, "crates", crateName, "src", "lib.rs");
  if (!(await exists(rootPath))) {
    missing.push(`${contextPath}: ${crateName} has no crate root`);
    continue;
  }
  declaredByCrate.set(crateName, await collectDeclaredModules(crateName));
}

for (const boundary of documentedBoundaries) {
  const crateName = boundary.split("::")[0];
  if (!declaredByCrate.get(crateName)?.has(boundary)) {
    missing.push(`${contextPath}: ${boundary} is not a declared Rust module boundary`);
  }
}

for (const [crateName, declared] of declaredByCrate) {
  for (const boundary of declared) {
    const segments = boundary.split("::");
    const moduleName = segments.at(-1);
    if (
      boundary !== crateName &&
      moduleName !== "tests" &&
      !documentedBoundarySet.has(boundary)
    ) {
      missing.push(`${contextPath}: ${boundary} is not documented for AI maintenance`);
    }
  }
}

if (missing.length > 0) {
  console.error(missing.join("\n"));
  process.exit(1);
}

console.log(
  `checked ${documentedBoundaries.length} AI context module boundaries and ${qualityGateWorkflows.length} workflow gates: source modules OK`,
);
