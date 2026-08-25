import { readdir, readFile } from "node:fs/promises";
import { join, resolve } from "node:path";

const redirectPolicy = ".redirect(reqwest::redirect::Policy::none())";
const clientBuilderPattern = /(?<![A-Za-z0-9_])(?:reqwest::)?Client::builder\(\)/g;
const clientNewPattern = /(?<![A-Za-z0-9_])(?:reqwest::)?Client::new\(\)/g;
const clientAliasPattern =
  /\b(?:pub\s+)?use\s+reqwest::(?:Client\s+as\s+[A-Za-z_][A-Za-z0-9_]*|\{(?:(?!\}).)*\bClient\s+as\s+[A-Za-z_][A-Za-z0-9_]*(?:(?!\}).)*\})\s*;/gs;
const reqwestModuleAliasPattern =
  /\b(?:pub\s+)?use\s+reqwest\s+as\s+[A-Za-z_][A-Za-z0-9_]*\s*;/g;
const directClientTypeAliasPattern =
  /\b(?:pub\s+)?type\s+[A-Za-z_][A-Za-z0-9_]*\s*=\s*reqwest::Client\s*;/g;
const command = "node scripts/check-http-redirect-gate.mjs";
const workflows = ["ci.yml", "release-qualification.yml"];

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
    "usage: node scripts/check-http-redirect-gate.mjs [workspace-root] | --self-test",
  );
}

function sourceViolations(file, source) {
  let directClientCount = 0;
  const violations = [];
  const builderStarts = Array.from(source.matchAll(clientBuilderPattern), (match) =>
    match.index + match[0].lastIndexOf("Client::builder()"),
  );
  for (const [index, start] of builderStarts.entries()) {
    directClientCount += 1;
    const build = source.indexOf(".build()", start);
    const nextBuilder = builderStarts[index + 1];
    if (build === -1 || (nextBuilder !== undefined && nextBuilder < build)) {
      violations.push(`${file}: could not identify Client::builder() configuration`);
    } else if (!source.slice(start, build).includes(redirectPolicy)) {
      violations.push(`${file}: direct HTTP client must disable automatic redirects`);
    }
  }

  if (Array.from(source.matchAll(clientNewPattern)).length > 0) {
    violations.push(
      `${file}: direct reqwest Client::new() is forbidden because it enables automatic redirects`,
    );
  }

  if (Array.from(source.matchAll(clientAliasPattern)).length > 0) {
    violations.push(`${file}: reqwest Client aliases are forbidden because they bypass this gate`);
  }
  if (Array.from(source.matchAll(reqwestModuleAliasPattern)).length > 0) {
    violations.push(`${file}: reqwest module aliases are forbidden because they bypass this gate`);
  }
  if (Array.from(source.matchAll(directClientTypeAliasPattern)).length > 0) {
    violations.push(`${file}: reqwest Client type aliases are forbidden because they bypass this gate`);
  }

  return { directClientCount, violations };
}

function workflowViolations(workflowSources) {
  const violations = [];
  for (const workflow of workflows) {
    if (!workflowSources.get(workflow)?.includes(command)) {
      violations.push(`${workflow}: missing HTTP redirect quality gate`);
    }
  }
  return violations;
}

function runSelfTest() {
  const configured = sourceViolations(
    "configured.rs",
    "let _client = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build();",
  );
  if (configured.directClientCount !== 1 || configured.violations.length > 0) {
    throw new Error("configured direct HTTP client was rejected");
  }

  const rejectedSources = [
    ["missing redirect policy", "let _client = reqwest::Client::builder().build();"],
    ["Client::new", "let _client = reqwest::Client::new();"],
    ["Client alias", "use reqwest::Client as HttpClient;"],
    ["module alias", "use reqwest as http;"],
    ["type alias", "type HttpClient = reqwest::Client;"],
  ];
  for (const [name, source] of rejectedSources) {
    if (sourceViolations("rejected.rs", source).violations.length === 0) {
      throw new Error(`${name} bypass was accepted`);
    }
  }

  const validWorkflows = new Map(workflows.map((workflow) => [workflow, `- run: ${command}`]));
  if (workflowViolations(validWorkflows).length > 0) {
    throw new Error("valid workflow gates were rejected");
  }
  validWorkflows.delete("ci.yml");
  if (!workflowViolations(validWorkflows).includes("ci.yml: missing HTTP redirect quality gate")) {
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

  console.log("HTTP redirect gate self-test OK");
}

const { selfTest, workspaceRoot: workspaceRootArgument } = parseCommandArguments(
  process.argv.slice(2),
);

if (selfTest) {
  runSelfTest();
  process.exit(0);
}

const workspaceRoot = resolve(workspaceRootArgument);
const cratesRoot = join(workspaceRoot, "crates");
const sourceFiles = [];
await collectRustFiles(cratesRoot, sourceFiles);

let directClientCount = 0;
const violations = [];
for (const file of sourceFiles) {
  const source = await readFile(file, "utf8");
  const result = sourceViolations(file, source);
  directClientCount += result.directClientCount;
  violations.push(...result.violations);
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

if (directClientCount === 0) {
  violations.push("no direct reqwest Client::builder() calls were found");
}
if (violations.length > 0) {
  throw new Error(`HTTP redirect quality gate failed:\n${violations.join("\n")}`);
}

console.log(`checked ${directClientCount} direct HTTP clients and ${workflows.length} workflow gates`);

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
