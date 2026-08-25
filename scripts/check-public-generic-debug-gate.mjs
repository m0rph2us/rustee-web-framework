import { readdir, readFile } from "node:fs/promises";
import { join, resolve } from "node:path";

const command = "node scripts/check-public-generic-debug-gate.mjs";
const workflows = ["ci.yml", "release-qualification.yml"];
const publicGenericDebugDerive =
  /#\s*\[\s*derive\s*\((?:(?!\)\s*\]).)*\bDebug\b(?:(?!\)\s*\]).)*\)\s*\]\s*(?:#\s*\[[^\]]*\]\s*)*pub\s+(?:struct|enum)\s+[A-Za-z_][A-Za-z0-9_]*\s*</gs;

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
    "usage: node scripts/check-public-generic-debug-gate.mjs [workspace-root] | --self-test",
  );
}

function publicGenericDebugDeriveLines(source) {
  return [...source.matchAll(publicGenericDebugDerive)].map(
    (match) => source.slice(0, match.index).split("\n").length,
  );
}

function workflowViolations(workflowSources) {
  const violations = [];
  for (const workflow of workflows) {
    if (!workflowSources.get(workflow)?.includes(command)) {
      violations.push(`${workflow}: missing public generic Debug quality gate`);
    }
  }
  return violations;
}

function runSelfTest() {
  const allowed = [
    "#[derive(Debug)]\npub struct NonGeneric;",
    "#[derive(Clone)]\npub struct WithoutDebug<T>(T);",
    "#[derive(Debug)]\npub(crate) struct Internal<T>(T);",
    "pub struct Explicit<T>(T);\nimpl<T> std::fmt::Debug for Explicit<T> {\n    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {\n        formatter.finish_non_exhaustive()\n    }\n}",
  ];
  for (const source of allowed) {
    if (publicGenericDebugDeriveLines(source).length > 0) {
      throw new Error(`safe declaration was rejected: ${source}`);
    }
  }

  const rejected = [
    "#[derive(Debug)]\npub struct Leaky<T>(T);",
    "#[derive(Clone, Debug)]\n#[repr(transparent)]\npub enum Leaky<T> { Value(T) }",
    "#[derive(\n    Debug,\n    Clone,\n)]\npub struct Leaky<T>(T);",
  ];
  for (const source of rejected) {
    if (publicGenericDebugDeriveLines(source).length !== 1) {
      throw new Error(`public generic Debug derive was accepted: ${source}`);
    }
  }

  const validWorkflows = new Map(workflows.map((workflow) => [workflow, `- run: ${command}`]));
  if (workflowViolations(validWorkflows).length > 0) {
    throw new Error("valid workflow gates were rejected");
  }
  validWorkflows.delete("ci.yml");
  if (
    !workflowViolations(validWorkflows).includes(
      "ci.yml: missing public generic Debug quality gate",
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

  console.log("public generic Debug gate self-test OK");
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
  for (const line of publicGenericDebugDeriveLines(source)) {
    violations.push(
      `${file}:${line}: public generic types must implement content-safe Debug explicitly`,
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
  throw new Error(`public generic Debug quality gate failed:\n${violations.join("\n")}`);
}

console.log(
  `checked ${sourceFiles.length} Rust sources and ${workflows.length} workflow gates: public generic Debug is explicit`,
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
