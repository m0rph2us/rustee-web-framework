import { readdir, readFile } from "node:fs/promises";
import { join, resolve } from "node:path";

const command = "node scripts/check-public-wildcard-reexports.mjs";
const workflows = ["ci.yml", "release-qualification.yml"];
const publicUseStatement = /^\s*pub\s+use\b[\s\S]*?;/gm;

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
    "usage: node scripts/check-public-wildcard-reexports.mjs [workspace-root] | --self-test",
  );
}

function publicWildcardReexportLines(source) {
  const lines = [];
  for (const match of source.matchAll(publicUseStatement)) {
    const statement = match[0];
    const hasDirectGlob = /::\s*\*(?=\s*(?:[,;}]))/.test(statement);
    const hasGroupedGlob = /::\s*\{[\s\S]*?\*/.test(statement);
    if (hasDirectGlob || hasGroupedGlob) {
      lines.push(source.slice(0, match.index).split("\n").length);
    }
  }
  return lines;
}

function workflowViolations(workflowSources) {
  const violations = [];
  for (const workflow of workflows) {
    if (!workflowSources.get(workflow)?.includes(command)) {
      violations.push(`${workflow}: missing public wildcard re-export quality gate`);
    }
  }
  return violations;
}

function runSelfTest() {
  const allowed = [
    "pub use crate::model::Model;",
    "pub use crate::{Model, Error};",
    "pub(crate) use crate::*;",
    "use crate::*;",
  ];
  for (const source of allowed) {
    if (publicWildcardReexportLines(source).length > 0) {
      throw new Error(`safe re-export was rejected: ${source}`);
    }
  }

  const rejected = [
    "pub use crate::*;",
    "pub use crate::{Model, *};",
    "pub use crate::{model::*, Error};",
    "pub use crate::{\n    model::*,\n    Error,\n};",
  ];
  for (const source of rejected) {
    if (publicWildcardReexportLines(source).length !== 1) {
      throw new Error(`public wildcard re-export was accepted: ${source}`);
    }
  }

  const validWorkflows = new Map(workflows.map((workflow) => [workflow, `- run: ${command}`]));
  if (workflowViolations(validWorkflows).length > 0) {
    throw new Error("valid workflow gates were rejected");
  }
  validWorkflows.delete("ci.yml");
  if (
    !workflowViolations(validWorkflows).includes(
      "ci.yml: missing public wildcard re-export quality gate",
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

  console.log("public wildcard re-export gate self-test OK");
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
  for (const line of publicWildcardReexportLines(source)) {
    violations.push(`${file}:${line}: public wildcard re-exports are not allowed`);
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
  throw new Error(`public wildcard re-export quality gate failed:\n${violations.join("\n")}`);
}

console.log(
  `checked ${sourceFiles.length} Rust sources and ${workflows.length} workflow gates: public re-exports are explicit`,
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
