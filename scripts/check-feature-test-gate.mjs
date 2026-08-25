import { readFile } from "node:fs/promises";

const workflowsDirectory = new URL("../.github/workflows/", import.meta.url);
const requiredChecks = [
  {
    workflow: "ci.yml",
    description: "workspace all-feature tests, doctests, and independent facade feature tests",
  },
  {
    workflow: "release-qualification.yml",
    description: "release workspace all-feature tests, doctests, and independent facade feature tests",
  },
];
const commands = [
  "cargo test --locked --workspace --all-features --all-targets",
  "cargo test --locked -p rustee --no-default-features --features macros",
  "cargo test --locked -p rustee --no-default-features --features openapi",
  "cargo test --locked --doc --workspace --all-features",
];

const missing = [];
for (const { workflow, description } of requiredChecks) {
  const source = await readFile(new URL(workflow, workflowsDirectory), "utf8");
  if (!commands.every((command) => source.includes(command))) {
    missing.push(`${workflow}: missing ${description}`);
  }
}

if (missing.length > 0) {
  throw new Error(`feature test quality gates are missing:\n${missing.join("\n")}`);
}

console.log(`checked ${requiredChecks.length} workspace feature test quality gates`);
