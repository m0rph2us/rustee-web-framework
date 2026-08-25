import { readFile } from "node:fs/promises";

const workflowsDirectory = new URL("../.github/workflows/", import.meta.url);
const requiredChecks = [
  { workflow: "ci.yml", description: "workspace all-feature Clippy" },
  {
    workflow: "release-qualification.yml",
    description: "release workspace all-feature Clippy",
  },
];
const command = "cargo clippy --locked --workspace --all-targets --all-features -- -D warnings";

const missing = [];
for (const { workflow, description } of requiredChecks) {
  const source = await readFile(new URL(workflow, workflowsDirectory), "utf8");
  if (!source.includes(command)) {
    missing.push(`${workflow}: missing ${description}`);
  }
}

if (missing.length > 0) {
  throw new Error(`feature Clippy quality gates are missing:\n${missing.join("\n")}`);
}

console.log(`checked ${requiredChecks.length} workspace all-feature Clippy quality gates`);
