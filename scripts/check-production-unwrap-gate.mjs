import { readFile } from "node:fs/promises";

const workflowsDirectory = new URL("../.github/workflows/", import.meta.url);
const requiredChecks = [
  { workflow: "ci.yml" },
  { workflow: "release-qualification.yml" },
];
const commands = [
  {
    command:
      "cargo clippy --locked --workspace --lib --all-features -- -D warnings -D clippy::unwrap_used",
    scope: "library",
  },
  {
    command:
      "cargo clippy --locked --workspace --bins --all-features -- -D warnings -D clippy::unwrap_used",
    scope: "binary",
  },
];

const missing = [];
for (const { workflow } of requiredChecks) {
  const source = await readFile(new URL(workflow, workflowsDirectory), "utf8");
  for (const { command, scope } of commands) {
    if (!source.includes(command)) {
      missing.push(`${workflow}: missing workspace all-feature ${scope} unwrap ban`);
    }
  }
}

if (missing.length > 0) {
  throw new Error(`production unwrap quality gates are missing:\n${missing.join("\n")}`);
}

console.log(
  `checked ${requiredChecks.length} workspace all-feature production unwrap quality gate sets`,
);
