import { readFile } from "node:fs/promises";

const workflowsDirectory = new URL("../.github/workflows/", import.meta.url);
const requiredChecks = [
  {
    workflow: "ci.yml",
    description: "workspace all-feature documentation",
    pattern:
      /RUSTDOCFLAGS="-D warnings -D missing_docs"\s+cargo doc --locked --workspace --all-features --no-deps/u,
  },
  {
    workflow: "release-qualification.yml",
    description: "release workspace all-feature documentation",
    pattern:
      /RUSTDOCFLAGS="-D warnings -D missing_docs"\s+cargo doc --locked --workspace --all-features --no-deps/u,
  },
];

const workflows = new Map();
for (const { workflow } of requiredChecks) {
  if (!workflows.has(workflow)) {
    workflows.set(workflow, await readFile(new URL(workflow, workflowsDirectory), "utf8"));
  }
}

const missing = requiredChecks.filter(({ workflow, pattern }) => !pattern.test(workflows.get(workflow)));
if (missing.length > 0) {
  throw new Error(
    `workflow documentation quality gates are missing:\n${missing
      .map(({ workflow, description }) => `${workflow}: ${description}`)
      .join("\n")}`,
  );
}

console.log(`checked ${requiredChecks.length} Rustdoc warning and public-documentation quality gates`);
