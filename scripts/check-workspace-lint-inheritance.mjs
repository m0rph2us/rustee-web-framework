import { execFile } from "node:child_process";
import { readFile } from "node:fs/promises";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const workflowsDirectory = new URL("../.github/workflows/", import.meta.url);
const command = "node scripts/check-workspace-lint-inheritance.mjs";
const requiredWorkflows = ["ci.yml", "release-qualification.yml"];
const requiredRootLintRules = [
  {
    section: "workspace.lints.rust",
    description: "forbidden unsafe code",
    pattern: /^unsafe_code\s*=\s*"forbid"(?:\s+#.*)?$/mu,
  },
  {
    section: "workspace.lints.rust",
    description: "missing public Debug implementations",
    pattern: /^missing_debug_implementations\s*=\s*"warn"(?:\s+#.*)?$/mu,
  },
  {
    section: "workspace.lints.clippy",
    description: "Clippy all warnings",
    pattern: /^all\s*=\s*\{\s*level\s*=\s*"warn",\s*priority\s*=\s*-1\s*\}(?:\s+#.*)?$/mu,
  },
  {
    section: "workspace.lints.clippy",
    description: "Clippy pedantic warnings",
    pattern: /^pedantic\s*=\s*\{\s*level\s*=\s*"warn",\s*priority\s*=\s*-1\s*\}(?:\s+#.*)?$/mu,
  },
];
const { stdout } = await execFileAsync(
  "cargo",
  ["metadata", "--locked", "--no-deps", "--format-version", "1"],
  { maxBuffer: 8 * 1024 * 1024 },
);
const metadata = JSON.parse(stdout);
const workspaceMembers = new Set(metadata.workspace_members);
const workspacePackages = metadata.packages.filter(({ id }) => workspaceMembers.has(id));

function sectionBody(manifest, name) {
  const escapedName = name.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
  return new RegExp(
    `^\\[${escapedName}\\][^\\S\\r\\n]*\\r?\\n([\\s\\S]*?)(?=^\\[[^\\r\\n]+\\]|(?![\\s\\S]))`,
    "mu",
  ).exec(manifest)?.[1];
}

function inheritsWorkspaceLints(manifest) {
  const lintsSection = sectionBody(manifest, "lints");
  return lintsSection !== undefined && /^workspace\s*=\s*true(?:\s+#.*)?$/mu.test(lintsSection);
}

const missing = [];
for (const { manifest_path: manifestPath, name } of workspacePackages) {
  const manifest = await readFile(manifestPath, "utf8");
  if (!inheritsWorkspaceLints(manifest)) {
    missing.push(`${name}: ${manifestPath}`);
  }
}

const rootManifest = await readFile(new URL("../Cargo.toml", import.meta.url), "utf8");
const missingRootLintRules = requiredRootLintRules
  .filter(({ section, pattern }) => !pattern.test(sectionBody(rootManifest, section) ?? ""))
  .map(({ description }) => description);

const missingWorkflows = [];
for (const workflow of requiredWorkflows) {
  const source = await readFile(new URL(workflow, workflowsDirectory), "utf8");
  if (!source.includes(command)) {
    missingWorkflows.push(workflow);
  }
}

if (missing.length > 0 || missingRootLintRules.length > 0 || missingWorkflows.length > 0) {
  const failures = [];
  if (missing.length > 0) {
    failures.push(`workspace packages must inherit the shared lint policy:\n${missing.join("\n")}`);
  }
  if (missingWorkflows.length > 0) {
    failures.push(`lint-inheritance gate is missing from workflows:\n${missingWorkflows.join("\n")}`);
  }
  if (missingRootLintRules.length > 0) {
    failures.push(`root lint policy is missing required rules:\n${missingRootLintRules.join("\n")}`);
  }
  throw new Error(failures.join("\n\n"));
}

console.log(
  `checked ${workspacePackages.length} workspace packages, ${requiredRootLintRules.length} root lint rules, and ${requiredWorkflows.length} workflows: shared lint policy inherited`,
);
