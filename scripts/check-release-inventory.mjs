import { execFileSync } from "node:child_process";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

const inventoryPath = resolve(process.argv[2] ?? "docs/release-inventory.html");
const document = await readFile(inventoryPath, "utf8");
const match = document.match(
  /<script id="workspace-release-inventory" type="application\/json">\s*([\s\S]*?)\s*<\/script>/,
);

if (!match) {
  throw new Error(`${inventoryPath}: workspace release inventory JSON was not found`);
}

let inventory;
try {
  inventory = JSON.parse(match[1]);
} catch (error) {
  throw new Error(`${inventoryPath}: invalid workspace release inventory JSON`, {
    cause: error,
  });
}

if (inventory.schema !== 1 || !Array.isArray(inventory.packages)) {
  throw new Error(`${inventoryPath}: unsupported workspace release inventory schema`);
}

const metadata = JSON.parse(
  execFileSync("cargo", ["metadata", "--no-deps", "--format-version=1", "--offline"], {
    encoding: "utf8",
  }),
);
const licenseArtifacts = await Promise.all([
  readFile(resolve(metadata.workspace_root, "LICENSE-APACHE"), "utf8"),
  readFile(resolve(metadata.workspace_root, "LICENSE-MIT"), "utf8"),
]);
const inventoryByName = new Map();
const failures = [];

if (!licenseArtifacts[0].includes("Apache License") || !licenseArtifacts[0].includes("Version 2.0")) {
  failures.push("LICENSE-APACHE must contain the Apache License, Version 2.0 text");
}
if (!licenseArtifacts[1].includes("MIT License")) {
  failures.push("LICENSE-MIT must contain the MIT License text");
}

for (const entry of inventory.packages) {
  if (
    typeof entry?.name !== "string" ||
    !["candidate", "workspace-only"].includes(entry.intent) ||
    typeof entry.track !== "string"
  ) {
    failures.push(`invalid inventory entry: ${JSON.stringify(entry)}`);
    continue;
  }
  if (inventoryByName.has(entry.name)) {
    failures.push(`duplicate inventory entry: ${entry.name}`);
    continue;
  }
  inventoryByName.set(entry.name, entry);
}

const metadataByName = new Map(metadata.packages.map((pkg) => [pkg.name, pkg]));

for (const name of inventoryByName.keys()) {
  if (!metadataByName.has(name)) {
    failures.push(`inventory contains non-workspace package: ${name}`);
  }
}
for (const name of metadataByName.keys()) {
  if (!inventoryByName.has(name)) {
    failures.push(`workspace package missing from inventory: ${name}`);
  }
}

for (const [name, pkg] of metadataByName) {
  const entry = inventoryByName.get(name);
  if (!entry) {
    continue;
  }

  if (entry.intent === "workspace-only") {
    if (!Array.isArray(pkg.publish) || pkg.publish.length !== 0) {
      failures.push(`${name}: workspace-only package must set publish = false`);
    }
    continue;
  }

  if (pkg.publish !== null) {
    failures.push(`${name}: publish candidate must not restrict or disable Cargo publication without inventory approval`);
  }
  if (!pkg.description?.trim()) {
    failures.push(`${name}: publish candidate needs a package description`);
  }
  if (!pkg.license?.trim()) {
    failures.push(`${name}: publish candidate needs a license expression`);
  }
  if (!pkg.repository?.startsWith("https://")) {
    failures.push(`${name}: publish candidate needs an HTTPS repository URL`);
  }
  if (pkg.edition !== "2024" || pkg.rust_version !== "1.94.1") {
    failures.push(`${name}: package metadata must retain the documented Rust 2024 / MSRV 1.94.1 baseline`);
  }
  if (!pkg.targets.some((target) => target.kind.includes("lib") || target.kind.includes("proc-macro"))) {
    failures.push(`${name}: publish candidate must expose a library or procedural macro target`);
  }
  for (const dependency of pkg.dependencies) {
    if (dependency.path && dependency.req === "*") {
      failures.push(`${name}: local dependency ${dependency.name} must declare a package version requirement`);
    }
  }
}

if (failures.length > 0) {
  console.error(failures.join("\n"));
  process.exit(1);
}

const candidates = inventory.packages.filter((entry) => entry.intent === "candidate").length;
const workspaceOnly = inventory.packages.length - candidates;
console.log(
  `checked ${inventory.packages.length} workspace packages: ${candidates} publish candidates, ${workspaceOnly} workspace-only`,
);
