import { execFileSync } from "node:child_process";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { readWorkspaceReleaseInventory } from "./workspace-release-inventory.mjs";

const REQUIRED_LICENSE_EXPRESSION = "MIT OR Apache-2.0";

function parseCommandArguments(commandArguments) {
  const selfTestCount = commandArguments.filter((argument) => argument === "--self-test").length;
  const selfTest = selfTestCount === 1;
  const unsupportedOptions = commandArguments.filter(
    (argument) => argument.startsWith("-") && argument !== "--self-test",
  );
  const inventoryArguments = commandArguments.filter((argument) => !argument.startsWith("-"));

  if (
    selfTestCount > 1 ||
    unsupportedOptions.length > 0 ||
    inventoryArguments.length > 1 ||
    (selfTest && inventoryArguments.length > 0)
  ) {
    throw new Error(
      "usage: node scripts/check-release-inventory.mjs [inventory.html] | --self-test",
    );
  }

  return { selfTest, inventoryArguments };
}

function localDependencyFailures(packageName, dependencies, inventoryByName) {
  const failures = [];

  for (const dependency of dependencies) {
    if (dependency.path && dependency.req === "*") {
      failures.push(`${packageName}: local dependency ${dependency.name} must declare a package version requirement`);
    }
    if (!dependency.path || dependency.kind === "dev") {
      continue;
    }

    const dependencyEntry = inventoryByName.get(dependency.name);
    if (dependencyEntry?.intent === "workspace-only") {
      failures.push(
        `${packageName}: publish candidate cannot depend on workspace-only package ${dependency.name} outside dev-dependencies`,
      );
    }
  }

  return failures;
}

function requiredLicenseExpressionFailure(packageName, license) {
  if (license !== REQUIRED_LICENSE_EXPRESSION) {
    return `${packageName}: publish candidate must use ${REQUIRED_LICENSE_EXPRESSION} as its SPDX license expression`;
  }
  return undefined;
}

function runSelfTest() {
  const defaultArguments = parseCommandArguments([]);
  const selfTestArguments = parseCommandArguments(["--self-test"]);
  if (
    defaultArguments.selfTest ||
    defaultArguments.inventoryArguments.length > 0 ||
    !selfTestArguments.selfTest
  ) {
    throw new Error("valid command-line arguments were not parsed as expected");
  }

  for (const invalidArguments of [
    ["--unknown"],
    ["--self-test", "--self-test"],
    ["first-inventory.html", "second-inventory.html"],
    ["--self-test", "inventory.html"],
  ]) {
    try {
      parseCommandArguments(invalidArguments);
      throw new Error(`${invalidArguments.join(" ")}: accepted invalid command-line arguments`);
    } catch (error) {
      if (!String(error).includes("usage:")) {
        throw error;
      }
    }
  }

  const inventoryByName = new Map([
    ["candidate", { intent: "candidate" }],
    ["workspace-only", { intent: "workspace-only" }],
  ]);
  const workspaceOnlyDependency = {
    name: "workspace-only",
    path: "/fixture/workspace-only",
    req: "^0.1.0",
  };
  const cases = [
    {
      name: "normal dependency",
      dependencies: [{ ...workspaceOnlyDependency, kind: null }],
      expected: [
        "candidate: publish candidate cannot depend on workspace-only package workspace-only outside dev-dependencies",
      ],
    },
    {
      name: "build dependency",
      dependencies: [{ ...workspaceOnlyDependency, kind: "build" }],
      expected: [
        "candidate: publish candidate cannot depend on workspace-only package workspace-only outside dev-dependencies",
      ],
    },
    {
      name: "dev dependency",
      dependencies: [{ ...workspaceOnlyDependency, kind: "dev" }],
      expected: [],
    },
    {
      name: "unversioned local dependency",
      dependencies: [{ name: "candidate", path: "/fixture/candidate", req: "*", kind: null }],
      expected: ["candidate: local dependency candidate must declare a package version requirement"],
    },
  ];

  for (const testCase of cases) {
    const actual = localDependencyFailures("candidate", testCase.dependencies, inventoryByName);
    if (JSON.stringify(actual) !== JSON.stringify(testCase.expected)) {
      throw new Error(`${testCase.name}: expected ${JSON.stringify(testCase.expected)}, got ${JSON.stringify(actual)}`);
    }
  }

  if (requiredLicenseExpressionFailure("candidate", REQUIRED_LICENSE_EXPRESSION)) {
    throw new Error("the required license expression was rejected");
  }
  if (!requiredLicenseExpressionFailure("candidate", "MIT")) {
    throw new Error("a different license expression was accepted");
  }

  console.log("release inventory dependency, license, and command-line boundaries OK");
}

const { selfTest, inventoryArguments } = parseCommandArguments(process.argv.slice(2));

if (selfTest) {
  runSelfTest();
  process.exit(0);
}

const inventoryPath = resolve(inventoryArguments[0] ?? "docs/release-inventory.html");
const metadata = JSON.parse(
  execFileSync(
    "cargo",
    ["metadata", "--no-deps", "--format-version=1", "--locked", "--offline"],
    { encoding: "utf8" },
  ),
);
const inventory = await readWorkspaceReleaseInventory(inventoryPath, metadata.packages);
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
  const licenseExpressionFailure = requiredLicenseExpressionFailure(name, pkg.license);
  if (licenseExpressionFailure) {
    failures.push(licenseExpressionFailure);
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
  failures.push(...localDependencyFailures(name, pkg.dependencies, inventoryByName));
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
