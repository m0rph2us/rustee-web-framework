import { execFileSync } from "node:child_process";

const dependencyRules = new Map([
  ["rustee-json", []],
  ["rustee-observability-core", []],
  ["rustee-core", ["rustee-json"]],
  ["rustee-router", ["rustee-core"]],
  ["rustee-server", ["rustee-core", "rustee-router"]],
  ["rustee-middleware", ["rustee-core", "rustee-router"]],
  ["rustee", ["rustee-core", "rustee-macros", "rustee-openapi", "rustee-router", "rustee-server"]],
]);

function parseCommandArguments(commandArguments) {
  if (commandArguments.length === 0) {
    return { selfTest: false };
  }
  if (commandArguments.length === 1 && commandArguments[0] === "--self-test") {
    return { selfTest: true };
  }
  throw new Error("usage: node scripts/check-core-dependency-boundaries.mjs [--self-test]");
}

function workspaceRuntimeDependencies(pkg, workspacePackageNames) {
  return new Set(
    pkg.dependencies
      .filter(
        (dependency) =>
          dependency.path && dependency.kind !== "dev" && workspacePackageNames.has(dependency.name),
      )
      .map((dependency) => dependency.name),
  );
}

function dependencyFailures(packages) {
  const packagesByName = new Map(packages.map((pkg) => [pkg.name, pkg]));
  const workspacePackageNames = new Set(packagesByName.keys());
  const failures = [];

  for (const [name, allowedDependencies] of dependencyRules) {
    const pkg = packagesByName.get(name);
    if (!pkg) {
      failures.push(`${name}: required core package is missing from cargo metadata`);
      continue;
    }

    const actualDependencies = workspaceRuntimeDependencies(pkg, workspacePackageNames);
    const allowed = new Set(allowedDependencies);
    const unexpected = [...actualDependencies].filter((dependency) => !allowed.has(dependency)).sort();
    const missing = [...allowed].filter((dependency) => !actualDependencies.has(dependency)).sort();

    if (unexpected.length > 0) {
      failures.push(`${name}: unexpected workspace dependency: ${unexpected.join(", ")}`);
    }
    if (missing.length > 0) {
      failures.push(`${name}: required workspace dependency is missing: ${missing.join(", ")}`);
    }
  }

  return failures;
}

function fixturePackages() {
  const names = new Set(dependencyRules.keys());
  for (const dependencies of dependencyRules.values()) {
    for (const dependency of dependencies) {
      names.add(dependency);
    }
  }

  return [...names].map((name) => ({
    name,
    dependencies: (dependencyRules.get(name) ?? []).map((dependency) => ({
      name: dependency,
      path: `/fixture/${dependency}`,
      kind: null,
    })),
  }));
}

function runSelfTest() {
  if (dependencyFailures(fixturePackages()).length > 0) {
    throw new Error("valid core dependency graph was rejected");
  }

  const unexpected = fixturePackages();
  unexpected.find((pkg) => pkg.name === "rustee-core").dependencies.push({
    name: "rustee-router",
    path: "/fixture/rustee-router",
    kind: null,
  });
  const unexpectedFailures = dependencyFailures(unexpected);
  if (!unexpectedFailures.includes("rustee-core: unexpected workspace dependency: rustee-router")) {
    throw new Error("unexpected core dependency was accepted");
  }

  const missing = fixturePackages();
  missing.find((pkg) => pkg.name === "rustee-server").dependencies = [];
  const missingFailures = dependencyFailures(missing);
  if (
    !missingFailures.includes(
      "rustee-server: required workspace dependency is missing: rustee-core, rustee-router",
    )
  ) {
    throw new Error("missing core dependency was accepted");
  }

  for (const invalidArguments of [["--unknown"], ["--self-test", "--self-test"]]) {
    try {
      parseCommandArguments(invalidArguments);
      throw new Error(`${invalidArguments.join(" ")}: accepted invalid command-line arguments`);
    } catch (error) {
      if (!String(error).includes("usage:")) {
        throw error;
      }
    }
  }

  console.log("core dependency boundary self-test OK");
}

const { selfTest } = parseCommandArguments(process.argv.slice(2));

if (selfTest) {
  runSelfTest();
  process.exit(0);
}

const metadata = JSON.parse(
  execFileSync("cargo", ["metadata", "--no-deps", "--format-version=1", "--locked", "--offline"], {
    encoding: "utf8",
  }),
);
const failures = dependencyFailures(metadata.packages);

if (failures.length > 0) {
  console.error(failures.join("\n"));
  process.exit(1);
}

console.log(`checked ${dependencyRules.size} core dependency boundaries`);
