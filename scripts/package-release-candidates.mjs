import { execFileSync } from "node:child_process";
import { readdir, rm } from "node:fs/promises";
import { dirname, join, relative } from "node:path";
import { readWorkspaceReleaseInventory } from "./workspace-release-inventory.mjs";

const packageOptions = process.argv.slice(2);
const unsupportedOptions = packageOptions.filter((option) => option !== "--offline" && option !== "--allow-dirty");

if (unsupportedOptions.length > 0) {
  throw new Error(`unsupported package-release-candidates options: ${unsupportedOptions.join(", ")}`);
}

const metadataOptions = packageOptions.includes("--offline") ? ["--offline"] : [];

const metadata = JSON.parse(
  execFileSync("cargo", ["metadata", "--no-deps", "--format-version=1", "--locked", ...metadataOptions], {
    encoding: "utf8",
  }),
);
const workspaceRoot = metadata.workspace_root;
const inventoryPath = join(workspaceRoot, "docs", "release-inventory.html");
const inventory = await readWorkspaceReleaseInventory(inventoryPath, metadata.packages);

const packageByName = new Map(metadata.packages.map((pkg) => [pkg.name, pkg]));
const candidates = inventory.packages.filter((entry) => entry.intent === "candidate");
const workspaceOnly = inventory.packages.filter((entry) => entry.intent === "workspace-only");
const failures = [];

for (const entry of [...candidates, ...workspaceOnly]) {
  if (!packageByName.has(entry.name)) {
    failures.push(`${entry.name}: listed in release inventory but absent from cargo metadata`);
  }
}
if (failures.length > 0) {
  throw new Error(failures.join("\n"));
}

const expectedArchives = new Set(
  candidates.map((entry) => {
    const pkg = packageByName.get(entry.name);
    return `${pkg.name}-${pkg.version}.crate`;
  }),
);
const packageDirectory = join(metadata.target_directory, "package");
const packageDirectoryRelativeToWorkspace = relative(workspaceRoot, packageDirectory);

if (
  packageDirectoryRelativeToWorkspace.startsWith("..") ||
  packageDirectoryRelativeToWorkspace === "" ||
  !packageDirectoryRelativeToWorkspace.startsWith("target/")
) {
  throw new Error(`refusing to clear unexpected package directory: ${packageDirectory}`);
}

function localWorkspaceDependencyClosure(pkg, dependencies = new Map()) {
  for (const dependency of pkg.dependencies) {
    if (!dependency.path || !packageByName.has(dependency.name)) {
      continue;
    }
    if (dependencies.has(dependency.name)) {
      continue;
    }

    const dependencyPackage = packageByName.get(dependency.name);
    dependencies.set(dependency.name, dependencyPackage);
    localWorkspaceDependencyClosure(dependencyPackage, dependencies);
  }
  return dependencies;
}

// Remove stale archives so the result describes exactly this inventory, not a prior workspace run.
await rm(packageDirectory, { recursive: true, force: true });

for (const entry of candidates) {
  const pkg = packageByName.get(entry.name);
  const localDependencies = localWorkspaceDependencyClosure(pkg);
  const patchOptions = [...localDependencies]
    .sort(([left], [right]) => left.localeCompare(right))
    .flatMap(([name, dependency]) => [
      "--config",
      `patch.crates-io.${name}.path=${JSON.stringify(dirname(dependency.manifest_path))}`,
    ]);

  // Cargo prepares dev-dependencies too. These command-only patches resolve every local
  // dependency without changing the versioned dependencies written into the source archive.
  // Keep Cargo's verification enabled so every reassembled archive is compiled before release.
  execFileSync(
    "cargo",
    [
      "package",
      "--package",
      pkg.name,
      "--quiet",
      "--locked",
      ...patchOptions,
      ...packageOptions,
    ],
    { cwd: workspaceRoot, stdio: "inherit" },
  );
}

const actualArchives = new Set((await readdir(packageDirectory)).filter((name) => name.endsWith(".crate")));
const missing = [...expectedArchives].filter((name) => !actualArchives.has(name));
const unexpected = [...actualArchives].filter((name) => !expectedArchives.has(name));

if (missing.length > 0 || unexpected.length > 0) {
  throw new Error(
    [
      missing.length > 0 ? `missing source archives: ${missing.sort().join(", ")}` : null,
      unexpected.length > 0 ? `unexpected source archives: ${unexpected.sort().join(", ")}` : null,
    ]
      .filter(Boolean)
      .join("\n"),
  );
}

console.log(`assembled ${actualArchives.size} inventory-approved source archives`);
