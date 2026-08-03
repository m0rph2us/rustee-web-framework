import { execFileSync } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const workspaceRoot = resolve(process.cwd());
const inventoryPath = join(workspaceRoot, "docs", "release-inventory.html");
const document = await readFile(inventoryPath, "utf8");
const match = document.match(
  /<script id="workspace-release-inventory" type="application\/json">\s*([\s\S]*?)\s*<\/script>/,
);

if (!match) {
  throw new Error(`${inventoryPath}: workspace release inventory JSON was not found`);
}

const inventory = JSON.parse(match[1]);
if (inventory.schema !== 1 || !Array.isArray(inventory.packages)) {
  throw new Error(`${inventoryPath}: unsupported workspace release inventory schema`);
}

const workspaceMetadata = JSON.parse(
  execFileSync("cargo", ["metadata", "--no-deps", "--format-version=1", "--locked"], {
    cwd: workspaceRoot,
    encoding: "utf8",
  }),
);
const workspacePackages = new Map(workspaceMetadata.packages.map((pkg) => [pkg.name, pkg]));
const candidates = inventory.packages.filter((entry) => entry.intent === "candidate");
const missingWorkspacePackages = candidates.filter((entry) => !workspacePackages.has(entry.name));

if (missingWorkspacePackages.length > 0) {
  throw new Error(
    `publish candidates absent from cargo metadata: ${missingWorkspacePackages.map((entry) => entry.name).join(", ")}`,
  );
}

const consumerRoot = await mkdtemp(join(tmpdir(), "rustee-published-candidates-"));
const manifestPath = join(consumerRoot, "Cargo.toml");

try {
  const dependencies = candidates
    .map((entry) => {
      const pkg = workspacePackages.get(entry.name);
      return `${pkg.name} = "=${pkg.version}"`;
    })
    .join("\n");

  await writeFile(
    manifestPath,
    `[package]\nname = "rustee-release-consumer-check"\nversion = "0.0.0"\nedition = "2024"\npublish = false\n\n[dependencies]\n${dependencies}\n`,
  );
  await mkdir(join(consumerRoot, "src"));
  await writeFile(join(consumerRoot, "src/lib.rs"), "pub fn candidate_registry_check() {}\n");

  execFileSync("cargo", ["generate-lockfile", "--manifest-path", manifestPath], {
    cwd: consumerRoot,
    stdio: "inherit",
  });
  execFileSync("cargo", ["check", "--manifest-path", manifestPath, "--locked"], {
    cwd: consumerRoot,
    stdio: "inherit",
  });

  const consumerMetadata = JSON.parse(
    execFileSync("cargo", ["metadata", "--manifest-path", manifestPath, "--locked", "--format-version=1"], {
      cwd: consumerRoot,
      encoding: "utf8",
    }),
  );
  const failures = [];

  for (const entry of candidates) {
    const expected = workspacePackages.get(entry.name);
    const resolved = consumerMetadata.packages.find(
      (pkg) => pkg.name === expected.name && pkg.version === expected.version,
    );

    if (!resolved) {
      failures.push(`${expected.name}@${expected.version}: registry resolution is missing`);
    } else if (!resolved.source?.startsWith("registry+")) {
      failures.push(`${expected.name}@${expected.version}: resolved from a non-registry source`);
    }
  }

  if (failures.length > 0) {
    throw new Error(failures.join("\n"));
  }

  console.log(`verified ${candidates.length} published candidates through a registry-source consumer compile`);
} finally {
  await rm(consumerRoot, { recursive: true, force: true });
}
