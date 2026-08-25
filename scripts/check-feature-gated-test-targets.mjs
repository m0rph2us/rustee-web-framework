import { execFileSync } from "node:child_process";
import { readFile } from "node:fs/promises";

const metadata = JSON.parse(
  execFileSync(
    "cargo",
    ["metadata", "--no-deps", "--format-version=1", "--locked", "--offline"],
    { encoding: "utf8" },
  ),
);
const featureGate = /^#!\[cfg\(feature\s*=\s*"([^"]+)"\)\]/mu;
const failures = [];
let checked = 0;

for (const pkg of metadata.packages) {
  for (const target of pkg.targets) {
    if (!target.kind.includes("test")) {
      continue;
    }
    const source = await readFile(target.src_path, "utf8");
    const feature = source.match(featureGate)?.[1];
    if (!feature) {
      continue;
    }
    checked += 1;
    if (!target["required-features"].includes(feature)) {
      failures.push(`${pkg.name}:${target.name} must declare required-features = ["${feature}"]`);
    }
  }
}

if (failures.length > 0) {
  throw new Error(`feature-gated integration tests need Cargo target requirements:\n${failures.join("\n")}`);
}

console.log(`checked ${checked} feature-gated integration test targets`);
