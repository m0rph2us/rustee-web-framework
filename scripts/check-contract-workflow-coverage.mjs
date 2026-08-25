import { access, readdir, readFile } from "node:fs/promises";
import { constants } from "node:fs";
import { resolve } from "node:path";

const cratesDirectory = resolve("crates");
const workflowsDirectory = resolve(".github/workflows");

function escapeRegularExpression(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

async function exists(path) {
  try {
    await access(path, constants.F_OK);
    return true;
  } catch {
    return false;
  }
}

async function rustSources(directory) {
  if (!(await exists(directory))) {
    return [];
  }
  const sourcePaths = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const entryPath = resolve(directory, entry.name);
    if (entry.isDirectory()) {
      sourcePaths.push(...(await rustSources(entryPath)));
    } else if (entry.isFile() && entry.name.endsWith(".rs")) {
      sourcePaths.push(entryPath);
    }
  }
  return sourcePaths;
}

async function packageHasIgnoredContract(packageDirectory) {
  const sourcePaths = [
    ...(await rustSources(resolve(packageDirectory, "src"))),
    ...(await rustSources(resolve(packageDirectory, "tests"))),
  ];
  for (const sourcePath of sourcePaths) {
    if ((await exists(sourcePath)) && (await readFile(sourcePath, "utf8")).includes("#[ignore")) {
      return true;
    }
  }
  return false;
}

const workflowNames = (await readdir(workflowsDirectory, { withFileTypes: true }))
  .filter((entry) => entry.isFile() && /\.ya?ml$/u.test(entry.name))
  .map((entry) => entry.name)
  .sort();
const workflows = await Promise.all(
  workflowNames.map(async (name) => ({ name, content: await readFile(resolve(workflowsDirectory, name), "utf8") })),
);
const missing = [];
let checked = 0;

for (const entry of await readdir(cratesDirectory, { withFileTypes: true })) {
  if (!entry.isDirectory()) {
    continue;
  }
  const packageDirectory = resolve(cratesDirectory, entry.name);
  const manifestPath = resolve(packageDirectory, "Cargo.toml");
  if (!(await exists(manifestPath)) || !(await packageHasIgnoredContract(packageDirectory))) {
    continue;
  }
  const manifest = await readFile(manifestPath, "utf8");
  const packageName = manifest.match(/^name\s*=\s*"([^"]+)"$/m)?.[1];
  if (!packageName) {
    throw new Error(`${manifestPath}: package name was not found`);
  }
  checked += 1;
  const invocation = new RegExp(
    `cargo\\s+test(?:\\s+--[a-z-]+(?:=[^\\s]+)?)*\\s+-p\\s+${escapeRegularExpression(packageName)}[^\\n]*--ignored`,
  );
  if (!workflows.some((workflow) => invocation.test(workflow.content))) {
    missing.push(packageName);
  }
}

if (missing.length > 0) {
  throw new Error(
    `ignored provider contracts must run in a workflow with --ignored:\n${missing.join("\n")}`,
  );
}

console.log(`checked ${checked} packages with ignored provider contracts across ${workflowNames.length} workflows`);
