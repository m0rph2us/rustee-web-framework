import { readdir, readFile } from "node:fs/promises";
const workflowDirectory = new URL("../.github/workflows/", import.meta.url);
const workflowNames = (await readdir(workflowDirectory))
  .filter((name) => /\.ya?ml$/u.test(name))
  .sort();
const unpinned = [];

for (const workflowName of workflowNames) {
  const workflow = await readFile(new URL(workflowName, workflowDirectory), "utf8");
  for (const [index, line] of workflow.split("\n").entries()) {
    const match = line.match(/^\s*(?:-\s+)?uses:\s+([^\s#]+)/);
    if (!match || match[1].startsWith("./")) {
      continue;
    }

    const [action, reference] = match[1].split("@", 2);
    if (!action || !reference || !/^[0-9a-f]{40}$/.test(reference)) {
      unpinned.push(`${workflowName}:${index + 1}: ${match[1]}`);
    }
  }
}

if (unpinned.length > 0) {
  throw new Error(`GitHub Actions must use full commit SHAs:\n${unpinned.join("\n")}`);
}

console.log(`checked ${workflowNames.length} workflows: action references use full commit SHAs`);
