import { readdir, readFile } from "node:fs/promises";

const workflowDirectory = new URL("../.github/workflows/", import.meta.url);
const imagePattern = /@sha256:[0-9a-f]{64}$/u;
const optionsWithValues = new Set([
  "--add-host",
  "--entrypoint",
  "--env",
  "--env-file",
  "--hostname",
  "--label",
  "--mount",
  "--name",
  "--network",
  "--platform",
  "--publish",
  "--user",
  "--volume",
  "--workdir",
]);
const workflowNames = (await readdir(workflowDirectory))
  .filter((name) => /\.ya?ml$/u.test(name))
  .sort();
const unpinned = [];
let containerCount = 0;

for (const workflowName of workflowNames) {
  const workflow = await readFile(new URL(workflowName, workflowDirectory), "utf8");
  for (const [index, line] of workflow.split("\n").entries()) {
    const match = line.match(/\bdocker\s+run\s+(.+)/u);
    if (!match) {
      continue;
    }

    containerCount += 1;
    const image = imageReference(match[1]);
    if (!image || !imagePattern.test(image)) {
      unpinned.push(`${workflowName}:${index + 1}: ${image ?? "missing image reference"}`);
    }
  }
}

if (unpinned.length > 0) {
  throw new Error(`Docker run images must use manifest digests:\n${unpinned.join("\n")}`);
}

console.log(`checked ${containerCount} Docker run images: manifest digests required`);

function imageReference(argumentsText) {
  const argumentsList = argumentsText.match(/"[^"]*"|'[^']*'|[^\s]+/gu) ?? [];
  for (let index = 0; index < argumentsList.length; index += 1) {
    const argument = argumentsList[index];
    if (argument === "--") {
      return argumentsList[index + 1];
    }
    if (!argument.startsWith("-")) {
      return argument;
    }
    if (optionsWithValues.has(argument)) {
      index += 1;
    }
  }
  return undefined;
}
