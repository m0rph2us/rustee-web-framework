import { readdir, readFile } from "node:fs/promises";
import { resolve } from "node:path";

const workflowDirectory = resolve(".github/workflows");
const cargoCommands = new Set(["bench", "check", "clippy", "doc", "test"]);
const cargoInvocation = /\bcargo(?:\s+\+[^\s]+)?\s+([a-z][a-z0-9-]*)\b/giu;

async function workflowFiles(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  return entries
    .filter((entry) => entry.isFile() && /\.ya?ml$/u.test(entry.name))
    .map((entry) => resolve(directory, entry.name))
    .sort();
}

function commandUsesLockedFlag(command, invocationStart) {
  const shellCommand = command.slice(invocationStart).split(/\s+(?:&&|\|\||;|\|)\s+/u, 1)[0];
  const commandArguments = shellCommand.split(" -- ", 1)[0];
  return /(?:^|\s)--locked(?:\s|$)/u.test(commandArguments);
}

const failures = [];
for (const workflow of await workflowFiles(workflowDirectory)) {
  const lines = (await readFile(workflow, "utf8")).split(/\r?\n/u);
  for (const [index, line] of lines.entries()) {
    for (const match of line.matchAll(cargoInvocation)) {
      const command = match[1].toLowerCase();
      if (!cargoCommands.has(command) || commandUsesLockedFlag(line, match.index)) {
        continue;
      }
      failures.push(`${workflow}:${index + 1} missing --locked for cargo ${command}`);
    }
  }
}

if (failures.length > 0) {
  console.error(failures.join("\n"));
  process.exit(1);
}

console.log("checked workflow Cargo bench, check, clippy, doc, and test commands: --locked required");
