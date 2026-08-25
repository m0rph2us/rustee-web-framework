import { readdir, readFile } from "node:fs/promises";
import { resolve } from "node:path";

const roots = ["crates", "examples", "tests"];
const prohibited = /\b(?:todo!|unimplemented!|TODO|FIXME)(?!\w)/g;

async function rustFiles(directory) {
  const files = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = resolve(directory, entry.name);
    if (entry.isDirectory()) {
      files.push(...(await rustFiles(path)));
    } else if (entry.isFile() && entry.name.endsWith(".rs")) {
      files.push(path);
    }
  }
  return files;
}

const sourceFiles = (await Promise.all(roots.map((root) => rustFiles(resolve(root))))).flat();
const violations = [];

for (const sourceFile of sourceFiles) {
  const source = await readFile(sourceFile, "utf8");
  for (const match of source.matchAll(new RegExp(prohibited))) {
    const line = source.slice(0, match.index).split("\n").length;
    violations.push(`${sourceFile}:${line}: prohibited incomplete-source marker ${match[0]}`);
  }
}

if (violations.length > 0) {
  throw new Error(`Rust sources must not contain incomplete implementation markers:\n${violations.join("\n")}`);
}

console.log(`checked ${sourceFiles.length} Rust source files: no incomplete implementation markers`);
