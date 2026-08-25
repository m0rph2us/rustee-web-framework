import { readdir, readFile, stat } from "node:fs/promises";
import { join, resolve } from "node:path";

const roots = ["docs", "crates", "examples", "fuzz", "tests", "scripts", ".github"];
const rootFiles = ["README.md", "AGENTS.md", "Cargo.toml"];
const sourceExtensions = new Set([".html", ".md", ".rs", ".mjs", ".yml", ".yaml", ".toml"]);
const files = [];

async function collectFiles(path) {
  for (const entry of await readdir(path)) {
    const candidate = join(path, entry);
    const metadata = await stat(candidate);

    if (metadata.isDirectory()) {
      await collectFiles(candidate);
    } else if (sourceExtensions.has(candidate.slice(candidate.lastIndexOf(".")))) {
      files.push(candidate);
    }
  }
}

for (const root of roots) {
  await collectFiles(resolve(root));
}
for (const file of rootFiles) {
  const path = resolve(file);
  try {
    await stat(path);
    files.push(path);
  } catch (error) {
    if (error?.code !== "ENOENT") {
      throw error;
    }
  }
}

const failures = [];
for (const file of files) {
  const content = await readFile(file, "utf8");
  const match = content.match(/[^\x00-\x7F]/);

  if (match) {
    const before = content.slice(0, match.index);
    const line = before.split("\n").length;
    failures.push(`${file}:${line}: non-ASCII content is not allowed`);
  }
}

if (failures.length > 0) {
  console.error(failures.join("\n"));
  process.exit(1);
}

console.log(`checked ${files.length} documentation and source files: ASCII-only content`);
