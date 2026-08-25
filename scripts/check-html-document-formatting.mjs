import { readdir, readFile } from "node:fs/promises";
import { join, resolve } from "node:path";

const documentRoot = resolve(process.argv[2] ?? "docs");
const failures = [];

async function collectHtmlDocuments(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const documents = [];

  for (const entry of entries.sort((left, right) => left.name.localeCompare(right.name))) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      documents.push(...(await collectHtmlDocuments(path)));
    } else if (entry.name.endsWith(".html")) {
      documents.push(path);
    }
  }

  return documents;
}

const documents = await collectHtmlDocuments(documentRoot);

for (const document of documents) {
  const source = await readFile(document, "utf8");
  const lines = source.split(/\r?\n/);

  if (!source.endsWith("\n")) {
    failures.push(`${document}: document must end with a newline`);
  }
  if (lines.length < 5) {
    failures.push(`${document}: document is too compressed for source review`);
  }
  if (lines[0]?.toLowerCase() !== "<!doctype html>" || !lines[1]?.startsWith("<html")) {
    failures.push(`${document}: document must keep the doctype and html root on separate lines`);
  }
  if (source.includes("</section><section")) {
    failures.push(`${document}: adjacent sections must remain on separate source lines`);
  }
}

if (failures.length > 0) {
  throw new Error(`HTML document formatting quality issues:\n${failures.join("\n")}`);
}

console.log(`checked ${documents.length} HTML documents: reviewable source formatting OK`);
