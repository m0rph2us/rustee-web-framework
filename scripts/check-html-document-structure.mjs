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

function headingText(source, level) {
  const pattern = new RegExp(`<h${level}(?:\\s[^>]*)?>([\\s\\S]*?)</h${level}>`, "g");
  return [...source.matchAll(pattern)].map((match) =>
    match[1].replace(/<[^>]*>/g, "").replace(/\s+/g, " ").trim(),
  );
}

const documents = await collectHtmlDocuments(documentRoot);

for (const document of documents) {
  const source = await readFile(document, "utf8");
  const h1 = headingText(source, 1);
  const h2 = headingText(source, 2);
  const duplicateH2 = [...new Set(h2.filter((heading, index) => h2.indexOf(heading) !== index))];

  if (h1.length !== 1) {
    failures.push(`${document}: expected exactly one h1 heading, found ${h1.length}`);
  }
  if (duplicateH2.length > 0) {
    failures.push(`${document}: duplicate h2 headings: ${duplicateH2.join(", ")}`);
  }
  if (!source.includes("Last updated:")) {
    failures.push(`${document}: missing Last updated footer`);
  }
}

if (failures.length > 0) {
  throw new Error(`HTML document structure quality issues:\n${failures.join("\n")}`);
}

console.log(
  `checked ${documents.length} HTML documents: one h1, unique h2 headings, and update footers OK`,
);
