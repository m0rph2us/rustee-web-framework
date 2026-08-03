import { readdir, readFile, stat } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";

const docsRoot = resolve(process.argv[2] ?? "docs");
const htmlFiles = [];

async function collectHtmlFiles(directory) {
  for (const entry of await readdir(directory)) {
    const path = join(directory, entry);

    if ((await stat(path)).isDirectory()) {
      await collectHtmlFiles(path);
    } else if (path.endsWith(".html")) {
      htmlFiles.push(path);
    }
  }
}

await collectHtmlFiles(docsRoot);

const missingLinks = [];
for (const file of htmlFiles) {
  const html = await readFile(file, "utf8");

  for (const match of html.matchAll(/href="([^"]+)"/g)) {
    const href = match[1];
    if (/^(https?:|mailto:|tel:|#)/.test(href)) {
      continue;
    }

    const target = href.split("#", 1)[0];
    if (!target) {
      continue;
    }

    try {
      await stat(resolve(dirname(file), target));
    } catch {
      missingLinks.push(`${file}: ${href}`);
    }
  }
}

if (missingLinks.length > 0) {
  console.error(missingLinks.join("\n"));
  process.exit(1);
}

console.log(`checked ${htmlFiles.length} HTML files: local links OK`);
