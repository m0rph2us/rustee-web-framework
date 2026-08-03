import { copyFile, mkdir, readdir, readFile, stat } from "node:fs/promises";
import { dirname, join, relative, resolve } from "node:path";

const sourceRoot = resolve(process.argv[2] ?? ".");
const outputRoot = resolve(process.argv[3] ?? "target/sbom");
const fileName = process.argv[4] ?? "sbom.cdx.json";
const sbomFiles = [];

async function collectSbomFiles(directory) {
  for (const entry of await readdir(directory)) {
    const path = join(directory, entry);
    const entryStat = await stat(path);

    if (entryStat.isDirectory()) {
      if (path === outputRoot || entry === ".git" || entry === "target") {
        continue;
      }
      await collectSbomFiles(path);
    } else if (entryStat.isFile() && entry === fileName) {
      sbomFiles.push(path);
    }
  }
}

await collectSbomFiles(sourceRoot);

if (sbomFiles.length === 0) {
  throw new Error(`no ${fileName} files found under ${sourceRoot}`);
}

for (const sbomFile of sbomFiles) {
  let document;
  try {
    document = JSON.parse(await readFile(sbomFile, "utf8"));
  } catch (error) {
    throw new Error(`${sbomFile}: invalid JSON`, { cause: error });
  }

  if (document.bomFormat !== "CycloneDX" || document.specVersion !== "1.5") {
    throw new Error(`${sbomFile}: expected a CycloneDX 1.5 JSON SBOM`);
  }

  const target = join(outputRoot, relative(sourceRoot, sbomFile));
  await mkdir(dirname(target), { recursive: true });
  await copyFile(sbomFile, target);
}

console.log(`validated and collected ${sbomFiles.length} CycloneDX 1.5 SBOMs`);
