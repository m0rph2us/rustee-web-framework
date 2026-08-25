import { readdir, readFile, stat } from "node:fs/promises";

const manifest = await readFile("fuzz/Cargo.toml", "utf8");
const workflow = await readFile(".github/workflows/fuzz.yml", "utf8");

const binEntryCount = [
  ...manifest.matchAll(/^\[\[bin\]\][^\S\r\n]*$/gmu),
].length;
const manifestTargets = [
  ...manifest.matchAll(
    /^\[\[bin\]\][^\S\r\n]*\r?\nname\s*=\s*"([^"]+)"\r?\npath\s*=\s*"([^"]+)"/gmu,
  ),
].map((match) => ({ name: match[1], path: match[2] }));
const manifestTargetNames = manifestTargets.map(({ name }) => name);
const matrixMatches = [
  ...workflow.matchAll(/^\s*target:\s*\[([^\]]+)\]\s*$/gmu),
];

if (
  manifestTargetNames.length === 0 ||
  manifestTargetNames.length !== binEntryCount ||
  matrixMatches.length !== 1
) {
  throw new Error(
    "expected every fuzz manifest target to declare a name and source path plus one workflow target matrix",
  );
}

const workflowTargets = matrixMatches[0][1]
  .split(",")
  .map((target) => target.trim())
  .filter(Boolean);
const duplicates = (targets) =>
  [...new Set(targets.filter((target, index) => targets.indexOf(target) !== index))].sort();
const missingFromWorkflow = manifestTargetNames.filter(
  (target) => !workflowTargets.includes(target),
);
const unknownToManifest = workflowTargets.filter(
  (target) => !manifestTargetNames.includes(target),
);
const duplicateManifestTargets = duplicates(manifestTargetNames);
const duplicateWorkflowTargets = duplicates(workflowTargets);
const corpusProblems = [];
const sourceProblems = [];

for (const { name, path } of manifestTargets) {
  if (path.startsWith("/") || path.split("/").includes("..")) {
    sourceProblems.push(`invalid target source path: ${name}`);
  } else {
    const sourcePath = new URL(`../fuzz/${path}`, import.meta.url);
    try {
      if (!(await stat(sourcePath)).isFile()) {
        sourceProblems.push(`missing target source: ${name}`);
      }
    } catch {
      sourceProblems.push(`missing target source: ${name}`);
    }
  }

  const corpusDirectory = new URL(`../fuzz/corpus/${name}/`, import.meta.url);
  try {
    const entries = await readdir(corpusDirectory, { withFileTypes: true });
    if (!entries.some((entry) => entry.isFile())) {
      corpusProblems.push(`missing seed corpus: ${name}`);
    }
  } catch {
    corpusProblems.push(`missing seed corpus: ${name}`);
  }
}

if (
  missingFromWorkflow.length > 0 ||
  unknownToManifest.length > 0 ||
  duplicateManifestTargets.length > 0 ||
  duplicateWorkflowTargets.length > 0 ||
  sourceProblems.length > 0 ||
  corpusProblems.length > 0
) {
  const problems = [
    missingFromWorkflow.length > 0 && `missing from workflow: ${missingFromWorkflow.join(", ")}`,
    unknownToManifest.length > 0 && `unknown to manifest: ${unknownToManifest.join(", ")}`,
    duplicateManifestTargets.length > 0 && `duplicate manifest targets: ${duplicateManifestTargets.join(", ")}`,
    duplicateWorkflowTargets.length > 0 && `duplicate workflow targets: ${duplicateWorkflowTargets.join(", ")}`,
    ...sourceProblems,
    ...corpusProblems,
  ].filter(Boolean);
  throw new Error(`fuzz target coverage mismatch:\n${problems.join("\n")}`);
}

console.log(
  `checked ${manifestTargetNames.length} fuzz targets, source files, scheduled workflow matrix, and seed corpora`,
);
