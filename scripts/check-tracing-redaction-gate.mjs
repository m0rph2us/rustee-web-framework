import { readdir, readFile } from "node:fs/promises";

const sourceRoots = ["crates", "examples"];
const rawDiagnosticField =
  /(?:\btracing::)?(?:trace|debug|info|warn|error)!\s*\((?:(?!\)\s*;)[\s\S])*?(?:%|\?)\s*(?:error|err|cause|source|failure|peer|peer_addr)\b/g;

function parseCommandArguments(commandArguments) {
  if (commandArguments.length === 0) {
    return { selfTest: false };
  }
  if (commandArguments.length === 1 && commandArguments[0] === "--self-test") {
    return { selfTest: true };
  }
  throw new Error("usage: node scripts/check-tracing-redaction-gate.mjs [--self-test]");
}

function rawDiagnosticFieldLines(source) {
  return [...source.matchAll(rawDiagnosticField)].map(
    (match) => source.slice(0, match.index).split("\n").length,
  );
}

async function rustSources(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const paths = await Promise.all(
    entries.map(async (entry) => {
      const path = `${directory}/${entry.name}`;
      if (entry.isDirectory()) {
        return rustSources(path);
      }
      return entry.isFile() && path.endsWith(".rs") ? [path] : [];
    }),
  );
  return paths.flat();
}

function runSelfTest() {
  const allowed = [
    'tracing::warn!(outcome = "upstream_unavailable", "request failed");',
    'tracing::debug!(connection_count = 4, "HTTP connection limit exceeded");',
  ];
  for (const source of allowed) {
    if (rawDiagnosticFieldLines(source).length > 0) {
      throw new Error(`safe tracing field was rejected: ${source}`);
    }
  }

  for (const field of ["error", "err", "cause", "source", "failure", "peer", "peer_addr"]) {
    for (const formatter of ["?", "%"]) {
      const source = `tracing::error!(${formatter}${field}, "request failed");`;
      if (rawDiagnosticFieldLines(source).length !== 1) {
        throw new Error(`sensitive diagnostic field was accepted: ${formatter}${field}`);
      }
    }
  }

  for (const invalidArguments of [["--unknown"], ["--self-test", "--self-test"]]) {
    try {
      parseCommandArguments(invalidArguments);
      throw new Error(`${invalidArguments.join(" ")}: accepted invalid command-line arguments`);
    } catch (error) {
      if (!String(error).includes("usage:")) {
        throw error;
      }
    }
  }

  console.log("tracing redaction gate self-test OK");
}

const { selfTest } = parseCommandArguments(process.argv.slice(2));

if (selfTest) {
  runSelfTest();
  process.exit(0);
}

const sourceFiles = (
  await Promise.all(sourceRoots.map((directory) => rustSources(directory)))
)
  .flat()
  .sort();
const violations = [];
for (const path of sourceFiles) {
  const source = await readFile(path, "utf8");
  for (const line of rawDiagnosticFieldLines(source)) {
    violations.push(`${path}:${line}: sensitive diagnostic field in tracing macro`);
  }
}

if (violations.length > 0) {
  throw new Error(`tracing redaction violations:\n${violations.join("\n")}`);
}

console.log(
  `checked ${sourceFiles.length} Rust sources: tracing diagnostics exclude raw failures and peer addresses`,
);
