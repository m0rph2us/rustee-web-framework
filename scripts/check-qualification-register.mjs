import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

const supportedOptions = new Set(["--require-qualified", "--self-test"]);

function parseArguments(arguments_) {
  const positionalArguments = arguments_.filter((argument) => !argument.startsWith("--"));
  for (const option of arguments_.filter((argument) => argument.startsWith("--"))) {
    if (!supportedOptions.has(option)) {
      throw new Error(`unsupported qualification-register option ${option}`);
    }
  }
  if (positionalArguments.length > 1) {
    throw new Error("qualification register accepts at most one document path");
  }
  return {
    registerPath: resolve(positionalArguments[0] ?? "docs/qualification-register.html"),
    requireQualified: arguments_.includes("--require-qualified"),
    runSelfTest: arguments_.includes("--self-test"),
  };
}

const { registerPath, requireQualified, runSelfTest } = parseArguments(process.argv.slice(2));
const requiredGates = new Map([
  ["release-source-and-provenance", "release"],
  ["managed-postgresql", "storage-identity"],
  ["managed-redis", "storage-identity"],
  ["managed-mongodb", "storage-identity"],
  ["external-oidc", "storage-identity"],
  ["nats-jetstream", "delivery"],
  ["rabbitmq", "delivery"],
  ["aws-sqs", "delivery"],
  ["kafka-and-schema-registry", "delivery"],
  ["live-ai-provider", "ai-mcp"],
  ["mcp-oauth-provider", "ai-mcp"],
  ["edge-delivery", "edge"],
  ["routing-benchmark-baseline", "performance"],
]);
const statuses = new Set(["pending", "in-progress", "qualified", "expired", "failed"]);
const baseFields = new Set(["id", "category", "status", "owner", "summary", "nextAction"]);
const datedStatusFields = new Set([...baseFields, "updatedOn"]);
const qualifiedFields = new Set([
  ...baseFields,
  "sourceRef",
  "environment",
  "provider",
  "scope",
  "reviewer",
  "recoveryOutcome",
  "procedureRef",
  "evidenceRef",
  "completedOn",
  "expiresOn",
]);
const datePattern = /^\d{4}-\d{2}-\d{2}$/;
const recordPattern =
  /<script id="qualification-register-data" type="application\/json">\s*([\s\S]*?)\s*<\/script>/;

function printableText(value, field, record, maximumLength) {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length > maximumLength ||
    !/^[\x20-\x7E]+$/.test(value)
  ) {
    throw new Error(`${record.id ?? "<unknown>"}: ${field} must be printable ASCII text up to ${maximumLength} characters`);
  }
  if (/:\/\/|(?:api[_ -]?key|authorization|password|secret|token)\s*[:=]/i.test(value)) {
    throw new Error(`${record.id}: ${field} must not include a URL or credential-like value`);
  }
}

function date(value, field, record) {
  if (!datePattern.test(value)) {
    throw new Error(`${record.id}: ${field} must be an ISO calendar date`);
  }
  const parsed = new Date(`${value}T00:00:00Z`);
  if (Number.isNaN(parsed.valueOf()) || parsed.toISOString().slice(0, 10) !== value) {
    throw new Error(`${record.id}: ${field} must be an ISO calendar date`);
  }
}

function calendarDateSelfTest() {
  const record = { id: "qualification-calendar-date-self-test" };
  for (const value of ["2024-02-29", "2026-02-28"]) {
    date(value, "date", record);
  }
  for (const value of ["2026-02-29", "2026-02-31", "2026-04-31"]) {
    try {
      date(value, "date", record);
    } catch {
      continue;
    }
    throw new Error(`${record.id}: accepted invalid calendar date ${value}`);
  }
}

function commandLineSelfTest() {
  const parsed = parseArguments(["--require-qualified", "docs/fixture.html"]);
  if (
    !parsed.requireQualified ||
    parsed.runSelfTest ||
    parsed.registerPath !== resolve("docs/fixture.html")
  ) {
    throw new Error("qualification command-line parser accepted an unexpected valid invocation");
  }
  for (const arguments_ of [["--require-qualifed"], ["docs/first.html", "docs/second.html"]]) {
    try {
      parseArguments(arguments_);
    } catch {
      continue;
    }
    throw new Error(`qualification command-line parser accepted invalid arguments ${arguments_.join(" ")}`);
  }
}

if (runSelfTest) {
  calendarDateSelfTest();
  commandLineSelfTest();
  console.log("qualification calendar date and command-line boundaries OK");
}

function evidenceReference(value, field, record) {
  printableText(value, field, record, 256);
  if (!/^(artifact|attestation|issue|report|runbook|workflow):[A-Za-z0-9._/#-]+$/.test(value)) {
    throw new Error(`${record.id}: ${field} must use a safe typed reference, not a URL or credential`);
  }
}

const document = await readFile(registerPath, "utf8");
const match = document.match(recordPattern);
if (!match) {
  throw new Error(`${registerPath}: qualification register JSON was not found`);
}

let ledger;
try {
  ledger = JSON.parse(match[1]);
} catch (error) {
  throw new Error(`${registerPath}: invalid qualification register JSON`, { cause: error });
}

if (ledger.schema !== 1 || !Array.isArray(ledger.records)) {
  throw new Error(`${registerPath}: qualification register must use schema 1 with records`);
}

const recordsById = new Map();
for (const record of ledger.records) {
  const allowedFields =
    record?.status === "qualified"
      ? qualifiedFields
      : ["in-progress", "failed", "expired"].includes(record?.status)
        ? datedStatusFields
        : baseFields;
  for (const field of Object.keys(record ?? {})) {
    if (!allowedFields.has(field)) {
      throw new Error(`${record?.id ?? "<unknown>"}: unexpected qualification field ${field}`);
    }
  }
  printableText(record?.id, "id", record ?? {}, 96);
  if (!/^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(record.id)) {
    throw new Error(`${record.id}: id must use lowercase hyphenated words`);
  }
  if (recordsById.has(record.id)) {
    throw new Error(`${record.id}: duplicate qualification record`);
  }
  printableText(record.category, "category", record, 40);
  if (requiredGates.get(record.id) !== record.category) {
    throw new Error(`${record.id}: category does not match the required qualification gate`);
  }
  if (!statuses.has(record.status)) {
    throw new Error(`${record.id}: unsupported qualification status`);
  }
  printableText(record.owner, "owner", record, 120);
  printableText(record.summary, "summary", record, 280);
  printableText(record.nextAction, "nextAction", record, 280);

  if (record.status === "in-progress" || record.status === "failed" || record.status === "expired") {
    if (record.owner === "unassigned") {
      throw new Error(`${record.id}: ${record.status} records require an assigned owner`);
    }
    date(record.updatedOn, "updatedOn", record);
  }
  if (record.status === "qualified") {
    if (record.owner === "unassigned") {
      throw new Error(`${record.id}: qualified records require an assigned owner`);
    }
    for (const field of ["sourceRef", "environment", "provider", "scope", "reviewer", "recoveryOutcome"]) {
      printableText(record[field], field, record, 280);
    }
    evidenceReference(record.procedureRef, "procedureRef", record);
    evidenceReference(record.evidenceRef, "evidenceRef", record);
    date(record.completedOn, "completedOn", record);
    date(record.expiresOn, "expiresOn", record);
    if (record.expiresOn < record.completedOn) {
      throw new Error(`${record.id}: expiresOn must not precede completedOn`);
    }
  }
  recordsById.set(record.id, record);
}

for (const id of requiredGates.keys()) {
  if (!recordsById.has(id)) {
    throw new Error(`${registerPath}: required qualification gate ${id} is missing`);
  }
}
if (recordsById.size !== requiredGates.size) {
  throw new Error(`${registerPath}: qualification register contains an unknown gate`);
}

if (requireQualified) {
  const today = new Date().toISOString().slice(0, 10);
  for (const record of recordsById.values()) {
    if (record.status !== "qualified") {
      throw new Error(`${record.id}: enterprise qualification requires status qualified`);
    }
    if (record.expiresOn < today) {
      throw new Error(`${record.id}: qualification evidence expired on ${record.expiresOn}`);
    }
  }
}

const counts = [...recordsById.values()].reduce((result, record) => {
  result.set(record.status, (result.get(record.status) ?? 0) + 1);
  return result;
}, new Map());
const summary = [...statuses]
  .map((status) => `${status}=${counts.get(status) ?? 0}`)
  .join(", ");
console.log(`checked ${recordsById.size} qualification gates: ${summary}`);
