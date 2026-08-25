import { readFile } from "node:fs/promises";

const inventoryPattern =
  /<script id="workspace-release-inventory" type="application\/json">\s*([\s\S]*?)\s*<\/script>/;
const intents = new Set(["candidate", "workspace-only"]);

export async function readWorkspaceReleaseInventory(inventoryPath, packages) {
  const document = await readFile(inventoryPath, "utf8");
  const match = document.match(inventoryPattern);
  if (!match) {
    throw new Error(`${inventoryPath}: workspace release inventory JSON was not found`);
  }

  let inventory;
  try {
    inventory = JSON.parse(match[1]);
  } catch (error) {
    throw new Error(`${inventoryPath}: invalid workspace release inventory JSON`, { cause: error });
  }
  if (inventory.schema !== 1) {
    throw new Error(`${inventoryPath}: unsupported workspace release inventory schema`);
  }
  if (inventory.generatedFromMetadata === true) {
    inventory = {
      schema: 1,
      packages: packages.map((pkg) => ({
        name: pkg.name,
        track: "workspace",
        intent: pkg.publish === null ? "candidate" : "workspace-only",
      })),
    };
  }
  if (!Array.isArray(inventory.packages)) {
    throw new Error(`${inventoryPath}: workspace release inventory packages were not found`);
  }

  const names = new Set();
  for (const entry of inventory.packages) {
    if (
      typeof entry?.name !== "string" ||
      typeof entry.track !== "string" ||
      !intents.has(entry.intent)
    ) {
      throw new Error(`${inventoryPath}: invalid inventory entry: ${JSON.stringify(entry)}`);
    }
    if (names.has(entry.name)) {
      throw new Error(`${inventoryPath}: duplicate inventory entry: ${entry.name}`);
    }
    names.add(entry.name);
  }
  return inventory;
}
