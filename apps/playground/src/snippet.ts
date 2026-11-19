import { normalizeAddress } from "@streamd/core";
import { NETWORKS } from "./client.js";
import type { PlaygroundNetwork } from "./client.js";

const PLAIN_DESCRIPTOR = /^export const (\w+) = defineEvent<\1>\("([^"]+)"\);$/gm;

const canonical = (type: string) => {
  const [address = "", ...rest] = type.trim().split("::");
  return [normalizeAddress(address) ?? address, ...rest].join("::");
};

/**
 * The name codegen gave `eventType`'s descriptor in `source`, or undefined
 * when there is none to import: a generic event's descriptor is a function of
 * its type arguments.
 */
export function descriptorName(source: string, eventType: string): string | undefined {
  const wanted = canonical(eventType);
  for (const [, name, type] of source.matchAll(PLAIN_DESCRIPTOR)) if (canonical(type!) === wanted) return name;
  return undefined;
}

export interface SnippetInput {
  network: PlaygroundNetwork;
  eventType: string;
  /** The generated descriptor to import from `./events.js`; inline when unset. */
  name?: string;
}

/** A Bun script following the event type; the API key comes from the environment, never from the page. */
export function snippet({ network, eventType, name }: SnippetInput): string {
  const descriptor = name ?? `{ kind: "module", eventType: ${JSON.stringify(eventType.trim())} }`;
  return [
    "// bun add @streamd/aptos @streamd/core @aptos-labs/ts-sdk",
    ...(name === undefined ? [] : ["// Save the generated types beside this file as events.ts."]),
    "// Run with: APTOS_API_KEY=<your key> bun main.ts (the key is optional)",
    'import { Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";',
    'import { AptosClient } from "@streamd/aptos";',
    ...(name === undefined ? [] : [`import { ${name} } from "./events.js";`]),
    "",
    "const apiKey = process.env.APTOS_API_KEY;",
    "// Bun's HTTP/2 support is incomplete, so the SDK is asked for HTTP/1.1.",
    "const aptos = new Aptos(",
    `  new AptosConfig({ network: Network.${NETWORKS[network].constant}, clientConfig: { http2: false, ...(apiKey && { API_KEY: apiKey }) } }),`,
    ");",
    "const client = new AptosClient({ aptos });",
    "",
    `client.on(${descriptor}, (event) => {`,
    "  console.log(event.version.toString(), JSON.stringify(event.data));",
    "});",
    "",
  ].join("\n");
}
