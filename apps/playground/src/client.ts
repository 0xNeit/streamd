import { Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { AptosClient, sendOnce } from "@streamd/aptos";

/** The networks the playground offers, mainnet first as the default. */
export const NETWORKS = {
  mainnet: { label: "Aptos mainnet", network: Network.MAINNET, constant: "MAINNET" },
  testnet: { label: "Aptos testnet", network: Network.TESTNET, constant: "TESTNET" },
} as const;

export type PlaygroundNetwork = keyof typeof NETWORKS;

// The public fullnodes share one anonymous quota per IP, and a module scan
// reads every transaction, so a keyless tab paces itself to leave room.
export const ANONYMOUS_REQUESTS_PER_SECOND = 2;
const REQUEST_TIMEOUT = 30_000;

/** An `Aptos` instance and an `AptosClient` over it; a blank key means anonymous. */
export function clientFor(network: PlaygroundNetwork, apiKey: string): { aptos: Aptos; client: AptosClient } {
  const key = apiKey.trim();
  const aptos = new Aptos(
    new AptosConfig({
      network: NETWORKS[network].network,
      // The SDK's browser client turns a plain-text 429 into a JSON syntax error.
      client: sendOnce(REQUEST_TIMEOUT),
      ...(key !== "" && { clientConfig: { API_KEY: key } }),
    }),
  );
  const client = new AptosClient({
    aptos,
    ...(key === "" && { maxRequestsPerSecond: ANONYMOUS_REQUESTS_PER_SECOND }),
    defaults: { maxErrors: 5 },
  });
  return { aptos, client };
}

/** A message for an error, naming a rate limit anywhere in its cause chain. */
export function describeError(err: unknown, keyed: boolean): string {
  for (let e: unknown = err; e instanceof Error; e = e.cause) {
    if ((e as { statusCode?: unknown }).statusCode === 429) {
      return keyed
        ? "Rate limited by the node, even with this API key. Stop, wait a minute and start again."
        : "Rate limited: the public node limits anonymous requests per IP. Add an API key, or stop and try again later.";
    }
  }
  return err instanceof Error ? err.message : String(err);
}
