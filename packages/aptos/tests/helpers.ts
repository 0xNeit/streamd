import { Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { AptosClient } from "../src/index.js";
import type { AptosClientConfig } from "../src/index.js";

/** Constructing the SDK performs no network I/O; the adapter given is what serves events. */
export function makeAptos(): Aptos {
  return new Aptos(new AptosConfig({ network: Network.DEVNET, clientConfig: { http2: false } }));
}

/** An `AptosClient` over the given config, with an offline SDK instance unless one is given. */
export function testClient(config: Omit<AptosClientConfig, "aptos"> & { aptos?: AptosClientConfig["aptos"] }): AptosClient {
  return new AptosClient({ ...config, aptos: config.aptos ?? makeAptos() });
}
