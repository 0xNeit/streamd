import { Network } from "@aptos-labs/ts-sdk";

/**
 * An Aptos-compatible network the SDK does not name, shaped as `AptosSettings`
 * so it passes straight to `new AptosConfig(preset)`. Aptos mainnet, testnet,
 * devnet and localnet are the SDK's own `Network` values instead.
 */
export interface NetworkPreset {
  readonly network: Network.CUSTOM;
  readonly fullnode: string;
  /** Indexer GraphQL endpoint, which `IndexerAccountAdapter` uses by default. */
  readonly indexer?: string;
  /** What the fullnode reports as `chain_id`. */
  readonly chainId: number;
}

function preset(value: NetworkPreset): NetworkPreset {
  return Object.freeze(value);
}

/** Networks streamd has been verified against, beyond those in the SDK's `Network`. */
export const networks = Object.freeze({
  movementMainnet: preset({
    network: Network.CUSTOM,
    fullnode: "https://mainnet.movementnetwork.xyz/v1",
    indexer: "https://indexer.mainnet.movementnetwork.xyz/v1/graphql",
    chainId: 126,
  }),
  movementTestnet: preset({
    network: Network.CUSTOM,
    fullnode: "https://testnet.movementnetwork.xyz/v1",
    indexer: "https://indexer.testnet.movementnetwork.xyz/v1/graphql",
    chainId: 250,
  }),
});
