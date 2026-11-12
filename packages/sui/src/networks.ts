// A type alias rather than an interface, so it meets the index signature `SuiGrpcClientOptions` carries.
/** A public Sui network, shaped as `SuiGrpcClientOptions` so it passes straight to `new SuiGrpcClient(preset)`. */
export type SuiNetworkPreset = Readonly<{
  network: "mainnet" | "testnet" | "devnet";
  baseUrl: string;
  /** Mysten Labs' Sui GraphQL endpoint, which `SuiAdapter` reads transactions the fullnode has pruned from. */
  graphqlUrl: string;
}>;

function preset(network: SuiNetworkPreset["network"]): SuiNetworkPreset {
  return Object.freeze({ network, baseUrl: `https://fullnode.${network}.sui.io:443`, graphqlUrl: `https://graphql.${network}.sui.io/graphql` });
}

/** Mysten Labs' public fullnodes, which serve the gRPC API `SuiAdapter` reads. */
export const networks = Object.freeze({
  mainnet: preset("mainnet"),
  testnet: preset("testnet"),
  devnet: preset("devnet"),
});
