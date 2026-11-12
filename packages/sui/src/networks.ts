import type { SuiGrpcClientOptions } from "@mysten/sui/grpc";
import { StreamdError } from "@streamd/core";

// A type alias rather than an interface, so it meets the index signature `SuiGrpcClientOptions` carries.
/** A public Sui network, shaped as `SuiGrpcClientOptions` so it passes straight to `new SuiGrpcClient(preset)`. */
export type SuiNetworkPreset = Readonly<{
  network: "mainnet" | "testnet" | "devnet";
  baseUrl: string;
  /** Mysten Labs' Sui GraphQL endpoint, which `SuiAdapter` reads transactions the fullnode has pruned from. */
  graphqlUrl: string;
}>;

/** A native gRPC `RpcTransport`, as `SuiGrpcClient` takes one. */
export type SuiTransport = Extract<SuiGrpcClientOptions, { transport: unknown }>["transport"];

/** The archive's client options: `new SuiGrpcClient(preset)` reads it over the transport given. */
export type SuiArchivePreset = Readonly<{ network: "mainnet"; transport: SuiTransport }>;

/** What `SuiGrpcClient` calls on its transport. */
const TRANSPORT_METHODS = ["mergeOptions", "unary", "serverStreaming", "clientStreaming", "duplex"];

function preset(network: SuiNetworkPreset["network"]): SuiNetworkPreset {
  return Object.freeze({ network, baseUrl: `https://fullnode.${network}.sui.io:443`, graphqlUrl: `https://graphql.${network}.sui.io/graphql` });
}

/**
 * Mysten Labs' mainnet archive, which keeps every checkpoint from 0, over
 * `transport`, a native gRPC transport to `mainnetArchive.host`. The archive
 * refuses gRPC-web, the only protocol browsers have, so this is for Node and
 * Bun. A function rather than a URL, since a URL alone would build a gRPC-web
 * client whose every call fails.
 */
const mainnetArchive = Object.freeze(
  Object.assign(
    (transport: SuiTransport): SuiArchivePreset => {
      const t = transport as unknown as Record<string, unknown> | null | undefined;
      if (!TRANSPORT_METHODS.every((m) => typeof t?.[m] === "function")) {
        throw new StreamdError("networks.mainnetArchive needs a native gRPC RpcTransport, such as @protobuf-ts/grpc-transport's GrpcTransport");
      }
      return Object.freeze({ network: "mainnet", transport });
    },
    { host: "archive.mainnet.sui.io:443" as const },
  ),
);

/** Mysten Labs' public fullnodes, which serve the gRPC API `SuiAdapter` reads, and the mainnet archive. */
export const networks = Object.freeze({
  mainnet: preset("mainnet"),
  testnet: preset("testnet"),
  devnet: preset("devnet"),
  mainnetArchive,
});
