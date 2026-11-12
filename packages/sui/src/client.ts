import { SuiGrpcClient, type SuiGrpcClientOptions } from "@mysten/sui/grpc";
import { StreamdClient, StreamdError } from "@streamd/core";
import type { EventAdapter, StreamdConfig } from "@streamd/core";
import { SuiAdapter } from "./adapter.js";

/** `StreamdClient`'s config, with the Sui gRPC client the Sui methods read through; `adapter` defaults to a `SuiAdapter` over it. */
export type SuiClientConfig = Omit<StreamdConfig, "adapter"> & {
  /** A `SuiGrpcClient`, or the options to build one, such as a preset from `networks`. */
  sui: SuiGrpcClient | SuiGrpcClientOptions;
  adapter?: EventAdapter;
};

/** A `StreamdClient` for Sui: everything the chain-neutral client does, over a `SuiAdapter` by default. */
export class SuiClient extends StreamdClient {
  protected readonly sui: SuiGrpcClient;

  constructor(config: SuiClientConfig) {
    const given: unknown = config?.sui;
    if (typeof given !== "object" || given === null) {
      throw new StreamdError("SuiClient requires `sui`, a SuiGrpcClient or its options");
    }
    // Structural, so a wrapped or fake client is taken as a client rather than as options.
    const isClient = typeof (given as SuiGrpcClient).ledgerService?.listEvents === "function";
    const sui = isClient ? (given as SuiGrpcClient) : new SuiGrpcClient(given as SuiGrpcClientOptions);
    const { sui: _given, ...rest } = config;
    super({ ...rest, adapter: config.adapter ?? new SuiAdapter(sui) });
    this.sui = sui;
  }
}
