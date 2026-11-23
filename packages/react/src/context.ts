import { createContext, createElement, useContext, type ReactNode } from "react";
import { StreamdError, type StreamdClient } from "@streamd/core";

const ClientContext = createContext<StreamdClient | undefined>(undefined);

export interface StreamdProviderProps {
  client: StreamdClient;
  children?: ReactNode;
}

/** Supplies the `StreamdClient` the hooks below it use when they are not given one. */
export function StreamdProvider({ client, children }: StreamdProviderProps) {
  return createElement(ClientContext.Provider, { value: client }, children);
}

/** The explicit `client` when given, else the nearest `StreamdProvider`'s. */
export function useStreamdClient(client?: StreamdClient): StreamdClient {
  const provided = useContext(ClientContext);
  const resolved = client ?? provided;
  if (resolved === undefined) {
    throw new StreamdError(
      "No StreamdClient: wrap the component in <StreamdProvider client={...}> or pass { client }",
    );
  }
  return resolved;
}

const SHARED = new Set(["balances", "eventsOf", "transfers"]);

/** `useStreamdClient`, refusing a client without `method`, which an `AptosClient` has, and a `SuiClient` for the shared ones. */
export function useClientWith<T extends StreamdClient>(
  method: "activity" | "balances" | "eventsOf" | "explain" | "positions" | "sales" | "swaps" | "transfers",
  client?: T,
): T {
  const resolved = useStreamdClient(client);
  if (typeof (resolved as unknown as Record<string, unknown>)[method] !== "function") {
    const examples = SHARED.has(method) ? "an AptosClient or a SuiClient" : "an AptosClient";
    throw new StreamdError(`This hook needs a client with ${method}, such as ${examples}`);
  }
  return resolved as T;
}
