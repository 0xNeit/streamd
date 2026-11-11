import { createContext, createElement, useContext, type ReactNode } from "react";
import { StreamdError, type StreamdClient } from "@streamd/core";
import type { AptosClient } from "@streamd/aptos";

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

/** `useStreamdClient`, refusing a client without the Aptos methods. */
export function useAptosClient(client?: AptosClient): AptosClient {
  const resolved = useStreamdClient(client);
  if (typeof (resolved as { eventsOf?: unknown }).eventsOf !== "function") {
    throw new StreamdError("This hook needs an AptosClient from @streamd/aptos");
  }
  return resolved as AptosClient;
}
