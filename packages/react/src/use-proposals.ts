import type { StreamdClient } from "@streamd/core";
import type { Proposal, Proposals, ProposalsOptions } from "@streamd/aptos";
import { useClientWith } from "./context.js";
import { useMonitor, type MonitorStatus } from "./monitor.js";

/** A client that tracks governance proposals: an `AptosClient`, or one shaped like it. */
export interface ProposalsClient extends StreamdClient {
  proposals(options?: ProposalsOptions): Proposals;
}

/** `proposals` options, minus `signal`: the hook stops it on unmount and on input change. */
export interface UseProposalsOptions extends Omit<ProposalsOptions, "signal"> {
  /** Use this client instead of the nearest `StreamdProvider`'s. */
  client?: ProposalsClient;
}

export interface UseProposalsResult {
  /** Every proposal seen, by id; a copy, so a changed proposal is a new object and the rest keep theirs. */
  proposals: ReadonlyMap<bigint, Proposal>;
  /** The last error, cleared when a proposal next changes. */
  error: Error | undefined;
  status: MonitorStatus;
}

const EMPTY: ReadonlyMap<bigint, Proposal> = new Map();

/** `client.proposals(options)` for a component, restarting only when the options change by value. */
export function useProposals(options: UseProposalsOptions = {}): UseProposalsResult {
  const { client: explicit, ...listener } = options;
  const client = useClientWith<ProposalsClient>("proposals", explicit);
  const { value, error, status } = useMonitor({
    client,
    enabled: true,
    inputs: null,
    options: listener,
    start: (opts: ProposalsOptions) => client.proposals(opts),
    snapshot: (handle: Proposals) => structuredClone(handle.value),
    apply: (previous: ReadonlyMap<bigint, Proposal>, proposal: Proposal) => new Map(previous).set(proposal.id, structuredClone(proposal)),
    empty: EMPTY,
  });
  return { proposals: value, error, status };
}
