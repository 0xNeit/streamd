import { StreamdError, eventId } from "@streamd/core";
import type { CreationNumberEventDescriptor, EventDescriptor, HandleEventDescriptor, StreamdEvent } from "@streamd/core";
import { address, amount } from "./records.js";
import type { Fields } from "./records.js";
import type { Proposal, ProposalState } from "./types.js";

const GOVERNANCE_EVENTS = "0x1::aptos_governance::GovernanceEvents";
const governanceHandle = (fieldName: string): HandleEventDescriptor => ({
  kind: "handle",
  accountAddress: "0x1",
  eventHandleStruct: GOVERNANCE_EVENTS,
  fieldName,
});
/**
 * The voting forum's handles sit inside `VotingForum<GovernanceProposal>.events`,
 * which a handle path cannot name, so they go by creation number: genesis gives
 * them 5 (create), 6, 7 (resolve) and 8 on mainnet, testnet and devnet alike.
 */
const forumHandle = (creationNumber: bigint): CreationNumberEventDescriptor => ({
  kind: "handle",
  accountAddress: "0x1",
  creationNumber,
});

/**
 * Every walk a tracker folds: the legacy handles, emitted until the
 * module-event migration, then one scan over the module events that replaced
 * them. Each handle is its own walk, never merged with another: handle events
 * carry their sequence number as `eventIndex`, so two handles one transaction
 * wrote would share an id and a merge would drop one.
 */
export const GOVERNANCE_WALKS: readonly EventDescriptor[] = [
  forumHandle(5n),
  governanceHandle("create_proposal_events"),
  governanceHandle("vote_events"),
  forumHandle(7n),
  {
    kind: "module",
    eventType: [
      "0x1::voting::CreateProposal",
      "0x1::aptos_governance::CreateProposal",
      "0x1::aptos_governance::Vote",
      "0x1::voting::ResolveProposal",
    ],
  },
];

const malformed = (event: StreamdEvent) =>
  new StreamdError(`${event.type} at ${eventId(event)} does not have its struct's fields`);

function u(field: unknown, event: StreamdEvent): bigint {
  const value = amount(field);
  if (value === undefined) throw malformed(event);
  return value;
}

function addr(field: unknown, event: StreamdEvent): string {
  const value = address(field);
  if (value === undefined) throw malformed(event);
  return value;
}

/** A Move `Option<u128>`, written `{ vec: [] }` or `{ vec: [value] }`. */
function optionalU(field: unknown, event: StreamdEvent): bigint | undefined {
  const vec = typeof field === "object" && field !== null ? (field as Fields).vec : undefined;
  if (!Array.isArray(vec) || vec.length > 1) throw malformed(event);
  return vec.length === 0 ? undefined : u(vec[0], event);
}

const HEX_BYTES = /^0x(?:[0-9a-fA-F]{2})*$/;
const utf8 = new TextDecoder("utf-8", { fatal: true });

function text(hex: string): string {
  if (!HEX_BYTES.test(hex)) return hex;
  const bytes = new Uint8Array((hex.length - 2) / 2);
  for (let i = 0; i < bytes.length; i++) bytes[i] = parseInt(hex.slice(2 + i * 2, 4 + i * 2), 16);
  try {
    return utf8.decode(bytes);
  } catch {
    return hex;
  }
}

/** A `SimpleMap<String, vector<u8>>`, written `{ data: [{ key, value }] }`. */
function metadata(field: unknown, event: StreamdEvent): Record<string, string> {
  const data = typeof field === "object" && field !== null ? (field as Fields).data : undefined;
  if (!Array.isArray(data)) throw malformed(event);
  const out: Record<string, string> = {};
  for (const entry of data as unknown[]) {
    const { key, value } = (entry ?? {}) as Fields;
    if (typeof key !== "string" || typeof value !== "string") throw malformed(event);
    out[key] = text(value);
  }
  return out;
}

function bool(field: unknown, event: StreamdEvent): boolean {
  if (typeof field !== "boolean") throw malformed(event);
  return field;
}

function hash(field: unknown, event: StreamdEvent): string {
  if (typeof field !== "string") throw malformed(event);
  return field;
}

/** Reads every field before writing any, so a malformed event changes nothing. */
type Apply = (proposal: Proposal, data: Fields, event: StreamdEvent) => void;

const votingCreate: Apply = (p, d, e) => {
  const fields = {
    executionHash: hash(d.execution_hash, e),
    metadata: metadata(d.metadata, e),
    expirationSecs: u(d.expiration_secs, e),
    minVoteThreshold: u(d.min_vote_threshold, e),
    createdAt: e.version,
  };
  const early = optionalU(d.early_resolution_vote_threshold, e);
  Object.assign(p, fields, early === undefined ? {} : { earlyResolutionVoteThreshold: early });
};

const governanceCreate: Apply = (p, d, e) => {
  const [proposer, stakePool] = [addr(d.proposer, e), addr(d.stake_pool, e)];
  Object.assign(p, { proposer, stakePool });
};

const vote: Apply = (p, d, e) => {
  const cast = {
    voter: addr(d.voter, e),
    stakePool: addr(d.stake_pool, e),
    votes: u(d.num_votes, e),
    shouldPass: bool(d.should_pass, e),
    version: e.version,
  };
  p.votes.push(cast);
  if (cast.shouldPass) p.yesVotes += cast.votes;
  else p.noVotes += cast.votes;
};

const resolve: Apply = (p, d, e) => {
  const [yesVotes, noVotes, early] = [u(d.yes_votes, e), u(d.no_votes, e), bool(d.resolved_early, e)];
  if (p.resolution === undefined) p.resolution = { version: e.version, yesVotes, noVotes, early, steps: 1 };
  else p.resolution.steps++;
};

const APPLY: Record<string, Apply> = {
  "0x1::voting::CreateProposalEvent": votingCreate,
  "0x1::voting::CreateProposal": votingCreate,
  "0x1::aptos_governance::CreateProposalEvent": governanceCreate,
  "0x1::aptos_governance::CreateProposal": governanceCreate,
  "0x1::aptos_governance::VoteEvent": vote,
  "0x1::aptos_governance::Vote": vote,
  "0x1::voting::ResolveProposal": resolve,
};

/**
 * Folds one governance event into `proposals`, returning the proposal it
 * changed, or `undefined` for an event it does not read. `fromHandle` says the
 * event came from a handle walk: a module scan also sees legacy handle events
 * (the legacy resolution shares its type with the module event), and those,
 * carrying a `guid`, are left to their handle walk so none is folded twice.
 * Throws a `StreamdError` for an event of a known type without its fields.
 */
export function applyGovernanceEvent(
  proposals: Map<bigint, Proposal>,
  event: StreamdEvent,
  fromHandle: boolean,
): Proposal | undefined {
  const apply = APPLY[event.type];
  if (apply === undefined || (!fromHandle && event.guid !== undefined)) return undefined;
  if (typeof event.data !== "object" || event.data === null) throw malformed(event);
  const data = event.data as Fields;
  const id = u(data.proposal_id, event);
  const proposal = proposals.get(id) ?? { id, yesVotes: 0n, noVotes: 0n, votes: [] };
  apply(proposal, data, event);
  proposals.set(id, proposal);
  return proposal;
}

/**
 * What `0x1::voting::get_proposal_state` would report at `nowSecs` (Unix
 * seconds, the wall clock when omitted): voting closes once `expirationSecs`
 * has passed or either side reaches the early-resolution threshold, and a
 * closed proposal succeeded when yes beats no and together they reach
 * `minVoteThreshold`. A resolution's figures stand in for the votes seen.
 * `undefined` when the creation was not seen.
 */
export function proposalState(
  proposal: Proposal,
  nowSecs: bigint = BigInt(Math.floor(Date.now() / 1000)),
): ProposalState | undefined {
  const { expirationSecs, minVoteThreshold, earlyResolutionVoteThreshold: early } = proposal;
  if (expirationSecs === undefined || minVoteThreshold === undefined) return undefined;
  const yes = proposal.resolution?.yesVotes ?? proposal.yesVotes;
  const no = proposal.resolution?.noVotes ?? proposal.noVotes;
  const closed = nowSecs > expirationSecs || (early !== undefined && (yes >= early || no >= early));
  if (!closed) return "pending";
  return yes > no && yes + no >= minVoteThreshold ? "succeeded" : "failed";
}
