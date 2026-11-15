import { RequestLimiter, StreamdError } from "@streamd/core";
import type {
  CreationNumberEventDescriptor,
  EventAdapter,
  EventDescriptor,
  HandleEventDescriptor,
  ReduceOptions,
  Reducer,
  Reduction,
  StreamdEvent,
} from "@streamd/core";
import { addr, amount, malformed, pageSource, u } from "./records.js";
import type { Fields } from "./records.js";
import type { Proposal, Proposals, ProposalsOptions, ProposalState } from "./types.js";

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

/** A Move `Option<u128>`, written `{ vec: [] }` or `{ vec: [value] }`. */
function optionalU(field: unknown, event: StreamdEvent): bigint | undefined {
  const vec = typeof field === "object" && field !== null ? (field as Fields).vec : undefined;
  if (!Array.isArray(vec) || vec.length > 1) throw malformed(event);
  return vec.length === 0 ? undefined : u(vec[0], event);
}

const HEX_BYTES = /^0x(?:[0-9a-fA-F]{2})*$/;
const CONTROL = /[\u0000-\u001f\u007f-\u009f]/;
const utf8 = new TextDecoder("utf-8", { fatal: true });

/** The bytes as text, or `hex` as written when they are not printable UTF-8 (a BCS `bool` flag is `0x01`). */
function text(hex: string): string {
  if (!HEX_BYTES.test(hex)) return hex;
  const bytes = new Uint8Array((hex.length - 2) / 2);
  for (let i = 0; i < bytes.length; i++) bytes[i] = parseInt(hex.slice(2 + i * 2, 4 + i * 2), 16);
  try {
    const decoded = utf8.decode(bytes);
    return CONTROL.test(decoded) ? hex : decoded;
  } catch {
    return hex;
  }
}

/** A `SimpleMap<String, vector<u8>>`, written `{ data: [{ key, value }] }`. */
function metadata(field: unknown, event: StreamdEvent): Record<string, string> {
  const data = typeof field === "object" && field !== null ? (field as Fields).data : undefined;
  if (!Array.isArray(data)) throw malformed(event);
  return Object.fromEntries(
    (data as unknown[]).map((entry) => {
      const { key, value } = (entry ?? {}) as Fields;
      if (typeof key !== "string" || typeof value !== "string") throw malformed(event);
      return [key, text(value)];
    }),
  );
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
  // Walks fold independently, so a proposal open across the migration gets its
  // legacy and module votes in fold order; keep them in version order.
  let at = p.votes.length;
  while (at > 0 && p.votes[at - 1]!.version > cast.version) at--;
  p.votes.splice(at, 0, cast);
  if (cast.shouldPass) p.yesVotes += cast.votes;
  else p.noVotes += cast.votes;
};

const resolve: Apply = (p, d, e) => {
  const [yesVotes, noVotes, early] = [u(d.yes_votes, e), u(d.no_votes, e), bool(d.resolved_early, e)];
  if (p.resolution === undefined) p.resolution = { version: e.version, yesVotes, noVotes, early, steps: 1 };
  else {
    p.resolution.steps++;
    if (e.version < p.resolution.version) Object.assign(p.resolution, { version: e.version, yesVotes, noVotes, early });
  }
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

const VOTING_CREATE = "0x1::voting::CreateProposal";
const GOVERNANCE_CREATE = "0x1::aptos_governance::CreateProposal";
const RESOLVE = "0x1::voting::ResolveProposal";
const FORUM: readonly string[] = ["0x1::governance_proposal::GovernanceProposal"];

/** A `0x1::voting` view of `0x1`'s governance forum at `version`. */
async function forum(adapter: EventAdapter, fn: string, args: readonly unknown[], version: bigint): Promise<unknown> {
  const [value] = await adapter.view!(
    { function: `0x1::voting::${fn}`, typeArguments: FORUM, functionArguments: ["0x1", ...args] },
    version,
  );
  return value;
}

/** The creation `aptos_governance` emits right after `voting`'s own, in the same transaction. */
function governed(create: StreamdEvent, next: StreamdEvent | undefined): boolean {
  return (
    next !== undefined &&
    next.type === GOVERNANCE_CREATE &&
    next.version === create.version &&
    next.eventIndex === create.eventIndex + 1 &&
    (next.data as Fields | null)?.proposal_id === (create.data as Fields | null)?.proposal_id
  );
}

/**
 * Whether `0x1`'s forum shows this resolution: the proposal existed there
 * before the event's version and, at it, either became resolved (a last
 * step) or took its next step's execution hash. Asking for the id first, at
 * the version before, keeps a forged id the forum lacks then (past its end,
 * or created in the same transaction) from aborting the views; no proposal
 * resolves in the transaction that creates it.
 */
async function resolvedHere(adapter: EventAdapter, event: StreamdEvent): Promise<boolean> {
  const id = (event.data as Fields | null)?.proposal_id;
  const { version } = event;
  // Left for the fold to refuse as malformed.
  if (amount(id) === undefined) return true;
  const next = amount(await forum(adapter, "next_proposal_id", [], version - 1n));
  if (next === undefined || amount(id)! >= next) return false;
  if ((await forum(adapter, "is_resolved", [id], version)) === true) {
    return (await forum(adapter, "is_resolved", [id], version - 1n)) !== true;
  }
  const [after, before] = await Promise.all([
    forum(adapter, "get_execution_hash", [id], version),
    forum(adapter, "get_execution_hash", [id], version - 1n),
  ]);
  return after !== before;
}

/**
 * The module walk's source. `0x1::voting` is a library any account can host a
 * forum on, and its events name no forum, so a creation is kept only beside
 * `aptos_governance`'s, which only governance emits, and a resolution only
 * when `0x1`'s forum shows it, by up to four views at its version and the one
 * before, at most two at a time. `aptos_governance` events need no check.
 */
export function governanceSource(adapter: EventAdapter, requestTimeout?: number): EventAdapter {
  if (typeof adapter.view !== "function") {
    throw new StreamdError("proposals needs an adapter that implements view");
  }
  // Outside the client's limiter, as transfers' store reads are: the page
  // asking holds a slot, so views queued behind it could wait forever. Timed,
  // so a view that never settles fails the page rather than hanging it.
  const views = new RequestLimiter(2, Infinity, requestTimeout).wrap(adapter);
  return pageSource(adapter, async (events) => {
    const kept: StreamdEvent[] = [];
    // ponytail: pairs within one page, which holds whole transactions on
    // FullnodeScanAdapter; a source splitting a transaction across pages
    // would drop that creation.
    for (const [i, event] of events.entries()) {
      if (event.guid === undefined && event.type === VOTING_CREATE && !governed(event, events[i + 1])) continue;
      if (event.guid === undefined && event.type === RESOLVE && !(await resolvedHere(views, event))) continue;
      kept.push(event);
    }
    return kept;
  });
}

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

const ALLOWED: readonly string[] = [
  "fromVersion",
  "pollInterval",
  "batchSize",
  "maxErrors",
  "maxBackoff",
  "maxPollInterval",
  "retries",
  "signal",
  "onError",
  "onChange",
] satisfies readonly (keyof ProposalsOptions)[];

/** `client.reduce()`, as the tracker calls it. */
export type Reduce = (
  descriptor: EventDescriptor,
  reducer: Reducer<null>,
  initial: null,
  options: ReduceOptions<null>,
) => Reduction<null>;

/**
 * `AptosClient.proposals()`: one reduction per walk, all folding into one
 * map. Folding is order-free across walks (sums and field sets per proposal),
 * so each walk backfills and follows on its own cursor; one stopping stops all,
 * since the map would otherwise go on without that walk's events.
 */
export function startProposals(
  reduce: Reduce,
  clientOnError: ((error: Error) => void) | undefined,
  options: ProposalsOptions = {},
): Proposals {
  const refused = Object.keys(options).find((k) => !ALLOWED.includes(k));
  if (refused !== undefined) {
    throw new StreamdError(`Unsupported proposals option: ${refused} (allowed: ${ALLOWED.join(", ")})`);
  }
  const { onChange, onError, ...rest } = options;
  const report = (err: Error) => (onError ?? clientOnError)?.(err);
  const proposals = new Map<bigint, Proposal>();
  const walks: Reduction<null>[] = [];
  const stop = () => walks.forEach((walk) => walk.unsubscribe());

  for (const descriptor of GOVERNANCE_WALKS) {
    const fromHandle = descriptor.kind === "handle";
    let changed: Proposal | undefined;
    const walk = reduce(
      descriptor,
      (state, event) => {
        changed = applyGovernanceEvent(proposals, event, fromHandle);
        return state;
      },
      null,
      {
        ...rest,
        onChange: (_, event) => {
          if (changed !== undefined) onChange?.(changed, event);
        },
        onError: (err) => {
          if (walks.some((w) => !w.active)) stop();
          report(err);
        },
      },
    );
    walks.push(walk);
  }

  const ready = Promise.all(walks.map((walk) => walk.ready)).then(
    () => proposals as ReadonlyMap<bigint, Proposal>,
    (err: unknown) => {
      stop();
      throw err;
    },
  );
  ready.catch(() => {});

  return {
    value: proposals,
    get: (id) => proposals.get(id),
    ready,
    get active() {
      return walks.every((walk) => walk.active);
    },
    unsubscribe: stop,
    [Symbol.dispose ?? Symbol.for("Symbol.dispose")]: stop,
  } as Proposals;
}
