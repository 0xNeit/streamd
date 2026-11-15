import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { MemoryAdapter, StreamdError, descriptorKey, parseEvent } from "@streamd/core";
import type { EventAdapter, EventDescriptor, FetchOptions, StreamdEvent, ViewRequest } from "@streamd/core";
import { GOVERNANCE_WALKS, applyGovernanceEvent, proposalState } from "../src/governance.js";
import { proposalState as exported } from "../src/index.js";
import type { Proposal } from "../src/types.js";
import type { AptosClient } from "../src/index.js";
import { testClient } from "./helpers.js";

const long = (hex: string) => `0x${hex.padStart(64, "0")}`;
const POOL_A = long("a");
const POOL_B = long("b");
const hex = (s: string) => `0x${Buffer.from(s).toString("hex")}`;

function ev(version: number, eventIndex: number, type: string, data: unknown, legacy = false): StreamdEvent {
  return {
    type,
    data,
    version: BigInt(version),
    sequenceNumber: 0n,
    eventIndex,
    ...(legacy && { guid: { creationNumber: 7n, accountAddress: "0x1" } }),
  };
}
const votingCreate = (v: number, id: string, early: string[] = [], legacy = false) =>
  ev(v, 0, `0x1::voting::CreateProposal${legacy ? "Event" : ""}`, {
    proposal_id: id,
    early_resolution_vote_threshold: { vec: early },
    execution_hash: "0xabcd",
    expiration_secs: "1000",
    metadata: { data: [{ key: "metadata_location", value: hex("https://example.com/p.json") }] },
    min_vote_threshold: "100",
  }, legacy);
const governanceCreate = (v: number, id: string) =>
  ev(v, 1, "0x1::aptos_governance::CreateProposal", { proposer: "0xc", stake_pool: "0xa", proposal_id: id, execution_hash: "0xabcd", proposal_metadata: { data: [] } });
const vote = (v: number, id: string, pool: string, votes: string, yes: boolean, legacy = false) =>
  ev(v, 0, `0x1::aptos_governance::Vote${legacy ? "Event" : ""}`, { proposal_id: id, voter: pool, stake_pool: pool, num_votes: votes, should_pass: yes }, legacy);
const resolve = (v: number, id: string, yes: string, no: string, legacy = false) =>
  ev(v, 0, "0x1::voting::ResolveProposal", { proposal_id: id, yes_votes: yes, no_votes: no, resolved_early: false }, legacy);

function fold(events: [StreamdEvent, boolean][]): Map<bigint, Proposal> {
  const map = new Map<bigint, Proposal>();
  for (const [event, fromHandle] of events) applyGovernanceEvent(map, event, fromHandle);
  return map;
}

describe("applyGovernanceEvent", () => {
  it("builds a proposal from its creation pair, votes and resolution", () => {
    const map = fold([
      [votingCreate(10, "3"), false],
      [governanceCreate(10, "3"), false],
      [vote(11, "3", "0xa", "60", true), false],
      [vote(12, "3", "0xb", "25", false), false],
      [vote(13, "3", "0xa", "15", true), false],
      [resolve(20, "3", "75", "25"), false],
    ]);
    const p = map.get(3n)!;
    expect(p).toMatchObject({
      id: 3n,
      proposer: long("c"),
      stakePool: POOL_A,
      executionHash: "0xabcd",
      metadata: { metadata_location: "https://example.com/p.json" },
      expirationSecs: 1000n,
      minVoteThreshold: 100n,
      createdAt: 10n,
      yesVotes: 75n,
      noVotes: 25n,
      resolution: { version: 20n, yesVotes: 75n, noVotes: 25n, early: false, steps: 1 },
    });
    expect(p.earlyResolutionVoteThreshold).toBeUndefined();
    expect(p.votes).toEqual([
      { voter: POOL_A, stakePool: POOL_A, votes: 60n, shouldPass: true, version: 11n },
      { voter: POOL_B, stakePool: POOL_B, votes: 25n, shouldPass: false, version: 12n },
      { voter: POOL_A, stakePool: POOL_A, votes: 15n, shouldPass: true, version: 13n },
    ]);
  });

  it("folds the legacy handle events the same way", () => {
    const map = fold([
      [votingCreate(10, "0", ["500"], true), true],
      [vote(11, "0", "0xa", "7", true, true), true],
      [resolve(12, "0", "7", "0", true), true],
    ]);
    expect(map.get(0n)).toMatchObject({ earlyResolutionVoteThreshold: 500n, yesVotes: 7n, resolution: { steps: 1 } });
  });

  it("folds votes seen before their creation, in any walk order", () => {
    const map = fold([
      [vote(11, "4", "0xa", "5", true), false],
      [resolve(20, "4", "9", "1"), true],
      [votingCreate(10, "4"), true],
    ]);
    expect(map.get(4n)).toMatchObject({ yesVotes: 5n, expirationSecs: 1000n, resolution: { yesVotes: 9n } });
  });

  it("counts a multi-step proposal's resolutions and keeps the first version", () => {
    const map = fold([20, 21, 22].map((v) => [resolve(v, "5", "9", "1"), false]));
    expect(map.get(5n)!.resolution).toEqual({ version: 20n, yesVotes: 9n, noVotes: 1n, early: false, steps: 3 });
  });

  it("keeps votes and the first resolution in version order whichever walk folds first", () => {
    const map = fold([
      [vote(13, "7", "0xb", "1", true), false],
      [resolve(22, "7", "9", "1"), false],
      [vote(11, "7", "0xa", "2", true, true), true],
      [vote(13, "7", "0xc", "3", false, true), true],
      [resolve(20, "7", "8", "2", true), true],
    ]);
    expect(map.get(7n)!.votes.map((v) => [v.version, v.votes])).toEqual([[11n, 2n], [13n, 1n], [13n, 3n]]);
    expect(map.get(7n)!.resolution).toEqual({ version: 20n, yesVotes: 8n, noVotes: 2n, early: false, steps: 2 });
  });

  it("keeps a metadata key named __proto__", () => {
    const create = votingCreate(10, "1");
    (create.data as { metadata: unknown }).metadata = { data: [{ key: "__proto__", value: hex("x") }] };
    const metadata = fold([[create, false]]).get(1n)!.metadata!;
    expect(Object.keys(metadata)).toEqual(["__proto__"]);
    expect(metadata["__proto__"]).toBe("x");
  });

  it("leaves a legacy resolution seen by the module scan to its handle walk", () => {
    const legacy = resolve(20, "6", "9", "1", true);
    const map = fold([[legacy, false], [legacy, true]]);
    expect(map.get(6n)!.resolution!.steps).toBe(1);
    expect(fold([[legacy, false]]).size).toBe(0);
  });

  it("ignores other event types", () => {
    const map = fold([[ev(1, 0, "0x1::voting::Vote", { proposal_id: "1", num_votes: "5" }), false]]);
    expect(map.size).toBe(0);
  });

  it("keeps metadata that is not printable UTF-8 as written", () => {
    const create = votingCreate(10, "1");
    const data = [{ key: "k", value: "0xff" }, { key: "odd", value: "0xabc" }, { key: "flag", value: "0x01" }, { key: "empty", value: "0x" }];
    (create.data as { metadata: unknown }).metadata = { data };
    expect(fold([[create, false]]).get(1n)!.metadata).toEqual({ k: "0xff", odd: "0xabc", flag: "0x01", empty: "" });
  });

  it("throws for a known event without its fields, changing nothing", () => {
    const map = fold([[vote(11, "2", "0xa", "5", true), false]]);
    const bad = vote(12, "2", "0xb", "-1", true);
    expect(() => applyGovernanceEvent(map, bad, false)).toThrow(StreamdError);
    expect(() => applyGovernanceEvent(map, bad, false)).toThrow("0x1::aptos_governance::Vote at 12:0 does not have its struct's fields");
    expect(() => applyGovernanceEvent(map, ev(1, 0, "0x1::voting::ResolveProposal", null), false)).toThrow(StreamdError);
    const create = votingCreate(10, "2", ["1", "2"]);
    expect(() => applyGovernanceEvent(map, create, false)).toThrow(StreamdError);
    expect(map.get(2n)).toMatchObject({ yesVotes: 5n, votes: [{ version: 11n }] });
    expect(map.get(2n)!.expirationSecs).toBeUndefined();
  });
});

describe("proposalState", () => {
  const created = (early: string[] = []) => fold([[votingCreate(10, "1", early), false]]).get(1n)!;
  const withVotes = (p: Proposal, yes: bigint, no: bigint) => ({ ...p, yesVotes: yes, noVotes: no });

  it("is pending until expiry, then succeeded or failed by the vote", () => {
    const p = created();
    expect(proposalState(withVotes(p, 90n, 10n), 1000n)).toBe("pending");
    expect(proposalState(withVotes(p, 90n, 10n), 1001n)).toBe("succeeded");
    expect(proposalState(withVotes(p, 50n, 50n), 1001n)).toBe("failed");
    expect(proposalState(withVotes(p, 60n, 30n), 1001n)).toBe("failed");
  });

  it("closes early once either side reaches the early threshold", () => {
    const p = created(["80"]);
    expect(proposalState(withVotes(p, 80n, 30n), 0n)).toBe("succeeded");
    expect(proposalState(withVotes(p, 20n, 80n), 0n)).toBe("failed");
    expect(proposalState(withVotes(p, 79n, 30n), 0n)).toBe("pending");
  });

  it("takes a resolution's figures over the votes seen", () => {
    const p = { ...created(), resolution: { version: 20n, yesVotes: 90n, noVotes: 10n, early: false, steps: 1 } };
    expect(proposalState(p, 1001n)).toBe("succeeded");
  });

  it("is undefined without the creation, and reads the wall clock by default", () => {
    expect(proposalState({ id: 1n, yesVotes: 1n, noVotes: 0n, votes: [] }, 0n)).toBeUndefined();
    expect(proposalState(withVotes(created(), 90n, 10n))).toBe("succeeded");
  });
});

/**
 * `0x1`'s forum as the views see it, given the module resolutions it really
 * made: an id is resolved from its last one on, and its execution hash moves
 * at each.
 */
function forumViews(genuine: StreamdEvent[]) {
  return (request: ViewRequest, version: bigint): unknown[] => {
    const id = String(request.functionArguments?.[1]);
    const steps = genuine.filter((e) => String((e.data as { proposal_id: string }).proposal_id) === id).map((e) => e.version);
    switch (request.function) {
      case "0x1::voting::next_proposal_id":
        return ["100"];
      case "0x1::voting::is_resolved":
        return [steps.length > 0 && version >= steps.at(-1)!];
      case "0x1::voting::get_execution_hash":
        return [`0x${steps.filter((v) => v <= version).length}`];
      default:
        throw new StreamdError(`unexpected view ${request.function}`);
    }
  };
}

const isModuleResolve = (e: StreamdEvent) => e.type === "0x1::voting::ResolveProposal" && e.guid === undefined;

/**
 * One `MemoryAdapter` per walk, in `GOVERNANCE_WALKS` order, so each handle serves only its
 * own events, with `0x1`'s forum views over `genuine` (the module walk's resolutions by default).
 */
function walks(sets: StreamdEvent[][] = [], genuine?: StreamdEvent[]) {
  const memories = GOVERNANCE_WALKS.map((_, i) => new MemoryAdapter(sets[i] ?? []));
  const keys = GOVERNANCE_WALKS.map(descriptorKey);
  const asked: [EventDescriptor, FetchOptions][] = [];
  const views: string[] = [];
  const answer = forumViews(genuine ?? (sets[4] ?? []).filter(isModuleResolve));
  const adapter: EventAdapter = {
    fetchEvents: (d, o) => (asked.push([d, o]), memories[keys.indexOf(descriptorKey(d))]!.fetchEvents(d, o)),
    view: async (request, version) => (views.push(`${request.function}:${String(request.functionArguments?.[1])}`), answer(request, version)),
  };
  return { adapter, memories, asked, views };
}
const until = async (done: () => boolean) => {
  for (let i = 0; i < 500; i++) {
    if (done()) return;
    await new Promise((r) => setTimeout(r, 2));
  }
  throw new Error("timed out waiting");
};
const settle = () => new Promise((r) => setTimeout(r, 50));
/** An event for a fresh proposal 9 into every walk, so a walk still following would fold it. */
function pushToEveryWalk(memories: MemoryAdapter[]) {
  memories[0]!.push(votingCreate(900, "9", [], true));
  memories[1]!.push(ev(900, 0, "0x1::aptos_governance::CreateProposalEvent", { proposer: "0xc", stake_pool: "0xa", proposal_id: "9", execution_hash: "0x", proposal_metadata: { data: [] } }, true));
  memories[2]!.push(vote(900, "9", "0xa", "1", true, true));
  memories[3]!.push(resolve(900, "9", "1", "0", true));
  memories[4]!.push(vote(900, "9", "0xb", "1", false));
}
const sorted = (keys: Iterable<bigint>) => [...keys].sort((a, b) => (a < b ? -1 : 1));
const LEGACY = [
  [votingCreate(10, "0", [], true)],
  [ev(10, 0, "0x1::aptos_governance::CreateProposalEvent", { proposer: "0xc", stake_pool: "0xa", proposal_id: "0", execution_hash: "0x", proposal_metadata: { data: [] } }, true)],
  [vote(11, "0", "0xa", "70", true, true), vote(12, "0", "0xb", "40", false, true)],
  [resolve(30, "0", "70", "40", true)],
];

describe("proposals", () => {
  it("folds the legacy handles and the module scan into one map, as no listener", async () => {
    const { adapter, asked } = walks([...LEGACY, [votingCreate(40, "1"), governanceCreate(40, "1"), vote(41, "1", "0xa", "5", true), resolve(42, "0", "1", "1", true)]]);
    const client = testClient({ adapter });
    const tracker = client.proposals({ batchSize: 7 });
    expect(client.listenerCount()).toBe(0);
    const value = await tracker.ready;
    expect(sorted(value.keys())).toEqual([0n, 1n]);
    expect(tracker.get(0n)).toMatchObject({ proposer: long("c"), yesVotes: 70n, noVotes: 40n, resolution: { steps: 1 } });
    expect(tracker.get(1n)).toMatchObject({ createdAt: 40n, yesVotes: 5n, votes: [{ stakePool: POOL_A }] });
    expect(exported(tracker.get(0n)!, 1001n)).toBe("succeeded");
    expect(new Set(asked.map(([d]) => descriptorKey(d)))).toEqual(new Set(GOVERNANCE_WALKS.map(descriptorKey)));
    expect(asked.every(([, o]) => o.limit === 7)).toBe(true);
    expect(tracker.active).toBe(true);
    tracker.unsubscribe();
    expect(tracker.active).toBe(false);
    client.destroy();
  });

  it("keeps a voting creation only beside governance's, and a resolution only when 0x1's forum shows it", async () => {
    const genuineResolve = resolve(70, "1", "9", "1");
    const forgedResolve = resolve(60, "1", "0", "9");
    const forgedCreate = { ...votingCreate(50, "1"), data: { ...(votingCreate(50, "1").data as object), expiration_secs: "5", metadata: { data: [{ key: "metadata_location", value: hex("https://evil.example") }] } } };
    const { adapter, views } = walks(
      [[], [], [], [], [votingCreate(40, "1"), governanceCreate(40, "1"), forgedCreate, forgedResolve, resolve(65, "5000", "1", "0"), genuineResolve]],
      [genuineResolve],
    );
    const client = testClient({ adapter });
    const tracker = client.proposals();
    await tracker.ready;
    expect(sorted(tracker.value.keys())).toEqual([1n]);
    expect(tracker.get(1n)).toMatchObject({
      expirationSecs: 1000n,
      createdAt: 40n,
      metadata: { metadata_location: "https://example.com/p.json" },
      resolution: { version: 70n, yesVotes: 9n, noVotes: 1n, steps: 1 },
    });
    expect(views).toContain("0x1::voting::next_proposal_id:undefined");
    expect(views.some((v) => v.endsWith(":5000"))).toBe(false);
    client.destroy();
  });

  it("refuses an adapter without view at the call", () => {
    const client = testClient({ adapter: new MemoryAdapter() });
    expect(() => client.proposals()).toThrow("proposals needs an adapter that implements view");
    client.destroy();
  });

  it("follows live, reporting each change through onChange", async () => {
    const { adapter, memories } = walks(LEGACY);
    const client = testClient({ adapter });
    const changes: [bigint, bigint, bigint][] = [];
    const errors: Error[] = [];
    const tracker = client.proposals({ pollInterval: 1, onError: (e) => errors.push(e), onChange: (p, e) => changes.push([p.id, p.yesVotes, e.version]) });
    await tracker.ready;
    expect(changes).toEqual([]);
    memories[4]!.push(vote(50, "0", "0xd", "3", true), ev(50, 1, "0x1::voting::Vote", { proposal_id: "0", num_votes: "3" }));
    await until(() => changes.length > 0);
    await settle();
    expect(changes).toEqual([[0n, 73n, 50n]]);
    expect(tracker.value.get(0n)!.votes.at(-1)).toMatchObject({ voter: long("d"), version: 50n });
    expect(errors).toEqual([]);
    tracker.unsubscribe();
    client.destroy();
  });

  it("resolves an empty map when there are no governance events", async () => {
    const client = testClient({ adapter: walks().adapter });
    expect((await client.proposals().ready).size).toBe(0);
    client.destroy();
  });

  it("folds nothing below fromVersion, in the handles and the module scan", async () => {
    const module = [vote(11, "0", "0xc", "1000", true), vote(13, "0", "0xd", "1", true)];
    const client = testClient({ adapter: walks([...LEGACY, module]).adapter });
    const tracker = client.proposals({ fromVersion: 12n });
    await tracker.ready;
    expect(tracker.get(0n)).toMatchObject({ yesVotes: 1n, noVotes: 40n, resolution: { version: 30n } });
    expect(tracker.get(0n)!.createdAt).toBeUndefined();
    client.destroy();
  });

  it("refuses an unknown option at the call", () => {
    const client = testClient({ adapter: walks().adapter });
    expect(() => client.proposals({ where: () => true } as never)).toThrow("Unsupported proposals option: where");
    client.destroy();
  });

  it("keeps every walk running through a transient error", async () => {
    const { adapter, memories } = walks(LEGACY);
    let flaky = false;
    const client = testClient({
      adapter: {
        ...adapter,
        fetchEvents: (d, o) => {
          if (flaky && descriptorKey(d) === descriptorKey(GOVERNANCE_WALKS[3]!)) {
            flaky = false;
            return Promise.reject(new Error("timeout"));
          }
          return adapter.fetchEvents(d, o);
        },
      },
    });
    const errors: string[] = [];
    const tracker = client.proposals({ pollInterval: 1, maxErrors: 3, onError: (e) => errors.push(e.message) });
    await tracker.ready;
    flaky = true;
    await until(() => errors.length > 0);
    pushToEveryWalk(memories);
    await until(() => tracker.get(9n)?.resolution !== undefined && tracker.get(9n)!.votes.length === 2);
    expect(errors).toHaveLength(1);
    expect(tracker.active).toBe(true);
    client.destroy();
  });

  it("stops every walk when one fails, in the backfill or live, reporting once", async () => {
    const { adapter, memories } = walks(LEGACY);
    let down: "backfill" | "live" | undefined = "backfill";
    const broken: EventAdapter = {
      ...adapter,
      fetchEvents: (d, o) =>
        down !== undefined && descriptorKey(d) === descriptorKey(GOVERNANCE_WALKS[3]!)
          ? Promise.reject(new StreamdError(`node down in the ${down}`))
          : adapter.fetchEvents(d, o),
    };
    const reported: string[] = [];
    const client = testClient({ adapter: broken, onError: (e) => reported.push(e.message) });
    const failing = client.proposals();
    await expect(failing.ready).rejects.toThrow("node down in the backfill");
    expect(reported).toEqual(["node down in the backfill"]);
    expect(failing.active).toBe(false);

    down = undefined;
    const tracker = client.proposals({ pollInterval: 1, maxErrors: 1 });
    await tracker.ready;
    down = "live";
    await until(() => reported.length > 1);
    expect(reported.slice(1)).toEqual(["Polling stopped on invalid input: node down in the live"]);
    expect(tracker.active).toBe(false);
    pushToEveryWalk(memories);
    await settle();
    expect(tracker.get(9n)).toBeUndefined();
    client.destroy();
  });

  it("stops on a malformed event, naming it", async () => {
    const client = testClient({ adapter: walks([[], [], [vote(11, "0", "0xa", "x", true, true)]]).adapter });
    const errors: Error[] = [];
    const tracker = client.proposals({ onError: (e) => errors.push(e) });
    await expect(tracker.ready).rejects.toThrow();
    expect(errors.map((e) => (e.cause as Error).message)).toEqual(["0x1::aptos_governance::VoteEvent at 11:0 does not have its struct's fields"]);
    expect(tracker.active).toBe(false);
    client.destroy();
  });

  it("stops every walk on unsubscribe, dispose, abort and destroy", async () => {
    const stops: [string, (t: ReturnType<AptosClient["proposals"]>, c: AptosClient, a: AbortController) => void][] = [
      ["unsubscribe", (t) => t.unsubscribe()],
      ["dispose", (t) => t[Symbol.dispose]()],
      ["abort", (_, __, a) => a.abort()],
      ["destroy", (_, c) => c.destroy()],
    ];
    for (const [name, stop] of stops) {
      const { adapter, memories } = walks(LEGACY);
      const client = testClient({ adapter });
      const controller = new AbortController();
      const tracker = client.proposals({ pollInterval: 1, signal: controller.signal });
      await tracker.ready;
      stop(tracker, client, controller);
      expect([name, tracker.active]).toEqual([name, false]);
      pushToEveryWalk(memories);
      await settle();
      expect([name, tracker.get(9n)]).toEqual([name, undefined]);
      client.destroy();
    }
  });
});

describe("proposals on recorded mainnet events", () => {
  // Read through the archive: proposal 134 (legacy handles, the module-event migration) with
  // its first and last votes, and module-era 205 (seven resolution steps) and 206, each with
  // its creation pair, its proposer's vote and the resolutions its proposer sent. The expected
  // values are what the 0x1::voting views returned at the head on 2026-11-15.
  const lines = readFileSync(join(import.meta.dir, "fixtures/governance.jsonl"), "utf8").trim().split("\n");
  const byWalk: StreamdEvent[][] = GOVERNANCE_WALKS.map(() => []);
  const walkOf: Record<string, number> = { "5": 0, "9": 1, "11": 2, "7": 3 };
  for (const line of lines) {
    const event = parseEvent(line);
    byWalk[event.guid === undefined ? 4 : walkOf[String(event.guid.creationNumber)]!]!.push(event);
  }
  const VIEWS = {
    134: { expirationSecs: 1748777742n, early: 57891609148426939n, yes: 31349781482383443n, no: 226631333512n, proposer: "0x201cf09644cd5d88aa6db2d1670011325eea2c3198ddfd0c1aa549be0003bb24", steps: 1 },
    205: { expirationSecs: 1790904050n, early: 60469361680466003n, yes: 38035733758228756n, no: 83960229647n, proposer: "0xdb009ab1a3259c4b27a0d8ff9d0e913e13e4c8b657fc73768f4e9bb811c7a1d8", steps: 7 },
    206: { expirationSecs: 1791301397n, early: 60480623643350086n, yes: 36674003480944999n, no: 85668843140n, proposer: "0xdb009ab1a3259c4b27a0d8ff9d0e913e13e4c8b657fc73768f4e9bb811c7a1d8", steps: 2 },
  } as const;

  // The forum views each module resolution was checked with, read from the archive at its
  // version and the one before, keyed `function:proposal id:version`.
  const recorded: Record<string, unknown[]> = JSON.parse(readFileSync(join(import.meta.dir, "fixtures/governance-views.json"), "utf8"));
  const view = async (request: ViewRequest, version: bigint) => {
    const key = `${request.function.split("::")[2]}:${request.functionArguments!.slice(1).join(",")}:${version}`;
    if (!(key in recorded)) throw new StreamdError(`no recorded view ${key}`);
    return recorded[key]!;
  };

  it("matches the voting views for each proposal", async () => {
    const client = testClient({ adapter: { ...walks(byWalk).adapter, view } });
    const tracker = client.proposals();
    const value = await tracker.ready;
    expect(sorted(value.keys())).toEqual([134n, 205n, 206n]);
    for (const [id, view] of Object.entries(VIEWS)) {
      const p = value.get(BigInt(id))!;
      expect(p).toMatchObject({
        proposer: view.proposer,
        expirationSecs: view.expirationSecs,
        minVoteThreshold: 30000000000000000n,
        earlyResolutionVoteThreshold: view.early,
        resolution: { yesVotes: view.yes, noVotes: view.no, early: false, steps: view.steps },
      });
      expect(p.metadata!.metadata_location).toStartWith("https://raw.githubusercontent.com/");
      expect(p.metadata!.IS_MULTI_STEP_PROPOSAL_KEY).toBe("0x01");
      expect(proposalState(p, 1794737473n)).toBe("succeeded");
    }
    expect(value.get(134n)!.executionHash).toBe("0x60ee085d13021a9d4e5202e0b975bbaedae70715e94eadf7c1c4c56311726c75");
    expect(value.get(134n)!.votes.map((v) => v.votes)).toEqual([1133893608n, 1230600340n]);
    expect(value.get(205n)!.resolution!.version).toBe(7454755509n);
    expect(value.get(206n)!.votes).toEqual([
      { voter: VIEWS[206].proposer, stakePool: "0x0324df1e27c4129a58d73851ae0e9366064dc666a73e747051e203694a4cb257", votes: 1440741395772259n, shouldPass: true, version: 7469427001n },
    ]);
    client.destroy();
  });
});
