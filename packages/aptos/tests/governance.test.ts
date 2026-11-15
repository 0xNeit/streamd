import { describe, expect, it } from "bun:test";
import { StreamdError } from "@streamd/core";
import type { StreamdEvent } from "@streamd/core";
import { applyGovernanceEvent, proposalState } from "../src/governance.js";
import type { Proposal } from "../src/types.js";

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

  it("keeps metadata that is not UTF-8 as written", () => {
    const create = votingCreate(10, "1");
    (create.data as { metadata: unknown }).metadata = { data: [{ key: "k", value: "0xff" }, { key: "odd", value: "0xabc" }] };
    expect(fold([[create, false]]).get(1n)!.metadata).toEqual({ k: "0xff", odd: "0xabc" });
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
