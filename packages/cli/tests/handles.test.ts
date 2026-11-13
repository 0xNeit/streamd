import { describe, expect, it } from "bun:test";
import type { EventHandleInfo } from "@streamd/aptos";
import { StreamdError } from "@streamd/core";
import { handleFlags, runHandles } from "../src/handles.js";
import type { HandlesIO } from "../src/handles.js";

const ONE = `0x${"1".padStart(64, "0")}`;

const TOP: EventHandleInfo = {
  descriptor: { kind: "handle", accountAddress: ONE, eventHandleStruct: "0x1::block::BlockResource", fieldName: "new_block_events" },
  resourceType: "0x1::block::BlockResource",
  path: ["new_block_events"],
  count: 1165098930n,
  guid: { creationNumber: 3n, accountAddress: "0x1" },
};

const NESTED: EventHandleInfo = {
  descriptor: { kind: "handle", accountAddress: ONE, creationNumber: 4n },
  resourceType: "0x1::voting::VotingForum<0x1::governance_proposal::GovernanceProposal>",
  path: ["events", "vote_events"],
  count: 12n,
  guid: { creationNumber: 4n, accountAddress: "0x1" },
};

function harness(list: HandlesIO["list"], write?: HandlesIO["write"]) {
  const controller = new AbortController();
  const lines: string[] = [];
  const errors: string[] = [];
  const asked: string[] = [];
  const io: HandlesIO = {
    list: (address) => (asked.push(address), list(address)),
    write: write ?? ((l) => void lines.push(l)),
    error: (m) => errors.push(m),
    signal: controller.signal,
  };
  const run = () => runHandles({ network: "mainnet", address: "alice.apt" }, io);
  return { run, lines, errors, asked, controller };
}

describe("runHandles", () => {
  it("prints one line per handle, bigints as decimal strings, with the tail flags", async () => {
    const h = harness(async () => [TOP, NESTED]);
    expect(await h.run()).toBe(0);
    expect(h.asked).toEqual(["alice.apt"]);
    expect(h.lines.every((l) => l.endsWith("}\n"))).toBe(true);
    expect(h.lines.map((l) => JSON.parse(l))).toEqual([
      {
        resourceType: "0x1::block::BlockResource",
        path: ["new_block_events"],
        count: "1165098930",
        guid: { creationNumber: "3", accountAddress: "0x1" },
        descriptor: { kind: "handle", accountAddress: ONE, eventHandleStruct: "0x1::block::BlockResource", fieldName: "new_block_events" },
        flags: `--account ${ONE} --handle 0x1::block::BlockResource --field new_block_events`,
      },
      {
        resourceType: "0x1::voting::VotingForum<0x1::governance_proposal::GovernanceProposal>",
        path: ["events", "vote_events"],
        count: "12",
        guid: { creationNumber: "4", accountAddress: "0x1" },
        descriptor: { kind: "handle", accountAddress: ONE, creationNumber: "4" },
        flags: `--account ${ONE} --creation-number 4`,
      },
    ]);
  });

  it("single-quotes a struct type the shell would split or redirect", () => {
    const flags = handleFlags({
      kind: "handle",
      accountAddress: ONE,
      eventHandleStruct: "0x1::pool::Pool<0x1::a::A, 0x1::b::B>",
      fieldName: "swap_events",
    });
    expect(flags).toBe(`--account ${ONE} --handle '0x1::pool::Pool<0x1::a::A, 0x1::b::B>' --field swap_events`);
    expect(handleFlags({ kind: "handle", accountAddress: ONE, eventHandleStruct: "0x1::a::B<it's>", fieldName: "f" })).toContain(
      `'0x1::a::B<it'\\''s>'`,
    );
  });

  it("prints nothing and exits 0 for an account with no handles", async () => {
    const h = harness(async () => []);
    expect(await h.run()).toBe(0);
    expect(h.lines).toEqual([]);
    expect(h.errors).toEqual([]);
  });

  it("exits 1 with the library's message when the read is refused or fails", async () => {
    const h = harness(async () => {
      throw new StreamdError("Aptos name alice.apt is unregistered, expired or has no target address");
    });
    expect(await h.run()).toBe(1);
    expect(h.errors).toEqual(["streamd: Aptos name alice.apt is unregistered, expired or has no target address"]);
    expect(h.lines).toEqual([]);
  });

  it("exits 1 when a write fails, and 0 when the output closed", async () => {
    const failing = harness(async () => [TOP, NESTED], () => Promise.reject(new Error("disk full")));
    expect(await failing.run()).toBe(1);
    expect(failing.errors).toEqual(["streamd: disk full"]);

    const closed = harness(async () => [TOP, NESTED], () => {
      closed.controller.abort();
      return Promise.reject(new Error("aborted"));
    });
    expect(await closed.run()).toBe(0);
    expect(closed.errors).toEqual([]);
  });
});
