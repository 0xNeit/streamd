import "./dom.js";
import { describe, expect, it } from "bun:test";
import { createElement } from "react";
import { AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { MemoryAdapter, StreamdClient, StreamdError } from "@streamd/core";
import { AptosClient, type MultisigState, type Proposal, type StakePool } from "@streamd/aptos";
import {
  StreamdProvider,
  useMultisig,
  useProposals,
  useStakePools,
  type UseMultisigResult,
  type UseProposalsResult,
  type UseStakePoolsResult,
} from "../src/index.js";
import { render, rerender, unmount, waitFor } from "./render.js";

const long = (hex: string) => `0x${hex.padStart(64, "0")}`;
const [ALICE, BOB] = [long("a11ce"), long("b0b")];
const proposal = (id: bigint, yesVotes = 0n) => ({ id, yesVotes, noVotes: 0n, votes: [] }) as unknown as Proposal;
const pool = (address: string, rewards = 1n) => ({ address, rewards: [{ epoch: 1n, rewards, fees: 0n, version: 1n }], commissionChanges: [], delegators: new Map() }) as unknown as StakePool;
const multisigState = (version: bigint) =>
  ({ address: ALICE, version, owners: [ALICE, BOB], threshold: 2n, lastResolved: 0n, nextSequenceNumber: 1n, pending: new Map() }) as unknown as MultisigState;

/** One started monitor, driven by hand. */
interface Started {
  method: string;
  address?: string;
  options: { onChange?: (record: never, event: never) => void; onError?: (error: never) => void; [key: string]: unknown };
  value: unknown;
  active: boolean;
  unsubscribed: number;
  previews: bigint[];
  resolve: () => void;
  reject: (err: Error) => void;
}

function stubClient() {
  const client = new AptosClient({
    adapter: new MemoryAdapter([]),
    aptos: new AptosConfig({ network: Network.DEVNET, clientConfig: { http2: false } }),
  });
  const started: Started[] = [];
  const stub = { refuse: undefined as Error | undefined };
  const make = (method: string, value: () => unknown) => (a?: unknown, b?: unknown) => {
    if (stub.refuse) throw stub.refuse;
    const multisig = method === "multisig";
    const options = ((multisig ? b : a) ?? {}) as Started["options"];
    let resolve!: () => void;
    let reject!: (err: Error) => void;
    const s: Started = { method, ...(multisig && { address: a as string }), options, value: value(), active: true, unsubscribed: 0, previews: [], resolve, reject };
    const ready = new Promise((res, rej) => {
      s.resolve = () => res(s.value);
      s.reject = rej;
    });
    started.push(s);
    return {
      get value() {
        return s.value;
      },
      ready,
      get active() {
        return s.active;
      },
      preview: async (seq: bigint) => {
        s.previews.push(seq);
        return { sequenceNumber: seq };
      },
      unsubscribe: () => void s.unsubscribed++,
    } as never;
  };
  client.proposals = make("proposals", () => new Map());
  client.stakePools = make("stakePools", () => new Map());
  client.multisig = make("multisig", () => multisigState(1n));
  return { client, started, stub };
}

function harness<R>(hook: (input: string | null | undefined, options: Record<string, unknown>) => R) {
  const { client, started, stub } = stubClient();
  const results: R[] = [];
  function View({ input, options }: { input: string | null | undefined; options: Record<string, unknown> }) {
    results.push(hook(input, { ...options }));
    return null;
  }
  const el = (input: string | null | undefined, options: Record<string, unknown> = {}, given: StreamdClient = client) =>
    createElement(StreamdProvider, { client: given }, createElement(View, { input, options }));
  return { el, started, stub, results, last: () => results.at(-1)! };
}

const proposalsHook = () => harness<UseProposalsResult>((_i, o) => useProposals(o));
const poolsHook = () => harness<UseStakePoolsResult>((_i, o) => useStakePools(o));
const multisigHook = () => harness<UseMultisigResult>((i, o) => useMultisig(i, o));

describe("useProposals", () => {
  it("goes from loading to live with a copy, then copies only the changed proposal", async () => {
    const t = proposalsHook();
    const seen: string[] = [];
    const root = await render(t.el(null, { fromVersion: 5n, onChange: (p: Proposal) => seen.push(`first ${p.id}`) }));
    await waitFor(() => expect(t.last().status).toBe("loading"));
    const s = t.started[0]!;
    expect(s.options.fromVersion).toBe(5n);
    const live = s.value as Map<bigint, Proposal>;
    live.set(1n, proposal(1n)).set(2n, proposal(2n));
    s.resolve();
    await waitFor(() => expect(t.last().status).toBe("live"));
    const first = t.last().proposals;
    expect(first).toEqual(live);
    expect(first.get(1n)).not.toBe(live.get(1n));

    await rerender(root, t.el(null, { fromVersion: 5n, onChange: (p: Proposal) => seen.push(`latest ${p.id}`) }));
    live.get(2n)!.yesVotes = 9n;
    s.options.onChange!(live.get(2n) as never, {} as never);
    await waitFor(() => expect(t.last().proposals.get(2n)?.yesVotes).toBe(9n));
    const second = t.last().proposals;
    expect(second).not.toBe(first);
    expect(second.get(1n)).toBe(first.get(1n));
    expect(second.get(2n)).not.toBe(live.get(2n));
    expect(first.get(2n)!.yesVotes).toBe(0n);
    expect(seen).toEqual(["latest 2"]);
    expect(t.started).toHaveLength(1);
    await unmount(root);
    expect(s.unsubscribed).toBe(1);
  });

  it("restarts on changed options by value only", async () => {
    const t = proposalsHook();
    const root = await render(t.el(null, { fromVersion: 1n }));
    await rerender(root, t.el(null, { fromVersion: 1n }));
    expect(t.started).toHaveLength(1);
    await rerender(root, t.el(null, { fromVersion: 2n }));
    expect(t.started.map((s) => s.unsubscribed)).toEqual([1, 0]);
    await unmount(root);
  });

  it("stops when ready rejects", async () => {
    const t = proposalsHook();
    const root = await render(t.el(null));
    t.started[0]!.reject(new StreamdError("start read failed"));
    await waitFor(() => expect(t.last().status).toBe("stopped"));
    expect(t.last().error?.message).toBe("start read failed");
    await unmount(root);
  });

  it("refuses a client without proposals", async () => {
    const t = proposalsHook();
    const plain = new StreamdClient({ adapter: new MemoryAdapter([]) });
    await expect(render(t.el(null, {}, plain))).rejects.toThrow("This hook needs a client with proposals");
  });
});

describe("useStakePools", () => {
  it("copies the pools on ready and the changed pool on a change", async () => {
    const t = poolsHook();
    const root = await render(t.el(null, { pool: ALICE }));
    const s = t.started[0]!;
    expect(s.options.pool).toBe(ALICE);
    const live = s.value as Map<string, StakePool>;
    live.set(ALICE, pool(ALICE)).set(BOB, pool(BOB));
    s.resolve();
    await waitFor(() => expect(t.last().pools.size).toBe(2));
    const first = t.last().pools;
    live.get(ALICE)!.rewards.push({ epoch: 2n, rewards: 3n, fees: 0n, version: 2n });
    s.options.onChange!(live.get(ALICE) as never, {} as never);
    await waitFor(() => expect(t.last().pools.get(ALICE)?.rewards).toHaveLength(2));
    expect(first.get(ALICE)!.rewards).toHaveLength(1);
    expect(t.last().pools.get(BOB)).toBe(first.get(BOB));
    await unmount(root);
  });

  it("stops on an error once the monitor is inactive, and not before", async () => {
    const t = poolsHook();
    const root = await render(t.el(null));
    const s = t.started[0]!;
    s.resolve();
    await waitFor(() => expect(t.last().status).toBe("live"));
    s.options.onError!(new StreamdError("one poll failed") as never);
    await waitFor(() => expect(t.last().error?.message).toBe("one poll failed"));
    expect(t.last().status).toBe("live");
    s.active = false;
    s.options.onError!(new StreamdError("gave up") as never);
    await waitFor(() => expect(t.last().status).toBe("stopped"));
    await unmount(root);
  });

  it("stops when the client refuses the options", async () => {
    const t = poolsHook();
    t.stub.refuse = new StreamdError("pool must be an address");
    const root = await render(t.el(null, { pool: "x" }));
    await waitFor(() => expect(t.last().status).toBe("stopped"));
    expect(t.last().error?.message).toBe("pool must be an address");
    await unmount(root);
  });
});

describe("useMultisig", () => {
  it("waits on a missing address, then follows one, copying the state per change", async () => {
    const t = multisigHook();
    const root = await render(t.el(undefined));
    expect(t.last()).toMatchObject({ state: undefined, status: "idle" });
    expect(t.started).toHaveLength(0);
    await expect(t.last().preview(1n)).rejects.toThrow("no running monitor");

    await rerender(root, t.el(ALICE, { fromVersion: 3n }));
    const s = t.started[0]!;
    expect(s).toMatchObject({ address: ALICE, options: { fromVersion: 3n } });
    s.resolve();
    await waitFor(() => expect(t.last().state?.version).toBe(1n));
    const live = s.value as MultisigState;
    expect(t.last().state).not.toBe(live);
    const preview = t.last().preview;
    live.version = 2n;
    s.options.onChange!(live as never, {} as never);
    await waitFor(() => expect(t.last().state?.version).toBe(2n));
    expect(t.last().preview).toBe(preview);
    expect(await t.last().preview(1n)).toEqual({ sequenceNumber: 1n } as never);
    expect(s.previews).toEqual([1n]);

    await rerender(root, t.el(BOB));
    expect(t.started.map((x) => x.address)).toEqual([ALICE, BOB]);
    expect(s.unsubscribed).toBe(1);
    await rerender(root, t.el(null));
    await waitFor(() => expect(t.last().status).toBe("idle"));
    expect(t.started[1]!.unsubscribed).toBe(1);
    await unmount(root);
  });
});
