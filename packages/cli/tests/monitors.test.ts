import { describe, expect, it } from "bun:test";
import type { MultisigState, Proposal, StakePool } from "@streamd/aptos";
import { StreamdError } from "@streamd/core";
import type { MonitorCommandOptions } from "../src/args.js";
import { monitorLine, runMonitorState } from "../src/monitors.js";

const A = `0x${"a".repeat(64)}`;

const proposal = (id: bigint, yesVotes = 0n): Proposal => ({ id, yesVotes, noVotes: 0n, votes: [] }) as unknown as Proposal;
const pool = (address: string, rewards = 1n): StakePool =>
  ({ address, rewards: [{ epoch: 1n, rewards, fees: 0n, version: 5n }], commissionChanges: [], delegators: new Map() }) as unknown as StakePool;
const state = (version: bigint): MultisigState =>
  ({ address: A, version, owners: [A], threshold: 1n, lastResolved: 0n, nextSequenceNumber: 1n, pending: [] }) as unknown as MultisigState;

type Opts = { fromVersion?: bigint; signal: AbortSignal; onChange: (r: unknown) => void; onError: (e: Error) => void };

/** A fake monitor client: `ready` resolves to `start()`, and the test drives `onChange` and `onError`. */
function harness(start: () => Promise<unknown>, write?: (line: string) => void) {
  const calls: { method: string; address?: string; options: Opts }[] = [];
  const out: string[] = [];
  const errors: string[] = [];
  const controller = new AbortController();
  const monitor = { active: true, unsubscribed: false };
  const fake = (method: string) => (a: unknown, b?: unknown) => {
    const options = (method === "multisig" ? b : a) as Opts;
    calls.push({ method, ...(method === "multisig" && { address: a as string }), options });
    return {
      ready: start(),
      get active() {
        return monitor.active;
      },
      unsubscribe: () => void (monitor.unsubscribed = true),
    };
  };
  const io = {
    client: { proposals: fake("proposals"), stakePools: fake("stakePools"), multisig: fake("multisig") },
    write: write ?? ((line: string) => void out.push(line)),
    error: (m: string) => void errors.push(m),
    signal: controller.signal,
  };
  const run = (options: Partial<MonitorCommandOptions> & Pick<MonitorCommandOptions, "command">) =>
    runMonitorState({ network: "mainnet", ...options } as MonitorCommandOptions, io as never);
  const settle = () => new Promise((r) => setTimeout(r, 10));
  return { run, calls, out, errors, controller, monitor, settle, options: () => calls[0]!.options };
}

describe("monitorLine", () => {
  it("writes bigints as decimal strings", () => {
    expect(monitorLine(proposal(3n, 10n ** 20n))).toBe('{"id":"3","yesVotes":"100000000000000000000","noVotes":"0","votes":[]}\n');
  });

  it("writes maps as arrays of their values", () => {
    const multisig = { ...state(4n), pending: new Map([[1n, { sequenceNumber: 1n, creator: A }]]) } as unknown as MultisigState;
    expect(JSON.parse(monitorLine(multisig)).pending).toEqual([{ sequenceNumber: "1", creator: A }]);
    const p = { ...pool(A), delegators: new Map([[A, { address: A, added: 5n }]]) } as unknown as StakePool;
    expect(JSON.parse(monitorLine(p)).delegators).toEqual([{ address: A, added: "5" }]);
  });
});

describe("runMonitorState", () => {
  it("prints every proposal by id once ready, then each change", async () => {
    const h = harness(async () => new Map([[5n, proposal(5n)], [2n, proposal(2n)]]));
    const done = h.run({ command: "proposals", fromVersion: 100n });
    await h.settle();
    h.options().onChange(proposal(5n, 7n));
    await h.settle();
    h.controller.abort();
    expect(await done).toBe(0);
    expect(h.calls[0]!.method).toBe("proposals");
    expect(h.options().fromVersion).toBe(100n);
    expect(h.options().signal).toBe(h.controller.signal);
    expect(h.out.map((l) => JSON.parse(l).id)).toEqual(["2", "5", "5"]);
    expect(JSON.parse(h.out[2]!).yesVotes).toBe("7");
    expect(h.monitor.unsubscribed).toBe(true);
  });

  it("prints stake pools and stops after --limit changes", async () => {
    const h = harness(async () => new Map([[A, pool(A)]]));
    const done = h.run({ command: "stake-pools", limit: 2 });
    await h.settle();
    for (const r of [2n, 3n, 4n]) h.options().onChange(pool(A, r));
    expect(await done).toBe(0);
    expect(h.calls[0]!.method).toBe("stakePools");
    expect(h.options().fromVersion).toBeUndefined();
    expect(h.out.map((l) => JSON.parse(l).rewards[0].rewards)).toEqual(["1", "2", "3"]);
  });

  it("follows one multisig account and takes each line when it changes, not when it is written", async () => {
    const live = state(10n);
    const h = harness(async () => live);
    const done = h.run({ command: "multisig", address: A, limit: 1 });
    await h.settle();
    live.version = 11n;
    h.options().onChange(live);
    live.version = 12n;
    expect(await done).toBe(0);
    expect(h.calls[0]).toMatchObject({ method: "multisig", address: A });
    expect(h.out.map((l) => JSON.parse(l).version)).toEqual(["10", "11"]);
  });

  it("exits 1 with the message when the start read fails", async () => {
    const h = harness(async () => {
      throw new StreamdError("pruned");
    });
    expect(await h.run({ command: "proposals" })).toBe(1);
    expect(h.errors).toEqual(["streamd: pruned"]);
    expect(h.out).toEqual([]);
  });

  it("exits 1 when the monitor stops", async () => {
    const h = harness(async () => new Map());
    const done = h.run({ command: "stake-pools" });
    await h.settle();
    h.options().onError(new StreamdError("one poll failed"));
    h.monitor.active = false;
    h.options().onError(new StreamdError("gave up"));
    expect(await done).toBe(1);
    expect(h.errors).toEqual(["streamd: one poll failed", "streamd: gave up"]);
  });

  it("exits 1 when a write fails", async () => {
    const h = harness(
      async () => new Map([[1n, proposal(1n)]]),
      () => {
        throw new Error("EPIPE");
      },
    );
    expect(await h.run({ command: "proposals" })).toBe(1);
    expect(h.errors).toEqual(["streamd: EPIPE"]);
  });

  it("exits 0 at once when the signal is already aborted", async () => {
    const h = harness(async () => new Map());
    h.controller.abort();
    expect(await h.run({ command: "proposals" })).toBe(0);
    expect(h.calls).toEqual([]);
  });

  it("exits 1 when the client refuses the options", async () => {
    const h = harness(async () => new Map());
    const io = {
      client: {
        multisig: () => {
          throw new StreamdError("not a multisig account");
        },
      },
      write: () => {},
      error: (m: string) => void h.errors.push(m),
      signal: new AbortController().signal,
    };
    expect(await runMonitorState({ network: "mainnet", command: "multisig", address: A }, io as never)).toBe(1);
    expect(h.errors).toEqual(["streamd: not a multisig account"]);
  });
});
