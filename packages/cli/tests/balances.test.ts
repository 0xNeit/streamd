import { describe, expect, it } from "bun:test";
import { AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { AptosClient } from "@streamd/aptos";
import { StreamdError, StreamdNetworkError } from "@streamd/core";
import type { BalanceChange, BalancesOptions, StreamdEvent } from "@streamd/core";
import type { BalancesCommandOptions } from "../src/args.js";
import { runBalances } from "../src/balances.js";
import type { BalancesIO } from "../src/balances.js";

const APT = `0x${"a".padStart(64, "0")}`;
const USDC = `0x${"b".padStart(64, "0")}`;

function change(asset: string, balance: bigint, version: bigint): StreamdEvent<BalanceChange> {
  return { type: "0x1::coin::balance", data: { asset, balance }, version, sequenceNumber: 0n, eventIndex: 0 };
}

const tick = () => new Promise((r) => setTimeout(r, 0));

/** A client whose `balances` is driven by hand: `ready` settles when told, and the test calls `onChange`/`onError`. */
function harness(opts: { throws?: Error; write?: (line: string) => void | Promise<void> } = {}) {
  const controller = new AbortController();
  const lines: string[] = [];
  const errors: string[] = [];
  let given: { address: string; options: BalancesOptions } | undefined;
  let active = true;
  let unsubscribed = false;
  let settle!: { resolve: (m: ReadonlyMap<string, bigint>) => void; reject: (e: unknown) => void };
  const ready = new Promise<ReadonlyMap<string, bigint>>((resolve, reject) => (settle = { resolve, reject }));
  ready.catch(() => {});
  const client = {
    balances(address: string, options: BalancesOptions) {
      if (opts.throws) throw opts.throws;
      given = { address, options };
      return {
        ready,
        get active() {
          return active;
        },
        unsubscribe: () => {
          active = false;
          unsubscribed = true;
        },
      };
    },
  } as unknown as BalancesIO["client"];
  const io: BalancesIO = {
    client,
    write: opts.write ?? ((l) => void lines.push(l)),
    error: (m) => errors.push(m),
    signal: controller.signal,
  };
  const run = (options: Partial<BalancesCommandOptions> = {}) =>
    runBalances({ network: "mainnet", address: "alice.apt", ...options }, io);
  const options = () => given!.options;
  const parsed = () => lines.map((l) => JSON.parse(l) as Record<string, string>);
  return {
    run, lines, errors, options, parsed, settle, controller,
    given: () => given,
    stop: () => void (active = false),
    unsubscribed: () => unsubscribed,
  };
}

describe("runBalances", () => {
  it("passes the address, the asset list and the poll interval, and nothing unset", async () => {
    const h = harness();
    void h.run({ asset: [APT, USDC], pollInterval: 500 });
    expect(h.given()!.address).toBe("alice.apt");
    expect(h.options().asset).toEqual([APT, USDC]);
    expect(h.options().pollInterval).toBe(500);
    h.controller.abort();

    const bare = harness();
    void bare.run();
    expect(Object.keys(bare.options()).sort()).toEqual(["onChange", "onError", "signal"]);
    bare.controller.abort();
  });

  it("prints the opening balances, then each change with previous and version, and exits 0 at the limit", async () => {
    const h = harness();
    const code = h.run({ limit: 2 });
    h.settle.resolve(new Map([[APT, 10n ** 20n], [USDC, 5n]]));
    await tick();
    h.options().onChange!(new Map(), change(APT, 10n ** 20n - 7n, 900n));
    h.options().onChange!(new Map(), change(APT, 3n, 901n));
    expect(await code).toBe(0);
    expect(h.parsed()).toEqual([
      { asset: APT, balance: "100000000000000000000" },
      { asset: USDC, balance: "5" },
      { asset: APT, balance: "99999999999999999993", previous: "100000000000000000000", version: "900" },
      { asset: APT, balance: "3", previous: "99999999999999999993", version: "901" },
    ]);
    expect(h.lines.every((l) => l.endsWith("\n"))).toBe(true);
    expect(h.unsubscribed()).toBe(true);
    expect(h.errors).toEqual([]);
  });

  it("exits 0 on abort", async () => {
    const h = harness();
    const code = h.run();
    h.settle.resolve(new Map([[APT, 1n]]));
    await tick();
    h.controller.abort();
    expect(await code).toBe(0);
    expect(h.lines.length).toBe(1);
  });

  it("reports transient errors and keeps following", async () => {
    const h = harness();
    const code = h.run({ limit: 1 });
    h.settle.resolve(new Map([[APT, 1n]]));
    await tick();
    h.options().onError!(new StreamdNetworkError("node busy"));
    h.options().onChange!(new Map(), change(APT, 2n, 5n));
    expect(await code).toBe(0);
    expect(h.errors).toEqual(["streamd: node busy"]);
    expect(h.lines.length).toBe(2);
  });

  it("exits 1 when polling gives up", async () => {
    const h = harness();
    const code = h.run();
    h.settle.resolve(new Map([[APT, 1n]]));
    await tick();
    h.stop();
    h.options().onError!(new StreamdError("Polling stopped after 5 consecutive errors"));
    expect(await code).toBe(1);
    expect(h.errors).toEqual(["streamd: Polling stopped after 5 consecutive errors"]);
  });

  it("exits 1 on a failed first read, reported once", async () => {
    const h = harness();
    const code = h.run();
    const err = new StreamdError("Not a fungible asset or coin type: 0xbad");
    h.stop();
    h.options().onError!(err);
    h.settle.reject(err);
    expect(await code).toBe(1);
    expect(h.errors).toEqual(["streamd: Not a fungible asset or coin type: 0xbad"]);
    expect(h.lines).toEqual([]);
  });

  it("exits 0 when the first read is abandoned by the signal", async () => {
    const h = harness();
    const code = h.run();
    h.controller.abort();
    h.settle.reject(new Error("aborted"));
    expect(await code).toBe(0);
    expect(h.errors).toEqual([]);
  });

  it("exits 1 when balances refuses the call", async () => {
    const h = harness({ throws: new StreamdError("Not a valid Aptos address: nope") });
    expect(await h.run({ address: "nope" })).toBe(1);
    expect(h.errors).toEqual(["streamd: Not a valid Aptos address: nope"]);
  });

  it("exits 1 when a write fails", async () => {
    const h = harness({ write: () => Promise.reject(new Error("disk full")) });
    const code = h.run();
    h.settle.resolve(new Map([[APT, 1n]]));
    expect(await code).toBe(1);
    expect(h.errors).toEqual(["streamd: disk full"]);
    expect(h.unsubscribed()).toBe(true);
  });

  it("exits 0 at once when already aborted", async () => {
    const h = harness();
    h.controller.abort();
    expect(await h.run()).toBe(0);
    expect(h.given()).toBeUndefined();
  });
});

describe("runBalances over AptosClient", () => {
  it("prints the opening balance before the first change read by the client", async () => {
    let head = 100n;
    const amounts = [7n, 7n, 9n];
    const adapter = {
      fetchEvents: async () => ({ events: [], nextCursor: "0", hasMore: false }),
      getHeadVersion: async () => head++,
      view: async () => [String(amounts.shift() ?? 9n)],
    };
    const client = new AptosClient({ aptos: new AptosConfig({ network: Network.MAINNET, clientConfig: { http2: false } }), adapter });
    const lines: string[] = [];
    const code = await runBalances(
      { network: "mainnet", address: "0x1", pollInterval: 1, limit: 1 },
      { client, write: (l) => void lines.push(l), error: () => {}, signal: new AbortController().signal },
    );
    client.destroy();
    expect(code).toBe(0);
    expect(lines.map((l) => JSON.parse(l))).toEqual([
      { asset: APT, balance: "7" },
      { asset: APT, balance: "9", previous: "7", version: "102" },
    ]);
  });
});
