import { describe, expect, it } from "bun:test";
import { StreamdClient } from "../src/client.js";
import { startBalances, formatAmount } from "../src/balances.js";
import type { BalanceReader } from "../src/balances.js";
import { StreamdError } from "../src/errors.js";
import type { BalanceChange, Balances, BalancesOptions, EventAdapter, StreamdEvent } from "../src/types.js";

const descriptor = { kind: "account", accountAddress: "0x1" } as const;
const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Balances keyed by upper case, so `canonical` is visible; `ledger` is what reads return. */
function reader(ledger: Map<string, bigint>, reads: string[] = []): BalanceReader {
  return {
    check(adapter) {
      if ((adapter as { refuse?: boolean }).refuse) throw new StreamdError("cannot read balances");
    },
    assets: (named) => [...new Set((named === undefined ? ["coin"] : typeof named === "string" ? [named] : named).map((n) => n.toUpperCase()))],
    canonical: (asset) => asset.toUpperCase(),
    type: (asset) => `balance::${asset}`,
    read: async (_adapter, owner, asset, version) => {
      reads.push(`${owner}:${asset}@${version}`);
      return ledger.get(asset) ?? 0n;
    },
  };
}

class Client extends StreamdClient {
  balances(read: BalanceReader, options?: BalancesOptions): Balances {
    return startBalances(
      {
        adapter: this.adapter,
        descriptor,
        onError: this.onError,
        subscribe: (callback, listener, wrap) => this.subscribe(descriptor, callback, listener, undefined, undefined, wrap),
        track: this.reduceHost.track,
      },
      () => Promise.resolve("0xa11ce"),
      read,
      options,
    );
  }
}

function client(head: { value: bigint }, extra: Partial<EventAdapter> & { refuse?: boolean } = {}) {
  const adapter = {
    fetchEvents: async () => ({ events: [], nextCursor: undefined, hasMore: false }),
    getHeadVersion: async () => head.value,
    ...extra,
  } as EventAdapter;
  return new Client({ adapter });
}

describe("startBalances", () => {
  it("reads at the head, reports a change once, and keys by canonical form", async () => {
    const head = { value: 5n };
    const ledger = new Map([["COIN", 10n]]);
    const reads: string[] = [];
    const changes: [ReadonlyMap<string, bigint>, StreamdEvent<BalanceChange>][] = [];
    const wallet = client(head).balances(reader(ledger, reads), { pollInterval: 5, onChange: (all, change) => changes.push([all, change]) });
    expect(await wallet.ready).toEqual(new Map([["COIN", 10n]]));
    expect(reads[0]).toBe("0xa11ce:COIN@5");
    await wait(30);
    expect(changes).toEqual([]);
    ledger.set("COIN", 12n);
    head.value = 6n;
    await wait(30);
    expect(changes.length).toBe(1);
    expect(changes[0]![1]).toMatchObject({ type: "balance::COIN", data: { asset: "COIN", balance: 12n }, version: 6n });
    expect(wallet.get("coin")).toBe(12n);
    expect(wallet.value).toEqual(new Map([["COIN", 12n]]));
    wallet.unsubscribe();
    expect(wallet.active).toBe(false);
  });

  it("skips a head behind the last one", async () => {
    const head = { value: 9n };
    const ledger = new Map([["COIN", 1n]]);
    const changes: unknown[] = [];
    using wallet = client(head).balances(reader(ledger), { pollInterval: 5, onChange: (_, c) => changes.push(c) });
    await wallet.ready;
    head.value = 8n;
    ledger.set("COIN", 0n);
    await wait(30);
    expect(changes).toEqual([]);
    expect(wallet.get("COIN")).toBe(1n);
  });

  it("refuses an unknown option and an empty list before the reader's check", () => {
    const c = client({ value: 1n }, { refuse: true });
    expect(() => c.balances(reader(new Map()), { fromVersion: 1n } as never)).toThrow("Unsupported balances option");
    expect(() => c.balances(reader(new Map()))).toThrow("cannot read balances");
    expect(() => client({ value: 1n }).balances(reader(new Map()), { asset: [] })).toThrow("empty");
    const noHead = new Client({ adapter: { fetchEvents: async () => ({ events: [], nextCursor: undefined, hasMore: false }) } });
    expect(() => noHead.balances(reader(new Map()))).toThrow("getHeadVersion");
  });

  it("stops reading after unsubscribe, and with an aborted signal", async () => {
    const reads: string[] = [];
    const wallet = client({ value: 1n }).balances(reader(new Map(), reads), { pollInterval: 5 });
    await wallet.ready;
    wallet.unsubscribe();
    const seen = reads.length;
    await wait(30);
    expect(reads.length).toBe(seen);

    const controller = new AbortController();
    controller.abort(new Error("cancelled"));
    const aborted = client({ value: 1n }).balances(reader(new Map()), { signal: controller.signal });
    await expect(aborted.ready).rejects.toThrow("cancelled");
    expect(aborted.active).toBe(false);
  });

  it("delivers all changed assets of a poll with the complete post-poll map", async () => {
    const head = { value: 5n };
    const ledger = new Map([["U", 100n], ["V", 0n]]);
    const snapshots: [string, bigint, ReadonlyMap<string, bigint>][] = [];
    const errors: Error[] = [];
    let shouldThrow = false;
    using wallet = client(head).balances(reader(ledger), {
      asset: ["u", "v"],
      pollInterval: 5,
      batchSize: 1,
      onError: (err) => errors.push(err),
      onChange: (all, change) => {
        if (shouldThrow && change.data.asset === "U") throw new Error("handler boom");
        snapshots.push([change.data.asset, change.version, new Map(all)]);
      },
    });
    expect(await wallet.ready).toEqual(new Map([["U", 100n], ["V", 0n]]));
    ledger.set("U", 0n);
    ledger.set("V", 100n);
    head.value = 6n;
    await wait(30);
    expect(snapshots).toEqual([
      ["U", 6n, new Map([["U", 0n], ["V", 100n]])],
      ["V", 6n, new Map([["U", 0n], ["V", 100n]])],
    ]);

    shouldThrow = true;
    ledger.set("U", 50n);
    ledger.set("V", 50n);
    head.value = 7n;
    await wait(30);
    expect(errors.map((e) => e.message)).toEqual([expect.stringContaining("Balance change handler threw")]);
    expect(snapshots).toHaveLength(3);
    expect(snapshots[2]).toEqual(["V", 7n, new Map([["U", 50n], ["V", 50n]])]);
    expect(wallet.value).toEqual(new Map([["U", 50n], ["V", 50n]]));
  });

  it("stops remaining batch notifications and errors when onChange unsubscribes", async () => {
    const head = { value: 5n };
    const ledger = new Map([["U", 100n], ["V", 0n]]);
    const seen: string[] = [];
    const errors: Error[] = [];
    const wallet = client(head).balances(reader(ledger), {
      asset: ["u", "v"],
      pollInterval: 5,
      onError: (err) => errors.push(err),
      onChange: (_all, change) => {
        seen.push(change.data.asset);
        wallet.unsubscribe();
        throw new Error("after stop");
      },
    });
    await wallet.ready;
    ledger.set("U", 0n);
    ledger.set("V", 100n);
    head.value = 6n;
    await wait(30);
    expect(seen).toEqual(["U"]);
    expect(errors).toEqual([]);
    expect(wallet.active).toBe(false);
  });

  it("rejects ready and never subscribes when unsubscribed during the initial read", async () => {
    let release!: () => void;
    const errors: Error[] = [];
    const base = client({ value: 1n });
    let subscribed = 0;
    const baseSub = (base as unknown as { subscribe: StreamdClient["subscribe"] }).subscribe.bind(base);
    (base as unknown as { subscribe: StreamdClient["subscribe"] }).subscribe = ((...args: Parameters<StreamdClient["subscribe"]>) => {
      subscribed++;
      return baseSub(...args);
    }) as StreamdClient["subscribe"];
    const r = reader(new Map([["COIN", 1n]]));
    const read = r.read;
    r.read = async (...args) => {
      await new Promise<void>((res) => (release = res));
      return read(...args);
    };
    const wallet = base.balances(r, { onError: (e) => errors.push(e) });
    await wait(5);
    wallet.unsubscribe();
    release();
    await expect(wallet.ready).rejects.toThrow("Balances stopped before the first read finished");
    expect(wallet.active).toBe(false);
    expect(subscribed).toBe(0);
    expect(errors).toEqual([]);
  });
});

describe("formatAmount", () => {
  it("renders base units exactly", () => {
    expect(formatAmount(1_500_000_000n, 9)).toBe("1.5");
    expect(formatAmount(-5n, 2)).toBe("-0.05");
    expect(() => formatAmount(1n, -1)).toThrow(StreamdError);
  });
});

describe("startBalances withValue", () => {
  const priced = (ledger: Map<string, bigint>, valued: string[]): BalanceReader => ({
    ...reader(ledger),
    // COIN is worth 3 quote units each; GEM has no price source.
    value: async (_adapter, asset, amount, version) => {
      valued.push(`${asset}=${amount}@${version}`);
      return asset === "COIN" ? { quote: "USD", decimals: 0, amount: amount * 3n, price: { numerator: 3n, denominator: 1n }, source: "par" } : undefined;
    },
  });

  it("values the first read and each change at its version, leaving unpriced assets out", async () => {
    const head = { value: 5n };
    const ledger = new Map([["COIN", 10n], ["GEM", 1n]]);
    const valued: string[] = [];
    const changes: StreamdEvent<BalanceChange>[] = [];
    using wallet = client(head).balances(priced(ledger, valued), {
      asset: ["coin", "gem"],
      withValue: true,
      pollInterval: 5,
      onChange: (_all, change) => changes.push(change),
    });
    await wallet.ready;
    expect([...wallet.values].map(([a, v]) => [a, v.amount])).toEqual([["COIN", 30n]]);
    await wait(30);
    expect(valued).toEqual(["COIN=10@5", "GEM=1@5"]);
    ledger.set("COIN", 12n);
    ledger.set("GEM", 2n);
    head.value = 6n;
    await wait(30);
    expect(changes.map((c) => [c.data.asset, c.data.value?.amount])).toEqual([["COIN", 36n], ["GEM", undefined]]);
    expect(wallet.values.get("COIN")!.amount).toBe(36n);
    expect(wallet.values.has("GEM")).toBe(false);
    expect(valued).toEqual(["COIN=10@5", "GEM=1@5", "COIN=12@6", "GEM=2@6"]);
  });

  it("reads no values without withValue, and refuses it from a reader that prices nothing", async () => {
    const valued: string[] = [];
    using wallet = client({ value: 5n }).balances(priced(new Map([["COIN", 1n]]), valued), { pollInterval: 5 });
    await wallet.ready;
    expect(wallet.values.size).toBe(0);
    expect(valued).toEqual([]);
    expect(() => client({ value: 5n }).balances(reader(new Map()), { withValue: true })).toThrow("withValue is not served on this chain");
  });
});
