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
});

describe("formatAmount", () => {
  it("renders base units exactly", () => {
    expect(formatAmount(1_500_000_000n, 9)).toBe("1.5");
    expect(formatAmount(-5n, 2)).toBe("-0.05");
    expect(() => formatAmount(1n, -1)).toThrow(StreamdError);
  });
});
