import { describe, expect, it } from "bun:test";
import { AccountAddress, Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { MemoryAdapter, StreamdError, StreamdNetworkError } from "@streamd/core";
import type { EventAdapter, StreamdEvent } from "@streamd/core";
import { AptosClient } from "../src/index.js";
import type { AptosClientConfig, Transfer } from "../src/index.js";
import { testClient } from "./helpers.js";

describe("transfers through the client", () => {
  const long = (hex: string) => `0x${hex.padStart(64, "0")}`;
  const [ALICE, BOB, USDC] = [long("a11ce"), long("b0b"), long("bae2")];
  const owners: Record<string, string> = { [long("1")]: ALICE, [long("2")]: BOB, [long("3")]: BOB };
  const fa = (version: number, eventIndex: number, side: string, store: number, amount = "5"): StreamdEvent => ({
    type: `0x1::fungible_asset::${side}`,
    data: { store: long(String(store)), amount },
    version: BigInt(version),
    sequenceNumber: 0n,
    eventIndex,
  });
  const events = [fa(1, 0, "Withdraw", 1), fa(1, 1, "Deposit", 2), fa(2, 0, "Deposit", 3, "8")];
  const withViews = (fail?: () => boolean): EventAdapter => {
    const memory = new MemoryAdapter(events);
    return {
      fetchEvents: (d, o) => memory.fetchEvents(d, o),
      view: async (request, _version) => {
        if (fail?.()) throw new StreamdNetworkError("unavailable", 503);
        const store = String(request.functionArguments![0]);
        if (request.function === "0x1::object::owner") return [owners[store]];
        return [{ inner: store === long("3") ? long("a") : USDC }];
      },
    };
  };

  it("returns paired records from getTransfers, narrowed by asset", async () => {
    const client = testClient({ adapter: withViews() });
    const all = await client.getTransfers({ fromVersion: 0n });
    expect(all.map((e) => e.data)).toEqual([
      { asset: USDC, amount: 5n, from: ALICE, to: BOB },
      { asset: long("a"), amount: 8n, from: undefined, to: BOB },
    ]);
    const apt = await client.getTransfers({ fromVersion: 0n, asset: "0xa" });
    expect(apt.map((e) => e.version)).toEqual([2n]);
    await expect(client.getTransfers({ asset: "nope" })).rejects.toBeInstanceOf(StreamdError);
  });

  it("returns coin, token object and legacy token records together in identity order", async () => {
    const NFT = long("deed");
    const LEGACY_ID = { token_data_id: { creator: ALICE, collection: "Apes", name: "Ape #1" }, property_version: "0" };
    const memory = new MemoryAdapter([
      ...events,
      { type: "0x1::object::Transfer", data: { object: NFT, from: ALICE, to: BOB }, version: 3n, sequenceNumber: 0n, eventIndex: 0 },
      { type: "0x3::token::TokenWithdraw", data: { account: BOB, id: LEGACY_ID, amount: "1" }, version: 3n, sequenceNumber: 0n, eventIndex: 1 },
      { type: "0x3::token::TokenDeposit", data: { account: ALICE, id: LEGACY_ID, amount: "1" }, version: 3n, sequenceNumber: 0n, eventIndex: 2 },
    ]);
    const views = withViews();
    const client = testClient({
      adapter: {
        fetchEvents: (d, o) => memory.fetchEvents(d, o),
        view: async (request, version) =>
          request.function === "0x4::token::creator" ? [ALICE] : views.view!(request, version),
      },
    });
    const records = await client.getTransfers({ fromVersion: 0n });
    expect(records.map((e) => [`${e.version}:${e.eventIndex}`, e.data])).toEqual([
      ["1:1", { asset: USDC, amount: 5n, from: ALICE, to: BOB }],
      ["2:0", { asset: long("a"), amount: 8n, from: undefined, to: BOB }],
      ["3:0", { asset: NFT, amount: 1n, from: ALICE, to: BOB }],
      ["3:2", { asset: `${ALICE}::Apes::Ape #1::0`, amount: 1n, from: BOB, to: ALICE }],
    ]);
  });

  it("follows live, retrying a poll whose view failed and delivering each transfer once", async () => {
    let reads = 0;
    const errors: Error[] = [];
    const seen: Transfer[] = [];
    const client = testClient({ adapter: withViews(() => ++reads === 1) });
    const sub = client.transfers((e) => void seen.push(e.data), {
      fromVersion: 0n,
      pollInterval: 1,
      onError: (err) => errors.push(err),
    });
    expect(client.listenerCount()).toBe(0);
    for (let i = 0; i < 200 && seen.length < 2; i++) await new Promise((r) => setTimeout(r, 2));
    expect(seen.map((t) => t.amount)).toEqual([5n, 8n]);
    expect(errors).toHaveLength(1);
    expect((errors[0] as StreamdError).cause).toBeInstanceOf(StreamdNetworkError);
    expect(sub.status.consecutiveErrors).toBe(0);
    client.destroy();
    expect(sub.active).toBe(false);
  });

  it("refuses an adapter without view", () => {
    const client = testClient({ adapter: new MemoryAdapter(events) });
    expect(() => client.transfers(() => {})).toThrow(StreamdError);
  });

  describe("account", () => {
    const named = (lookup: () => Promise<AccountAddress | undefined>) => {
      const aptos = new Aptos(new AptosConfig({ network: Network.MAINNET }));
      let lookups = 0;
      aptos.getTargetAddress = () => (lookups++, lookup());
      return { client: testClient({ adapter: withViews(), aptos }), lookups: () => lookups };
    };
    const alice = AccountAddress.from(ALICE);

    it("keeps one account's records, for an address in any form", async () => {
      const client = testClient({ adapter: withViews() });
      const sent = await client.getTransfers({ fromVersion: 0n, account: `0x${ALICE.slice(2).toUpperCase()}` });
      expect(sent.map((e) => e.version)).toEqual([1n]);
      const bob = await client.getTransfers({ fromVersion: 0n, account: BOB });
      expect(bob.map((e) => e.version)).toEqual([1n, 2n]);
    });

    it("resolves a name once for a query", async () => {
      const { client, lookups } = named(async () => alice);
      const records = await client.getTransfers({ fromVersion: 0n, account: "alice.apt" });
      expect(records.map((e) => e.version)).toEqual([1n]);
      expect(lookups()).toBe(1);
    });

    it("refuses a malformed value at the call", () => {
      const client = testClient({ adapter: withViews() });
      expect(() => client.transfers(() => {}, { account: "0xnothex" })).toThrow(StreamdError);
    });

    it("refuses construction without aptos", () => {
      expect(() => new AptosClient({ adapter: withViews() } as unknown as AptosClientConfig)).toThrow("requires `aptos`");
    });

    it("stops a listener on an unregistered name, and retries a failed lookup", async () => {
      const gone = named(async () => undefined);
      const errors: Error[] = [];
      const stopped = gone.client.transfers(() => {}, {
        fromVersion: 0n,
        pollInterval: 1,
        account: "nobody.apt",
        onError: (err) => errors.push(err),
      });
      for (let i = 0; i < 200 && stopped.active; i++) await new Promise((r) => setTimeout(r, 2));
      expect(stopped.active).toBe(false);
      expect(errors[0]!.message).toContain("nobody.apt");

      let calls = 0;
      const flaky = named(async () => {
        if (++calls === 1) throw new TypeError("fetch failed");
        return alice;
      });
      const seen: Transfer[] = [];
      const sub = flaky.client.transfers((e) => void seen.push(e.data), {
        fromVersion: 0n,
        pollInterval: 1,
        maxBackoff: 1,
        account: "alice.apt",
        onError: () => {},
      });
      for (let i = 0; i < 200 && seen.length < 1; i++) await new Promise((r) => setTimeout(r, 2));
      expect(seen.map((t) => t.from)).toEqual([ALICE]);
      expect(flaky.lookups()).toBe(2);
      sub.unsubscribe();
    });
  });
});

describe("withNames", () => {
  const long = (hex: string) => `0x${hex.padStart(64, "0")}`;
  const [ALICE, BOB] = [long("a11ce"), long("b0b")];
  const TYPE = "0x1::a::B";
  const at = (version: number, sender?: string): StreamdEvent => ({
    type: TYPE,
    data: {},
    version: BigInt(version),
    sequenceNumber: 0n,
    eventIndex: 0,
    ...(sender !== undefined && { sender }),
  });
  const HISTORY = [at(1, ALICE), at(2, BOB), at(3), at(4, "0xa11ce")];

  /** An `AptosClient` on `network` whose SDK answers ANS reads from a fixed table, counting them. */
  const named = (network: Network) => {
    const aptos = new Aptos(new AptosConfig({ network, clientConfig: { http2: false } }));
    const reads: string[] = [];
    aptos.getPrimaryName = async ({ address }) => {
      reads.push(`primary ${address.toString()}`);
      return address.toString() === ALICE ? "alice" : undefined;
    };
    aptos.getTargetAddress = async ({ name }) => {
      reads.push(`target ${name}`);
      return name === "alice.apt" ? AccountAddress.from(ALICE) : undefined;
    };
    return { client: testClient({ adapter: new MemoryAdapter(HISTORY), aptos }), reads };
  };

  it("attaches each sender's primary name on mainnet, one lookup per sender", async () => {
    const { client, reads } = named(Network.MAINNET);
    const events = await client.getEvents({ kind: "module", eventType: TYPE }, { withNames: true });
    expect(events.map((e) => e.senderName)).toEqual(["alice.apt", null, undefined, "alice.apt"]);
    expect(reads).toEqual([`primary ${ALICE}`, "target alice.apt", `primary ${BOB}`]);
  });

  it("names a live subscription's events", async () => {
    const { client } = named(Network.LOCAL);
    const seen = await client.once({ kind: "module", eventType: TYPE }, { fromVersion: 0n, withNames: true });
    expect(seen.senderName).toBe("alice.apt");
    client.destroy();
  });

  it("is refused at the call on networks without Aptos names, which still work without it", async () => {
    for (const network of [Network.TESTNET, Network.DEVNET, Network.CUSTOM]) {
      const aptos = new Aptos(new AptosConfig({ network, clientConfig: { http2: false }, ...(network === Network.CUSTOM && { fullnode: "http://127.0.0.1:1/v1" }) }));
      const client = testClient({ adapter: new MemoryAdapter(HISTORY), aptos });
      expect(() => client.on({ kind: "module", eventType: TYPE }, () => {}, { withNames: true })).toThrow(
        `withNames needs Aptos names, which network ${network} does not serve`,
      );
      await expect(client.getEvents({ kind: "module", eventType: TYPE }, { withNames: true })).rejects.toThrow(StreamdError);
      expect(await client.getEvents({ kind: "module", eventType: TYPE })).toHaveLength(4);
      client.destroy();
    }
  });
});
