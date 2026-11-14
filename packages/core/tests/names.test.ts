import { describe, expect, it } from "bun:test";
import { StreamdError } from "../src/errors.js";
import { DEFAULT_NAME_TTL, SenderNames } from "../src/names.js";
import type { StreamdEvent } from "../src/types.js";

const ALICE = `0x${"a".repeat(64)}`;
const BOB = `0x${"b".repeat(64)}`;

function ev(sender?: string, version = 1n): StreamdEvent {
  return { type: "0x1::a::B", data: {}, version, sequenceNumber: 0n, eventIndex: 0, ...(sender !== undefined && { sender }) };
}

/** A cache over a lookup that logs each address and answers from `names`, on a clock the test moves. */
function setup(names: Record<string, string | Error> = { [ALICE]: "alice.apt" }) {
  const lookups: string[] = [];
  const clock = { now: 0 };
  const cache = new SenderNames(undefined, () => clock.now);
  cache.serve(async (address) => {
    lookups.push(address);
    const answer = names[address];
    if (answer instanceof Error) throw answer;
    return answer;
  });
  return { cache, lookups, clock };
}

describe("SenderNames", () => {
  it("attaches the sender's name to a copy, looking up the long form", async () => {
    const { cache, lookups } = setup();
    const event = ev("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    const named = await cache.attach(event, DEFAULT_NAME_TTL);
    expect(named.senderName).toBe("alice.apt");
    expect(event.senderName).toBeUndefined();
    expect(lookups).toEqual([ALICE]);
  });

  it("reuses a name within the expiry and looks it up again after it", async () => {
    const { cache, lookups, clock } = setup();
    await cache.attach(ev(ALICE), 1000);
    clock.now = 1000;
    await cache.attach(ev(ALICE), 1000);
    expect(lookups).toHaveLength(1);
    clock.now = 1001;
    await cache.attach(ev(ALICE), 1000);
    expect(lookups).toHaveLength(2);
    // A shorter expiry on another call reads the same entry by its own rule.
    expect((await cache.attach(ev(ALICE), 0)).senderName).toBe("alice.apt");
    expect(lookups).toHaveLength(2);
    clock.now = 1002;
    await cache.attach(ev(ALICE), 0);
    expect(lookups).toHaveLength(3);
  });

  it("caches a miss as null", async () => {
    const { cache, lookups } = setup();
    expect((await cache.attach(ev(BOB), 1000)).senderName).toBeNull();
    expect((await cache.attach(ev(BOB), 1000)).senderName).toBeNull();
    expect(lookups).toEqual([BOB]);
  });

  it("shares one lookup between concurrent callers", async () => {
    const { cache, lookups } = setup();
    const named = await Promise.all([cache.attach(ev(ALICE), 0), cache.attach(ev(ALICE), 1000), cache.attach(ev(ALICE), 0)]);
    expect(named.map((e) => e.senderName)).toEqual(["alice.apt", "alice.apt", "alice.apt"]);
    expect(lookups).toHaveLength(1);
  });

  it("rejects on a failed lookup and does not cache it", async () => {
    const { cache, lookups } = setup({ [ALICE]: new Error("down") });
    await expect(cache.attach(ev(ALICE), 1000)).rejects.toThrow("down");
    await expect(cache.attach(ev(ALICE), 1000)).rejects.toThrow("down");
    expect(lookups).toHaveLength(2);
  });

  it("rejects only an aborted caller's wait", async () => {
    const { cache } = setup();
    const abort = new AbortController();
    const aborted = cache.attach(ev(ALICE), 0, abort.signal);
    const other = cache.attach(ev(ALICE), 0);
    abort.abort(new Error("stopped"));
    await expect(aborted).rejects.toThrow("stopped");
    expect((await other).senderName).toBe("alice.apt");
  });

  it("looks up nothing for an event without a sender or with one that is not an address", async () => {
    const { cache, lookups } = setup();
    for (const event of [ev(), ev("alice.apt")]) {
      const out = await cache.attach(event, 0);
      expect(out).toBe(event);
      expect("senderName" in out).toBe(false);
    }
    expect(lookups).toEqual([]);
  });

  it("evicts the entry fetched longest ago past 10,000 addresses", async () => {
    const { cache, lookups } = setup();
    const address = (i: number) => `0x${i.toString(16).padStart(64, "0")}`;
    for (let i = 1; i <= 10_001; i++) await cache.attach(ev(address(i)), Infinity);
    expect(lookups).toHaveLength(10_001);
    await cache.attach(ev(address(10_001)), Infinity);
    await cache.attach(ev(address(2)), Infinity);
    expect(lookups).toHaveLength(10_001);
    await cache.attach(ev(address(1)), Infinity);
    expect(lookups).toHaveLength(10_002);
  });

  it("reads withNames into an expiry", () => {
    const { cache } = setup();
    expect(cache.ttl(undefined)).toBeUndefined();
    expect(cache.ttl(false)).toBeUndefined();
    expect(cache.ttl(true)).toBe(DEFAULT_NAME_TTL);
    expect(DEFAULT_NAME_TTL).toBe(300_000);
    expect(cache.ttl({})).toBe(DEFAULT_NAME_TTL);
    expect(cache.ttl({ ttl: 0 })).toBe(0);
    expect(cache.ttl({ ttl: Infinity })).toBe(Infinity);
  });

  it.each([
    ["a string", "yes"],
    ["null", null],
    ["an array", []],
    ["an unknown key", { ttL: 5 }],
    ["a negative ttl", { ttl: -1 }],
    ["a NaN ttl", { ttl: NaN }],
    ["a string ttl", { ttl: "5" }],
  ])("refuses %s", (_name, value) => {
    expect(() => setup().cache.ttl(value)).toThrow(StreamdError);
  });

  it("refuses withNames when no lookup is served, with the reason given", () => {
    const cache = new SenderNames();
    expect(cache.ttl(undefined)).toBeUndefined();
    expect(() => cache.ttl(true)).toThrow("withNames needs a client that looks up names");
    cache.serve("no names on this network");
    expect(() => cache.ttl(true)).toThrow("no names on this network");
  });
});
