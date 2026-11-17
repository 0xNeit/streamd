import { describe, expect, it } from "bun:test";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { StreamdClient } from "../src/client.js";
import { StreamdError, StreamdNetworkError } from "../src/errors.js";
import { whereField } from "../src/filter.js";
import { RequestLimiter } from "../src/limiter.js";
import { DEFAULT_NAME_TTL, SenderNames } from "../src/names.js";
import type { EventAdapter, EventDescriptor, StreamdConfig, StreamdEvent } from "../src/types.js";

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
    clock.now = 999;
    await cache.attach(ev(ALICE), 1000);
    expect(lookups).toHaveLength(1);
    clock.now = 1000;
    await cache.attach(ev(ALICE), 1000);
    expect(lookups).toHaveLength(2);
    // Another call's expiry reads the same entry by its own rule; 0 reuses nothing.
    expect((await cache.attach(ev(ALICE), 5000)).senderName).toBe("alice.apt");
    expect(lookups).toHaveLength(2);
    await cache.attach(ev(ALICE), 0);
    expect(lookups).toHaveLength(3);
  });

  it("names any address through the cache attach uses", async () => {
    const { cache, lookups } = setup();
    expect(await cache.name(ALICE, DEFAULT_NAME_TTL)).toBe("alice.apt");
    expect(await cache.name(BOB, DEFAULT_NAME_TTL)).toBeNull();
    expect((await cache.attach(ev(ALICE), DEFAULT_NAME_TTL)).senderName).toBe("alice.apt");
    expect(lookups).toEqual([ALICE, BOB]);
  });

  it("looks a name up within a held slot without queueing on the limiter, sharing the cache", async () => {
    const limiter = new RequestLimiter(1);
    const lookups: string[] = [];
    const cache = new SenderNames(limiter);
    cache.serve(async (address) => (lookups.push(address), "alice.apt"));
    const named = await limiter.run(() => cache.nameWithinSlot(ALICE, DEFAULT_NAME_TTL));
    expect(named).toBe("alice.apt");
    expect(await cache.name(ALICE, DEFAULT_NAME_TTL)).toBe("alice.apt");
    expect(lookups).toEqual([ALICE]);
  });

  it("never joins an in-slot lookup to one queued for the slot it holds", async () => {
    const limiter = new RequestLimiter(1);
    const cache = new SenderNames(limiter);
    cache.serve(async () => "alice.apt");
    let queued: Promise<string | null> | undefined;
    const page = limiter.run(async () => {
      queued = cache.name(ALICE, DEFAULT_NAME_TTL);
      return cache.nameWithinSlot(ALICE, DEFAULT_NAME_TTL);
    });
    const timeout = new Promise((r) => setTimeout(() => r("deadlock"), 300));
    expect(await Promise.race([page, timeout])).toBe("alice.apt");
    expect(await queued).toBe("alice.apt");
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

const TYPE = "0x1::a::B";
const D: EventDescriptor = { kind: "module", eventType: TYPE };

function at(version: bigint, sender: string | undefined, keep = true, eventIndex = 0): StreamdEvent {
  return { ...ev(sender, version), eventIndex, data: { keep } };
}

/** A client serving names from `answers`; a function answer runs per lookup, so a test can fail one. */
class NamedClient extends StreamdClient {
  readonly lookups: string[] = [];

  constructor(
    adapter: EventAdapter,
    answers: Record<string, string | ((n: number) => string | undefined)> = { [ALICE]: "alice.apt" },
    config: Omit<StreamdConfig, "adapter"> = {},
  ) {
    super({ ...config, adapter });
    this.names.serve(async (address) => {
      this.lookups.push(address);
      const answer = answers[address];
      return typeof answer === "function" ? answer(this.lookups.length) : answer;
    });
  }
}

describe("withNames on queries", () => {
  const HISTORY = [at(1n, ALICE), at(2n, BOB, false), at(3n, BOB), at(4n, undefined), at(5n, ALICE), at(6n, ALICE)];

  it("names only the events where kept and limit returned, one lookup per sender", async () => {
    const client = new NamedClient(new MemoryAdapter(HISTORY));
    const events = await client.getEvents(D, { where: whereField("keep", true), limit: 4, withNames: true });
    expect(events.map((e) => [e.version, e.senderName])).toEqual([
      [1n, "alice.apt"],
      [3n, null],
      [4n, undefined],
      [5n, "alice.apt"],
    ]);
    expect("senderName" in events[2]!).toBe(false);
    expect(client.lookups).toEqual([ALICE, BOB]);
  });

  it("shares the cache across calls, and ttl 0 looks up again", async () => {
    const client = new NamedClient(new MemoryAdapter(HISTORY));
    await client.getEvents(D, { withNames: true });
    await client.getEvents(D, { withNames: { ttl: 60_000 } });
    expect(client.lookups).toEqual([ALICE, BOB]);
    await client.getEvents(D, { limit: 1, withNames: { ttl: 0 } });
    expect(client.lookups).toEqual([ALICE, BOB, ALICE]);
  });

  it("names iterateEvents, getTransactions groups and replay deliveries", async () => {
    const client = new NamedClient(new MemoryAdapter([...HISTORY, at(6n, ALICE, true, 1)]));
    const iterated: (string | null | undefined)[] = [];
    for await (const event of client.iterateEvents(D, { toVersion: 1n, withNames: true })) iterated.push(event.senderName);
    expect(iterated).toEqual(["alice.apt"]);
    const groups = await client.getTransactions(D, { fromVersion: 5n, withNames: true });
    expect(groups.map((g) => [g.version, g.senderName, g.events.map((e) => e.senderName)])).toEqual([
      [5n, "alice.apt", ["alice.apt"]],
      [6n, "alice.apt", ["alice.apt", "alice.apt"]],
    ]);
    const replayed: (string | null | undefined)[] = [];
    await client.replay(D, (e) => void replayed.push(e.senderName), { toVersion: 3n, withNames: true }).done;
    expect(replayed).toEqual(["alice.apt", null, null]);
  });

  it("rejects on a failed lookup, and retries it under retries", async () => {
    const busy = (n: number) => {
      if (n === 1) throw new StreamdNetworkError("name read failed", 503, undefined, 1);
      return "alice.apt";
    };
    const failing = new NamedClient(new MemoryAdapter(HISTORY), { [ALICE]: busy });
    await expect(failing.getEvents(D, { withNames: true })).rejects.toThrow("name read failed");
    const retried = new NamedClient(new MemoryAdapter(HISTORY), { [ALICE]: busy });
    const events = await retried.getEvents(D, { limit: 1, withNames: true, retries: 1 });
    expect(events[0]!.senderName).toBe("alice.apt");
    expect(retried.lookups).toEqual([ALICE, ALICE]);
  });

  it("is refused by a client that serves no names, and a bad value by any", async () => {
    const plain = new StreamdClient({ adapter: new MemoryAdapter(HISTORY) });
    await expect(plain.getEvents(D, { withNames: true })).rejects.toThrow("withNames needs a client that looks up names");
    await expect(plain.getTransactions(D, { withNames: true })).rejects.toThrow(StreamdError);
    expect(await plain.getEvents(D, { withNames: false })).toHaveLength(6);
    const named = new NamedClient(new MemoryAdapter(HISTORY));
    await expect(named.getEvents(D, { withNames: { ttl: -1 } })).rejects.toThrow("ttl");
  });

  it("looks up distinct cold senders on a page concurrently while keeping event order", async () => {
    const senders = [1, 2, 3, 4].map((i) => `0x${String(i).padStart(64, "0")}`);
    const page = [...senders.map((s, idx) => at(BigInt(idx + 1), s)), at(5n, senders[0])];
    const client = new StreamdClient({ adapter: new MemoryAdapter(page) });
    let inFlight = 0;
    let peak = 0;
    const waiters = new Map<string, (name: string) => void>();
    (client as unknown as { names: SenderNames }).names.serve(
      (address) =>
        new Promise<string>((resolve) => {
          peak = Math.max(peak, ++inFlight);
          waiters.set(address, (name) => {
            inFlight--;
            resolve(name);
          });
          if (waiters.size === senders.length) {
            // Resolve in reverse order so concurrency and out-of-order settlement are exercised.
            for (const s of [...senders].reverse()) {
              waiters.get(s)!(s === senders[0] ? "one.apt" : `n${Number(BigInt(s))}.apt`);
            }
          }
        }),
    );
    const timeout = new Promise<never>((_, reject) =>
      setTimeout(() => reject(new Error(`only ${waiters.size}/${senders.length} lookups started concurrently`)), 500),
    );
    const events = await Promise.race([client.getEvents(D, { withNames: true }), timeout]);
    expect(peak).toBe(4);
    expect(events.map((e) => [e.version, e.senderName])).toEqual([
      [1n, "one.apt"],
      [2n, "n2.apt"],
      [3n, "n3.apt"],
      [4n, "n4.apt"],
      [5n, "one.apt"],
    ]);
    client.destroy();
  });

  it("retries failed prefetched lookups cleanly without leaving rejected cache entries", async () => {
    const senders = [1, 2].map((i) => `0x${String(i).padStart(64, "0")}`);
    const page = [at(1n, senders[0]), at(2n, senders[1]), at(3n, senders[0])];
    const counts = new Map<string, number>();
    const client = new NamedClient(new MemoryAdapter(page), {
      [senders[0]!]: () => {
        const n = (counts.get(senders[0]!) ?? 0) + 1;
        counts.set(senders[0]!, n);
        if (n === 1) throw new StreamdNetworkError("transient 1", 503, undefined, 1);
        return "one.apt";
      },
      [senders[1]!]: () => {
        const n = (counts.get(senders[1]!) ?? 0) + 1;
        counts.set(senders[1]!, n);
        if (n === 1) throw new StreamdNetworkError("transient 2", 503, undefined, 1);
        return "two.apt";
      },
    });
    const events = await client.getEvents(D, { withNames: { ttl: 0 }, retries: 1 });
    expect(events.map((e) => e.senderName)).toEqual(["one.apt", "two.apt", "one.apt"]);
    expect(counts.get(senders[0]!)).toBe(2);
    expect(counts.get(senders[1]!)).toBe(2);
    client.destroy();
  });
});

async function until(done: () => boolean, ms = 2000): Promise<void> {
  const end = Date.now() + ms;
  while (!done()) {
    if (Date.now() > end) throw new Error("timed out waiting");
    await new Promise((r) => setTimeout(r, 2));
  }
}

describe("withNames on listeners", () => {
  const HISTORY = [at(1n, ALICE), at(2n, BOB, false), at(3n, BOB), at(4n, undefined), at(5n, ALICE)];
  const FAST = { fromVersion: 0n, pollInterval: 5, maxBackoff: 10 };

  it("names each event on() and onBatch() deliver, looking up nothing for skipped ones", async () => {
    const client = new NamedClient(new MemoryAdapter(HISTORY));
    const seen: [bigint, string | null | undefined][] = [];
    const batches: (string | null | undefined)[][] = [];
    const where = whereField("keep", true);
    client.on(D, (e) => void seen.push([e.version, e.senderName]), { ...FAST, where, withNames: true });
    client.onBatch(D, (es) => void batches.push(es.map((e) => e.senderName)), { ...FAST, where, withNames: true });
    await until(() => seen.length === 4 && batches.flat().length === 4);
    client.destroy();
    expect(seen).toEqual([
      [1n, "alice.apt"],
      [3n, null],
      [4n, undefined],
      [5n, "alice.apt"],
    ]);
    expect(batches.flat()).toEqual(["alice.apt", null, undefined, "alice.apt"]);
    expect(client.lookups.filter((a) => a === BOB)).toHaveLength(1);
    expect(client.lookups.filter((a) => a === ALICE)).toHaveLength(1);
  });

  it("fails the poll on a failed lookup and delivers the event named on the next", async () => {
    const answers = {
      [ALICE]: "alice.apt",
      [BOB]: (n: number) => {
        if (n === 2) throw new StreamdNetworkError("name read failed", 503);
        return "bob.apt";
      },
    };
    const client = new NamedClient(new MemoryAdapter(HISTORY), answers);
    const seen: [bigint, string | null | undefined][] = [];
    const errors: Error[] = [];
    client.on(D, (e) => void seen.push([e.version, e.senderName]), {
      ...FAST,
      withNames: true,
      onError: (err) => void errors.push(err),
    });
    await until(() => seen.length === 5);
    client.destroy();
    expect(errors.map((e) => [e.name, (e as { cause?: Error }).cause?.message])).toEqual([
      ["StreamdPollingError", "name read failed"],
    ]);
    expect(seen).toEqual([
      [1n, "alice.apt"],
      [2n, "bob.apt"],
      [3n, "bob.apt"],
      [4n, undefined],
      [5n, "alice.apt"],
    ]);
  });

  it("names onTransaction groups and their events", async () => {
    const client = new NamedClient(new MemoryAdapter([at(1n, ALICE), at(1n, ALICE, true, 1), at(2n, BOB)]));
    const groups: [bigint, string | null | undefined, (string | null | undefined)[]][] = [];
    client.onTransaction(D, (g) => void groups.push([g.version, g.senderName, g.events.map((e) => e.senderName)]), {
      ...FAST,
      withNames: true,
    });
    await until(() => groups.length === 2);
    client.destroy();
    expect(groups).toEqual([
      [1n, "alice.apt", ["alice.apt", "alice.apt"]],
      [2n, null, [null]],
    ]);
  });

  it("is refused at the call by a client that serves no names", () => {
    const plain = new StreamdClient({ adapter: new MemoryAdapter(HISTORY) });
    expect(() => plain.on(D, () => {}, { withNames: true })).toThrow("withNames needs a client that looks up names");
    expect(() => plain.onBatch(D, () => {}, { withNames: { ttl: 5 } })).toThrow(StreamdError);
    expect(plain.listenerCount()).toBe(0);
    plain.destroy();
  });

  it("looks up distinct cold senders concurrently on both on() and filtered onBatch()", async () => {
    const senders = [1, 2, 3, 4].map((i) => `0x${String(i).padStart(64, "0")}`);
    const skipped = `0x${"9".padStart(64, "0")}`;
    const events = [...senders.map((s, idx) => at(BigInt(idx + 1), s)), at(5n, skipped, false)];

    const runConcurrent = async (mode: "on" | "batch") => {
      const client = new StreamdClient({ adapter: new MemoryAdapter(events) });
      let inFlight = 0;
      let peak = 0;
      const started: string[] = [];
      const resolvers: Array<() => void> = [];
      (client as unknown as { names: SenderNames }).names.serve(
        (address) => {
          started.push(address);
          if (address === skipped) return Promise.resolve("skipped.apt");
          return new Promise<string>((resolve) => {
            peak = Math.max(peak, ++inFlight);
            resolvers.push(() => {
              inFlight--;
              resolve(`n${Number(BigInt(address))}.apt`);
            });
            if (resolvers.length === senders.length) {
              for (const done of resolvers.splice(0)) done();
            }
          });
        },
      );
      const delivered: [bigint, string | null | undefined][] = [];
      if (mode === "on") {
        client.on(D, (e) => void delivered.push([e.version, e.senderName]), {
          ...FAST,
          withNames: true,
        });
      } else {
        client.onBatch(
          D,
          (batch) => {
            for (const e of batch) delivered.push([e.version, e.senderName]);
          },
          { ...FAST, where: whereField("keep", true), withNames: true },
        );
      }
      await until(() => delivered.length === (mode === "on" ? 5 : 4), 1000);
      client.destroy();
      expect(peak).toBe(4);
      if (mode === "batch") expect(started).not.toContain(skipped);
      expect(delivered.slice(0, 4)).toEqual([
        [1n, "n1.apt"],
        [2n, "n2.apt"],
        [3n, "n3.apt"],
        [4n, "n4.apt"],
      ]);
    };

    await runConcurrent("on");
    await runConcurrent("batch");
  });

  it("delivers only the prefix resolved before a batch name failure and retries cleanly", async () => {
    const senders = [1, 2, 3].map((i) => `0x${String(i).padStart(64, "0")}`);
    const page = senders.map((s, idx) => at(BigInt(idx + 1), s));
    let failSecond = true;
    const client = new NamedClient(new MemoryAdapter(page), {
      [senders[0]!]: "n1.apt",
      [senders[1]!]: () => {
        if (failSecond) {
          failSecond = false;
          throw new StreamdNetworkError("name 2 down", 503);
        }
        return "n2.apt";
      },
      [senders[2]!]: () => {
        if (failSecond) throw new StreamdNetworkError("name 3 also down", 503);
        return "n3.apt";
      },
    });
    const batches: [bigint, string | null | undefined][][] = [];
    const errors: Error[] = [];
    client.onBatch(
      D,
      (batch) => void batches.push(batch.map((e) => [e.version, e.senderName])),
      { ...FAST, withNames: true, onError: (err) => void errors.push(err) },
    );
    await until(() => batches.flat().length === 3);
    client.destroy();
    expect(errors).toHaveLength(1);
    expect(batches).toEqual([
      [[1n, "n1.apt"]],
      [
        [2n, "n2.apt"],
        [3n, "n3.apt"],
      ],
    ]);
  });
});
