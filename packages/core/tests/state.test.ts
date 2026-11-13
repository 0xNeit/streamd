import { describe, it, expect } from "bun:test";
import { StreamdClient } from "../src/client.js";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { StreamdError, StreamdNetworkError } from "../src/errors.js";
import type {
  EventAdapter,
  EventDescriptor,
  StreamdEvent,
  ViewRequest,
} from "../src/types.js";

const PING = "0x1::test::Ping";
const PONG = "0x1::test::Pong";
const BALANCE = "0x1::coin::balance";
const pings: EventDescriptor = { kind: "module", eventType: PING };
const pongs: EventDescriptor = { kind: "module", eventType: PONG };

function ev(version: number, type = PING): StreamdEvent {
  return { type, data: { amount: String(version * 10) }, version: BigInt(version), sequenceNumber: 0n, eventIndex: 0 };
}

interface ViewCall {
  request: ViewRequest;
  version: bigint;
  descriptor: EventDescriptor | undefined;
}

/** A `MemoryAdapter` whose `view` answers `[version as a string]`, or runs `answer`. */
function viewing(
  events: StreamdEvent[],
  answer: (call: ViewCall, n: number) => unknown = (c) => [String(c.version)],
) {
  const calls: ViewCall[] = [];
  const adapter: EventAdapter = new MemoryAdapter(events);
  adapter.view = async (request, version, descriptor) => {
    const call = { request, version, descriptor };
    calls.push(call);
    return answer(call, calls.length) as unknown[];
  };
  return { adapter, calls };
}

const balanceOf = (event: StreamdEvent): ViewRequest => ({
  function: BALANCE,
  typeArguments: ["0x1::aptos_coin::AptosCoin"],
  functionArguments: [(event.data as { amount: unknown }).amount],
});

describe("withState on queries", () => {
  it("attaches the view result read at each event's version", async () => {
    const { adapter, calls } = viewing([ev(1), ev(2)]);
    const events = await new StreamdClient({ adapter }).getEvents(pings, {
      fromVersion: 0n,
      withState: balanceOf,
    });
    expect(events.map((e) => e.state)).toEqual([["1"], ["2"]]);
    expect(calls.map((c) => [c.version, c.request.functionArguments, c.descriptor])).toEqual([
      [1n, ["10"], pings],
      [2n, ["20"], pings],
    ]);
  });

  it("runs after decode, so decoded fields reach the request", async () => {
    const { adapter, calls } = viewing([ev(1)]);
    await new StreamdClient({ adapter }).getEvents(pings, {
      fromVersion: 0n,
      decode: ["amount"],
      withState: balanceOf,
    });
    expect(calls[0]!.request.functionArguments).toEqual([10n]);
  });

  it("delivers an event without state when the builder returns undefined", async () => {
    const { adapter, calls } = viewing([ev(1), ev(2)]);
    const events = await new StreamdClient({ adapter }).getEvents(pings, {
      fromVersion: 0n,
      withState: (e) => (e.version === 1n ? undefined : balanceOf(e)),
    });
    expect("state" in events[0]!).toBe(false);
    expect(events[1]!.state).toEqual(["2"]);
    expect(calls).toHaveLength(1);
  });

  it("calls only for events that pass where, limit and orderBy", async () => {
    const { adapter, calls } = viewing([ev(1), ev(2), ev(3), ev(4), ev(5)]);
    const client = new StreamdClient({ adapter });
    await client.getEvents(pings, {
      fromVersion: 0n,
      where: (e) => e.version % 2n === 1n,
      limit: 2,
      withState: balanceOf,
    });
    expect(calls.map((c) => c.version)).toEqual([1n, 3n]);
    calls.length = 0;
    const latest = await client.getEvents(pings, {
      fromVersion: 0n,
      orderBy: "desc",
      limit: 2,
      withState: balanceOf,
    });
    expect(latest.map((e) => e.state)).toEqual([["5"], ["4"]]);
    expect(calls.map((c) => c.version)).toEqual([5n, 4n]);
  });

  it("does not change the adapter's events", async () => {
    const events = [ev(1)];
    const { adapter } = viewing(events);
    await new StreamdClient({ adapter }).getEvents(pings, { fromVersion: 0n, withState: balanceOf });
    expect("state" in events[0]!).toBe(false);
  });

  it("routes a merge's view calls by its first descriptor", async () => {
    const { adapter, calls } = viewing([ev(1, PING), ev(2, PONG)]);
    const events = await new StreamdClient({ adapter }).getEvents([pongs, pings], {
      fromVersion: 0n,
      withState: balanceOf,
    });
    expect(events.map((e) => [e.type, e.state])).toEqual([
      [PING, ["1"]],
      [PONG, ["2"]],
    ]);
    expect(calls.map((c) => c.descriptor)).toEqual([pongs, pongs]);
  });

  it("works through getEventPages and iterateEvents", async () => {
    const { adapter } = viewing([ev(1), ev(2), ev(3)]);
    const client = new StreamdClient({ adapter });
    const pages: unknown[][] = [];
    for await (const page of client.getEventPages(pings, { fromVersion: 0n, withState: balanceOf }, 2)) {
      pages.push(page.map((e) => e.state));
    }
    expect(pages).toEqual([[["1"], ["2"]], [["3"]]]);
    const states: unknown[] = [];
    for await (const e of client.iterateEvents(pings, { fromVersion: 0n, withState: balanceOf })) {
      states.push(e.state);
    }
    expect(states).toEqual([["1"], ["2"], ["3"]]);
  });

  it("retries a failed view with the query's retries", async () => {
    const { adapter, calls } = viewing([ev(1)], (c, n) => {
      if (n === 1) throw new StreamdNetworkError("flaky", 503, undefined, 0);
      return [String(c.version)];
    });
    const events = await new StreamdClient({ adapter }).getEvents(pings, {
      fromVersion: 0n,
      retries: 1,
      withState: balanceOf,
    });
    expect(events[0]!.state).toEqual(["1"]);
    expect(calls).toHaveLength(2);
  });

  it("rejects on a builder throw, an invalid request, a failed or malformed view", async () => {
    const boom = new Error("builder bug");
    const cases: [ReturnType<typeof viewing>, (e: StreamdEvent) => ViewRequest | undefined, unknown][] = [
      [viewing([ev(1)]), () => { throw boom; }, boom],
      [viewing([ev(1)]), () => ({ function: "nope" }) as never, StreamdError],
      [viewing([ev(1)], () => { throw new StreamdNetworkError("gone", 410); }), balanceOf, StreamdNetworkError],
      [viewing([ev(1)], () => ({ not: "an array" })), balanceOf, StreamdError],
    ];
    for (const [{ adapter, calls }, withState, expected] of cases) {
      const err = await new StreamdClient({ adapter })
        .getEvents(pings, { fromVersion: 0n, withState })
        .catch((e: unknown) => e);
      if (expected === boom) expect(err).toBe(boom);
      else expect(err).toBeInstanceOf(expected as typeof StreamdError);
      if (withState !== balanceOf) expect(calls).toHaveLength(0);
    }
  });

  it("refuses a non-function withState and an adapter without view", async () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter([ev(1)]) });
    const notFn = await client
      .getEvents(pings, { withState: "0x1::coin::balance" as never })
      .catch((e: Error) => e);
    expect((notFn as Error).message).toContain("withState must be a function");
    const noView = await client
      .getEvents(pings, { fromVersion: 0n, withState: balanceOf })
      .catch((e: Error) => e);
    expect(noView).toBeInstanceOf(StreamdError);
    expect((noView as Error).message).toContain("implements view");
  });

  it("keeps withState out of the adapter's fetch options", async () => {
    const { adapter } = viewing([ev(1)]);
    const seen: object[] = [];
    const fetch = adapter.fetchEvents.bind(adapter);
    adapter.fetchEvents = (d, options) => (seen.push(options), fetch(d, options));
    await new StreamdClient({ adapter }).getEvents(pings, { fromVersion: 0n, withState: balanceOf });
    expect(seen.every((o) => !("withState" in o))).toBe(true);
  });

  it("goes through maxConcurrentRequests", async () => {
    let inFlight = 0;
    let most = 0;
    const { adapter } = viewing([ev(1), ev(2)], async (c) => {
      most = Math.max(most, ++inFlight);
      await new Promise((r) => setTimeout(r, 5));
      inFlight--;
      return [String(c.version)];
    });
    const client = new StreamdClient({ adapter, maxConcurrentRequests: 1 });
    await Promise.all([
      client.getEvents(pings, { fromVersion: 0n, withState: balanceOf }),
      client.getEvents(pings, { fromVersion: 0n, withState: balanceOf }),
    ]);
    expect(most).toBe(1);
  });
});

const until = async (done: () => boolean) => {
  for (let i = 0; i < 200 && !done(); i++) await new Promise((r) => setTimeout(r, 2));
  expect(done()).toBe(true);
};

describe("withState on listeners", () => {
  it("delivers state through on, onBatch, once and stream", async () => {
    const { adapter, calls } = viewing([ev(1), ev(2)]);
    const client = new StreamdClient({ adapter });
    const opts = { fromVersion: 0n, pollInterval: 5, withState: balanceOf };
    const seen: unknown[] = [];
    const sub = client.on(pings, (e) => void seen.push(e.state), opts);
    const batches: unknown[][] = [];
    const batchSub = client.onBatch(pings, (es) => void batches.push(es.map((e) => e.state)), opts);
    await until(() => seen.length === 2 && batches.length === 1);
    sub.unsubscribe();
    batchSub.unsubscribe();
    expect(seen).toEqual([["1"], ["2"]]);
    expect(batches).toEqual([[["1"], ["2"]]]);
    expect((await client.once(pings, opts)).state).toEqual(["1"]);
    for await (const e of client.stream(pings, opts)) {
      expect(e.state).toEqual(["1"]);
      break;
    }
    expect(calls.every((c) => c.descriptor === pings)).toBe(true);
  });

  it("reports a builder throw or invalid request and skips the event", async () => {
    const { adapter } = viewing([ev(1), ev(2), ev(3)]);
    const errors: Error[] = [];
    const seen: bigint[] = [];
    const sub = new StreamdClient({ adapter }).on(pings, (e) => void seen.push(e.version), {
      fromVersion: 0n,
      pollInterval: 5,
      onError: (err) => errors.push(err),
      withState: (e) => {
        if (e.version === 1n) throw new Error("builder bug");
        if (e.version === 2n) return { function: "bad" } as never;
        return balanceOf(e);
      },
    });
    await until(() => seen.length === 1);
    expect(sub.active).toBe(true);
    sub.unsubscribe();
    expect(seen).toEqual([3n]);
    expect(errors.map((e) => e.constructor)).toEqual([StreamdError, StreamdError]);
    expect(sub.status.consecutiveErrors).toBe(0);
  });

  it("fails the poll on a failed view and delivers the event on the retry", async () => {
    const { adapter, calls } = viewing([ev(1), ev(2), ev(3)], (c, n) => {
      if (n === 2) throw new StreamdNetworkError("flaky", 503);
      return [String(c.version)];
    });
    const errors: Error[] = [];
    const seen: [bigint, unknown][] = [];
    const sub = new StreamdClient({ adapter }).on(pings, (e) => void seen.push([e.version, e.state]), {
      fromVersion: 0n,
      pollInterval: 1,
      onError: (err) => errors.push(err),
      withState: balanceOf,
    });
    await until(() => seen.length === 3);
    sub.unsubscribe();
    expect(seen).toEqual([[1n, ["1"]], [2n, ["2"]], [3n, ["3"]]]);
    expect(calls.map((c) => c.version)).toEqual([1n, 2n, 2n, 3n]);
    expect(errors).toHaveLength(1);
    expect((errors[0] as StreamdError).cause).toBeInstanceOf(StreamdNetworkError);
  });

  it("abandons a hung view on pause, so resume reads the event again", async () => {
    const { adapter, calls } = viewing([ev(1)], (c, n) =>
      n === 1 ? new Promise(() => {}) : [String(c.version)],
    );
    const seen: unknown[] = [];
    const sub = new StreamdClient({ adapter }).on(pings, (e) => void seen.push(e.state), {
      fromVersion: 0n,
      pollInterval: 1,
      withState: balanceOf,
    });
    await until(() => calls.length === 1);
    sub.pause();
    sub.resume();
    await until(() => seen.length === 1);
    sub.unsubscribe();
    expect(seen).toEqual([["1"]]);
    expect(calls).toHaveLength(2);
  });

  it("redelivers a batch abandoned by a pause during a view", async () => {
    const { adapter, calls } = viewing([ev(1), ev(2), ev(3)], (c, n) =>
      n === 2 ? new Promise(() => {}) : [String(c.version)],
    );
    const batches: bigint[][] = [];
    const sub = new StreamdClient({ adapter }).onBatch(
      pings,
      (es) => void batches.push(es.map((e) => e.version)),
      { fromVersion: 0n, pollInterval: 1, withState: balanceOf },
    );
    await until(() => calls.length === 2);
    sub.pause();
    sub.resume();
    await until(() => batches.flat().length >= 3);
    sub.unsubscribe();
    expect(batches).toEqual([[1n, 2n, 3n]]);
  });

  it("frees a hung view's request slot when the subscription ends", async () => {
    const { adapter, calls } = viewing([ev(1)], () => new Promise(() => {}));
    const client = new StreamdClient({ adapter, maxConcurrentRequests: 1 });
    const sub = client.on(pings, () => {}, { fromVersion: 0n, withState: balanceOf });
    await until(() => calls.length === 1);
    sub.unsubscribe();
    expect(await client.getEvents(pings, { fromVersion: 0n })).toHaveLength(1);
    client.destroy();
  });

  it("delivers a partial batch before failing the poll", async () => {
    const { adapter } = viewing([ev(1), ev(2), ev(3)], (c, n) => {
      if (n === 2) throw new StreamdNetworkError("flaky", 503);
      return [String(c.version)];
    });
    const batches: bigint[][] = [];
    const sub = new StreamdClient({ adapter }).onBatch(
      pings,
      (es) => void batches.push(es.map((e) => e.version)),
      { fromVersion: 0n, pollInterval: 1, onError: () => {}, withState: balanceOf },
    );
    await until(() => batches.flat().length === 3);
    sub.unsubscribe();
    expect(batches).toEqual([[1n], [2n, 3n]]);
  });

  it("stops on an adapter without view", async () => {
    const errors: Error[] = [];
    const sub = new StreamdClient({ adapter: new MemoryAdapter([ev(1)]) }).on(pings, () => {}, {
      fromVersion: 0n,
      pollInterval: 5,
      onError: (err) => errors.push(err),
      withState: balanceOf,
    });
    await until(() => errors.length === 1);
    expect(sub.active).toBe(false);
    expect(errors[0]!.message).toContain("implements view");
  });

  it("refuses a non-function withState when subscribing", () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter() });
    expect(() => client.on(pings, () => {}, { withState: {} as never })).toThrow(
      "withState must be a function",
    );
  });
});

describe("withState edge cases", () => {
  it("does not retry a throwing builder on queries", async () => {
    const { adapter } = viewing([ev(1)]);
    let built = 0;
    const boom = new Error("builder bug");
    const err = await new StreamdClient({ adapter })
      .getEvents(pings, {
        fromVersion: 0n,
        retries: 2,
        withState: () => {
          built++;
          throw boom;
        },
      })
      .catch((e: unknown) => e);
    expect(err).toBe(boom);
    expect(built).toBe(1);
  });

  it("stops a subscription at once on a view refused as invalid input", async () => {
    const { adapter, calls } = viewing([ev(1)], () => {
      throw new StreamdError("aborted");
    });
    const errors: Error[] = [];
    const sub = new StreamdClient({ adapter }).on(pings, () => {}, {
      fromVersion: 0n,
      pollInterval: 1,
      onError: (err) => errors.push(err),
      withState: balanceOf,
    });
    await until(() => errors.length === 1);
    expect(sub.active).toBe(false);
    expect(calls).toHaveLength(1);
  });

  for (const mode of ["on", "onBatch"] as const) {
    it(`delivers nothing through ${mode} once unsubscribed during a view`, async () => {
      let release!: () => void;
      let started = false;
      const { adapter } = viewing([ev(1)], async (c) => {
        started = true;
        await new Promise<void>((r) => (release = r));
        return [String(c.version)];
      });
      const client = new StreamdClient({ adapter });
      const seen: unknown[] = [];
      const opts = { fromVersion: 0n, pollInterval: 5, withState: balanceOf };
      const sub =
        mode === "on"
          ? client.on(pings, (e) => void seen.push(e), opts)
          : client.onBatch(pings, (es) => void seen.push(es), opts);
      await until(() => started);
      sub.unsubscribe();
      release();
      await new Promise((r) => setTimeout(r, 20));
      expect(seen).toHaveLength(0);
    });
  }
});

describe("withState object reads", () => {
  type ReadCall = { request: unknown; at: { version: bigint; transactionHash?: string }; descriptor: unknown };
  /** A `MemoryAdapter` whose `readObjects` answers one `id@version` per id, or runs `answer`. */
  function reading(events: StreamdEvent[], answer?: (call: ReadCall) => unknown) {
    const calls: ReadCall[] = [];
    const adapter: EventAdapter = new MemoryAdapter(events);
    adapter.readObjects = async (request, at, descriptor) => {
      const call = { request, at, descriptor };
      calls.push(call);
      return (answer?.(call) ?? request.objects.map((id) => `${id}@${at.version}`)) as unknown[];
    };
    return { adapter, calls };
  }
  const hashed = (version: number): StreamdEvent => ({ ...ev(version), transactionHash: `tx${version}` });
  const objects = () => ({ objects: ["0xa", "0xb"] });

  it("attaches one entry per object, read at the event's transaction", async () => {
    const { adapter, calls } = reading([hashed(1), hashed(2)]);
    const events = await new StreamdClient({ adapter }).getEvents(pings, { fromVersion: 0n, withState: objects });
    expect(events.map((e) => e.state)).toEqual([["0xa@1", "0xb@1"], ["0xa@2", "0xb@2"]]);
    expect(calls.map((c) => [c.request, c.at.version, c.at.transactionHash, c.descriptor])).toEqual([
      [{ objects: ["0xa", "0xb"] }, 1n, "tx1", pings],
      [{ objects: ["0xa", "0xb"] }, 2n, "tx2", pings],
    ]);
  });

  it("delivers object state through on", async () => {
    const { adapter } = reading([hashed(1)]);
    const seen: unknown[] = [];
    const sub = new StreamdClient({ adapter }).on(pings, (e) => void seen.push(e.state), {
      fromVersion: 0n,
      pollInterval: 5,
      withState: objects,
    });
    await until(() => seen.length === 1);
    sub.unsubscribe();
    expect(seen).toEqual([["0xa@1", "0xb@1"]]);
  });

  it("refuses malformed object requests as builder bugs", async () => {
    const bad = [
      { objects: [] },
      { objects: ["0xa", ""] },
      { objects: [7] },
      { objects: "0xa" },
      { objects: ["0xa"], function: BALANCE },
    ];
    for (const request of bad) {
      const { adapter, calls } = reading([hashed(1)]);
      const err = await new StreamdClient({ adapter })
        .getEvents(pings, { fromVersion: 0n, withState: () => request as never })
        .catch((e: unknown) => e);
      expect(err).toBeInstanceOf(StreamdError);
      expect(calls).toHaveLength(0);
    }
    const { adapter } = reading([hashed(1), hashed(2)]);
    const errors: Error[] = [];
    const seen: bigint[] = [];
    const sub = new StreamdClient({ adapter }).on(pings, (e) => void seen.push(e.version), {
      fromVersion: 0n,
      pollInterval: 5,
      onError: (err) => errors.push(err),
      withState: (e) => (e.version === 1n ? ({ objects: [] } as never) : objects()),
    });
    await until(() => seen.length === 1);
    sub.unsubscribe();
    expect(seen).toEqual([2n]);
    expect(errors.map((e) => (e.cause as Error).message)).toEqual(["objects must be a non-empty array of non-empty object ids"]);
  });

  it("rejects without readObjects, and on a result of the wrong shape", async () => {
    const plain = new StreamdClient({ adapter: new MemoryAdapter([hashed(1)]) });
    expect(await plain.getEvents(pings, { fromVersion: 0n, withState: objects }).catch((e: Error) => e.message)).toBe(
      "withState object reads need an adapter that implements readObjects",
    );
    for (const answer of [{ not: "an array" }, ["one"]]) {
      const { adapter } = reading([hashed(1)], () => answer);
      const err = await new StreamdClient({ adapter }).getEvents(pings, { fromVersion: 0n, withState: objects }).catch((e: Error) => e);
      expect((err as Error).message).toBe("readObjects returned no array of 2 for 1:0");
    }
  });

  it("goes through maxConcurrentRequests and leaves views as they were", async () => {
    let inFlight = 0;
    let most = 0;
    const { adapter } = reading([hashed(1), hashed(2)], () => undefined);
    const read = adapter.readObjects!.bind(adapter);
    adapter.readObjects = async (...args) => {
      most = Math.max(most, ++inFlight);
      await new Promise((r) => setTimeout(r, 5));
      inFlight--;
      return read(...args);
    };
    adapter.view = async (_r, version) => [String(version)];
    const client = new StreamdClient({ adapter, maxConcurrentRequests: 1 });
    await Promise.all([
      client.getEvents(pings, { fromVersion: 0n, withState: objects }),
      client.getEvents(pings, { fromVersion: 0n, withState: objects }),
    ]);
    expect(most).toBe(1);
    const viewed = await client.getEvents(pings, { fromVersion: 0n, withState: balanceOf });
    expect(viewed.map((e) => e.state)).toEqual([["1"], ["2"]]);
  });
});
