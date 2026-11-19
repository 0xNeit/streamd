import { describe, it, expect } from "bun:test";
import { TabSharedAdapter } from "../src/adapters/tabs.js";
import type { BroadcastChannelLike, LockManagerLike } from "../src/adapters/tabs.js";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { StreamdClient } from "../src/client.js";
import { StreamdError, StreamdNetworkError } from "../src/errors.js";
import type { EventAdapter, EventDescriptor, EventPage, FetchOptions, StreamdEvent } from "../src/types.js";

const desc: EventDescriptor = { kind: "module", eventType: "0x1::m::E" };
const settle = () => new Promise((r) => setTimeout(r, 5));

/** One lock name's queue, granted in request order as the browser does. */
class FakeLocks implements LockManagerLike {
  private held = false;
  private readonly queue: Array<() => void> = [];

  request(_name: string, options: { signal?: AbortSignal }, callback: (lock: unknown) => Promise<void>) {
    return new Promise<unknown>((resolve, reject) => {
      const grant = () => {
        this.held = true;
        callback(null)
          .then(resolve, reject)
          .finally(() => {
            this.held = false;
            this.queue.shift()?.();
          });
      };
      if (!this.held) return grant();
      this.queue.push(grant);
      options.signal?.addEventListener("abort", () => {
        const at = this.queue.indexOf(grant);
        if (at >= 0) this.queue.splice(at, 1), reject(new Error("AbortError"));
      });
    });
  }
}

/** Channels on one name: each message reaches every other open channel, structured-cloned. */
function channelHub() {
  const open = new Set<BroadcastChannelLike>();
  return (): BroadcastChannelLike => {
    const channel: BroadcastChannelLike = {
      onmessage: null,
      postMessage(message) {
        if (!open.has(channel)) throw new Error("InvalidStateError");
        for (const other of open) {
          if (other === channel) continue;
          const data = structuredClone(message);
          setTimeout(() => other.onmessage?.({ data }));
        }
      },
      close: () => void open.delete(channel),
    };
    open.add(channel);
    return channel;
  };
}

function source(page: (options: FetchOptions) => EventPage = (o) => ({
  events: [],
  nextCursor: String(Number(o.cursor ?? 0) + 1),
  hasMore: false,
})) {
  const calls: FetchOptions[] = [];
  const adapter: EventAdapter = {
    fetchEvents: async (_d, options) => (calls.push(options), page(options)),
    getHeadVersion: async () => 7n,
  };
  return { adapter, calls };
}

function tabs(
  n: number,
  adapter: EventAdapter,
  extra: { shareFor?: number; requestTimeout?: number; joinHeadStarts?: boolean } = {},
) {
  const locks = new FakeLocks();
  const channel = channelHub();
  return Array.from(
    { length: n },
    () => new TabSharedAdapter(adapter, { name: "t", locks, channel: channel(), ...extra }),
  );
}

describe("TabSharedAdapter", () => {
  it("refuses a missing name and a bad shareFor", () => {
    const { adapter } = source();
    expect(() => new TabSharedAdapter(adapter, { name: "" })).toThrow(StreamdError);
    expect(() => new TabSharedAdapter(adapter, { name: "t", shareFor: 0 })).toThrow(/shareFor/);
    expect(() => new TabSharedAdapter(adapter, { name: "t", requestTimeout: 0 })).toThrow(/requestTimeout/);
  });

  it("calls the source directly without Web Locks", async () => {
    const s = source();
    const a = new TabSharedAdapter(s.adapter, { name: "t" });
    await a.fetchEvents(desc, { cursor: "1" });
    await a.fetchEvents(desc, { cursor: "1" });
    expect(s.calls).toHaveLength(2);
    expect(a.isLeader).toBe(false);
  });

  it("passes the other methods through", async () => {
    const [a] = tabs(1, source().adapter);
    expect(await a!.getHeadVersion!(desc)).toBe(7n);
    expect(a!.view).toBeUndefined();
  });

  it("elects one leader and hands over when it closes", async () => {
    const s = source();
    const [a, b, c] = tabs(3, s.adapter);
    await settle();
    expect([a!.isLeader, b!.isLeader, c!.isLeader]).toEqual([true, false, false]);
    a!.close();
    await settle();
    expect([a!.isLeader, b!.isLeader, c!.isLeader]).toEqual([false, true, false]);
    await expect(a!.fetchEvents(desc)).rejects.toThrow(/closed/);
  });

  it("serves a waiting request once its tab leads", async () => {
    const s = source();
    const [a, b] = tabs(2, s.adapter);
    await settle();
    const waiting = b!.fetchEvents(desc, { cursor: "4" });
    a!.close();
    expect((await waiting).nextCursor).toBe("5");
  });

  it("rejects waiting requests on close", async () => {
    const [, b] = tabs(2, source().adapter);
    await settle();
    const waiting = b!.fetchEvents(desc, { cursor: "4" });
    b!.close();
    await expect(waiting).rejects.toThrow(/closed/);
  });

  it("makes one source request for a request every tab sends", async () => {
    const s = source();
    const all = tabs(3, s.adapter);
    await settle();
    const pages = await Promise.all(all.map((t) => t.fetchEvents(desc, { cursor: "4", limit: 5 })));
    expect(s.calls).toHaveLength(1);
    expect(pages.map((p) => p.nextCursor)).toEqual(["5", "5", "5"]);
    expect(typeof (await all[1]!.fetchEvents(desc, { fromVersion: 3n })).nextCursor).toBe("string");
    expect(s.calls[1]!.fromVersion).toBe(3n);
  });

  it("joins a start at the head to the newest caught-up page of its walk", async () => {
    const s = source();
    const [a, b] = tabs(2, s.adapter, { joinHeadStarts: true });
    await settle();
    await a!.fetchEvents(desc, { cursor: "9", limit: 5 });
    expect((await b!.fetchEvents(desc, { limit: 5 })).nextCursor).toBe("10");
    expect(s.calls).toHaveLength(1);
    await b!.fetchEvents(desc, { limit: 6 });
    await b!.fetchEvents(desc, { limit: 5, fromVersion: 1n });
    expect(s.calls).toHaveLength(3);
  });

  it("does not join a walk to a page that was not caught up", async () => {
    const s = source((o) => ({ events: [], nextCursor: String(Number(o.cursor ?? 0) + 1), hasMore: true }));
    const [a, b] = tabs(2, s.adapter, { joinHeadStarts: true });
    await settle();
    await a!.fetchEvents(desc, { cursor: "9" });
    await b!.fetchEvents(desc, {});
    expect(s.calls).toHaveLength(2);
  });

  it("joins no start unless asked, and never for a handle or account walk", async () => {
    const handle: EventDescriptor = { kind: "handle", accountAddress: "0x1", creationNumber: 5n };
    for (const [d, join] of [[desc, false], [handle, true]] as const) {
      const s = source();
      const [a, b] = tabs(2, s.adapter, { joinHeadStarts: join });
      await settle();
      await a!.fetchEvents(d, { cursor: "9" });
      expect((await b!.fetchEvents(d, {})).nextCursor).toBe("1");
      expect(s.calls).toHaveLength(2);
    }
  });

  it("gives up on a request the leader never answers after requestTimeout", async () => {
    const stuck: EventAdapter = { fetchEvents: () => new Promise(() => {}) };
    const [, b] = tabs(2, stuck, { requestTimeout: 20 });
    await settle();
    const failed = await b!.fetchEvents(desc, { cursor: "1" }).catch((e: unknown) => e);
    expect(failed).toBeInstanceOf(StreamdNetworkError);
    expect((b as unknown as { pending: Map<number, unknown> }).pending.size).toBe(0);
  });

  it("asks the next leader for a request the old one took with it", async () => {
    let call = 0;
    const s: EventAdapter = {
      fetchEvents: (_d, o) =>
        ++call === 1 ? new Promise(() => {}) : Promise.resolve({ events: [], nextCursor: `after ${o.cursor}`, hasMore: false }),
    };
    const [a, , c] = tabs(3, s);
    await settle();
    const waiting = c!.fetchEvents(desc, { cursor: "4" });
    await settle();
    expect(call).toBe(1);
    a!.close();
    expect((await waiting).nextCursor).toBe("after 4");
  });

  it("rebuilds a source failure as the same kind of error", async () => {
    const s: EventAdapter = {
      fetchEvents: (_d, o) =>
        Promise.reject(
          o.cursor === "1"
            ? new StreamdNetworkError("rate limited", 429, undefined, 2000)
            : o.cursor === "2"
              ? new Error("broken")
              : new StreamdError("bad input"),
        ),
    };
    const [, b] = tabs(2, s);
    await settle();
    const failed = await b!.fetchEvents(desc, { cursor: "1" }).catch((e: unknown) => e);
    expect(failed).toBeInstanceOf(StreamdNetworkError);
    expect(failed).toMatchObject({ message: "rate limited", statusCode: 429, retryAfter: 2000 });
    // Retried, as the leading tab's own poller would retry it.
    const other = await b!.fetchEvents(desc, { cursor: "2" }).catch((e: unknown) => e);
    expect(other).toBeInstanceOf(StreamdNetworkError);
    expect((other as Error).message).toBe("broken");
    const input = await b!.fetchEvents(desc, { cursor: "3" }).catch((e: unknown) => e);
    expect(input).toBeInstanceOf(StreamdError);
    expect(input).not.toBeInstanceOf(StreamdNetworkError);
  });

  it("ignores messages that are not its own or not well formed", async () => {
    const locks = new FakeLocks();
    const hub = channelHub();
    const s = source();
    const a = new TabSharedAdapter(s.adapter, { name: "t", locks, channel: hub() });
    const b = new TabSharedAdapter(s.adapter, { name: "t", locks, channel: hub() });
    const stranger = hub();
    await settle();
    stranger.postMessage({ kind: "request", from: 5, id: "x" });
    stranger.postMessage({ kind: "request", from: "s", id: 1, descriptor: "x" });
    stranger.postMessage("noise");
    await settle();
    expect(a.isLeader).toBe(true);
    expect(s.calls).toHaveLength(0);
    const tab = (b as unknown as { tab: string }).tab;
    const waiting = b.fetchEvents(desc, { cursor: "1" });
    stranger.postMessage({ kind: "reply", to: tab, id: 1, page: { events: [{ version: "1" }], hasMore: false } });
    await expect(waiting).rejects.toThrow(/malformed/);
  });

  it("delivers every event once to two clients across a handover", async () => {
    const ev = (version: number): StreamdEvent => ({
      type: "0x1::m::E",
      data: { n: version },
      version: BigInt(version),
      sequenceNumber: 0n,
      eventIndex: 0,
    });
    const memory = new MemoryAdapter([ev(1), ev(2)]);
    let sourceCalls = 0;
    const counted: EventAdapter = { fetchEvents: (d, o) => (sourceCalls++, memory.fetchEvents(d, o)) };
    const [ta, tb] = tabs(2, counted, { shareFor: 15 });
    const clients = [ta!, tb!].map((adapter) => new StreamdClient({ adapter, defaults: { pollInterval: 20 } }));
    const seen: bigint[][] = [[], []];
    clients.forEach((client, i) => client.on(desc, (e) => void seen[i]!.push(e.version), { fromVersion: 0n }));
    await new Promise((r) => setTimeout(r, 120));
    const polls = sourceCalls;
    memory.push(ev(3));
    await new Promise((r) => setTimeout(r, 60));
    ta!.close();
    clients[0]!.destroy();
    memory.push(ev(4), ev(5));
    await new Promise((r) => setTimeout(r, 120));
    clients[1]!.destroy();
    tb!.close();
    expect(seen[0]).toEqual([1n, 2n, 3n]);
    expect(seen[1]).toEqual([1n, 2n, 3n, 4n, 5n]);
    // Two tabs polling every 20 ms for 120 ms would make about 12 requests on their own.
    expect(polls).toBeLessThanOrEqual(8);
  });
});
