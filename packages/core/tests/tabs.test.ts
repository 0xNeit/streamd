import { describe, it, expect } from "bun:test";
import { TabSharedAdapter } from "../src/adapters/tabs.js";
import type { BroadcastChannelLike, LockManagerLike } from "../src/adapters/tabs.js";
import { StreamdError } from "../src/errors.js";
import type { EventAdapter, EventDescriptor, EventPage, FetchOptions } from "../src/types.js";

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

function tabs(n: number, adapter: EventAdapter, extra: { shareFor?: number } = {}) {
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
});
