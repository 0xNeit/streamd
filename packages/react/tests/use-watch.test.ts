import "./dom.js";
import { describe, expect, it } from "bun:test";
import { StrictMode, createElement } from "react";
import { renderToString } from "react-dom/server";
import {
  MemoryAdapter,
  normalizeAddress,
  StreamdClient,
  StreamdError,
  StreamdNetworkError,
  type EventAdapter,
  type StreamdEvent,
  type Watch,
} from "@streamd/core";
import { StreamdProvider, useWatch, type UseWatchOptions, type UseWatchResult } from "../src/index.js";
import { render, rerender, unmount, waitFor } from "./render.js";

const FAST = { fromVersion: 0n, pollInterval: 5, maxBackoff: 10 } satisfies UseWatchOptions;
const A = "0xa";
const B = "0xb";
const C = "0xc";

function ev(version: bigint, sender: string): StreamdEvent {
  return { type: "0x1::test::Event", data: {}, version, sequenceNumber: 0n, eventIndex: 0, sender };
}

/**
 * A `MemoryAdapter` answering each `account` walk with the events its address
 * sent, counting fetches, failing for addresses in `failing`, and counting
 * `watch`, `add` and `remove` calls on the client.
 */
function setup(events: StreamdEvent[] = [ev(1n, A), ev(2n, B), ev(3n, C)]) {
  const memory = new MemoryAdapter(events);
  const chain = { memory, fetches: 0, failing: new Set<string>() };
  const adapter: EventAdapter = {
    getHeadVersion: () => memory.getHeadVersion(),
    fetchEvents: async (d, o) => {
      chain.fetches++;
      if (d.kind !== "account") throw new Error("account descriptors only");
      const address = normalizeAddress(d.accountAddress)!;
      if (chain.failing.has(address)) throw new StreamdNetworkError("down");
      const page = await memory.fetchEvents(d, o);
      return { ...page, events: page.events.filter((e) => normalizeAddress(e.sender!) === address) };
    },
  };
  const client = new StreamdClient({ adapter });
  const calls = { watch: 0, add: [] as string[], remove: [] as string[] };
  const watch = client.watch.bind(client);
  client.watch = ((...args: Parameters<typeof watch>) => {
    calls.watch++;
    const w: Watch = watch(...args);
    const add = w.add.bind(w);
    const remove = w.remove.bind(w);
    w.add = (a) => (calls.add.push(a), add(a));
    w.remove = (a) => (calls.remove.push(a), remove(a));
    return w;
  }) as typeof client.watch;
  const results: UseWatchResult<unknown>[] = [];
  function Watcher({ list, options }: { list: string[] | null; options: UseWatchOptions }) {
    // Inline options on purpose: a new object every render.
    results.push(useWatch(list, { ...options }));
    return null;
  }
  const el = (list: string[] | null, options: UseWatchOptions = FAST) =>
    createElement(StreamdProvider, { client }, createElement(Watcher, { list, options }));
  const last = () => results.at(-1)!;
  /** Each delivery as `version@address`. */
  const seen = () => last().data.map((d) => `${d.event.version}@${d.address}`);
  return { chain, client, calls, el, results, last, seen };
}

const pause = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe("useWatch", () => {
  it("goes live and tags each event with its watched address", async () => {
    const t = setup();
    const root = await render(t.el([A, B]));
    expect(t.results[0]!.status).toBe("idle");
    await waitFor(() => expect(t.seen().sort()).toEqual(["1@0xa", "2@0xb"]));
    expect(t.last().status).toBe("live");
    expect(t.last().addresses).toEqual([A, B]);
    t.chain.memory.push(ev(4n, B), ev(5n, C));
    await waitFor(() => expect(t.seen()).toContain("4@0xb"));
    await pause(30);
    expect(t.seen()).not.toContain("5@0xc");
    await unmount(root);
  });

  it("adds a newly listed address to the running watch", async () => {
    const t = setup();
    const root = await render(t.el([A]));
    await waitFor(() => expect(t.seen()).toEqual(["1@0xa"]));
    await rerender(root, t.el([A, C]));
    await waitFor(() => expect(t.last().addresses).toEqual([A, C]));
    expect(t.calls.add).toEqual([C]);
    t.chain.memory.push(ev(4n, C));
    // An added walk starts as on() without a start: on MemoryAdapter, its oldest event.
    await waitFor(() => expect(t.seen()).toEqual(["1@0xa", "3@0xc", "4@0xc"]));
    expect(t.calls.watch).toBe(1);
    await unmount(root);
  });

  it("removes an address that leaves the list, without restarting", async () => {
    const t = setup();
    const root = await render(t.el([A, B]));
    await waitFor(() => expect(t.seen()).toHaveLength(2));
    await rerender(root, t.el([A]));
    await waitFor(() => expect(t.last().addresses).toEqual([A]));
    expect(t.calls.remove).toEqual([normalizeAddress(B)!]);
    t.chain.memory.push(ev(4n, B), ev(5n, A));
    await waitFor(() => expect(t.seen()).toContain("5@0xa"));
    await pause(30);
    expect(t.seen()).not.toContain("4@0xb");
    expect(t.seen()).toHaveLength(3);
    expect(t.calls.watch).toBe(1);
    await unmount(root);
  });

  it("leaves the watch alone for a list equal by canonical address", async () => {
    const t = setup();
    const root = await render(t.el([A, B]));
    await waitFor(() => expect(t.seen()).toHaveLength(2));
    const longB = normalizeAddress(B)!;
    await rerender(root, t.el([longB, "0xA"]));
    await rerender(root, t.el([B, A, "0x0A"]));
    await pause(20);
    expect(t.calls).toEqual({ watch: 1, add: [], remove: [] });
    // Addresses keep the spelling they were first added with.
    expect(t.last().addresses).toEqual([A, B]);
    await unmount(root);
  });

  it("starts an empty list live and adds to it without restarting", async () => {
    const t = setup();
    const root = await render(t.el([]));
    await waitFor(() => expect(t.last().status).toBe("live"));
    expect(t.last().addresses).toEqual([]);
    await rerender(root, t.el([B]));
    t.chain.memory.push(ev(4n, B));
    await waitFor(() => expect(t.seen()).toEqual(["2@0xb", "4@0xb"]));
    expect(t.calls.watch).toBe(1);
    await unmount(root);
  });

  it("drops an address whose walk gives up, and does not re-add it while still listed", async () => {
    const t = setup();
    t.chain.failing.add(normalizeAddress(B)!);
    const errors: Error[] = [];
    const opts: UseWatchOptions = { ...FAST, maxErrors: 2, onError: (e) => void errors.push(e) };
    const root = await render(t.el([A, B], opts));
    await waitFor(() => expect(t.last().addresses).toEqual([A]));
    expect(t.last().error).toBeInstanceOf(Error);
    expect(t.last().status).toBe("live");
    expect(errors.length).toBeGreaterThan(0);
    t.chain.failing.clear();
    await rerender(root, t.el([A, B, C], opts));
    await waitFor(() => expect(t.last().addresses).toEqual([A, C]));
    expect(t.calls.add).toEqual([C]);
    t.chain.memory.push(ev(4n, A));
    await waitFor(() => expect(t.seen()).toContain("4@0xa"));
    expect(t.last().error).toBeUndefined();
    // Leaving the list and coming back adds it again.
    await rerender(root, t.el([A, C], opts));
    await rerender(root, t.el([A, C, B], opts));
    await waitFor(() => expect(t.last().addresses).toEqual([A, C, B]));
    expect(t.calls.watch).toBe(1);
    await unmount(root);
  });

  it("restarts with empty data when an option changes", async () => {
    const t = setup();
    const root = await render(t.el([A, B]));
    await waitFor(() => expect(t.seen()).toHaveLength(2));
    await rerender(root, t.el([A, B], { ...FAST, fromVersion: 2n }));
    await waitFor(() => expect(t.seen()).toEqual(["2@0xb"]));
    expect(t.calls.watch).toBe(2);
    await rerender(root, t.el([A, B], { ...FAST, fromVersion: 2n, maxEvents: 5 }));
    await pause(20);
    expect(t.calls.watch).toBe(2);
    await unmount(root);
  });

  it("stays idle with a null list", async () => {
    const t = setup();
    const root = await render(t.el(null));
    expect(t.last()).toEqual({ data: [], error: undefined, status: "idle", addresses: [] });
    expect(t.calls.watch).toBe(0);
    await unmount(root);
  });

  it("starts nothing during server rendering", () => {
    const t = setup();
    renderToString(t.el([A]));
    expect(t.calls.watch).toBe(0);
    expect(t.chain.fetches).toBe(0);
    expect(t.last().status).toBe("idle");
  });

  it("stops fetching on unmount under StrictMode", async () => {
    const t = setup();
    const root = await render(createElement(StrictMode, null, t.el([A, B])));
    await waitFor(() => expect(t.seen().sort()).toEqual(["1@0xa", "2@0xb"]));
    // StrictMode mounted, unmounted and remounted the effect.
    expect(t.calls.watch).toBe(2);
    expect(t.calls.add).toEqual([]);
    await unmount(root);
    const fetches = t.chain.fetches;
    await pause(40);
    expect(t.chain.fetches).toBe(fetches);
  });

  it("refuses a malformed address and a bad maxEvents while rendering", () => {
    const t = setup();
    expect(() => renderToString(t.el([A, "alice.apt"]))).toThrow(StreamdError);
    for (const maxEvents of [0, -1, 1.5]) {
      expect(() => renderToString(t.el([A], { maxEvents }))).toThrow(StreamdError);
    }
  });
});
