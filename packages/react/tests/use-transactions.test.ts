import "./dom.js";
import { describe, expect, it } from "bun:test";
import { StrictMode, createElement } from "react";
import { renderToString } from "react-dom/server";
import {
  defineEvent,
  MemoryAdapter,
  StreamdClient,
  StreamdError,
  StreamdNetworkError,
  type EventAdapter,
  type EventDescriptor,
  type StreamdEvent,
} from "@streamd/core";
import {
  StreamdProvider,
  useTransactions,
  type UseTransactionsOptions,
  type UseTransactionsResult,
} from "../src/index.js";
import { OTHER, TYPE } from "./fixtures.js";
import { render, rerender, unmount, waitFor } from "./render.js";

const FAST = { fromVersion: 0n, pollInterval: 5, maxBackoff: 10 } satisfies UseTransactionsOptions;
const DEPOSIT: EventDescriptor = { kind: "module", eventType: TYPE };
const WITHDRAW: EventDescriptor = { kind: "module", eventType: OTHER };

function ev(version: bigint, eventIndex = 0, type = TYPE): StreamdEvent {
  return { type, data: { v: `${version}:${eventIndex}` }, version, sequenceNumber: 0n, eventIndex };
}

/** A `MemoryAdapter` that counts fetches and fails while `failing` is set. */
function setup(events: StreamdEvent[] = [ev(1n), ev(2n), ev(2n, 1), ev(3n)]) {
  const memory = new MemoryAdapter(events);
  const chain = { memory, fetches: 0, failing: false };
  const adapter: EventAdapter = {
    getHeadVersion: () => memory.getHeadVersion(),
    fetchEvents: async (d, o) => {
      chain.fetches++;
      if (chain.failing) throw new StreamdNetworkError("rate limited");
      return memory.fetchEvents(d, o);
    },
  };
  const client = new StreamdClient({ adapter });
  let subscribes = 0;
  const onTransaction = client.onTransaction.bind(client);
  client.onTransaction = ((...args: Parameters<typeof onTransaction>) => (subscribes++, onTransaction(...args))) as typeof client.onTransaction;
  const results: UseTransactionsResult<unknown>[] = [];
  // Inline descriptors and options on purpose: new objects every render.
  function Txs({ types, options }: { types: string[] | string | null; options: UseTransactionsOptions }) {
    const descriptor =
      types === null ? null
      : typeof types === "string" ? { kind: "module" as const, eventType: types as typeof TYPE }
      : types.map((t) => ({ kind: "module" as const, eventType: t as typeof TYPE }));
    results.push(useTransactions(descriptor as EventDescriptor | null, { ...options }));
    return null;
  }
  const el = (types: string[] | string | null, options: UseTransactionsOptions = FAST) =>
    createElement(StreamdProvider, { client }, createElement(Txs, { types, options }));
  const last = () => results.at(-1)!;
  /** Each group as `version:eventIndex,eventIndex`. */
  const groups = () => last().data.map((g) => `${g.version}:${g.events.map((e) => e.eventIndex).join(",")}`);
  return { chain, client, el, results, last, groups, subscribes: () => subscribes };
}

const pause = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe("useTransactions", () => {
  it("goes live and delivers each transaction's events as one group", async () => {
    const t = setup();
    const root = await render(t.el(TYPE));
    expect(t.results[0]!.status).toBe("idle");
    await waitFor(() => expect(t.groups()).toEqual(["1:0", "2:0,1", "3:0"]));
    expect(t.last().status).toBe("live");
    t.chain.memory.push(ev(7n), ev(7n, 1), ev(7n, 2));
    await waitFor(() => expect(t.groups()).toEqual(["1:0", "2:0,1", "3:0", "7:0,1,2"]));
    expect(t.last().data.at(-1)!.events.map((e) => e.data)).toEqual([{ v: "7:0" }, { v: "7:1" }, { v: "7:2" }]);
    await unmount(root);
  });

  it("keeps only the most recent maxTransactions, oldest first", async () => {
    const t = setup();
    const root = await render(t.el(TYPE, { ...FAST, maxTransactions: 2 }));
    await waitFor(() => expect(t.groups()).toEqual(["2:0,1", "3:0"]));
    t.chain.memory.push(ev(4n), ev(4n, 1));
    await waitFor(() => expect(t.groups()).toEqual(["3:0", "4:0,1"]));
    await unmount(root);
  });

  it("merges a list of descriptors into one group per transaction", async () => {
    const t = setup([ev(1n), ev(1n, 1, OTHER), ev(2n, 0, OTHER)]);
    const root = await render(t.el([TYPE, OTHER]));
    await waitFor(() => expect(t.groups()).toEqual(["1:0,1", "2:0"]));
    t.chain.memory.push(ev(5n, 0, OTHER), ev(5n, 1), ev(5n, 2, OTHER));
    await waitFor(() => expect(t.groups()).toEqual(["1:0,1", "2:0", "5:0,1,2"]));
    expect(t.last().data.at(-1)!.events.map((e) => e.type)).toEqual([OTHER, TYPE, OTHER]);
    await unmount(root);
  });

  it("keeps one subscription for an equal inline descriptor and restarts on a different one", async () => {
    const t = setup([ev(1n), ev(2n, 0, OTHER)]);
    const root = await render(t.el(TYPE));
    await waitFor(() => expect(t.groups()).toEqual(["1:0"]));
    await rerender(root, t.el(TYPE));
    await rerender(root, t.el(TYPE, { ...FAST }));
    expect(t.subscribes()).toBe(1);

    await rerender(root, t.el(OTHER));
    expect(t.subscribes()).toBe(2);
    await waitFor(() => expect(t.groups()).toEqual(["2:0"]));
    await rerender(root, t.el([OTHER, TYPE]));
    expect(t.subscribes()).toBe(3);
    await waitFor(() => expect(t.groups()).toEqual(["1:0", "2:0"]));
    await unmount(root);
  });

  it("reports errors, then stopped once maxErrors is reached", async () => {
    const t = setup();
    const errors: Error[] = [];
    const root = await render(t.el(TYPE, { ...FAST, maxErrors: 2, onError: (e) => errors.push(e) }));
    await waitFor(() => expect(t.groups()).toHaveLength(3));
    t.chain.failing = true;
    await waitFor(() => expect(t.last().status).toBe("stopped"));
    expect(t.last().error).toBeInstanceOf(Error);
    expect(t.groups()).toHaveLength(3);
    expect(errors.length).toBeGreaterThan(0);
    await unmount(root);
  });

  it("stays idle with a null descriptor", async () => {
    const t = setup();
    const root = await render(t.el(null));
    expect(t.last()).toEqual({ data: [], error: undefined, status: "idle" });
    expect(t.subscribes()).toBe(0);
    await unmount(root);
  });

  it("starts nothing during server rendering", () => {
    const t = setup();
    renderToString(t.el(TYPE));
    expect(t.subscribes()).toBe(0);
    expect(t.chain.fetches).toBe(0);
    expect(t.last().status).toBe("idle");
  });

  it("stops fetching on unmount under StrictMode", async () => {
    const t = setup();
    const root = await render(createElement(StrictMode, null, t.el([TYPE, OTHER])));
    await waitFor(() => expect(t.groups()).toEqual(["1:0", "2:0,1", "3:0"]));
    // StrictMode mounted, unmounted and remounted the effect.
    expect(t.subscribes()).toBe(2);
    await unmount(root);
    const fetches = t.chain.fetches;
    await pause(40);
    expect(t.chain.fetches).toBe(fetches);
  });

  it("refuses a bad maxTransactions, an empty list and a list with a cursor while rendering", () => {
    const t = setup();
    for (const maxTransactions of [0, -1, 1.5]) {
      expect(() => renderToString(t.el(TYPE, { maxTransactions }))).toThrow(StreamdError);
    }
    expect(() => renderToString(t.el([]))).toThrow("at least one descriptor");
    expect(() => renderToString(t.el([TYPE, OTHER], { cursor: "c" }))).toThrow("cursor cannot resume");
    expect(() => renderToString(t.el([TYPE], { cursor: "c" }))).not.toThrow();
  });
});

// Compiled by the typecheck and never run: a typed list infers the union of its payloads.
export function typesOfLists(): void {
  const X = defineEvent<{ x: string }>("0x1::m::X");
  const Y = defineEvent<{ y: number }>("0x1::m::Y");
  for (const group of useTransactions([X, Y]).data) {
    void (group.events[0]!.data satisfies { x: string } | { y: number });
    // @ts-expect-error the union is not one of its members
    void (group.events[0]!.data satisfies { x: string });
  }
  void (useTransactions(X).data[0]!.events[0]!.data.x satisfies string);
}
