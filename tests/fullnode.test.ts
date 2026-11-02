import { describe, it, expect } from "bun:test";
import { Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { FullnodeScanAdapter } from "../src/adapters/fullnode.js";
import { StreamdError, StreamdNetworkError } from "../src/errors.js";
import type { EventDescriptor } from "../src/types.js";

const TYPE = "0x1::test::Ping";
const OTHER = "0x1::test::Pong";

interface Reply {
  status?: number;
  data: unknown;
}

interface Call {
  path: string;
  params: Record<string, unknown>;
  headers: Record<string, unknown>;
}

/**
 * A real `Aptos` instance whose transport is a function, so every adapter path
 * runs through the SDK exactly as in production, with no network.
 */
function fakeNode(
  route: (path: string, params: Record<string, unknown>) => Reply,
  clientConfig: Record<string, unknown> = {},
) {
  const calls: Call[] = [];
  const client = {
    async provider(req: {
      url: string;
      params?: Record<string, unknown>;
      headers?: Record<string, unknown>;
    }) {
      const path = req.url.replace(/^http:\/\/fake\/v1\/?/, "");
      const params = { ...(req.params ?? {}) };
      calls.push({ path, params, headers: { ...(req.headers ?? {}) } });
      const reply = route(path, params);
      const status = reply.status ?? 200;
      return { status, statusText: status === 200 ? "OK" : "Error", data: reply.data, headers: {} };
    },
  };
  const aptos = new Aptos(
    new AptosConfig({
      network: Network.LOCAL,
      fullnode: "http://fake/v1",
      client: client as never,
      clientConfig: { http2: false, ...clientConfig },
    }),
  );
  return { aptos, calls };
}

function txn(version: number, types: string[] = [TYPE]) {
  return {
    version: String(version),
    type: "user_transaction",
    events: types.map((type) => ({ type, data: { v: version }, sequence_number: "0" })),
  };
}

/** Serves ledger info and a contiguous run of transactions starting at 0. */
function chain(head: number, txns: ReturnType<typeof txn>[]) {
  return (path: string, params: Record<string, unknown>): Reply => {
    if (path === "") return { data: { ledger_version: String(head) } };
    if (path === "transactions") {
      const start = Number(params.start ?? 0);
      const limit = Number(params.limit ?? 25);
      return { data: txns.filter((t) => Number(t.version) >= start).slice(0, limit) };
    }
    throw new Error(`unexpected path ${path}`);
  };
}

const moduleDesc: EventDescriptor = { kind: "module", eventType: TYPE };

describe("FullnodeScanAdapter module scan", () => {
  it("starts at the chain head when there is no cursor or fromVersion", async () => {
    const txns = Array.from({ length: 11 }, (_, i) => txn(i));
    const { aptos, calls } = fakeNode(chain(10, txns));
    const page = await new FullnodeScanAdapter(aptos).fetchEvents(moduleDesc, {});

    const scan = calls.find((c) => c.path === "transactions");
    expect(scan?.params.start).toBe(10);
    expect(page.events.map((e) => e.version)).toEqual([10n]);
    expect(page.nextCursor).toBe("11");
  });

  it("stops at toVersion and resumes from the next version", async () => {
    const txns = Array.from({ length: 5 }, (_, i) => txn(i));
    const { aptos } = fakeNode(chain(100, txns));
    const page = await new FullnodeScanAdapter(aptos).fetchEvents(moduleDesc, {
      fromVersion: 0n,
      toVersion: 1n,
    });

    expect(page.events.map((e) => e.version)).toEqual([0n, 1n]);
    expect(page.nextCursor).toBe("2");
    expect(page.hasMore).toBe(false);
  });

  it("keeps only the descriptor's type and records each event's index", async () => {
    const { aptos } = fakeNode(chain(100, [txn(0, [OTHER, TYPE, TYPE])]));
    const page = await new FullnodeScanAdapter(aptos).fetchEvents(moduleDesc, {
      fromVersion: 0n,
      toVersion: 0n,
    });

    expect(page.events.map((e) => e.eventIndex)).toEqual([1, 2]);
  });

  it("reports hasMore when it stops on the scan budget", async () => {
    const txns = Array.from({ length: 50 }, (_, i) => txn(i, [OTHER]));
    const { aptos, calls } = fakeNode(chain(100, txns));
    const adapter = new FullnodeScanAdapter(aptos, { scanBatch: 1, maxScan: 2 });
    const page = await adapter.fetchEvents(moduleDesc, { fromVersion: 0n });

    expect(page.events).toEqual([]);
    expect(page.hasMore).toBe(true);
    expect(page.nextCursor).toBe("2");
    expect(calls.filter((c) => c.path === "transactions")).toHaveLength(2);
  });
});

const ACCOUNT = "0x" + "a".repeat(64);
const accountDesc: EventDescriptor = { kind: "account", accountAddress: ACCOUNT };

/** Serves one account's history; its sequence number is the history length. */
function account(history: ReturnType<typeof txn>[]) {
  return (path: string, params: Record<string, unknown>): Reply => {
    if (path === `accounts/${ACCOUNT}`) {
      return { data: { sequence_number: String(history.length), authentication_key: ACCOUNT } };
    }
    if (path === `accounts/${ACCOUNT}/transactions`) {
      const start = Number(params.start ?? 0);
      const limit = Number(params.limit ?? 25);
      return { data: history.slice(start, start + limit) };
    }
    throw new Error(`unexpected path ${path}`);
  };
}

function firstStart(calls: Call[]): unknown {
  return calls.find((c) => c.path === `accounts/${ACCOUNT}/transactions`)?.params.start;
}

describe("FullnodeScanAdapter account walk", () => {
  it("reports no more once the history is exhausted", async () => {
    const { aptos } = fakeNode(account([txn(5), txn(9)]));
    const page = await new FullnodeScanAdapter(aptos).fetchEvents(accountDesc, {
      offset: 0,
    });

    expect(page.events.map((e) => e.version)).toEqual([5n, 9n]);
    expect(page.hasMore).toBe(false);
    expect(page.nextCursor).toBe("2");
  });

  it("resolves its start from cursor, then offset, then a version filter, then the tip", async () => {
    const history = [txn(5), txn(9), txn(12)];
    const start = async (options: Record<string, unknown>) => {
      const { aptos, calls } = fakeNode(account(history));
      await new FullnodeScanAdapter(aptos).fetchEvents(accountDesc, options);
      return firstStart(calls);
    };

    expect(await start({ cursor: "2", offset: 1 })).toBe(2);
    expect(await start({ offset: 1, fromVersion: 9n })).toBe(1);
    expect(await start({ fromVersion: 9n })).toBe(0);
    expect(await start({})).toBe(3);
  });
});

const HANDLE_PATH = `accounts/0x${"0".repeat(63)}1/events/0x1%3A%3Atest%3A%3AStore/pings`;
const handleDesc: EventDescriptor = {
  kind: "handle",
  accountAddress: "0x1",
  eventHandleStruct: "0x1::test::Store",
  fieldName: "pings",
};

function handleEvent(seq: number, version: number) {
  return { type: TYPE, data: {}, sequence_number: String(seq), version: String(version) };
}

/** Serves a handle's events by sequence number. */
function handle(events: ReturnType<typeof handleEvent>[]) {
  return (path: string, params: Record<string, unknown>): Reply => {
    if (path !== HANDLE_PATH) throw new Error(`unexpected path ${path}`);
    const start = Number(params.start ?? 0);
    const limit = Number(params.limit ?? 25);
    return { data: events.slice(start, start + limit) };
  };
}

describe("FullnodeScanAdapter handle events", () => {
  it("advances the cursor by what the node returned, not by what survived filtering", async () => {
    const events = [handleEvent(0, 3), handleEvent(1, 7), handleEvent(2, 9)];
    const { aptos } = fakeNode(handle(events));
    const page = await new FullnodeScanAdapter(aptos).fetchEvents(handleDesc, {
      limit: 2,
      fromVersion: 5n,
    });

    expect(page.events.map((e) => e.version)).toEqual([7n]);
    expect(page.nextCursor).toBe("2");
    expect(page.hasMore).toBe(true);
  });

  it("gives an event the same index whichever page it arrives on", async () => {
    const events = [handleEvent(0, 3), handleEvent(1, 3), handleEvent(2, 4)];
    const { aptos } = fakeNode(handle(events));
    const adapter = new FullnodeScanAdapter(aptos);
    const first = await adapter.fetchEvents(handleDesc, { limit: 2 });
    const second = await adapter.fetchEvents(handleDesc, { cursor: "1", limit: 2 });

    const indexOf = (page: typeof first) =>
      page.events.find((e) => e.sequenceNumber === 1n)?.eventIndex;
    expect(indexOf(first)).toBe(1);
    expect(indexOf(second)).toBe(1);
  });

  it("throws on a non-ok response", async () => {
    const { aptos } = fakeNode(() => ({ status: 503, data: { message: "down" } }));
    await expect(
      new FullnodeScanAdapter(aptos).fetchEvents(handleDesc, {}),
    ).rejects.toThrow();
  });

  it("requests a short address in its long form", async () => {
    const { aptos, calls } = fakeNode(handle([]));
    await new FullnodeScanAdapter(aptos).fetchEvents(handleDesc, {});
    expect(calls.map((c) => c.path)).toEqual([HANDLE_PATH]);
  });

  it("refuses an address that is not one, before any request", async () => {
    const { aptos, calls } = fakeNode(handle([]));
    const adapter = new FullnodeScanAdapter(aptos);
    await expect(
      adapter.fetchEvents({ ...handleDesc, accountAddress: "0x1/../../transactions?" }, {}),
    ).rejects.toThrow("Not a valid Aptos address");
    expect(calls).toHaveLength(0);
  });

  it("sends the configured headers and API key", async () => {
    const { aptos, calls } = fakeNode(handle([]), {
      API_KEY: "secret",
      HEADERS: { "x-team": "streamd" },
    });
    await new FullnodeScanAdapter(aptos).fetchEvents(handleDesc, {});
    expect(calls[0]?.headers["x-team"]).toBe("streamd");
    expect(calls[0]?.headers.Authorization).toBe("Bearer secret");
  });
});

describe("FullnodeScanAdapter errors", () => {
  const failing = () => ({ status: 429, data: { message: "slow down" } });

  for (const descriptor of [moduleDesc, accountDesc, handleDesc]) {
    it(`surfaces a failed ${descriptor.kind} request as StreamdNetworkError with its status`, async () => {
      const { aptos } = fakeNode(failing);
      const err = await new FullnodeScanAdapter(aptos)
        .fetchEvents(descriptor, { offset: 0, fromVersion: 0n })
        .catch((e: unknown) => e);

      expect(err).toBeInstanceOf(StreamdNetworkError);
      expect((err as StreamdNetworkError).statusCode).toBe(429);
      expect((err as StreamdNetworkError).cause).toBeDefined();
    });
  }

  const badInput: [string, EventDescriptor, Record<string, unknown>][] = [
    ["handle address", { ...handleDesc, accountAddress: "nope" }, {}],
    ["account address", { ...accountDesc, accountAddress: "nope" }, {}],
    ["module cursor", moduleDesc, { cursor: "abc" }],
    ["account cursor", accountDesc, { cursor: "abc" }],
    ["handle cursor", handleDesc, { cursor: "abc" }],
  ];

  for (const [name, descriptor, options] of badInput) {
    it(`refuses a bad ${name} as input, not as a network failure`, async () => {
      const { aptos, calls } = fakeNode(handle([]));
      const err = await new FullnodeScanAdapter(aptos)
        .fetchEvents(descriptor, options)
        .catch((e: unknown) => e);

      expect(err).toBeInstanceOf(StreamdError);
      expect(err).not.toBeInstanceOf(StreamdNetworkError);
      expect(calls).toHaveLength(0);
    });
  }
});
