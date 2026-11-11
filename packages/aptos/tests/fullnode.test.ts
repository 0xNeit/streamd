import { describe, it, expect } from "bun:test";
import { Account, Aptos, AptosConfig, Network, parseTypeTag } from "@aptos-labs/ts-sdk";
import { FullnodeScanAdapter } from "../src/fullnode.js";
import { StreamdClient } from "@streamd/core";
import { FallbackAdapter } from "@streamd/core";
import { StreamdError, StreamdNetworkError } from "@streamd/core";
import { checkDescriptor } from "@streamd/core";
import { listEventHandles } from "@streamd/core";
import { withHooks } from "@streamd/core";
import type { EventDescriptor, ModuleEventDescriptor } from "@streamd/core";

const TYPE = "0x1::test::Ping";
const OTHER = "0x1::test::Pong";

interface Reply {
  status?: number;
  data: unknown;
}

interface Call {
  method?: string;
  body?: unknown;
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
  network: Network = Network.LOCAL,
) {
  const calls: Call[] = [];
  const client = {
    async provider(req: {
      method?: string;
      body?: unknown;
      url: string;
      params?: Record<string, unknown>;
      headers?: Record<string, unknown>;
    }) {
      const path = req.url.replace(/^http:\/\/fake\/v1\/?/, "");
      const params = { ...(req.params ?? {}) };
      calls.push({ method: req.method, body: req.body, path, params, headers: { ...(req.headers ?? {}) } });
      const reply = route(path, params);
      const status = reply.status ?? 200;
      return { status, statusText: status === 200 ? "OK" : "Error", data: reply.data, headers: {} };
    },
  };
  const aptos = new Aptos(
    new AptosConfig({
      network,
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

  it("matches multiple eventTypes in one transaction scan and preserves order", async () => {
    const GENERIC = "0x1::pool::Swap<0x1::a::CoinA, 0x1::b::CoinB>" as const;
    const UNLISTED = "0x1::test::Ignored";
    const txns = [
      txn(0, [UNLISTED, OTHER, TYPE]),
      txn(1, [UNLISTED]),
      txn(2, [GENERIC, UNLISTED, TYPE]),
    ];
    const { aptos, calls } = fakeNode(chain(2, txns));
    const page = await new FullnodeScanAdapter(aptos).fetchEvents(
      { kind: "module", eventType: [TYPE, OTHER, GENERIC, TYPE] },
      { fromVersion: 0n, toVersion: 2n },
    );

    expect(
      page.events.map((e) => ({
        version: e.version,
        index: e.eventIndex,
        type: e.type,
      })),
    ).toEqual([
      { version: 0n, index: 1, type: OTHER },
      { version: 0n, index: 2, type: TYPE },
      { version: 2n, index: 0, type: GENERIC },
      { version: 2n, index: 2, type: TYPE },
    ]);
    expect(page.nextCursor).toBe("3");
    expect(calls.filter((c) => c.path === "transactions")).toHaveLength(1);
  });

  it("populates transactionHash and timestamp on module events when present on the transaction", async () => {
    const txns = [
      {
        version: "0",
        hash: "0xgenesis",
        type: "genesis_transaction",
        events: [{ type: TYPE, data: { v: 0 }, sequence_number: "0" }],
      },
      {
        version: "1",
        hash: "0xdeadbeef",
        timestamp: "1700000000123456",
        type: "user_transaction",
        events: [
          { type: OTHER, data: { v: 1 }, sequence_number: "0" },
          { type: TYPE, data: { v: 1 }, sequence_number: "0" },
          { type: TYPE, data: { v: 2 }, sequence_number: "1" },
        ],
      },
      {
        version: "2",
        hash: "0xskipped",
        timestamp: "not-a-bigint",
        type: "user_transaction",
        events: [{ type: OTHER, data: { v: 2 }, sequence_number: "0" }],
      },
    ];
    const { aptos } = fakeNode(chain(2, txns));
    const page = await new FullnodeScanAdapter(aptos).fetchEvents(moduleDesc, {
      fromVersion: 0n,
      toVersion: 2n,
    });

    expect(page.events).toHaveLength(3);
    expect(page.events[0]?.transactionHash).toBe("0xgenesis");
    expect(page.events[0]?.timestamp).toBeUndefined();
    expect("timestamp" in page.events[0]!).toBe(false);

    expect(page.events[1]?.transactionHash).toBe("0xdeadbeef");
    expect(page.events[1]?.timestamp).toBe(1700000000123456n);
    expect(page.events[2]?.transactionHash).toBe("0xdeadbeef");
    expect(page.events[2]?.timestamp).toBe(1700000000123456n);
  });

  it("populates sender on module events from user transactions only", async () => {
    const SENDER = "0x" + "b".repeat(64);
    const txns = [
      { ...txn(0), type: "block_metadata_transaction" },
      { ...txn(1, [TYPE, OTHER, TYPE]), sender: SENDER },
    ];
    const { aptos } = fakeNode(chain(1, txns));
    const page = await new FullnodeScanAdapter(aptos).fetchEvents(moduleDesc, {
      fromVersion: 0n,
      toVersion: 1n,
    });

    expect(page.events).toHaveLength(3);
    expect("sender" in page.events[0]!).toBe(false);
    expect(page.events[1]?.sender).toBe(SENDER);
    expect(page.events[2]?.sender).toBe(SENDER);
  });

  it("populates entryFunction from entry function payloads only", async () => {
    const payload = {
      type: "entry_function_payload",
      function: "0x1::aptos_account::transfer",
      type_arguments: [],
      arguments: ["0x" + "c".repeat(64), "100"],
    };
    const txns = [
      { ...txn(0), payload },
      { ...txn(1, [TYPE, OTHER, TYPE]), payload: { ...payload, type_arguments: ["0x1::aptos_coin::AptosCoin"] } },
      { ...txn(2), payload: { type: "script_payload", code: { bytecode: "0x" }, type_arguments: [], arguments: [] } },
      { ...txn(3), payload: { type: "multisig_payload", multisig_address: "0x2", transaction_payload: payload } },
      { ...txn(4), type: "block_metadata_transaction" },
      { ...txn(5), payload: { ...payload, function: 7 } },
      { ...txn(6), payload: { ...payload, type_arguments: [1] } },
      { ...txn(7), payload: { ...payload, arguments: "100" } },
    ];
    const { aptos } = fakeNode(chain(7, txns));
    const page = await new FullnodeScanAdapter(aptos).fetchEvents(moduleDesc, {
      fromVersion: 0n,
      toVersion: 7n,
    });

    expect(page.events.map((e) => e.version)).toEqual([0n, 1n, 1n, 2n, 3n, 4n, 5n, 6n, 7n]);
    expect(page.events[0]?.entryFunction).toEqual({
      function: "0x1::aptos_account::transfer",
      typeArguments: [],
      arguments: ["0x" + "c".repeat(64), "100"],
    });
    expect(page.events[1]?.entryFunction?.typeArguments).toEqual(["0x1::aptos_coin::AptosCoin"]);
    expect(page.events[2]?.entryFunction).toEqual(page.events[1]!.entryFunction!);
    for (const event of page.events.slice(3)) expect("entryFunction" in event).toBe(false);
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

  it("populates transactionHash and timestamp on account events", async () => {
    const history = [
      {
        ...txn(5, [TYPE]),
        hash: "0xacc5",
        timestamp: "1700000005000000",
      },
      {
        ...txn(9, [TYPE]),
        hash: "0xacc9",
        timestamp: "1700000009000000",
      },
    ];
    const { aptos } = fakeNode(account(history));
    const page = await new FullnodeScanAdapter(aptos).fetchEvents(accountDesc, {
      offset: 0,
    });

    expect(page.events.map((e) => e.transactionHash)).toEqual(["0xacc5", "0xacc9"]);
    expect(page.events.map((e) => e.timestamp)).toEqual([
      1700000005000000n,
      1700000009000000n,
    ]);
  });

  it("populates sender on account events", async () => {
    const history = [{ ...txn(5), sender: ACCOUNT }];
    const { aptos } = fakeNode(account(history));
    const page = await new FullnodeScanAdapter(aptos).fetchEvents(accountDesc, {
      offset: 0,
    });

    expect(page.events.map((e) => e.sender)).toEqual([ACCOUNT]);
  });

  it("populates entryFunction on account events", async () => {
    const payload = { type: "entry_function_payload", function: "0x1::coin::transfer", type_arguments: [], arguments: [] };
    const history = [{ ...txn(5), payload }];
    const { aptos } = fakeNode(account(history));
    const page = await new FullnodeScanAdapter(aptos).fetchEvents(accountDesc, { offset: 0 });

    expect(page.events.map((e) => e.entryFunction?.function)).toEqual(["0x1::coin::transfer"]);
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
function handle(events: (ReturnType<typeof handleEvent> & { guid?: unknown })[]) {
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
    expect(first.events[0]?.transactionHash).toBeUndefined();
    expect(first.events[0]?.timestamp).toBeUndefined();
    expect("transactionHash" in first.events[0]!).toBe(false);
    expect("timestamp" in first.events[0]!).toBe(false);
    expect("sender" in first.events[0]!).toBe(false);
    expect("entryFunction" in first.events[0]!).toBe(false);
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

const CREATION_PATH = `accounts/0x${"0".repeat(63)}1/events/7`;
const creationDesc: EventDescriptor = { kind: "handle", accountAddress: "0x1", creationNumber: 7n };

describe("FullnodeScanAdapter handle events by creation number", () => {
  const serve = (events: ReturnType<typeof handleEvent>[]) =>
    (path: string, params: Record<string, unknown>): Reply => {
      if (path !== CREATION_PATH) throw new Error(`unexpected path ${path}`);
      const start = Number(params.start ?? 0);
      return { data: events.slice(start, start + Number(params.limit ?? 25)) };
    };
  const events = [handleEvent(0, 3), handleEvent(1, 7), handleEvent(2, 9)];

  it("reads the creation-number endpoint and pages like the field form", async () => {
    const { aptos, calls } = fakeNode(serve(events), {
      API_KEY: "secret",
      HEADERS: { "x-team": "streamd" },
    });
    const adapter = new FullnodeScanAdapter(aptos);
    const first = await adapter.fetchEvents(creationDesc, { limit: 2 });
    const second = await adapter.fetchEvents(creationDesc, { cursor: first.nextCursor!, limit: 2 });

    expect(calls.map((c) => [c.path, c.params.start, c.params.limit])).toEqual([
      [CREATION_PATH, "0", 2],
      [CREATION_PATH, "2", 2],
    ]);
    expect(calls[0]?.headers["x-team"]).toBe("streamd");
    expect(calls[0]?.headers.Authorization).toBe("Bearer secret");
    expect(first).toMatchObject({ nextCursor: "2", hasMore: true, final: true });
    expect(first.events.map((e) => [e.version, e.eventIndex])).toEqual([[3n, 0], [7n, 1]]);
    expect(second.events.map((e) => e.version)).toEqual([9n]);
    expect(second.hasMore).toBe(false);
    expect(second.final).toBeUndefined();
  });

  const bad: [string, unknown][] = [
    ["both forms", { ...handleDesc, creationNumber: 7n }],
    ["a negative creationNumber", { ...creationDesc, creationNumber: -1n }],
    ["a number creationNumber", { ...creationDesc, creationNumber: 7 }],
    ["a string creationNumber", { ...creationDesc, creationNumber: "7" }],
    ["an over-u64 creationNumber", { ...creationDesc, creationNumber: 2n ** 64n }],
    ["no form at all", { kind: "handle", accountAddress: "0x1" }],
    ["an empty fieldName", { ...handleDesc, fieldName: "" }],
  ];
  for (const [name, descriptor] of bad) {
    it(`refuses ${name} as input, before any request`, async () => {
      const { aptos, calls } = fakeNode(serve(events));
      const err = await new FullnodeScanAdapter(aptos)
        .fetchEvents(descriptor as EventDescriptor, {})
        .catch((e: unknown) => e);
      expect(err).toBeInstanceOf(StreamdError);
      expect(err).not.toBeInstanceOf(StreamdNetworkError);
      expect(calls).toHaveLength(0);
    });
  }

  it("accepts the largest u64 creation number", async () => {
    const max = 2n ** 64n - 1n;
    const { aptos, calls } = fakeNode(() => ({ data: [] }));
    await new FullnodeScanAdapter(aptos).fetchEvents({ ...creationDesc, creationNumber: max }, {});
    expect(calls[0]?.path).toBe(`accounts/0x${"0".repeat(63)}1/events/${max}`);
  });
});

describe("FullnodeScanAdapter guids", () => {
  const ZERO = { creation_number: "0", account_address: "0x0" };
  const legacy = { creation_number: "3", account_address: "0x1" };
  const guidTxn = (version: number) => ({
    version: String(version),
    type: "user_transaction",
    events: [
      { type: TYPE, data: { v: version }, sequence_number: "0", guid: ZERO },
      { type: TYPE, data: { v: version }, sequence_number: "5", guid: legacy },
    ],
  });

  it("carries a legacy event's guid and none for the zero guid on the module scan", async () => {
    const { aptos } = fakeNode(chain(1, [guidTxn(0), guidTxn(1)]));
    const page = await new FullnodeScanAdapter(aptos).fetchEvents(moduleDesc, { fromVersion: 1n });
    expect("guid" in page.events[0]!).toBe(false);
    expect(page.events[1]!.guid).toEqual({ creationNumber: 3n, accountAddress: "0x1" });
  });

  it("carries guids on the account walk", async () => {
    const { aptos } = fakeNode(account([guidTxn(4)]));
    const page = await new FullnodeScanAdapter(aptos).fetchEvents(accountDesc, { offset: 0 });
    expect(page.events.map((e) => e.guid)).toEqual([
      undefined,
      { creationNumber: 3n, accountAddress: "0x1" },
    ]);
  });

  it("keeps creation number 0 at a real account and drops a malformed guid", async () => {
    const first = { creation_number: "0", account_address: "0x1" };
    const { aptos } = fakeNode(
      handle([
        { ...handleEvent(0, 3), guid: first },
        { ...handleEvent(1, 4), guid: { creation_number: "x", account_address: "0x1" } },
        { ...handleEvent(2, 5), guid: { creation_number: "1", account_address: "0x" } },
      ]),
    );
    const page = await new FullnodeScanAdapter(aptos).fetchEvents(handleDesc, {});
    expect(page.events.map((e) => e.guid)).toEqual([
      { creationNumber: 0n, accountAddress: "0x1" },
      undefined,
      undefined,
    ]);
  });

  it("carries the handle's guid on handle events", async () => {
    const { aptos } = fakeNode(handle([{ ...handleEvent(0, 3), guid: legacy }]));
    const page = await new FullnodeScanAdapter(aptos).fetchEvents(handleDesc, {});
    expect(page.events[0]!.guid).toEqual({ creationNumber: 3n, accountAddress: "0x1" });
  });
});

describe("FullnodeScanAdapter page finality", () => {
  const ten = Array.from({ length: 10 }, (_, i) => txn(i));
  const moduleFinal = async (head: number, options: Record<string, unknown>) => {
    const { aptos } = fakeNode(chain(head, ten));
    const adapter = new FullnodeScanAdapter(aptos, { scanBatch: 2 });
    return (await adapter.fetchEvents(moduleDesc, options)).final;
  };

  it("marks a module page final only when its scan stayed below the head", async () => {
    expect(await moduleFinal(9, { fromVersion: 0n, limit: 3 })).toBe(true);
    expect(await moduleFinal(9, { cursor: "2", toVersion: 4n })).toBe(true);
    expect(await moduleFinal(9, { fromVersion: 6n, limit: 50 })).toBeUndefined();
    expect(await moduleFinal(9, { fromVersion: 8n, limit: 1 })).toBeUndefined();
    expect(await moduleFinal(20, { fromVersion: 8n, limit: 50 })).toBeUndefined();
  });

  it("never marks a module page final when its start came from the head", async () => {
    expect(await moduleFinal(9, {})).toBeUndefined();
    expect(await moduleFinal(9, { toVersion: 3n })).toBeUndefined();
  });

  const accountFinal = async (length: number, options: Record<string, unknown>) => {
    const { aptos } = fakeNode(account(Array.from({ length }, (_, i) => txn(i))));
    const adapter = new FullnodeScanAdapter(aptos, { scanBatch: 2 });
    return (await adapter.fetchEvents(accountDesc, options)).final;
  };

  it("marks an account page final only when every batch came back full", async () => {
    expect(await accountFinal(10, { offset: 0, limit: 3 })).toBe(true);
    expect(await accountFinal(10, { cursor: "4", toVersion: 4n })).toBe(true);
    expect(await accountFinal(5, { offset: 2, limit: 3 })).toBeUndefined();
    expect(await accountFinal(4, { offset: 0, limit: 50 })).toBeUndefined();
    expect(await accountFinal(4, { fromVersion: 0n, limit: 50 })).toBeUndefined();
  });

  it("never marks an account page final when its start came from the latest sequence number", async () => {
    expect(await accountFinal(10, {})).toBeUndefined();
  });

  it("marks a handle page final only when the node returned a full page", async () => {
    const events = [handleEvent(0, 3), handleEvent(1, 7), handleEvent(2, 9)];
    const fetch = async (options: Record<string, unknown>) => {
      const { aptos } = fakeNode(handle(events));
      return (await new FullnodeScanAdapter(aptos).fetchEvents(handleDesc, options)).final;
    };
    expect(await fetch({ limit: 2 })).toBe(true);
    expect(await fetch({ limit: 2, fromVersion: 8n })).toBe(true);
    expect(await fetch({ cursor: "2", limit: 2 })).toBeUndefined();
  });
});

describe("FullnodeScanAdapter scan progress", () => {
  const ten = Array.from({ length: 10 }, (_, i) => txn(i, i % 3 === 0 ? [TYPE] : [OTHER]));
  const moduleThrough = async (head: number, options: Record<string, unknown>) => {
    const { aptos } = fakeNode(chain(head, ten.slice(0, head + 1)));
    const adapter = new FullnodeScanAdapter(aptos, { scanBatch: 2 });
    return (await adapter.fetchEvents(moduleDesc, options)).throughVersion;
  };

  it("reports the last module version scanned, matches or not", async () => {
    expect(await moduleThrough(9, { fromVersion: 0n, limit: 2 })).toBe(3n);
    expect(await moduleThrough(9, { fromVersion: 1n, limit: 50 })).toBe(9n);
    expect(await moduleThrough(9, { cursor: "2", toVersion: 4n })).toBe(4n);
    expect(await moduleThrough(9, {})).toBe(9n);
  });

  it("leaves module progress unset when nothing was scanned", async () => {
    expect(await moduleThrough(9, { fromVersion: 5n, toVersion: 4n })).toBeUndefined();
    expect(await moduleThrough(9, { fromVersion: 0n, limit: 0 })).toBeUndefined();
    expect(await moduleThrough(4, { fromVersion: 7n })).toBeUndefined();
  });

  const accountThrough = async (options: Record<string, unknown>) => {
    const history = [txn(3), txn(8), txn(12), txn(20)];
    const { aptos } = fakeNode(account(history));
    const adapter = new FullnodeScanAdapter(aptos, { scanBatch: 2 });
    return (await adapter.fetchEvents(accountDesc, options)).throughVersion;
  };

  it("reports the version of the last account transaction examined", async () => {
    expect(await accountThrough({ offset: 0, limit: 1 })).toBe(8n);
    expect(await accountThrough({ fromVersion: 15n, limit: 50 })).toBe(20n);
    expect(await accountThrough({ toVersion: 10n })).toBe(10n);
    expect(await accountThrough({ cursor: "4" })).toBeUndefined();
    expect(await accountThrough({})).toBeUndefined();
  });

  const handleThrough = async (options: Record<string, unknown>) => {
    const events = [handleEvent(0, 3), handleEvent(1, 7), handleEvent(2, 7)];
    const { aptos } = fakeNode(handle(events));
    return (await new FullnodeScanAdapter(aptos).fetchEvents(handleDesc, options)).throughVersion;
  };

  it("settles a full handle page only below its last event's version", async () => {
    expect(await handleThrough({ limit: 2 })).toBe(6n);
    expect(await handleThrough({ cursor: "2", limit: 2 })).toBe(7n);
    expect(await handleThrough({ limit: 5, fromVersion: 8n })).toBe(7n);
    expect(await handleThrough({ cursor: "3", limit: 2 })).toBeUndefined();
  });

  it("never reports a negative handle version", async () => {
    const { aptos } = fakeNode(handle([handleEvent(0, 0)]));
    const page = await new FullnodeScanAdapter(aptos).fetchEvents(handleDesc, { limit: 1 });
    expect(page.throughVersion).toBeUndefined();
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
      expect((err as StreamdNetworkError).cause).toBeUndefined();
    });
  }

  const badInput: [string, EventDescriptor, Record<string, unknown>][] = [
    ["handle address", { ...handleDesc, accountAddress: "nope" }, {}],
    ["account address", { ...accountDesc, accountAddress: "nope" }, {}],
    ["empty module eventType array", { kind: "module", eventType: [] }, {}],
    [
      "empty module eventType string",
      { kind: "module", eventType: "" as `${string}::${string}::${string}` },
      {},
    ],
    [
      "two-segment module eventType",
      { kind: "module", eventType: "0x1::only_two" as `${string}::${string}::${string}` },
      {},
    ],
    [
      "four-segment module eventType",
      { kind: "module", eventType: "0x1::a::b::c" as `${string}::${string}::${string}` },
      {},
    ],
    [
      "module eventType array with bad entry",
      {
        kind: "module",
        eventType: [TYPE, "0x1::bad" as `${string}::${string}::${string}`],
      },
      { limit: 0 },
    ],
    ["module cursor", moduleDesc, { cursor: "abc" }],
    ["account cursor", accountDesc, { cursor: "abc" }],
    ["handle cursor", handleDesc, { cursor: "abc" }],
    ["account offset", accountDesc, { offset: 1.5 }],
    ["handle offset", handleDesc, { offset: -1 }],
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

// The SDK caches ledger info per network for 10s, so each case uses a network
// no other test in this file reads the ledger through.
describe("FullnodeScanAdapter head version", () => {
  it("reports the ledger version", async () => {
    const { aptos } = fakeNode(chain(42, []), {}, Network.DEVNET);
    expect(await new FullnodeScanAdapter(aptos).getHeadVersion()).toBe(42n);
  });

  it("surfaces a failed ledger read as StreamdNetworkError with its status", async () => {
    const { aptos } = fakeNode(
      () => ({ status: 503, data: { message: "down" } }),
      {},
      Network.TESTNET,
    );
    const err = await new FullnodeScanAdapter(aptos)
      .getHeadVersion()
      .catch((e: unknown) => e);

    expect(err).toBeInstanceOf(StreamdNetworkError);
    expect((err as StreamdNetworkError).statusCode).toBe(503);
  });
});

describe("FullnodeScanAdapter earliest version", () => {
  it("reports the oldest unpruned ledger version", async () => {
    const { aptos } = fakeNode(
      (path) => {
        if (path === "") {
          return { data: { ledger_version: "900", oldest_ledger_version: "120" } };
        }
        throw new Error(`unexpected path ${path}`);
      },
      {},
      Network.MAINNET,
    );
    expect(await new FullnodeScanAdapter(aptos).getEarliestVersion()).toBe(120n);
  });

  it("surfaces a failed ledger read as StreamdNetworkError with its status", async () => {
    const { aptos } = fakeNode(
      () => ({ status: 503, data: { message: "down" } }),
      {},
      Network.SHELBYNET,
    );
    const err = await new FullnodeScanAdapter(aptos)
      .getEarliestVersion()
      .catch((e: unknown) => e);

    expect(err).toBeInstanceOf(StreamdNetworkError);
    expect((err as StreamdNetworkError).statusCode).toBe(503);
  });
});

describe("FullnodeScanAdapter ledger reads", () => {
  it("reads each custom network's own head, uncached", async () => {
    const a = fakeNode(chain(100, []), {}, Network.CUSTOM);
    const b = fakeNode(chain(200, []), {}, Network.CUSTOM);
    expect(await new FullnodeScanAdapter(a.aptos).getHeadVersion()).toBe(100n);
    expect(await new FullnodeScanAdapter(b.aptos).getHeadVersion()).toBe(200n);
    expect(await new FullnodeScanAdapter(a.aptos).getHeadVersion()).toBe(100n);
    expect(a.calls).toHaveLength(2);
  });
});

describe("FullnodeScanAdapter block heights", () => {
  it("reports the oldest and newest block heights from one ledger read", async () => {
    const { aptos, calls } = fakeNode(() => ({
      data: { oldest_block_height: "1142401405", block_height: "1165817595" },
    }));
    expect(await new FullnodeScanAdapter(aptos).getBlockHeights()).toEqual({
      oldest: 1142401405n,
      newest: 1165817595n,
    });
    expect(calls.map((c) => c.path)).toEqual([""]);
  });

  it("surfaces a failed ledger read as StreamdNetworkError with its status", async () => {
    const { aptos } = fakeNode(() => ({ status: 429, data: { message: "slow down" } }));
    const err = await new FullnodeScanAdapter(aptos).getBlockHeights().catch((e: unknown) => e);

    expect(err).toBeInstanceOf(StreamdNetworkError);
    expect((err as StreamdNetworkError).statusCode).toBe(429);
  });
});

describe("FullnodeScanAdapter blocks", () => {
  it("maps a block to bigints without fetching its transactions", async () => {
    const { aptos, calls } = fakeNode((path) => {
      if (path === "blocks/by_height/42") {
        return {
          data: {
            block_height: "42",
            block_hash: "0xabc",
            block_timestamp: "1793063568344144",
            first_version: "7781054619",
            last_version: "7781054626",
            transactions: null,
          },
        };
      }
      throw new Error(`unexpected path ${path}`);
    });
    const block = await new FullnodeScanAdapter(aptos).getBlock(
      { kind: "module", eventType: TYPE },
      42n,
    );

    expect(block).toEqual({
      height: 42n,
      firstVersion: 7781054619n,
      lastVersion: 7781054626n,
      timestamp: 1793063568344144n,
    });
    expect(calls[0]!.params.with_transactions).toBe(false);
  });

  it("surfaces a pruned block as StreamdNetworkError with its status", async () => {
    const { aptos } = fakeNode(() => ({
      status: 410,
      data: { error_code: "block_pruned", message: "Block(1) has been pruned" },
    }));
    const err = await new FullnodeScanAdapter(aptos)
      .getBlock({ kind: "module", eventType: TYPE }, 1n)
      .catch((e: unknown) => e);

    expect(err).toBeInstanceOf(StreamdNetworkError);
    expect((err as StreamdNetworkError).statusCode).toBe(410);
  });
});

describe("FullnodeScanAdapter transaction lookup", () => {
  const HASH = `0x${"ab".repeat(32)}`;
  const SENDER = "0xcafe";
  const committed = {
    type: "user_transaction",
    version: "77",
    hash: HASH,
    sender: SENDER,
    timestamp: "1700000000000000",
    success: true,
    vm_status: "Executed successfully",
    events: [
      { type: TYPE, data: { n: "1" }, sequence_number: "0", guid: { creation_number: "0", account_address: "0x0" } },
      { type: OTHER, data: { n: "2" }, sequence_number: "4", guid: { creation_number: "3", account_address: SENDER } },
    ],
  };

  it("maps a committed transaction's events as the scan does", async () => {
    const { aptos, calls } = fakeNode(() => ({ data: committed }));
    const found = await new FullnodeScanAdapter(aptos).getTransactionEvents(HASH);

    expect(calls[0]!.path).toBe(`transactions/by_hash/${HASH}`);
    expect(found).toEqual({
      version: 77n,
      success: true,
      vmStatus: "Executed successfully",
      events: [
        { type: TYPE, data: { n: "1" }, version: 77n, sequenceNumber: 0n, eventIndex: 0, transactionHash: HASH, sender: SENDER, timestamp: 1700000000000000n },
        {
          type: OTHER,
          data: { n: "2" },
          version: 77n,
          sequenceNumber: 4n,
          eventIndex: 1,
          transactionHash: HASH,
          sender: SENDER,
          timestamp: 1700000000000000n,
          guid: { creationNumber: 3n, accountAddress: SENDER },
        },
      ],
    });
  });

  it("carries the transaction's entry function on a looked-up transaction", async () => {
    const payload = { type: "entry_function_payload", function: "0x1::test::ping", type_arguments: [], arguments: ["1"] };
    const { aptos } = fakeNode(() => ({ data: { ...committed, payload } }));
    const found = await new FullnodeScanAdapter(aptos).getTransactionEvents(HASH);

    const calls = found!.events.map((e) => e.entryFunction);
    expect(calls).toEqual([
      { function: "0x1::test::ping", typeArguments: [], arguments: ["1"] },
      { function: "0x1::test::ping", typeArguments: [], arguments: ["1"] },
    ]);
  });

  it("reports a failed transaction with its vm status", async () => {
    const { aptos } = fakeNode(() => ({
      data: { ...committed, success: false, vm_status: "Move abort: 0x1", events: [] },
    }));
    const found = await new FullnodeScanAdapter(aptos).getTransactionEvents(HASH);
    expect(found).toEqual({ version: 77n, success: false, vmStatus: "Move abort: 0x1", events: [] });
    const { vm_status: _, ...silent } = committed;
    const quiet = fakeNode(() => ({ data: silent }));
    expect((await new FullnodeScanAdapter(quiet.aptos).getTransactionEvents(HASH))!.vmStatus).toBe("");
  });

  it("gives undefined for an unknown or pending transaction", async () => {
    const unknown = fakeNode(() => ({
      status: 404,
      data: { error_code: "transaction_not_found", message: "not found" },
    }));
    expect(await new FullnodeScanAdapter(unknown.aptos).getTransactionEvents(HASH)).toBeUndefined();
    const pending = fakeNode(() => ({ data: { type: "pending_transaction", hash: HASH } }));
    expect(await new FullnodeScanAdapter(pending.aptos).getTransactionEvents(HASH)).toBeUndefined();
  });

  it("refuses a malformed hash before any request", async () => {
    const { aptos, calls } = fakeNode(() => ({ data: committed }));
    const adapter = new FullnodeScanAdapter(aptos);
    for (const bad of ["0x1234", `${"ab".repeat(32)}`, `0x${"ab".repeat(31)}/../`, `0x${"zz".repeat(32)}`, `../0x${"ab".repeat(32)}`, Object.create(null)]) {
      const err = await adapter.getTransactionEvents(bad as string).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(StreamdError);
      expect(err).not.toBeInstanceOf(StreamdNetworkError);
    }
    expect(calls).toHaveLength(0);
    const long = await adapter.getTransactionEvents(`0x${"a".repeat(5000)}`).catch((e: Error) => e);
    expect((long as Error).message.length).toBeLessThan(150);
  });

  it("rejects a server error as StreamdNetworkError and a malformed reply as StreamdError", async () => {
    const down = fakeNode(() => ({ status: 500, data: { message: "boom" } }));
    const err = await new FullnodeScanAdapter(down.aptos).getTransactionEvents(HASH).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdNetworkError);
    expect((err as StreamdNetworkError).statusCode).toBe(500);

    for (const reply of [
      { ...committed, success: "yes" },
      { ...committed, version: "abc" },
      { ...committed, hash: `0x${"cd".repeat(32)}` },
    ]) {
      const odd = fakeNode(() => ({ data: reply }));
      const bad = await new FullnodeScanAdapter(odd.aptos).getTransactionEvents(HASH).catch((e: unknown) => e);
      expect(bad).toBeInstanceOf(StreamdError);
      expect(bad).not.toBeInstanceOf(StreamdNetworkError);
    }
  });
});

/** A transfer built offline: explicit ABI and options mean the build makes no request. */
function buildTransfer(aptos: Aptos) {
  const sender = Account.generate();
  return aptos.transaction.build.simple({
    sender: sender.accountAddress,
    data: {
      function: "0x1::aptos_account::transfer",
      functionArguments: [sender.accountAddress, 0],
      abi: { typeParameters: [], parameters: [parseTypeTag("address"), parseTypeTag("u64")] },
    },
    options: { accountSequenceNumber: 0, gasUnitPrice: 100, maxGasAmount: 1000, expireTimestamp: 9_999_999_999 },
  });
}

function simulated(overrides: Record<string, unknown> = {}) {
  return {
    type: "user_transaction",
    version: "42",
    hash: "0x" + "ab".repeat(32),
    sender: "0x1",
    timestamp: "1700000000000000",
    success: true,
    vm_status: "Executed successfully",
    events: [
      { type: TYPE, data: { v: 1 }, sequence_number: "3", guid: { creation_number: "2", account_address: "0x1" } },
      { type: OTHER, data: { v: 2 }, sequence_number: "0" },
    ],
    ...overrides,
  };
}

describe("FullnodeScanAdapter simulation", () => {
  it("POSTs to the simulate endpoint and maps the events without a transaction hash", async () => {
    const { aptos, calls } = fakeNode((path) => {
      if (path === "transactions/simulate") return { data: [simulated()] };
      throw new Error(`unexpected path ${path}`);
    });
    const transaction = await buildTransfer(aptos);
    const found = await new FullnodeScanAdapter(aptos).simulateTransactionEvents({ transaction });
    expect(calls.map((c) => [c.method, c.path])).toEqual([["POST", "transactions/simulate"]]);
    expect(found.version).toBe(42n);
    expect(found.success).toBe(true);
    expect(found.vmStatus).toBe("Executed successfully");
    expect(found.events).toEqual([
      {
        type: TYPE,
        data: { v: 1 },
        version: 42n,
        sequenceNumber: 3n,
        eventIndex: 0,
        sender: "0x1",
        guid: { creationNumber: 2n, accountAddress: "0x1" },
        timestamp: 1700000000000000n,
      },
      {
        type: OTHER,
        data: { v: 2 },
        version: 42n,
        sequenceNumber: 0n,
        eventIndex: 1,
        sender: "0x1",
        timestamp: 1700000000000000n,
      },
    ]);
  });

  it("reports a failed simulation's vm status", async () => {
    const { aptos } = fakeNode(() => ({
      data: [simulated({ success: false, vm_status: "Move abort: EINSUFFICIENT_BALANCE", events: [] })],
    }));
    const found = await new FullnodeScanAdapter(aptos).simulateTransactionEvents({
      transaction: await buildTransfer(aptos),
    });
    expect(found.success).toBe(false);
    expect(found.vmStatus).toBe("Move abort: EINSUFFICIENT_BALANCE");
  });

  it("refuses a malformed response", async () => {
    for (const data of [
      [],
      {},
      [null],
      [simulated({ version: "x" })],
      [simulated({ version: "" })],
      [simulated({ version: "0x2a" })],
      [simulated({ success: "yes" })],
      [simulated({ events: [{ type: TYPE, data: {}, sequence_number: "nope" }] })],
    ]) {
      const { aptos } = fakeNode(() => ({ data }));
      const bad = await new FullnodeScanAdapter(aptos)
        .simulateTransactionEvents({ transaction: await buildTransfer(aptos) })
        .catch((e: unknown) => e);
      expect(bad).toBeInstanceOf(StreamdError);
      expect((bad as Error).message).toBe("Fullnode returned a malformed simulation");
    }
  });

  it("refuses a non-transaction and keys that do not fit it before any request", async () => {
    const { aptos, calls } = fakeNode(() => ({ data: [simulated()] }));
    const transaction = await buildTransfer(aptos);
    const adapter = new FullnodeScanAdapter(aptos);
    for (const simulation of [
      undefined,
      null,
      {},
      { transaction: null },
      { transaction: "0x1234" },
      { transaction: {} },
      { transaction: { rawTransaction: {}, secondarySignerAddresses: "0xb" } },
      { transaction, signerPublicKey: "0xkey" },
      { transaction, secondarySignersPublicKeys: [] },
      { transaction, feePayerPublicKey: Account.generate().publicKey },
    ]) {
      const bad = await adapter.simulateTransactionEvents(simulation as never).catch((e: unknown) => e);
      expect(bad).toBeInstanceOf(StreamdError);
      expect((bad as Error).message).not.toContain("0x1234");
    }
    expect(calls).toEqual([]);
  });

  it("sends the signer's public key for the auth-key check", async () => {
    const { aptos, calls } = fakeNode(() => ({ data: [simulated({ vm_status: undefined })] }));
    const transaction = await buildTransfer(aptos);
    const adapter = new FullnodeScanAdapter(aptos);
    const key = Account.generate().publicKey;
    expect((await adapter.simulateTransactionEvents({ transaction })).vmStatus).toBe("");
    await adapter.simulateTransactionEvents({ transaction, signerPublicKey: key });
    const hex = (body: unknown) => Buffer.from(body as Uint8Array).toString("hex");
    const keyHex = Buffer.from(key.toUint8Array()).toString("hex");
    expect(hex(calls[0]!.body)).not.toContain(keyHex);
    expect(hex(calls[1]!.body)).toContain(keyHex);
  });

  it("simulates a multi-agent transaction with each signer's key, and a sponsored one with the fee payer's", async () => {
    const { aptos, calls } = fakeNode(() => ({ data: [simulated()] }));
    const adapter = new FullnodeScanAdapter(aptos);
    const [sender, second, third, payer] = [0, 1, 2, 3].map(() => Account.generate());
    const options = { accountSequenceNumber: 0, gasUnitPrice: 100, maxGasAmount: 1000, expireTimestamp: 9_999_999_999 };
    const data = {
      function: "0x1::aptos_account::transfer" as const,
      functionArguments: [sender!.accountAddress, 0],
      abi: { typeParameters: [], parameters: [parseTypeTag("address"), parseTypeTag("u64")] },
    };
    const multi = await aptos.transaction.build.multiAgent({
      sender: sender!.accountAddress,
      secondarySignerAddresses: [second!.accountAddress, third!.accountAddress],
      data,
      options,
      withFeePayer: true,
    });
    multi.feePayerAddress = payer!.accountAddress;
    const found = await adapter.simulateTransactionEvents({
      transaction: multi,
      signerPublicKey: sender!.publicKey,
      secondarySignersPublicKeys: [second!.publicKey, undefined],
      feePayerPublicKey: payer!.publicKey,
    });
    expect(found.events).toHaveLength(2);
    const sponsored = await aptos.transaction.build.simple({ sender: sender!.accountAddress, data, options, withFeePayer: true });
    sponsored.feePayerAddress = payer!.accountAddress;
    await adapter.simulateTransactionEvents({ transaction: sponsored, feePayerPublicKey: payer!.publicKey });
    const hex = (body: unknown) => Buffer.from(body as Uint8Array).toString("hex");
    const keyHex = (a: Account) => Buffer.from(a.publicKey.toUint8Array()).toString("hex");
    expect(calls.map((c) => c.path)).toEqual(["transactions/simulate", "transactions/simulate"]);
    expect(hex(calls[0]!.body)).toContain(keyHex(second!));
    expect(hex(calls[0]!.body)).not.toContain(keyHex(third!));
    expect(hex(calls[0]!.body)).toContain(keyHex(payer!));
    expect(hex(calls[0]!.body)).toContain(Buffer.from(third!.accountAddress.toUint8Array()).toString("hex"));
    expect(hex(calls[1]!.body)).toContain(keyHex(payer!));
    expect(hex(calls[1]!.body)).not.toContain(keyHex(sender!));
  });

  it("rejects a node error with its status and no cause", async () => {
    const { aptos } = fakeNode(() => ({ status: 400, data: { message: "Invalid transaction" } }));
    const bad = await new FullnodeScanAdapter(aptos)
      .simulateTransactionEvents({ transaction: await buildTransfer(aptos) })
      .catch((e: unknown) => e);
    expect(bad).toBeInstanceOf(StreamdNetworkError);
    expect((bad as StreamdNetworkError).statusCode).toBe(400);
    expect((bad as StreamdNetworkError).cause).toBeUndefined();
  });
});

describe("FullnodeScanAdapter view", () => {
  const BALANCE = "0x1::coin::balance";

  it("POSTs one JSON view at the ledger version and returns its values", async () => {
    const { aptos, calls } = fakeNode(() => ({ data: ["1500"] }));
    const state = await new FullnodeScanAdapter(aptos).view(
      {
        function: BALANCE,
        typeArguments: ["0x1::aptos_coin::AptosCoin"],
        functionArguments: ["0xa", 7n, [1n, 2n]],
      },
      42n,
    );
    expect(state).toEqual(["1500"]);
    expect(calls.map((c) => [c.method, c.path, c.params.ledger_version])).toEqual([
      ["POST", "view", 42n],
    ]);
    expect(calls[0]!.body).toEqual({
      function: BALANCE,
      type_arguments: ["0x1::aptos_coin::AptosCoin"],
      arguments: ["0xa", "7", ["1", "2"]],
    });
  });

  it("sends empty type arguments and arguments when omitted", async () => {
    const { aptos, calls } = fakeNode(() => ({ data: [] }));
    await new FullnodeScanAdapter(aptos).view({ function: "0x1::block::get_epoch_interval_secs" }, 0n);
    expect(calls[0]!.body).toEqual({
      function: "0x1::block::get_epoch_interval_secs",
      type_arguments: [],
      arguments: [],
    });
  });

  it("refuses malformed requests and versions before any request", async () => {
    const { aptos, calls } = fakeNode(() => ({ data: [] }));
    const adapter = new FullnodeScanAdapter(aptos);
    const bad: [unknown, unknown][] = [
      [null, 1n],
      [{ function: "coin::balance" }, 1n],
      [{ function: "0x1::coin" }, 1n],
      [{ function: "0xzz::coin::balance" }, 1n],
      [{ function: `0x${"1".repeat(65)}::m::f` }, 1n],
      [{ function: BALANCE, typeArguments: [1] }, 1n],
      [{ function: "0x1::coin::balance/../x" }, 1n],
      [{ function: 7 }, 1n],
      [{ function: BALANCE, typeArguments: "0x1::aptos_coin::AptosCoin" }, 1n],
      [{ function: BALANCE, typeArguments: [""] }, 1n],
      [{ function: BALANCE, functionArguments: "0xa" }, 1n],
      [{ function: BALANCE }, -1n],
      [{ function: BALANCE }, 2n ** 64n],
      [{ function: BALANCE }, 1],
    ];
    for (const [request, version] of bad) {
      const err = await adapter.view(request as never, version as never).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(StreamdError);
      expect(err).not.toBeInstanceOf(StreamdNetworkError);
    }
    expect(calls).toHaveLength(0);
  });

  it("never echoes arguments and shows only the start of a bad function id", async () => {
    const { aptos } = fakeNode(() => ({ data: [] }));
    const adapter = new FullnodeScanAdapter(aptos);
    const long = await adapter.view({ function: `0x1::${"m".repeat(200)}` } as never, 1n).catch((e: Error) => e);
    expect((long as Error).message.length).toBeLessThan(200);
    const args = await adapter
      .view({ function: BALANCE, functionArguments: "secret-arg" } as never, 1n)
      .catch((e: Error) => e);
    expect((args as Error).message).not.toContain("secret-arg");
  });

  it("refuses an aborting view as invalid input, with no cause", async () => {
    const { aptos } = fakeNode(() => ({ status: 400, data: { message: "nope" } }));
    const err = await new FullnodeScanAdapter(aptos)
      .view({ function: BALANCE, functionArguments: ["0xa"] }, 5n)
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect(err).not.toBeInstanceOf(StreamdNetworkError);
    expect((err as StreamdError).cause).toBeUndefined();
    expect((err as Error).message).toContain(`${BALANCE} at version 5`);
  });

  it("rejects a pruned version and other node errors as network errors with status and no cause", async () => {
    for (const status of [410, 503]) {
      const { aptos } = fakeNode(() => ({ status, data: { message: "down" } }));
      const err = await new FullnodeScanAdapter(aptos).view({ function: BALANCE }, 5n).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(StreamdNetworkError);
      expect((err as StreamdNetworkError).statusCode).toBe(status);
      expect((err as StreamdNetworkError).cause).toBeUndefined();
    }
  });

  it("lets FallbackAdapter answer a pruned view from another node", async () => {
    const pruned = fakeNode(() => ({ status: 410, data: { message: "pruned" } }));
    const archive = fakeNode(() => ({ data: ["7"] }));
    const adapter = new FallbackAdapter([
      new FullnodeScanAdapter(pruned.aptos),
      new FullnodeScanAdapter(archive.aptos),
    ]);
    expect(await adapter.view!({ function: BALANCE }, 5n)).toEqual(["7"]);
  });

  it("sends the values it checked and refuses unserialisable arguments", async () => {
    const { aptos, calls } = fakeNode(() => ({ data: [] }));
    const adapter = new FullnodeScanAdapter(aptos);
    let reads = 0;
    const shifty = {
      get function() {
        return reads++ === 0 ? BALANCE : "0x1::other::thing";
      },
    };
    await adapter.view(shifty as never, 1n);
    expect((calls[0]!.body as { function: string }).function).toBe(BALANCE);
    const circular: unknown[] = [];
    circular.push(circular);
    const err = await adapter.view({ function: BALANCE, functionArguments: circular }, 1n).catch((e: Error) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect(err).not.toBeInstanceOf(StreamdNetworkError);
    expect((err as Error).message).toContain("JSON-serialisable");
    expect(calls).toHaveLength(1);
  });
});

/** True when `needle` is a string anywhere in `value`'s object graph. */
function reaches(value: unknown, needle: string, seen = new Set<unknown>()): boolean {
  if (typeof value === "string") return value.includes(needle);
  if (typeof value !== "object" || value === null || seen.has(value)) return false;
  seen.add(value);
  return Object.getOwnPropertyNames(value).some((key) =>
    reaches((value as Record<string, unknown>)[key], needle, seen),
  );
}

describe("FullnodeScanAdapter errors and the API key", () => {
  const KEY = "sk-streamd-test-key";
  // A network no other test reads the ledger through, so the SDK's ledger-info cache never answers.
  const failing = (status: number) => () =>
    fakeNode(() => ({ status, data: { message: "boom" } }), { API_KEY: KEY }, Network.NETNA);

  for (const status of [400, 500]) {
    it(`keeps the API key out of every error from a ${status}`, async () => {
      const make = failing(status);
      const calls: [string, (aptos: Aptos) => Promise<unknown>][] = [
        ["module events", (a) => new FullnodeScanAdapter(a).fetchEvents(moduleDesc, { fromVersion: 0n })],
        ["account events", (a) => new FullnodeScanAdapter(a).fetchEvents(accountDesc, { fromVersion: 0n })],
        ["handle events", (a) => new FullnodeScanAdapter(a).fetchEvents(handleDesc, {})],
        ["head version", (a) => new FullnodeScanAdapter(a).getHeadVersion()],
        ["earliest version", (a) => new FullnodeScanAdapter(a).getEarliestVersion()],
        ["block heights", (a) => new FullnodeScanAdapter(a).getBlockHeights()],
        ["block", (a) => new FullnodeScanAdapter(a).getBlock(moduleDesc, 1n)],
        ["transaction", (a) => new FullnodeScanAdapter(a).getTransactionEvents("0x" + "ab".repeat(32))],
        ["view", (a) => new FullnodeScanAdapter(a).view({ function: "0x1::coin::balance" }, 1n)],
        ["checkDescriptor", (a) => checkDescriptor(a, moduleDesc as ModuleEventDescriptor)],
        ["listEventHandles", (a) => listEventHandles(a, "0x1")],
        [
          "withHooks onResponse",
          async (a) => {
            let reported: unknown;
            const hooked = withHooks(new FullnodeScanAdapter(a), {
              onResponse: (r) => void (reported = r.error),
            });
            await hooked.fetchEvents(handleDesc, {}).catch(() => {});
            throw reported;
          },
        ],
      ];
      for (const [name, call] of calls) {
        const err = await call(make().aptos).then(
          () => new Error(`${name} did not fail`),
          (e: unknown) => e,
        );
        expect([name, err]).toEqual([name, expect.any(StreamdError)]);
        expect([name, reaches(err, KEY)]).toEqual([name, false]);
      }
    });
  }
});

describe("FullnodeScanAdapter resource changes", () => {
  const RESOURCE = "0x1::coin::CoinStore<0x1::aptos_coin::AptosCoin>";
  const OWNER = `0x${"0".repeat(60)}cafe`;
  const STRANGER = `0x${"be".repeat(32)}`;
  const resourceDesc: EventDescriptor = { kind: "resource", accountAddress: OWNER, resourceType: RESOURCE };

  function write(address: string, type = RESOURCE, data: unknown = { coin: { value: "1" } }) {
    return { type: "write_resource", address, state_key_hash: "0x00", data: { type, data } };
  }

  function changed(version: number, changes: unknown[], events: string[] = []) {
    return {
      ...txn(version, events),
      hash: `0x${String(version).padStart(64, "0")}`,
      sender: "0xbeef",
      timestamp: "1700000000000000",
      changes,
    };
  }

  it("delivers writes and deletes of the resource, indexed past the events", async () => {
    const txns = [
      changed(0, [write(STRANGER), write(OWNER)], [TYPE, TYPE]),
      changed(1, [write(OWNER, OTHER), { type: "delete_resource", address: OWNER, resource: RESOURCE }]),
      changed(2, [{ type: "write_table_item", handle: "0x1", key: "0x", value: "0x" }]),
    ];
    const { aptos } = fakeNode(chain(100, txns as never));
    const page = await new FullnodeScanAdapter(aptos).fetchEvents(resourceDesc, {
      fromVersion: 0n,
      toVersion: 2n,
    });

    expect(page.events).toEqual([
      {
        type: RESOURCE,
        data: { coin: { value: "1" } },
        version: 0n,
        sequenceNumber: 0n,
        eventIndex: 3,
        transactionHash: txns[0]!.hash,
        sender: "0xbeef",
        timestamp: 1700000000000000n,
      },
      {
        type: RESOURCE,
        data: null,
        version: 1n,
        sequenceNumber: 0n,
        eventIndex: 1,
        transactionHash: txns[1]!.hash,
        sender: "0xbeef",
        timestamp: 1700000000000000n,
      },
    ]);
    expect(page.nextCursor).toBe("3");
    expect(page.throughVersion).toBe(2n);
  });

  it("serves getEvents through the client from a transaction with no events", async () => {
    const { aptos } = fakeNode(chain(100, [changed(4, [write(OWNER)])] as never));
    const client = new StreamdClient({ adapter: new FullnodeScanAdapter(aptos) });
    const events = await client.getEvents(resourceDesc, { fromVersion: 4n, toVersion: 4n });
    expect(events.map((e) => [e.version, e.eventIndex])).toEqual([[4n, 0]]);
  });

  it("refuses a bad address or resource type before any request", async () => {
    const { aptos, calls } = fakeNode(chain(100, []));
    const adapter = new FullnodeScanAdapter(aptos);
    for (const bad of [
      { ...resourceDesc, accountAddress: "nope" },
      { ...resourceDesc, resourceType: "CoinStore" },
    ] as EventDescriptor[]) {
      const err = await adapter.fetchEvents(bad, { fromVersion: 0n }).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(StreamdError);
      expect(err).not.toBeInstanceOf(StreamdNetworkError);
    }
    expect(calls).toHaveLength(0);
  });
});
