import { describe, it, expect } from "bun:test";
import { Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { IndexerAccountAdapter } from "../src/indexer.js";
import { StreamdError, StreamdNetworkError } from "@streamd/core";
import { checkAdapter } from "@streamd/core";

const URL_ = "http://indexer.test/v1/graphql";

interface GraphqlCall {
  query: string;
  variables: Record<string, unknown>;
  headers: Record<string, string>;
}

type Answer = Response | Record<string, unknown>;

/** A fake `fetch` answering each GraphQL POST with `answer(call)`, as JSON unless it is a Response. */
function fakeIndexer(answer: (call: GraphqlCall) => Answer) {
  const calls: GraphqlCall[] = [];
  const fetch = async (url: string, init: RequestInit): Promise<Response> => {
    expect(url).toBe(URL_);
    const body = JSON.parse(String(init.body)) as { query: string; variables: Record<string, unknown> };
    const call = { ...body, headers: init.headers as Record<string, string> };
    calls.push(call);
    const reply = answer(call);
    return reply instanceof Response ? reply : Response.json(reply);
  };
  return { fetch, calls };
}

/** An `Aptos` whose fullnode transport is `route`, so SDK reads run with no network. */
function fakeNode(route: (path: string) => { status?: number; data: unknown }) {
  const paths: string[] = [];
  const client = {
    async provider(req: { url: string }) {
      const path = req.url.replace(/^http:\/\/fake\/v1\/?/, "");
      paths.push(path);
      const reply = route(path);
      const status = reply.status ?? 200;
      return { status, statusText: status === 200 ? "OK" : "Error", data: reply.data, headers: {} };
    },
  };
  const aptos = new Aptos(
    new AptosConfig({
      network: Network.LOCAL,
      fullnode: "http://fake/v1",
      client: client as never,
      clientConfig: { http2: false },
    }),
  );
  return { aptos, paths };
}

function processed(version: number | string) {
  return { data: { processor_status: [{ last_success_version: version }] } };
}

function adapterWith(answer: (call: GraphqlCall) => Answer, options = {}) {
  const indexer = fakeIndexer(answer);
  const { aptos } = fakeNode(() => ({ status: 404, data: { message: "not found" } }));
  const adapter = new IndexerAccountAdapter(aptos, { indexerUrl: URL_, fetch: indexer.fetch, ...options });
  return { adapter, calls: indexer.calls, aptos };
}

describe("IndexerAccountAdapter options", () => {
  const { aptos } = fakeNode(() => ({ data: {} }));

  it("defaults the URL to the network's Indexer", () => {
    const mainnet = new Aptos(new AptosConfig({ network: Network.MAINNET, clientConfig: { http2: false } }));
    expect(() => new IndexerAccountAdapter(mainnet)).not.toThrow();
  });

  it("refuses a custom network with no Indexer URL", () => {
    const custom = new Aptos(
      new AptosConfig({ network: Network.CUSTOM, fullnode: "http://fake/v1", clientConfig: { http2: false } }),
    );
    expect(() => new IndexerAccountAdapter(custom)).toThrow(StreamdError);
  });

  it.each([
    [{ indexerUrl: "ftp://x" }],
    [{ indexerUrl: "not a url" }],
    [{ indexerUrl: 5 }],
    [{ indexerUrl: URL_, apiKey: "" }],
    [{ indexerUrl: URL_, apiKey: 42 }],
    [{ indexerUrl: URL_, fetch: "nope" }],
    [{ indexerUrl: URL_, maxScan: 0 }],
    [{ indexerUrl: URL_, maxScan: 1.5 }],
  ])("refuses %p", (options) => {
    expect(() => new IndexerAccountAdapter(aptos, options as never)).toThrow(StreamdError);
  });
});

describe("IndexerAccountAdapter bounds", () => {
  it("reads the head from the account transactions processor", async () => {
    const { adapter, calls } = adapterWith(() => processed(4321));
    expect(await adapter.getHeadVersion()).toBe(4321n);
    expect(calls[0]!.query).toContain("processor_status");
    expect(calls[0]!.variables).toEqual({ processor: "account_transactions_processor" });
    expect(calls[0]!.headers.Authorization).toBeUndefined();
  });

  it("accepts a version sent as a decimal string", async () => {
    const { adapter } = adapterWith(() => processed("9007199254740993"));
    expect(await adapter.getHeadVersion()).toBe(9007199254740993n);
  });

  it("sends the API key as a bearer token", async () => {
    const { adapter, calls } = adapterWith(() => processed(1), { apiKey: "secret" });
    await adapter.getHeadVersion();
    expect(calls[0]!.headers.Authorization).toBe("Bearer secret");
  });

  it.each([
    [{ data: { processor_status: [] } }],
    [{ data: { processor_status: [{ last_success_version: -1 }] } }],
    [{ data: { processor_status: [{ last_success_version: "1e3" }] } }],
    [{ data: null }],
    [{}],
  ])("rejects a malformed answer %p as a StreamdError", async (body) => {
    const { adapter } = adapterWith(() => body);
    const err = await adapter.getHeadVersion().catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect(err).not.toBeInstanceOf(StreamdNetworkError);
  });

  it("rejects a 200 body that is not JSON as a network error, so it is retried", async () => {
    const { adapter } = adapterWith(() => new Response("<html>"));
    const err = (await adapter.getHeadVersion().catch((e: unknown) => e)) as StreamdNetworkError;
    expect(err).toBeInstanceOf(StreamdNetworkError);
    expect(err.statusCode).toBe(200);
  });

  it("rejects HTTP 429 as a network error carrying Retry-After", async () => {
    const { adapter } = adapterWith(
      () => new Response("slow down", { status: 429, headers: { "Retry-After": "7" } }),
      { apiKey: "secret" },
    );
    const err = (await adapter.getHeadVersion().catch((e: unknown) => e)) as StreamdNetworkError;
    expect(err).toBeInstanceOf(StreamdNetworkError);
    expect(err.statusCode).toBe(429);
    expect(err.retryAfter).toBe(7000);
    expect(err.message).not.toContain("secret");
  });

  it.each([[429], ["429"]])("rejects a GraphQL rate-limit error (code %p) as a 429 network error", async (code) => {
    const { adapter } = adapterWith(() =>
      Response.json(
        { errors: [{ message: "rate limit exceeded", extensions: { code } }] },
        { headers: { "Retry-After": "3" } },
      ),
    );
    const err = (await adapter.getHeadVersion().catch((e: unknown) => e)) as StreamdNetworkError;
    expect(err).toBeInstanceOf(StreamdNetworkError);
    expect(err.statusCode).toBe(429);
    expect(err.retryAfter).toBe(3000);
    expect(err.message).toContain("rate limit exceeded");
  });

  it("rejects any other GraphQL error as a network error with no status", async () => {
    const { adapter } = adapterWith(() => ({ errors: [{ message: "x".repeat(500) }] }));
    const err = (await adapter.getHeadVersion().catch((e: unknown) => e)) as StreamdNetworkError;
    expect(err).toBeInstanceOf(StreamdNetworkError);
    expect(err.statusCode).toBeUndefined();
    expect(err.message.length).toBeLessThan(260);
  });

  it("rejects a fetch that throws as a network error", async () => {
    const { aptos } = fakeNode(() => ({ data: {} }));
    const adapter = new IndexerAccountAdapter(aptos, {
      indexerUrl: URL_,
      apiKey: "secret",
      fetch: async () => {
        throw new TypeError("connection refused");
      },
    });
    const err = (await adapter.getHeadVersion().catch((e: unknown) => e)) as StreamdNetworkError;
    expect(err).toBeInstanceOf(StreamdNetworkError);
    expect(err.message).toContain("connection refused");
    expect(err.cause).toBeUndefined();
  });

  it("reads the earliest version from the fullnode", async () => {
    const indexer = fakeIndexer(() => processed(1));
    const earliest = (reply: { status?: number; data: unknown }) =>
      new IndexerAccountAdapter(fakeNode(() => reply).aptos, { indexerUrl: URL_, fetch: indexer.fetch })
        .getEarliestVersion();
    expect(await earliest({ data: { oldest_ledger_version: "77" } })).toBe(77n);
    await expect(earliest({ data: { oldest_ledger_version: "x" } })).rejects.toBeInstanceOf(StreamdError);
    const err = (await earliest({ status: 503, data: { message: "down" } }).catch((e: unknown) => e)) as StreamdNetworkError;
    expect(err).toBeInstanceOf(StreamdNetworkError);
    expect(err.statusCode).toBe(503);
  });
});

// One hex digit short and mixed case, as callers may write it; the Indexer wants 64 lowercase.
const ACCOUNT = `0x${"AB".repeat(31)}C`;
const PADDED = `0x0${"ab".repeat(31)}c`;
const DEPOSIT = "0x1::coin::DepositEvent";
const FEE = "0x1::transaction_fee::FeeStatement";
const account = { kind: "account", accountAddress: ACCOUNT } as const;

function txn(version: number, types: string[] = [DEPOSIT, FEE]) {
  return {
    version: String(version),
    hash: `0x${version.toString(16).padStart(64, "0")}`,
    sender: "0xdef",
    timestamp: "1700000000000000",
    type: "user_transaction",
    events: types.map((type, i) => ({ type, data: { i }, sequence_number: "0" })),
  };
}

/**
 * An Indexer listing `versions` for the account, processed through `indexed`,
 * over a fullnode serving `txn(v)` at each of them and oldest version `oldest`.
 */
function chain(versions: number[], indexed: number, oldest = 0) {
  const state = { indexed, txns: new Map(versions.map((v) => [v, txn(v)] as [number, unknown])) };
  const indexer = fakeIndexer(({ query, variables }) => {
    if (query.includes("processor_status")) return processed(state.indexed);
    expect(variables.address).toBe(PADDED);
    const from = Number(variables.from);
    const to = Number(variables.to);
    const rows = versions
      .filter((v) => v >= from && v <= to)
      .slice(0, Number(variables.limit))
      .map((v) => ({ transaction_version: v }));
    return { data: { account_transactions: rows } };
  });
  const node = fakeNode((path) => {
    if (path === "") return { data: { oldest_ledger_version: String(oldest) } };
    const v = Number(path.replace("transactions/by_version/", ""));
    const found = state.txns.get(v);
    return found === undefined ? { status: 404, data: { message: "not found" } } : { data: found };
  });
  const make = (options = {}) =>
    new IndexerAccountAdapter(node.aptos, { indexerUrl: URL_, fetch: indexer.fetch, ...options });
  return { make, state, calls: indexer.calls, paths: node.paths };
}

const ids = (page: { events: { version: bigint; eventIndex: number }[] }) =>
  page.events.map((e) => `${e.version}:${e.eventIndex}`);

describe("IndexerAccountAdapter fetchEvents", () => {
  it("returns every listed transaction's events, received ones included", async () => {
    const { make, calls, paths } = chain([10, 20], 50);
    const page = await make().fetchEvents(account, { fromVersion: 0n });
    expect(ids(page)).toEqual(["10:0", "10:1", "20:0", "20:1"]);
    expect(page).toMatchObject({ nextCursor: "51", hasMore: false, throughVersion: 50n });
    expect(page.final).toBeUndefined();
    expect(page.events[0]).toMatchObject({ type: DEPOSIT, sender: "0xdef", timestamp: 1700000000000000n });
    expect(paths).toEqual(["", "transactions/by_version/10", "transactions/by_version/20"]);
    // The processed version is read in its own request, before any listing.
    expect(calls[0]!.query).toContain("processor_status");
    expect(calls[1]!.variables).toEqual({ address: PADDED, from: "0", to: "50", limit: 100 });
  });

  it("keeps only eventTypes", async () => {
    const { make } = chain([10], 50);
    const page = await make().fetchEvents({ ...account, eventTypes: [FEE] }, { fromVersion: 0n });
    expect(ids(page)).toEqual(["10:1"]);
  });

  it("stops at a transaction boundary once limit is met, then resumes from the cursor", async () => {
    const { make } = chain([10, 20, 30], 50);
    const adapter = make();
    const first = await adapter.fetchEvents(account, { fromVersion: 0n, limit: 3 });
    expect(ids(first)).toEqual(["10:0", "10:1", "20:0", "20:1"]);
    expect(first).toMatchObject({ nextCursor: "21", hasMore: true, throughVersion: 20n });
    const second = await adapter.fetchEvents(account, { cursor: first.nextCursor, limit: 3 });
    expect(ids(second)).toEqual(["30:0", "30:1"]);
    expect(second).toMatchObject({ nextCursor: "51", hasMore: false, throughVersion: 50n });
  });

  it("reads at most maxScan transactions per call", async () => {
    const { make, paths } = chain([10, 20, 30], 50);
    const adapter = make({ maxScan: 2 });
    const page = await adapter.fetchEvents({ ...account, eventTypes: ["0x1::none::None"] }, { fromVersion: 0n });
    expect(page).toMatchObject({ events: [], nextCursor: "21", hasMore: true, throughVersion: 20n });
    expect(paths.filter((p) => p !== "")).toHaveLength(2);
  });

  it("pages the listing past a full Indexer page", async () => {
    const versions = Array.from({ length: 150 }, (_, i) => i + 1);
    const { make, calls } = chain(versions, 200);
    const page = await make({ maxScan: 1000 }).fetchEvents(account, { fromVersion: 0n, limit: 1000 });
    expect(page.events).toHaveLength(300);
    expect(page).toMatchObject({ nextCursor: "201", hasMore: false });
    const listings = calls.filter((c) => c.query.includes("account_transactions"));
    expect(listings.map((c) => c.variables.from)).toEqual(["0", "101"]);
  });

  it("starts at the processed version with neither cursor nor fromVersion", async () => {
    const { make, state } = chain([10, 60], 50);
    const adapter = make();
    const tail = await adapter.fetchEvents(account, {});
    expect(tail).toMatchObject({ events: [], nextCursor: "51", hasMore: false, throughVersion: 50n });
    state.indexed = 70;
    const next = await adapter.fetchEvents(account, { cursor: tail.nextCursor });
    expect(ids(next)).toEqual(["60:0", "60:1"]);
    expect(next.throughVersion).toBe(70n);
  });

  it("raises fromVersion to the fullnode's oldest version", async () => {
    const { make, calls } = chain([10, 40], 50, 30);
    const page = await make().fetchEvents(account, { fromVersion: 5n });
    expect(ids(page)).toEqual(["40:0", "40:1"]);
    expect(calls[1]!.variables.from).toBe("30");
  });

  it("ends at toVersion", async () => {
    const { make } = chain([10, 40], 50);
    const page = await make().fetchEvents(account, { fromVersion: 0n, toVersion: 20n });
    expect(ids(page)).toEqual(["10:0", "10:1"]);
    expect(page).toMatchObject({ nextCursor: "21", hasMore: false, throughVersion: 20n });
  });

  it("leaves the cursor where it was while the Indexer is behind it", async () => {
    const { make, calls } = chain([10], 50);
    const page = await make().fetchEvents(account, { cursor: "60" });
    expect(page).toEqual({ events: [], nextCursor: "60", hasMore: false });
    expect(calls).toHaveLength(1);
  });

  it("reverses a page for orderBy desc", async () => {
    const { make } = chain([10], 50);
    const page = await make().fetchEvents(account, { fromVersion: 0n, orderBy: "desc" });
    expect(ids(page)).toEqual(["10:1", "10:0"]);
  });

  it.each([
    ["a module descriptor", { kind: "module", eventType: DEPOSIT }, {}],
    ["a bad address", { kind: "account", accountAddress: "nope" }, {}],
    ["a foreign cursor", account, { cursor: "abc" }],
    ["an offset", account, { offset: 2 }],
  ])("refuses %s before any request", async (_label, descriptor, options) => {
    const { make, calls } = chain([10], 50);
    const err = await make()
      .fetchEvents(descriptor as never, options)
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect(err).not.toBeInstanceOf(StreamdNetworkError);
    expect(calls).toHaveLength(0);
  });

  it.each([[0], [Number.NaN]])("returns an empty page for limit %p without a request", async (limit) => {
    const { make, calls } = chain([10], 50);
    expect(await make().fetchEvents(account, { limit })).toEqual({ events: [], hasMore: false });
    expect(calls).toHaveLength(0);
  });

  it("returns an empty page at fromVersion while the Indexer is behind it", async () => {
    const { make } = chain([10], 50);
    const page = await make().fetchEvents(account, { fromVersion: 80n });
    expect(page).toEqual({ events: [], nextCursor: "80", hasMore: false });
  });

  it.each([
    ["descending", [{ transaction_version: 20 }, { transaction_version: 10 }]],
    ["repeated", [{ transaction_version: 10 }, { transaction_version: 10 }]],
    ["above the bound", [{ transaction_version: 999 }]],
    ["below the start", [{ transaction_version: 1 }]],
    ["not versions", [{ transaction_version: "x" }]],
    ["not a list", "rows"],
    ["more than asked for", Array.from({ length: 101 }, (_, i) => ({ transaction_version: 10 + i }))],
  ])("refuses a listing whose rows are %s", async (_label, rows) => {
    const { aptos } = fakeNode((path) => ({ data: txn(Number(path.split("/").at(-1))) }));
    const indexer = fakeIndexer(({ query }) =>
      query.includes("processor_status") ? processed(500) : { data: { account_transactions: rows } },
    );
    const adapter = new IndexerAccountAdapter(aptos, { indexerUrl: URL_, fetch: indexer.fetch });
    const err = await adapter.fetchEvents(account, { cursor: "5" }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect(err).not.toBeInstanceOf(StreamdNetworkError);
  });

  it.each([
    ["another version", { ...txn(10), version: "11" }],
    ["events that are not a list", { ...txn(10), events: {} }],
    ["an event with no type", { ...txn(10), events: [{ data: {}, sequence_number: "0" }] }],
    ["a bad sequence number", { ...txn(10), events: [{ type: DEPOSIT, sequence_number: "-1" }] }],
    ["a bad timestamp", { ...txn(10), timestamp: "soon" }],
    ["a null guid", { ...txn(10), events: [{ type: DEPOSIT, sequence_number: "0", guid: null }] }],
  ])("refuses a transaction carrying %s", async (_label, body) => {
    const { make, state } = chain([10], 50);
    state.txns.set(10, body);
    const err = await make().fetchEvents(account, { fromVersion: 0n }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect(err).not.toBeInstanceOf(StreamdNetworkError);
  });

  it("rejects a pruned or missing transaction as a network error", async () => {
    const { make, state } = chain([10], 50);
    state.txns.delete(10);
    const err = (await make().fetchEvents(account, { fromVersion: 0n }).catch((e: unknown) => e)) as StreamdNetworkError;
    expect(err).toBeInstanceOf(StreamdNetworkError);
    expect(err.statusCode).toBe(404);
  });

  it("keeps the EventAdapter contract by checkAdapter", async () => {
    const { make } = chain([10, 20, 30, 40, 50], 60);
    const report = await checkAdapter(make(), account, { fromVersion: 0n, limit: 2 });
    expect(report.results.filter((r) => r.status === "fail")).toEqual([]);
    expect(report.ok).toBe(true);
    expect(report.pagesFetched).toBe(3);
    expect(report.results.find((r) => r.name === "through-version")?.status).toBe("pass");
    expect(report.results.find((r) => r.name === "cursor-precedence")?.status).toBe("pass");
  });
});
