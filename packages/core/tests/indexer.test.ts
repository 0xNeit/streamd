import { describe, it, expect } from "bun:test";
import { Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { IndexerAccountAdapter } from "../src/adapters/indexer.js";
import { StreamdError, StreamdNetworkError } from "../src/errors.js";

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

  it("rejects a body that is not JSON as a StreamdError", async () => {
    const { adapter } = adapterWith(() => new Response("<html>"));
    const err = await adapter.getHeadVersion().catch((e: unknown) => e);
    expect(err).not.toBeInstanceOf(StreamdNetworkError);
    expect(err).toBeInstanceOf(StreamdError);
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

  it("rejects a GraphQL rate-limit error as a 429 network error", async () => {
    const { adapter } = adapterWith(() => ({
      errors: [{ message: "rate limit exceeded", extensions: { code: 429 } }],
    }));
    const err = (await adapter.getHeadVersion().catch((e: unknown) => e)) as StreamdNetworkError;
    expect(err).toBeInstanceOf(StreamdNetworkError);
    expect(err.statusCode).toBe(429);
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
    const { adapter, aptos } = adapterWith(() => processed(1));
    aptos.getLedgerInfo = async () => ({ oldest_ledger_version: "77" }) as never;
    expect(await adapter.getEarliestVersion()).toBe(77n);
    aptos.getLedgerInfo = async () => ({ oldest_ledger_version: "x" }) as never;
    expect(adapter.getEarliestVersion()).rejects.toBeInstanceOf(StreamdError);
    aptos.getLedgerInfo = async () => {
      throw Object.assign(new Error("down"), { status: 503 });
    };
    const err = (await adapter.getEarliestVersion().catch((e: unknown) => e)) as StreamdNetworkError;
    expect(err).toBeInstanceOf(StreamdNetworkError);
    expect(err.statusCode).toBe(503);
  });
});
