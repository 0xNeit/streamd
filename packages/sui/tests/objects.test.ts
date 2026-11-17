import { describe, expect, it } from "bun:test";
import { StreamdClient, StreamdError } from "@streamd/core";
import { SuiAdapter } from "../src/adapter.js";
import { graphqlStatesOf, objectChangesOf, objectStatesOf, type ChangedObject, type GraphqlEffects, type ObjectChangeTransaction } from "../src/objects.js";
import { LONG, digest, fakeLedger, grpcError, str, withTransfers } from "./helpers.js";

const POOL = LONG("9001");
const OTHER = LONG("9002");
const TYPE = `${LONG("dee9")}::pool::Pool`;
const json = (value: string) => ({ kind: { oneofKind: "structValue" as const, structValue: { fields: { value: str(value) } } } });

function txn(changes: ChangedObject[], objects: ObjectChangeTransaction["objects"] = { objects: [] }): ObjectChangeTransaction {
  return {
    digest: "D1",
    timestamp: { seconds: 1_700_000_000n, nanos: 5_000 },
    transaction: { sender: "0xa11ce" },
    events: { events: [{}, {}] },
    effects: { changedObjects: changes },
    objects,
  };
}

describe("objectChangesOf", () => {
  it("gives a mutation the output version's contents, indexed past the events", () => {
    const changes = objectChangesOf(
      txn(
        [
          { objectId: OTHER, outputState: 2, outputVersion: 8n, objectType: TYPE },
          { objectId: "0x9001", inputVersion: 7n, outputState: 2, outputVersion: 8n, objectType: "0xdee9::pool::Pool" },
        ],
        { objects: [{ objectId: POOL, version: 7n, json: json("old") }, { objectId: POOL, version: 8n, json: json("new") }] },
      ),
      42n,
      POOL,
    );
    expect(changes).toEqual([
      {
        type: TYPE,
        data: { value: "new" },
        version: 42n,
        sequenceNumber: 0n,
        eventIndex: 3,
        transactionHash: "D1",
        timestamp: 1_700_000_000_000_005n,
        sender: LONG("a11ce"),
      },
    ]);
  });

  it("gives a creation its contents", () => {
    const [created] = objectChangesOf(
      txn([{ objectId: POOL, outputState: 2, outputVersion: 1n, objectType: TYPE }], { objects: [{ objectId: POOL, version: 1n, json: json("v") }] }),
      1n,
      POOL,
    );
    expect(created).toMatchObject({ data: { value: "v" }, eventIndex: 2 });
  });

  it("gives a deletion or wrap null, typed from the input object when the change has no type", () => {
    const [deleted] = objectChangesOf(
      txn([{ objectId: POOL, inputVersion: 7n, outputState: 1 }], { objects: [{ objectId: POOL, version: 7n, objectType: TYPE, json: json("last") }] }),
      9n,
      POOL,
    );
    expect(deleted).toMatchObject({ type: TYPE, data: null, version: 9n });
  });

  it("ignores other objects' changes", () => {
    expect(objectChangesOf(txn([{ objectId: OTHER, outputState: 1, objectType: TYPE }]), 1n, POOL)).toEqual([]);
    expect(objectChangesOf({}, 1n, POOL)).toEqual([]);
  });

  it("refuses a malformed answer", () => {
    for (const changes of [
      [{ objectId: POOL, outputState: 2, outputVersion: 8n, objectType: TYPE }],
      [{ objectId: POOL, outputVersion: 8n, objectType: TYPE }],
      [{ objectId: POOL, inputVersion: 7n, outputState: 1 }],
    ]) {
      expect(() => objectChangesOf(txn(changes), 1n, POOL)).toThrow(StreamdError);
    }
  });
});

const CLOCK = LONG("6");
const CREATED = LONG("9003");
const GONE = LONG("9004");
const UNTOUCHED = LONG("9005");

describe("objectStatesOf", () => {
  const state = {
    effects: {
      changedObjects: [
        { objectId: POOL, inputVersion: 7n, outputState: 2, outputVersion: 8n },
        { objectId: CREATED, outputState: 2, outputVersion: 8n },
        { objectId: GONE, inputVersion: 5n, outputState: 1 },
        { objectId: LONG("9006"), outputState: 3, outputVersion: 1n },
        { objectId: LONG("9007"), outputState: 4 },
      ],
      unchangedConsensusObjects: [
        { kind: 1, objectId: "0x6", version: 30n },
        { kind: 4, objectId: UNTOUCHED, version: 2n },
      ],
    },
    objects: {
      objects: [
        { objectId: POOL, version: 7n, json: json("old") },
        { objectId: POOL, version: 8n, json: json("new") },
        { objectId: CREATED, version: 8n, json: json("born") },
        { objectId: GONE, version: 5n, json: json("last") },
        { objectId: "0x6", version: 30n, json: json("tick") },
      ],
    },
  };

  it("reads each object as the transaction left it, in request order", () => {
    expect(objectStatesOf(state, [CLOCK, POOL, GONE, CREATED, POOL], "D1")).toEqual([
      { value: "tick" },
      { value: "new" },
      null,
      { value: "born" },
      { value: "new" },
    ]);
  });

  it("refuses objects outside the effects, packages, accumulators and a missing version", () => {
    const refusal = (id: string) => {
      try {
        objectStatesOf(state, [id], "D1");
      } catch (err) {
        expect(err).toBeInstanceOf(StreamdError);
        return (err as Error).message;
      }
      throw new Error("not refused");
    };
    expect(refusal(OTHER)).toContain(`Transaction D1 did not write or read ${OTHER}`);
    expect(refusal(UNTOUCHED)).toContain("did not write or read");
    expect(refusal(LONG("9006"))).toContain("as no Move object");
    expect(refusal(LONG("9007"))).toContain("as no Move object");
    const missing = { ...state, objects: { objects: [] } };
    expect(() => objectStatesOf(missing, [POOL], "D1")).toThrow(`Sui node returned transaction D1 without ${POOL} at version 8`);
  });
});

describe("graphqlStatesOf", () => {
  const object = (address: string, value: string) => ({ address, version: 8, asMoveObject: { contents: { json: { value } } } });
  const effects: GraphqlEffects = {
    objectChanges: {
      pageInfo: { hasNextPage: false },
      nodes: [
        { address: POOL, outputState: object(POOL, "new") },
        { address: GONE, outputState: null },
        { address: LONG("9006"), outputState: { address: LONG("9006"), version: 1, asMoveObject: null } },
      ],
    },
    unchangedConsensusObjects: {
      pageInfo: { hasNextPage: false },
      nodes: [{ __typename: "ConsensusObjectRead", object: object(CLOCK, "tick") }, { __typename: "ConsensusObjectCancelled" }],
    },
  };

  it("reads each object as the transaction left it, in request order", () => {
    expect(graphqlStatesOf(effects, [CLOCK, POOL, GONE], "D1")).toEqual([{ value: "tick" }, { value: "new" }, null]);
  });

  it("refuses objects outside the effects, non-objects, a further page and a malformed answer", () => {
    expect(() => graphqlStatesOf(effects, [OTHER], "D1")).toThrow(`Transaction D1 did not write or read ${OTHER}`);
    expect(() => graphqlStatesOf(effects, [LONG("9006")], "D1")).toThrow("as no Move object");
    const paged = { ...effects, objectChanges: { ...effects.objectChanges, pageInfo: { hasNextPage: true } } };
    expect(() => graphqlStatesOf(paged, [OTHER], "D1")).toThrow("more objects than one GraphQL page holds");
    expect(graphqlStatesOf(paged, [POOL], "D1")).toEqual([{ value: "new" }]);
    expect(() => graphqlStatesOf({ objectChanges: {} } as never, [POOL], "D1")).toThrow(StreamdError);
  });
});

describe("SuiAdapter readObjects memoization", () => {
  const TX = digest(7);
  const at = { version: 1n, transactionHash: TX };

  function txnStateLedger(responses: (() => unknown)[]) {
    const { client } = fakeLedger([1], []);
    const digests: string[] = [];
    client.ledgerService.getTransaction = async ({ digest }) => {
      digests.push(digest);
      const result = responses.shift()!();
      if (result instanceof Error) throw result;
      return { response: { transaction: result as never } };
    };
    return { client, digests };
  }

  it("memoizes concurrent and sequential gRPC state reads, keeps untouched errors from evicting, and returns fresh objects", async () => {
    const raw = {
      effects: { changedObjects: [{ objectId: POOL, inputVersion: 7n, outputState: 2, outputVersion: 8n }], unchangedConsensusObjects: [] },
      objects: { objects: [{ objectId: POOL, version: 8n, json: json("new") }] },
    };
    const { client, digests } = txnStateLedger([() => raw]);
    const adapter = new SuiAdapter(client);
    const [a, b] = await Promise.all([adapter.readObjects({ objects: [POOL] }, at), adapter.readObjects({ objects: [POOL] }, at)]);
    expect(digests).toEqual([TX]);
    (a[0] as Record<string, string>).value = "mutated";
    expect(b).toEqual([{ value: "new" }]);
    await expect(adapter.readObjects({ objects: [OTHER] }, at)).rejects.toThrow("did not write or read");
    expect(await adapter.readObjects({ objects: [POOL] }, at)).toEqual([{ value: "new" }]);
    expect(digests).toEqual([TX]);
  });

  it("evicts failed gRPC and GraphQL reads so retries succeed, and memoizes successful GraphQL lookups", async () => {
    const raw = {
      effects: { changedObjects: [{ objectId: POOL, outputState: 2, outputVersion: 8n }], unchangedConsensusObjects: [] },
      objects: { objects: [{ objectId: POOL, version: 8n, json: json("ok") }] },
    };
    const grpcRetry = txnStateLedger([() => grpcError("UNAVAILABLE", "down"), () => raw]);
    const grpcAdapter = new SuiAdapter(grpcRetry.client);
    await expect(grpcAdapter.readObjects({ objects: [POOL] }, at)).rejects.toThrow("down");
    expect(await grpcAdapter.readObjects({ objects: [POOL] }, at)).toEqual([{ value: "ok" }]);
    expect(grpcRetry.digests).toEqual([TX, TX]);

    const gql = txnStateLedger([() => grpcError("NOT_FOUND"), () => grpcError("NOT_FOUND"), () => grpcError("NOT_FOUND")]);
    const gqlAdapter = new SuiAdapter(gql.client, { graphqlUrl: "https://gql.example/graphql" });
    let posts = 0;
    const savedFetch = globalThis.fetch;
    globalThis.fetch = (async () => {
      posts++;
      if (posts === 1) return new Response("boom", { status: 500 });
      return Response.json({
        data: {
          transaction: {
            effects: {
              objectChanges: { pageInfo: { hasNextPage: false }, nodes: [{ address: POOL, outputState: { address: POOL, version: 8, asMoveObject: { contents: { json: { value: "gql" } } } } }] },
              unchangedConsensusObjects: { pageInfo: { hasNextPage: false }, nodes: [] },
            },
          },
        },
      });
    }) as unknown as typeof fetch;
    try {
      await expect(gqlAdapter.readObjects({ objects: [POOL] }, at)).rejects.toThrow("HTTP 500");
      const first = await gqlAdapter.readObjects({ objects: [POOL] }, at);
      (first[0] as Record<string, string>).value = "mutated";
      expect(await gqlAdapter.readObjects({ objects: [POOL] }, at)).toEqual([{ value: "gql" }]);
      expect(posts).toBe(2);
    } finally {
      globalThis.fetch = savedFetch;
    }
  });

  it("reuses transactions from an object walk in readObjects without another getTransaction call", async () => {
    const { client: base, calls: baseCalls } = fakeLedger([1], []);
    const { client: withTx, calls } = withTransfers(base, [1], [
      { cp: 0n, ti: 0n, changes: [], objects: [{ id: POOL, version: 8n, value: "fresh" }] },
    ]);
    const listTransactions = withTx.ledgerService.listTransactions!;
    const wrappedClient = {
      ...withTx,
      ledgerService: {
        ...withTx.ledgerService,
        listTransactions(input: Parameters<typeof listTransactions>[0]) {
          const { responses } = listTransactions(input);
          return {
            responses: (async function* () {
              for await (const frame of responses) {
                if (frame.transaction !== undefined) frame.transaction.digest = TX;
                yield frame;
              }
            })(),
          };
        },
      },
    };
    const sui = new SuiAdapter(wrappedClient);
    const client = new StreamdClient({ adapter: sui });
    const events = await client.getEvents({ kind: "object", objectId: POOL }, { fromVersion: 0n, withState: () => ({ objects: [POOL] }) });
    expect(events).toHaveLength(1);
    expect(events[0]!.state).toEqual([{ value: "fresh" }]);
    expect(baseCalls.transactions).toEqual([]);
    expect((calls.list[0]!.readMask as { paths: string[] }).paths).toContain("effects.unchanged_consensus_objects");
    client.destroy();
  });
});
