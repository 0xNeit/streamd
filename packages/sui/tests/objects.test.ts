import { describe, expect, it } from "bun:test";
import { StreamdError } from "@streamd/core";
import { graphqlStatesOf, objectChangesOf, objectStatesOf, type ChangedObject, type GraphqlEffects, type ObjectChangeTransaction } from "../src/objects.js";
import { LONG, str } from "./helpers.js";

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
