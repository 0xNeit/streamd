import { describe, expect, it } from "bun:test";
import { StreamdError } from "@streamd/core";
import { objectChangesOf, type ChangedObject, type ObjectChangeTransaction } from "../src/objects.js";
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
