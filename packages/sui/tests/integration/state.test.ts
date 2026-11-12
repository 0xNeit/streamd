import { describe, expect, it } from "bun:test";
import { SuiGrpcClient } from "@mysten/sui/grpc";
import { SuiAdapter, SuiClient, networks } from "../../src/index.js";
import type { SuiLedgerClient } from "../../src/index.js";

/** Hits Sui mainnet and its GraphQL service; skipped unless `STREAMD_INTEGRATION=1`. */
const enabled = process.env.STREAMD_INTEGRATION === "1";

const CLOCK = `0x${"6".padStart(64, "0")}`;
const OBJECT_WRITE = 2;
const READ_ONLY_ROOT = 1;
const MASK = ["effects.changed_objects", "effects.unchanged_consensus_objects"];

describe.skipIf(!enabled)("SuiAdapter object state (mainnet)", () => {
  const grpc = new SuiGrpcClient(networks.mainnet);
  /** The same client, except that every transaction lookup is pruned, so reads go to GraphQL. */
  const pruned = {
    network: "mainnet",
    ledgerService: Object.assign(Object.create(grpc.ledgerService) as SuiLedgerClient["ledgerService"], {
      getTransaction: async () => {
        throw Object.assign(new Error("not found"), { code: "NOT_FOUND" });
      },
    }),
  } as SuiLedgerClient;

  /** A transaction some way behind the head that wrote an object and read a consensus object without writing it. */
  async function readerAndWriter(): Promise<{ digest: string; read: string; written: string }> {
    const head = (await grpc.ledgerService.getServiceInfo({})).response.checkpointHeight!;
    for (let checkpoint = head - 200n; checkpoint > head - 260n; checkpoint--) {
      const { response } = await grpc.ledgerService.getCheckpoint({
        checkpointId: { oneofKind: "sequenceNumber", sequenceNumber: checkpoint },
        readMask: { paths: ["transactions.digest", ...MASK.map((p) => `transactions.${p}`)] },
      });
      for (const txn of response.checkpoint?.transactions ?? []) {
        const read = txn.effects?.unchangedConsensusObjects.find((u) => u.kind === READ_ONLY_ROOT);
        const written = txn.effects?.changedObjects.find((c) => c.outputState === OBJECT_WRITE && c.objectId !== CLOCK);
        if (read?.objectId && written?.objectId) return { digest: txn.digest!, read: read.objectId, written: written.objectId };
      }
    }
    throw new Error("no transaction both read and wrote a consensus object in 60 checkpoints");
  }

  it("gives each Clock change its own contents as state", async () => {
    using sui = new SuiClient({ sui: grpc });
    const changes = await sui.getEvents({ kind: "object", objectId: "0x6" }, {
      fromVersion: -3000n,
      limit: 4,
      withState: () => ({ objects: ["0x6"] }),
    });
    expect(changes.length).toBeGreaterThan(1);
    for (const change of changes) expect(change.state).toEqual([change.data]);
  }, 120_000);

  it("reads the same states over gRPC and over GraphQL, a read-only consensus input included", async () => {
    const { digest, read, written } = await readerAndWriter();
    const request = { objects: [read, written] };
    const at = { version: 0n, transactionHash: digest };
    const overGrpc = await new SuiAdapter(grpc).readObjects(request, at);
    const overGraphql = await new SuiAdapter(pruned).readObjects(request, at);
    expect(overGrpc.every((state) => typeof state === "object" && state !== null)).toBe(true);
    expect(overGraphql).toEqual(overGrpc);
  }, 120_000);

  it("reads a transaction from months back through GraphQL", async () => {
    const response = await fetch(networks.mainnet.graphqlUrl, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        query: "{ checkpoint(sequenceNumber: 300000000) { transactions(first: 10) { nodes { digest effects { objectChanges(first: 20) { nodes { address outputState { version } } } } } } } }",
      }),
    });
    const body = (await response.json()) as {
      data: { checkpoint: { transactions: { nodes: { digest: string; effects: { objectChanges: { nodes: { address: string; outputState: object | null }[] } } }[] } } };
    };
    const found = body.data.checkpoint.transactions.nodes
      .map((t) => ({ digest: t.digest, change: t.effects.objectChanges.nodes.find((c) => c.outputState !== null && c.address !== CLOCK) }))
      .find((t) => t.change !== undefined)!;
    const adapter = new SuiAdapter(grpc);
    const lookup = grpc.ledgerService.getTransaction({ digest: found.digest, readMask: { paths: ["digest"] } });
    expect(await lookup.then(() => "found", (err: { code?: string }) => err.code)).toBe("NOT_FOUND");
    const [state] = await adapter.readObjects({ objects: [found.change!.address] }, { version: 0n, transactionHash: found.digest });
    expect((state as { id: string }).id).toBe(found.change!.address);
  }, 120_000);

  it("refuses an object the transaction did not touch", async () => {
    const { digest } = await readerAndWriter();
    const stranger = `0x${"5".padStart(64, "0")}`;
    await expect(new SuiAdapter(grpc).readObjects({ objects: [stranger] }, { version: 0n, transactionHash: digest })).rejects.toThrow(
      "did not write or read",
    );
  }, 120_000);
});
