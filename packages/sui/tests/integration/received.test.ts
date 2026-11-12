import { describe, expect, it } from "bun:test";
import { SuiGrpcClient } from "@mysten/sui/grpc";
import { eventId } from "@streamd/core";
import type { EventDescriptor, StreamdEvent } from "@streamd/core";
import { SuiAdapter, SuiClient, networks } from "../../src/index.js";

/** Hits Sui mainnet; skipped unless `STREAMD_INTEGRATION=1`. */
const enabled = process.env.STREAMD_INTEGRATION === "1";

const shape = (e: StreamdEvent) => ({ id: eventId(e), type: e.type, data: e.data, sender: e.sender, hash: e.transactionHash, call: e.entryFunction });

describe.skipIf(!enabled)("SuiAdapter received account descriptors (mainnet)", () => {
  const grpc = new SuiGrpcClient(networks.mainnet);
  const adapter = new SuiAdapter(grpc);

  it("delivers the events of a transaction the address only received, which the signed walk misses", async () => {
    using sui = new SuiClient({ sui: grpc });
    const records = await sui.getTransfers({ fromVersion: -600n, toVersion: "head" });
    let found: { recipient: string; signer: string; version: bigint; events: StreamdEvent[] } | undefined;
    for (const r of records) {
      if (r.data.to === undefined || r.data.to === r.sender) continue;
      const txn = await adapter.getTransactionEvents(r.transactionHash!);
      if (txn !== undefined && txn.events.length > 0) {
        found = { recipient: r.data.to, signer: r.sender!, version: r.version, events: txn.events };
        break;
      }
    }
    expect(found).toBeDefined();
    const { recipient, signer, version, events } = found!;
    const range = { fromVersion: version, toVersion: version };
    const received = await sui.getEvents({ kind: "account", accountAddress: recipient, received: true }, range);
    expect(received.map(shape)).toEqual(events.map(shape));
    expect(await sui.getEvents({ kind: "account", accountAddress: recipient }, range)).toEqual([]);
    const signed = await sui.getEvents({ kind: "account", accountAddress: signer, received: true }, range);
    expect(signed.map(eventId)).toEqual(events.map(eventId));
    const typed = await sui.getEvents({ kind: "account", accountAddress: recipient, received: true, eventTypes: [events[0]!.type] }, range);
    expect(typed.map(eventId)).toEqual(events.filter((e) => e.type === events[0]!.type).map(eventId));
  }, 180_000);

  it("pages by cursor with no gap or repeat", async () => {
    const head = await adapter.getHeadVersion();
    using sui = new SuiClient({ sui: grpc });
    // The busiest signer of recent transfers, so the walk has several transactions to page over.
    const counts = new Map<string, number>();
    for (const r of await sui.getTransfers({ fromVersion: head - 600n, toVersion: head })) counts.set(r.sender!, (counts.get(r.sender!) ?? 0) + 1);
    const [address] = [...counts].sort((a, b) => b[1] - a[1])[0]!;
    const descriptor: EventDescriptor = { kind: "account", accountAddress: address, received: true };
    // The node may end a page early at its scan limit, so both walks follow hasMore to the end.
    const walk = async (limit: number) => {
      const out: StreamdEvent[] = [];
      let page = await adapter.fetchEvents(descriptor, { fromVersion: head - 1500n, toVersion: head, limit });
      for (let i = 0; ; i++) {
        out.push(...page.events);
        if (!page.hasMore || i > 300) return out;
        page = await adapter.fetchEvents(descriptor, { cursor: page.nextCursor, toVersion: head, limit });
      }
    };
    const whole = await walk(100);
    const paged = await walk(1);
    expect(new Set(whole.map((e) => e.version)).size).toBeGreaterThan(1);
    expect(paged.map(eventId)).toEqual(whole.map(eventId));
    expect(new Set(paged.map(eventId)).size).toBe(paged.length);
  }, 180_000);
});
