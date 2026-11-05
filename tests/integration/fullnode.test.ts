import { describe, it, expect } from "bun:test";
import { Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { FullnodeScanAdapter } from "../../src/adapters/fullnode.js";
import { StreamdClient } from "../../src/client.js";
import { involvesAddress } from "../../src/filter.js";
import type { EventDescriptor, StreamdEvent } from "../../src/types.js";

/**
 * Hits mainnet. Run with `bun run test:integration`; skipped otherwise so the
 * unit suite stays offline and deterministic.
 */
const enabled = process.env.STREAMD_INTEGRATION === "1";

// Emitted by essentially every user transaction — dense enough that a short
// scan is guaranteed to find some.
const FEE_STATEMENT: EventDescriptor = {
  kind: "module",
  eventType: "0x1::transaction_fee::FeeStatement",
};

// Emitted once per block via a legacy EventHandle.
const NEW_BLOCK: EventDescriptor = {
  kind: "handle",
  accountAddress: "0x1",
  eventHandleStruct: "0x1::block::BlockResource",
  fieldName: "new_block_events",
};

function mainnet() {
  return new Aptos(new AptosConfig({ network: Network.MAINNET, clientConfig: { http2: false } }));
}

describe.skipIf(!enabled)("FullnodeScanAdapter (mainnet)", () => {
  it("advances the cursor past windows with no matches", async () => {
    const aptos = mainnet();
    const adapter = new FullnodeScanAdapter(aptos, {
      scanBatch: 100,
      maxScan: 200,
    });

    // A type nothing ever emits, so every scanned window matches nothing.
    // This is precisely the case that used to stall: the cursor advanced only
    // from matched events, so a subscription re-read one window forever.
    const missing: EventDescriptor = {
      kind: "module",
      eventType: "0x1::nonexistent_module::NeverEmitted",
    };

    const head = BigInt((await aptos.getLedgerInfo()).ledger_version);
    const start = head - 5000n;

    let cursor: string | undefined;
    const cursors: bigint[] = [];

    for (let i = 0; i < 3; i++) {
      const p = await adapter.fetchEvents(missing, {
        cursor,
        fromVersion: cursor === undefined ? start : undefined,
        limit: 25,
        orderBy: "asc",
      });

      expect(p.events).toHaveLength(0);
      expect(p.nextCursor).toBeDefined();
      cursors.push(BigInt(p.nextCursor!));
      cursor = p.nextCursor;
    }

    expect(cursors[0]).toBeGreaterThan(start);
    expect(cursors[1]).toBeGreaterThan(cursors[0]);
    expect(cursors[2]).toBeGreaterThan(cursors[1]);
  }, 60_000);

  it("fetches module events in ascending version order", async () => {
    const client = new StreamdClient({ aptos: mainnet() });
    const head = BigInt((await mainnet().getLedgerInfo()).ledger_version);

    const events = await client.getEvents(FEE_STATEMENT, {
      fromVersion: head - 200n,
      limit: 5,
    });

    expect(events.length).toBeGreaterThan(0);
    expect(events.length).toBeLessThanOrEqual(5);
    for (const event of events) {
      expect(event.type).toBe("0x1::transaction_fee::FeeStatement");
    }
    for (let i = 1; i < events.length; i++) {
      expect(events[i].version >= events[i - 1].version).toBe(true);
    }

    client.destroy();
  }, 60_000);

  it("fetches legacy handle events", async () => {
    const client = new StreamdClient({ aptos: mainnet() });

    const events = await client.getEvents(NEW_BLOCK, { limit: 3 });

    expect(events).toHaveLength(3);
    expect(events[0].type).toBe("0x1::block::NewBlockEvent");
    // Handle events carry real sequence numbers, unlike module events.
    expect(events[1].sequenceNumber).toBe(events[0].sequenceNumber + 1n);

    client.destroy();
  }, 60_000);

  it("walks an account's history and filters by event type", async () => {
    const client = new StreamdClient({ aptos: mainnet() });

    // A busy mainnet account; offset 0 starts at its first transaction.
    const account = "0xd503b95164384a5ebbccbb5c4bdc8b4a5893d9651e9953abda8e1c22fcc1181d";

    const all = await client.getEvents(
      { kind: "account", accountAddress: account },
      { offset: 0, limit: 20 },
    );
    expect(all.length).toBeGreaterThan(0);

    const types = [...new Set(all.map((e) => e.type))];
    expect(types.length).toBeGreaterThan(0);

    const filtered = await client.getEvents(
      { kind: "account", accountAddress: account, eventTypes: [types[0]] },
      { offset: 0, limit: 20 },
    );
    expect(filtered.length).toBeGreaterThan(0);
    for (const e of filtered) expect(e.type).toBe(types[0]);

    // Deep history is reachable: these are the account's earliest events,
    // far behind the chain head that a module scan is limited to.
    expect(all[0].version).toBeLessThan(
      BigInt((await mainnet().getLedgerInfo()).ledger_version),
    );

    client.destroy();
  }, 60_000);

  it("finds the account in its own events via involvesAddress", async () => {
    const client = new StreamdClient({ aptos: mainnet() });
    const account = "0xd503b95164384a5ebbccbb5c4bdc8b4a5893d9651e9953abda8e1c22fcc1181d";

    // maxPages caps the walk. Without it a predicate this narrow keeps paging
    // until `limit` fills, which for this account means its whole history.
    const unfiltered = await client.getEvents(
      { kind: "account", accountAddress: account },
      { offset: 0, limit: 50, maxPages: 2 },
    );
    const filtered = await client.getEvents(
      { kind: "account", accountAddress: account },
      { offset: 0, limit: 50, maxPages: 2, where: involvesAddress(account) },
    );

    // The predicate is doing real work, not passing everything through.
    expect(filtered.length).toBeLessThan(unfiltered.length);

    // Every surviving event genuinely mentions the account in its payload.
    for (const e of filtered) {
      expect(involvesAddress(account)(e)).toBe(true);
    }

    client.destroy();
  }, 60_000);

  it("maxPages bounds a filtered walk", async () => {
    const client = new StreamdClient({ aptos: mainnet() });
    const account = "0xd503b95164384a5ebbccbb5c4bdc8b4a5893d9651e9953abda8e1c22fcc1181d";

    const started = Date.now();
    const events = await client.getEvents(
      { kind: "account", accountAddress: account },
      {
        offset: 0,
        limit: 1000,
        maxPages: 1,
        // Matches nothing, so an unbounded walk would traverse the whole
        // account history rather than returning.
        where: () => false,
      },
    );

    expect(events).toHaveLength(0);
    expect(Date.now() - started).toBeLessThan(20_000);

    client.destroy();
  }, 60_000);

  it("streams distinct, ascending events without stalling", async () => {
    const client = new StreamdClient({ aptos: mainnet() });
    const received: StreamdEvent[] = [];

    await new Promise<void>((resolve, reject) => {
      const sub = client.on(
        FEE_STATEMENT,
        (event) => {
          received.push(event);
          if (received.length >= 30) {
            sub.unsubscribe();
            resolve();
          }
        },
        {
          pollInterval: 1000,
          batchSize: 10,
          onError: (err) => {
            sub.unsubscribe();
            reject(err);
          },
        },
      );

      setTimeout(() => {
        sub.unsubscribe();
        reject(new Error(`timed out with ${received.length} events`));
      }, 45_000);
    });

    const keys = received.map((e) => `${e.version}:${e.eventIndex}`);
    expect(new Set(keys).size).toBe(keys.length); // no duplicates
    for (let i = 1; i < received.length; i++) {
      expect(received[i].version >= received[i - 1].version).toBe(true);
    }

    client.destroy();
  }, 60_000);
});
