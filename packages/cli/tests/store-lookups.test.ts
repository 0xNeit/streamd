import { Database } from "bun:sqlite";
import { afterAll, describe, expect, it } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { AptosClient } from "@streamd/aptos";
import { StreamdAssertionError, StreamdError, StreamdTimeoutError, eventId } from "@streamd/core";
import type { EventAdapter, StreamdEvent } from "@streamd/core";
import { SqliteEventStore, StoreAdapter } from "@streamd/store-sqlite";
import { PostgresEventStore, PostgresStoreAdapter } from "@streamd/store-postgres";
import { SuiClient, networks } from "@streamd/sui";

const HASH = "0x" + "ab".repeat(32);
const UPPER = "0x" + "AB".repeat(32);
const DIGEST = "5QCqxf87tpC2WqD2hvZu1dPnrDa6ukeSztVDtjH3UteV";
const SWAPPED = "5qcQXF87TPc2wQd2HVzU1DpNRdA6UKEsZTvdTJh3uTEv";
const SWAP = "0x1::dex::Swap";
const FEE = "0x1::transaction_fee::FeeStatement";
const SUI_EVENT = "0x2::coin::CoinCreated";

function ev(version: bigint, eventIndex: number, transactionHash: string, type: string): StreamdEvent {
  return { type, data: { amount: String(eventIndex) }, version, sequenceNumber: 0n, eventIndex, transactionHash };
}

const EVENTS = [ev(10n, 1, HASH, FEE), ev(10n, 0, HASH, SWAP), ev(11n, 0, DIGEST, SUI_EVENT)];
const ids = (events: StreamdEvent[]) => events.map(eventId);

async function stores(): Promise<[string, EventAdapter][]> {
  const db = new Database(":memory:");
  new SqliteEventStore(db).write(EVENTS);
  const pg = new PGlite();
  await new PostgresEventStore(pg).write(EVENTS);
  afterAll(async () => {
    db.close();
    await pg.close();
  });
  return [
    ["StoreAdapter", new StoreAdapter(db)],
    ["PostgresStoreAdapter", new PostgresStoreAdapter(pg)],
  ];
}

describe("chain clients over a saved store", async () => {
  for (const [name, adapter] of await stores()) {
    // A node no test reaches: every answer below comes from the store.
    const aptos = new AptosClient({
      aptos: new AptosConfig({ network: Network.MAINNET, clientConfig: { http2: false } }),
      adapter,
    });

    it(`AptosClient.eventsOf and eventsAt answer from ${name}`, async () => {
      expect(ids(await aptos.eventsOf(UPPER))).toEqual(["10:0", "10:1"]);
      expect(ids(await aptos.eventsOf(HASH, { kind: "module", eventType: SWAP }))).toEqual(["10:0"]);
      expect(ids(await aptos.eventsAt(10n, { kind: "module", eventType: FEE }))).toEqual(["10:1"]);
      await expect(aptos.eventsAt(12n)).rejects.toThrow(StreamdError);
      await expect(aptos.eventsOf("0x" + "ee".repeat(32), undefined, { timeout: 250 })).rejects.toBeInstanceOf(StreamdTimeoutError);
    });

    it(`AptosClient.expectEvents asserts over ${name}`, async () => {
      const expectation = aptos.expectEvents(HASH);
      expect(ids(await expectation.toEmit({ kind: "module", eventType: SWAP }))).toEqual(["10:0"]);
      await expectation.notToEmit({ kind: "module", eventType: "0x1::dex::Other" });
      await expect(expectation.notToEmit({ kind: "module", eventType: FEE })).rejects.toBeInstanceOf(StreamdAssertionError);
    });

    it(`SuiClient.eventsOf finds a base58 digest in ${name} exactly as stored`, async () => {
      const sui = new SuiClient({ sui: networks.testnet, adapter });
      expect(ids(await sui.eventsOf(DIGEST))).toEqual(["11:0"]);
      await expect(sui.eventsOf(SWAPPED, undefined, { timeout: 250 })).rejects.toBeInstanceOf(StreamdTimeoutError);
    });
  }
});
