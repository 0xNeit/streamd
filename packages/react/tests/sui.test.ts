import "./dom.js";
import { describe, expect, it } from "bun:test";
import { createElement } from "react";
import type { SuiGrpcClient } from "@mysten/sui/grpc";
import { MemoryAdapter, StreamdClient, StreamdError, StreamdTransactionError, type MoveEventType, type Transfer } from "@streamd/core";
import { SuiClient } from "@streamd/sui";
import { LONG, digest, fakeLedger, withTransfers, type FakeListed } from "../../sui/tests/helpers.js";
import { StreamdProvider, useTransactionEvents, useTransfers } from "../src/index.js";
import type { UseEventsResult, UseTransactionEventsResult } from "../src/index.js";
import { render, unmount, waitFor } from "./render.js";

const SUI = `${LONG("2")}::sui::SUI`;
const [ALICE, BOB] = [LONG("a11ce"), LONG("b0b")];
const STAKE = "0x3::validator::StakingRequestEvent" as MoveEventType;
const [OK, FAILED] = [digest(1), digest(2)];

/** A `SuiClient` over the fake ledger: two SUI payments, and two looked-up transactions. */
function suiClient(): SuiClient {
  const sizes = [1, 2];
  const listed: FakeListed[] = [
    { cp: 1n, ti: 0n, changes: [[ALICE, SUI, -10n], [BOB, SUI, 10n]] },
    { cp: 1n, ti: 1n, changes: [[BOB, SUI, -4n], [ALICE, SUI, 4n]] },
  ];
  const ledger = fakeLedger(sizes, [], undefined, 0n, [
    { digest: OK, cp: 1n, ti: 0n, events: [{ type: STAKE, sender: "0xa11ce", amount: "5" }] },
    { digest: FAILED, cp: 1n, ti: 1n, error: "MoveAbort(m::z, 101) in command 1", events: [] },
  ]);
  const wired = withTransfers(ledger.client, sizes, listed);
  return new SuiClient({ sui: wired.client as unknown as SuiGrpcClient, defaults: { pollInterval: 5 } });
}

describe("hooks over a SuiClient", () => {
  it("useTransfers follows Sui transfer records", async () => {
    const client = suiClient();
    const results: UseEventsResult<Transfer>[] = [];
    function Feed() {
      results.push(useTransfers({ fromVersion: 0n }));
      return null;
    }
    const root = await render(createElement(StreamdProvider, { client }, createElement(Feed)));
    await waitFor(() => expect(results.at(-1)!.events).toHaveLength(2));
    expect(results.at(-1)!.events.map((e) => e.data)).toEqual([
      { asset: SUI, amount: 10n, from: ALICE, to: BOB },
      { asset: SUI, amount: 4n, from: BOB, to: ALICE },
    ]);
    expect(results.at(-1)!.status).toBe("live");
    await unmount(root);
    client.destroy();
  });

  it("useTransactionEvents returns a Sui transaction's events, and a failed one's status", async () => {
    const client = suiClient();
    const results: UseTransactionEventsResult<unknown>[] = [];
    function Tx({ hash }: { hash: string }) {
      results.push(useTransactionEvents(hash, { client }));
      return null;
    }
    const root = await render(createElement(Tx, { hash: OK }));
    await waitFor(() => expect(results.at(-1)!.loading).toBe(false));
    expect(results.at(-1)!.data!.map((e) => [e.version, e.transactionHash, e.sender])).toEqual([[1n, OK, ALICE]]);
    await unmount(root);

    const failed = await render(createElement(Tx, { hash: FAILED }));
    await waitFor(() => expect(results.at(-1)!.loading).toBe(false));
    expect(results.at(-1)!.error).toBeInstanceOf(StreamdTransactionError);
    expect(results.at(-1)!.vmStatus).toBe("MoveAbort(m::z, 101) in command 1");
    await unmount(failed);
    client.destroy();
  });

  it("refuses a client without the method, naming both chains' clients", async () => {
    const plain = new StreamdClient({ adapter: new MemoryAdapter() });
    const errors: unknown[] = [];
    function Probe() {
      for (const hook of [() => useTransfers({}), () => useTransactionEvents(OK)]) {
        try {
          hook();
        } catch (err) {
          errors.push(err);
        }
      }
      return null;
    }
    const root = await render(createElement(StreamdProvider, { client: plain }, createElement(Probe)));
    expect(errors.length).toBeGreaterThanOrEqual(2);
    expect(errors[0]).toBeInstanceOf(StreamdError);
    expect((errors[0] as Error).message).toBe("This hook needs a client with transfers, such as an AptosClient or a SuiClient");
    expect((errors[1] as Error).message).toBe("This hook needs a client with eventsOf, such as an AptosClient or a SuiClient");
    await unmount(root);
  });
});
