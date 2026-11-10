import { describe, expect, it } from "bun:test";
import { StreamdError, StreamdNetworkError, StreamdTimeoutError, StreamdTransactionError } from "@streamd/core";
import type { StreamdEvent } from "@streamd/core";
import type { TxOptions } from "../src/args.js";
import { runTx } from "../src/tx.js";

const HASH = `0x${"ab".repeat(32)}`;
const options: TxOptions = { hash: HASH, network: "mainnet" };

function ev(index: number): StreamdEvent {
  return { type: "0x1::m::E", data: { n: "1" }, version: 9n, sequenceNumber: 0n, eventIndex: index };
}

async function run(eventsOf: () => Promise<StreamdEvent[]>, opts = options, signal = new AbortController().signal) {
  const lines: string[] = [];
  const errors: string[] = [];
  const client = { eventsOf: eventsOf as never };
  const code = await runTx(opts, { client, write: (l) => void lines.push(l), error: (m) => errors.push(m), signal });
  return { code, lines, errors };
}

describe("runTx", () => {
  it("prints every event as a JSON line and exits 0", async () => {
    const { code, lines, errors } = await run(async () => [ev(0), ev(1)]);
    expect(code).toBe(0);
    expect(errors).toEqual([]);
    expect(lines.map((l) => (JSON.parse(l) as { eventIndex: number }).eventIndex)).toEqual([0, 1]);
  });

  it("exits 0 for a successful transaction with no events", async () => {
    const { code, lines } = await run(async () => []);
    expect(code).toBe(0);
    expect(lines).toEqual([]);
  });

  it("passes the hash and timeout, and leaves an unset timeout out", async () => {
    const calls: unknown[][] = [];
    const client = { eventsOf: (async (...a: unknown[]) => (calls.push(a), [])) as never };
    const io = { client, write: () => {}, error: () => {}, signal: new AbortController().signal };
    await runTx({ ...options, timeout: 500 }, io);
    await runTx(options, io);
    expect(calls[0]![0]).toBe(HASH);
    expect(calls[0]![2]).toMatchObject({ timeout: 500 });
    expect(calls[1]![2]).not.toHaveProperty("timeout");
  });

  it("reports a failed transaction's status, printing nothing", async () => {
    const { code, lines, errors } = await run(async () => {
      throw new StreamdTransactionError("failed", HASH, 9n, "Move abort in 0x1::coin: EINSUFFICIENT_BALANCE");
    });
    expect(code).toBe(1);
    expect(lines).toEqual([]);
    expect(errors).toEqual(["streamd: transaction failed at version 9: Move abort in 0x1::coin: EINSUFFICIENT_BALANCE"]);
  });

  it("exits 1 when it is not found in time, or the node fails", async () => {
    const late = await run(async () => {
      throw new StreamdTimeoutError("not committed within 500ms", 500);
    });
    expect(late.code).toBe(1);
    expect(late.errors[0]).toContain("not committed");
    const down = await run(async () => {
      throw new StreamdNetworkError("fullnode down", 503);
    });
    expect(down.code).toBe(1);
    expect(down.errors).toEqual(["streamd: fullnode down"]);
  });

  it("exits 130, quietly, when interrupted while waiting", async () => {
    const controller = new AbortController();
    const { code, errors } = await run(
      async () => {
        controller.abort();
        throw new StreamdError("stopped");
      },
      options,
      controller.signal,
    );
    expect(code).toBe(130);
    expect(errors).toEqual([]);
  });

  it("exits 1 on a failed write, and 0 if the output simply closed", async () => {
    const client = { eventsOf: (async () => [ev(0)]) as never };
    const errors: string[] = [];
    const failing = { client, error: (m: string) => errors.push(m), signal: new AbortController().signal };
    expect(await runTx(options, { ...failing, write: () => Promise.reject(new Error("disk full")) })).toBe(1);
    expect(errors).toEqual(["streamd: disk full"]);
    const controller = new AbortController();
    const closed = await runTx(options, {
      client,
      error: () => {},
      signal: controller.signal,
      write: () => {
        controller.abort();
        throw new Error("EPIPE");
      },
    });
    expect(closed).toBe(0);
  });
});
