import { describe, expect, it } from "bun:test";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { runTail } from "../src/tail.js";
import type { TailOptions } from "../src/args.js";
import { MemoryAdapter, StreamdClient, StreamdError, StreamdNetworkError } from "@streamd/core";
import type { EventAdapter, StreamdEvent } from "@streamd/core";

const TYPE = "0x1::coin::DepositEvent";

function ev(version: number): StreamdEvent {
  return {
    type: TYPE,
    data: { amount: BigInt(version * 10) },
    version: BigInt(version),
    sequenceNumber: 0n,
    eventIndex: 0,
  };
}

const options = (extra: Partial<TailOptions> = {}): TailOptions => ({
  descriptor: { kind: "module", eventType: TYPE },
  network: "mainnet",
  pollInterval: 1,
  ...extra,
});

function harness(adapter: EventAdapter, signal?: AbortSignal) {
  const lines: string[] = [];
  const errors: string[] = [];
  const client = new StreamdClient({ adapter });
  const io = {
    client,
    write: (line: string) => {
      lines.push(line);
    },
    error: (message: string) => {
      errors.push(message);
    },
    signal,
  };
  return { lines, errors, io, client };
}

describe("runTail", () => {
  it("prints aligned lines with --format pretty, and JSON without it", async () => {
    const pretty = harness(new MemoryAdapter([ev(1)]));
    expect(await runTail(options({ limit: 1, format: "pretty" }), pretty.io)).toBe(0);
    expect(pretty.lines[0]).toStartWith("-");
    expect(pretty.lines[0]).toEndWith('{"amount":"10"}\n');
    const json = harness(new MemoryAdapter([ev(1)]));
    await runTail(options({ limit: 1, format: "json" }), json.io);
    expect(JSON.parse(json.lines[0]!).version).toBe("1");
  });

  it("prints events as JSON lines in order and exits 0 at the limit", async () => {
    const { lines, errors, io } = harness(new MemoryAdapter([ev(2), ev(1), ev(3)]));
    expect(await runTail(options({ limit: 2 }), io)).toBe(0);
    expect(lines).toHaveLength(2);
    expect(lines.every((l) => l.endsWith("\n"))).toBe(true);
    expect(lines.map((l) => JSON.parse(l).version)).toEqual(["1", "2"]);
    expect(JSON.parse(lines[0]!).data).toEqual({ amount: "10" });
    expect(errors).toEqual([]);
  });

  it("passes fromVersion through to the subscription", async () => {
    const { lines, io } = harness(new MemoryAdapter([ev(1), ev(2), ev(3)]));
    expect(await runTail(options({ limit: 1, fromVersion: 3n }), io)).toBe(0);
    expect(JSON.parse(lines[0]!).version).toBe("3");
  });

  it("exits 0 when the signal aborts mid-stream", async () => {
    const controller = new AbortController();
    const { lines, errors, io } = harness(new MemoryAdapter([ev(1)]), controller.signal);
    io.write = (line: string) => {
      lines.push(line);
      setTimeout(() => controller.abort(), 5);
    };
    expect(await runTail(options(), io)).toBe(0);
    expect(lines).toHaveLength(1);
    expect(errors).toEqual([]);
  });

  it("exits 0 with nothing written when the signal is already aborted", async () => {
    const controller = new AbortController();
    controller.abort();
    const { lines, errors, io } = harness(new MemoryAdapter([ev(1)]), controller.signal);
    expect(await runTail(options(), io)).toBe(0);
    expect(lines).toEqual([]);
    expect(errors).toEqual([]);
  });

  it("reports a fatal adapter error once and exits 1", async () => {
    const adapter: EventAdapter = {
      fetchEvents: async () => {
        throw new StreamdError("Not a cursor this adapter issued: x");
      },
    };
    const { lines, errors, io } = harness(adapter);
    expect(await runTail(options(), io)).toBe(1);
    expect(lines).toEqual([]);
    expect(errors).toEqual(["streamd: Polling stopped on invalid input: Not a cursor this adapter issued: x"]);
  });

  it("reports a transient error and keeps printing", async () => {
    const memory = new MemoryAdapter([ev(1)]);
    let calls = 0;
    const adapter: EventAdapter = {
      fetchEvents: async (descriptor, fetchOptions) => {
        if (calls++ === 0) throw new StreamdNetworkError("node 503", 503);
        return memory.fetchEvents(descriptor, fetchOptions);
      },
    };
    const { lines, errors, io } = harness(adapter);
    expect(await runTail(options({ limit: 1 }), io)).toBe(0);
    expect(errors).toEqual(["streamd: Polling failed (1/10): node 503"]);
    expect(lines).toHaveLength(1);
  });

  it("exits 0 when a write fails because the signal aborted", async () => {
    const controller = new AbortController();
    const { errors, io } = harness(new MemoryAdapter([ev(1)]), controller.signal);
    io.write = () => {
      controller.abort();
      return Promise.reject(new Error("write EPIPE"));
    };
    expect(await runTail(options(), io)).toBe(0);
    expect(errors).toEqual([]);
  });

  it("awaits each write before taking the next event", async () => {
    const { lines, io } = harness(new MemoryAdapter([ev(1), ev(2)]));
    let pending = 0;
    let maxPending = 0;
    io.write = async (line: string) => {
      maxPending = Math.max(maxPending, ++pending);
      await new Promise((r) => setTimeout(r, 5));
      lines.push(line);
      pending--;
    };
    expect(await runTail(options({ limit: 2 }), io)).toBe(0);
    expect(lines).toHaveLength(2);
    expect(maxPending).toBe(1);
  });
});

describe("runTail with a stub client", () => {
  type StreamOptions = Parameters<StreamdClient["stream"]>[1];

  function stub(run: (opts: StreamOptions) => AsyncGenerator<StreamdEvent>) {
    const seen: StreamOptions[] = [];
    const errors: string[] = [];
    const io = {
      client: {
        stream: ((_descriptor: unknown, opts: StreamOptions) => {
          seen.push(opts);
          return run(opts);
        }) as unknown as StreamdClient["stream"],
      },
      write: (_line: string): void | Promise<void> => {},
      error: (message: string) => {
        errors.push(message);
      },
    };
    return { seen, errors, io };
  }

  it("does not hold on to errors reported before the latest one", async () => {
    let first: WeakRef<Error> | undefined;
    const reportTwo = (opts: StreamOptions) => {
      const err = new Error("first");
      first = new WeakRef(err);
      opts?.onError?.(err);
      opts?.onError?.(new Error("second"));
    };
    const { errors, io } = stub(async function* (opts) {
      reportTwo(opts);
      yield ev(1);
    });
    let released: boolean | undefined;
    io.write = () => {
      Bun.gc(true);
      released = first!.deref() === undefined;
    };
    expect(await runTail(options({ limit: 1 }), io)).toBe(0);
    expect(errors).toEqual(["streamd: first", "streamd: second"]);
    expect(released).toBe(true);
  });

  it("leaves unset fromVersion and pollInterval out of the stream options", async () => {
    const { seen, io } = stub(async function* () {});
    const unset: TailOptions = { descriptor: { kind: "module", eventType: TYPE }, network: "mainnet" };
    expect(await runTail(unset, io)).toBe(0);
    expect(seen[0]).not.toHaveProperty("fromVersion");
    expect(seen[0]).not.toHaveProperty("pollInterval");
  });

  it("passes fromVersion and pollInterval through unchanged", async () => {
    const { seen, io } = stub(async function* () {});
    expect(await runTail(options({ fromVersion: 7n, pollInterval: 2500 }), io)).toBe(0);
    expect(seen[0]?.fromVersion).toBe(7n);
    expect(seen[0]?.pollInterval).toBe(2500);
  });

  it("reports a write failure with its cause and exits 1 when not aborted", async () => {
    const { errors, io } = stub(async function* () {
      yield ev(1);
    });
    io.write = () => Promise.reject(new Error("write failed", { cause: new Error("EIO") }));
    expect(await runTail(options(), io)).toBe(1);
    expect(errors).toEqual(["streamd: write failed: EIO"]);
  });
});

describe("runTail --from-time", () => {
  it("passes the time through, and leaves it out when unset", () => {
    const seen: unknown[] = [];
    const client = {
      stream: (_d: unknown, listener: unknown) => {
        seen.push(listener);
        return (async function* () {})();
      },
    };
    const io = { client: client as never, write: () => {}, error: () => {} };
    return runTail(options({ fromTime: 123n }), io).then(() =>
      runTail(options(), io).then(() => {
        expect(seen[0]).toMatchObject({ fromTime: 123n });
        expect(seen[1]).not.toHaveProperty("fromTime");
      }),
    );
  });
});

describe("runTail --cursor-file", () => {
  const dir = () => mkdtempSync(join(tmpdir(), "streamd-tail-"));

  /** Runs until `count` lines arrive, then lets the cursor land and aborts. */
  async function until(adapter: EventAdapter, extra: Partial<TailOptions>, count: number) {
    const controller = new AbortController();
    const { lines, errors, io, client } = harness(adapter, controller.signal);
    const running = runTail(options(extra), io);
    for (let i = 0; i < 300 && lines.length < count; i++) await new Promise((r) => setTimeout(r, 2));
    await new Promise((r) => setTimeout(r, 15));
    controller.abort();
    const code = await running;
    client.destroy();
    return { code, lines, errors };
  }
  const versions = (lines: string[]) => lines.map((l) => (JSON.parse(l) as { version: string }).version);

  it("saves its position and resumes past what it printed", async () => {
    const cursorFile = join(dir(), "c");
    const events = [ev(1), ev(2)];
    const first = await until(new MemoryAdapter(events), { fromVersion: 0n, cursorFile }, 2);
    expect(first.code).toBe(0);
    expect(versions(first.lines)).toEqual(["1", "2"]);
    expect(readFileSync(cursorFile, "utf8").trim()).not.toBe("");

    events.push(ev(3));
    const second = await until(new MemoryAdapter(events), { fromVersion: 0n, cursorFile }, 1);
    expect(versions(second.lines)).toEqual(["3"]);
  });

  it("prefers a saved cursor to a start option", async () => {
    const cursorFile = join(dir(), "c");
    const seen: Record<string, unknown>[] = [];
    const client = {
      stream: (_d: unknown, listener: Record<string, unknown>) => {
        seen.push(listener);
        return Object.assign((async function* () {})(), { cursor: undefined });
      },
    };
    writeFileSync(cursorFile, "abc\n");
    await runTail(options({ fromVersion: 5n, cursorFile }), { client: client as never, write: () => {}, error: () => {} });
    expect(seen[0]).toMatchObject({ cursor: "abc" });
  });

  it("writes nothing without the flag", async () => {
    const { code, lines } = await until(new MemoryAdapter([ev(1)]), { fromVersion: 0n }, 1);
    expect(code).toBe(0);
    expect(lines).toHaveLength(1);
  });

  it("exits 1 when the file cannot be read", async () => {
    const path = dir();
    const { lines, errors, io, client } = harness(new MemoryAdapter([ev(1)]));
    expect(await runTail(options({ cursorFile: path }), io)).toBe(1);
    expect(errors[0]).toContain("cannot read cursor file");
    expect(lines).toEqual([]);
    client.destroy();
  });

  it("exits 1 when the file cannot be written", async () => {
    const parent = dir();
    const cursorFile = join(parent, "no-such-dir", "c");
    const { code, errors } = await until(new MemoryAdapter([ev(1), ev(2)]), { fromVersion: 0n, cursorFile }, 1);
    expect(code).toBe(1);
    expect(errors.join()).toContain("cannot write cursor file");
  });
});

describe("runTail filter", () => {
  it("prints only matching events, counting --limit after the filter", async () => {
    const { lines, io } = harness(new MemoryAdapter([ev(1), ev(2), ev(3), ev(4), ev(5)]));
    const filter = { senders: [], functions: [], where: [{ path: "amount", op: ">=" as const, value: "30" }] };
    expect(await runTail(options({ fromVersion: 0n, limit: 2, filter }), io)).toBe(0);
    expect(lines.map((l) => (JSON.parse(l) as { version: string }).version)).toEqual(["3", "4"]);
  });
});

describe("runTail decode", () => {
  it("reports an event that does not decode, skips it and keeps printing", async () => {
    const named = (v: number, amount: unknown): StreamdEvent => ({ ...ev(v), data: { amount } });
    const { lines, errors, io } = harness(new MemoryAdapter([named(1, "10"), named(2, "oops"), named(3, "30")]));
    expect(await runTail(options({ fromVersion: 0n, limit: 2, decode: ["amount"] }), io)).toBe(0);
    expect(lines.map((l) => (JSON.parse(l) as { version: string }).version)).toEqual(["1", "3"]);
    expect(lines[0]).toContain('"data":{"amount":"10"}');
    expect(errors.join()).toContain('Cannot decode field "amount" of 2:0 as an unsigned integer');
  });
});
