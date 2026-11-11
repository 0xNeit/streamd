import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { createHmac } from "node:crypto";
import { existsSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { runForward } from "../src/forward.js";
import type { ForwardIO } from "../src/forward.js";
import type { ForwardOptions } from "../src/args.js";
import { MemoryAdapter, StreamdClient, StreamdError } from "@streamd/core";
import type { EventAdapter, StreamdEvent } from "@streamd/core";

const TYPE = "0x1::coin::DepositEvent";
const SECRET = "s3cret-token";

function ev(version: number): StreamdEvent {
  return {
    type: TYPE,
    data: { amount: BigInt(version * 10) },
    version: BigInt(version),
    sequenceNumber: 0n,
    eventIndex: 0,
  };
}

interface Received {
  body: unknown;
  raw: string;
  headers: Headers;
}

type Handler = (received: Received, n: number) => Response | Promise<Response>;

let server: ReturnType<typeof Bun.serve>;
let handler: Handler;
let received: Received[];
let dir: string;

beforeEach(() => {
  received = [];
  handler = () => new Response("ok");
  server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch: async (req) => {
      const raw = await req.text();
      const entry = { body: JSON.parse(raw) as unknown, raw, headers: req.headers };
      received.push(entry);
      return handler(entry, received.length);
    },
  });
  dir = mkdtempSync(join(tmpdir(), "streamd-forward-"));
});

afterEach(() => {
  server.stop(true);
  rmSync(dir, { recursive: true, force: true });
});

const cursorFile = () => join(dir, "cursor");

const options = (extra: Partial<ForwardOptions> = {}): ForwardOptions => ({
  descriptor: { kind: "module", eventType: TYPE },
  network: "mainnet",
  pollInterval: 1,
  to: `http://127.0.0.1:${server.port}/hook?token=${SECRET}`,
  cursorFile: cursorFile(),
  retries: 3,
  timeout: 2000,
  headers: {},
  ...extra,
});

function harness(adapter: EventAdapter, headVersion = () => adapter.getHeadVersion!({ kind: "module", eventType: TYPE })) {
  const controller = new AbortController();
  const errors: string[] = [];
  const io: ForwardIO = {
    client: new StreamdClient({ adapter }),
    error: (m) => void errors.push(m),
    signal: controller.signal,
    retryDelay: 1,
    headVersion,
  };
  return { io, errors, controller };
}

async function until(condition: () => boolean, ms = 2000): Promise<void> {
  const deadline = Date.now() + ms;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error("timed out waiting");
    await new Promise((r) => setTimeout(r, 2));
  }
}

const versions = (r: Received) => (r.body as { version: string }[]).map((e) => e.version);

describe("runForward signing", () => {
  const signature = (raw: string, secret: string) =>
    `sha256=${createHmac("sha256", secret).update(raw, "utf8").digest("hex")}`;

  async function forward(events: StreamdEvent[], extra: Partial<ForwardOptions>, done: () => boolean) {
    const h = harness(new MemoryAdapter(events));
    const run = runForward(options(extra), h.io);
    await until(done);
    h.controller.abort();
    await run;
    return h;
  }

  it("signs the exact bytes of the body, non-ASCII included", async () => {
    const event = { ...ev(1), data: { memo: "caf\u00e9 \ud83d\ude80" } };
    await forward([event], { secret: "key" }, () => existsSync(cursorFile()));
    const { raw, headers } = received[0]!;
    expect(raw).toContain("caf\u00e9");
    expect(headers.get("x-streamd-signature")).toBe(signature(raw, "key"));
    expect(headers.get("x-streamd-signature")).toMatch(/^sha256=[0-9a-f]{64}$/);
  });

  it("sends the same signature on a retry", async () => {
    handler = (_r, n) => new Response("", { status: n === 1 ? 500 : 200 });
    await forward([ev(1)], { secret: "key" }, () => existsSync(cursorFile()));
    expect(received).toHaveLength(2);
    expect(received[1]!.headers.get("x-streamd-signature")).toBe(received[0]!.headers.get("x-streamd-signature"));
    expect(received[1]!.headers.get("x-streamd-signature")).toBe(signature(received[1]!.raw, "key"));
  });

  it("sends no signature without a secret", async () => {
    await forward([ev(1)], {}, () => existsSync(cursorFile()));
    expect(received[0]!.headers.has("x-streamd-signature")).toBe(false);
  });

  it("keeps the other headers alongside the signature", async () => {
    await forward([ev(1)], { secret: "key", headers: { authorization: "Bearer t" } }, () => existsSync(cursorFile()));
    expect(received[0]!.headers.get("authorization")).toBe("Bearer t");
    expect(received[0]!.headers.get("x-streamd-signature")).toBeTruthy();
  });

  it("never prints the secret", async () => {
    handler = () => new Response("", { status: 400 });
    const h = harness(new MemoryAdapter([ev(1)]));
    expect(await runForward(options({ secret: SECRET, to: `http://127.0.0.1:${server.port}/hook` }), h.io)).toBe(1);
    expect(h.errors.length).toBeGreaterThan(0);
    expect(h.errors.join("\n")).not.toContain(SECRET);
  });
});

describe("runForward", () => {
  it("POSTs a batch as a JSON array and checkpoints after the ack", async () => {
    const { io, errors, controller } = harness(new MemoryAdapter([ev(1), ev(2)]));
    const run = runForward(options({ headers: { Authorization: `Bearer ${SECRET}` } }), io);
    await until(() => existsSync(cursorFile()));
    controller.abort();
    expect(await run).toBe(0);

    expect(received).toHaveLength(1);
    expect(received[0]!.body).toEqual([
      { type: TYPE, data: { amount: "10" }, version: "1", sequenceNumber: "0", eventIndex: 0 },
      { type: TYPE, data: { amount: "20" }, version: "2", sequenceNumber: "0", eventIndex: 0 },
    ]);
    expect(received[0]!.headers.get("content-type")).toBe("application/json");
    expect(received[0]!.headers.get("authorization")).toBe(`Bearer ${SECRET}`);
    expect(readFileSync(cursorFile(), "utf8")).toBe("2\n");
    expect(errors).toEqual([]);
  });

  it("resumes from the cursor file without redelivering", async () => {
    writeFileSync(cursorFile(), "2\n");
    const { io, controller } = harness(new MemoryAdapter([ev(1), ev(2), ev(3)]));
    const run = runForward(options({ fromVersion: 1n }), io);
    await until(() => readFileSync(cursorFile(), "utf8") === "3\n");
    controller.abort();
    expect(await run).toBe(0);
    expect(received.map(versions)).toEqual([["3"]]);
  });

  it("retries a 500 with backoff, then delivers", async () => {
    handler = (_r, n) => new Response("", { status: n < 3 ? 500 : 200 });
    const { io, errors, controller } = harness(new MemoryAdapter([ev(1)]));
    const run = runForward(options(), io);
    await until(() => existsSync(cursorFile()));
    controller.abort();
    expect(await run).toBe(0);
    expect(received).toHaveLength(3);
    expect(errors).toEqual([
      "streamd: delivery failed (attempt 1/4): HTTP 500; retrying in 1ms",
      "streamd: delivery failed (attempt 2/4): HTTP 500; retrying in 2ms",
    ]);
  });

  it("waits the Retry-After time on a 429 instead of the backoff", async () => {
    handler = (_r, n) =>
      new Response("", n === 1 ? { status: 429, headers: { "retry-after": "0" } } : {});
    const { io, errors, controller } = harness(new MemoryAdapter([ev(1)]));
    io.retryDelay = 60_000;
    const run = runForward(options(), io);
    await until(() => existsSync(cursorFile()));
    controller.abort();
    expect(await run).toBe(0);
    expect(errors).toEqual(["streamd: delivery failed (attempt 1/4): HTTP 429; retrying in 0ms"]);
  });

  it("exits 1 past the retry bound, leaving no cursor", async () => {
    handler = () => new Response("", { status: 503 });
    const { io, errors } = harness(new MemoryAdapter([ev(1)]));
    expect(await runForward(options({ retries: 1 }), io)).toBe(1);
    expect(received).toHaveLength(2);
    expect(errors.at(-1)).toBe("streamd: delivery failed: HTTP 503");
    expect(existsSync(cursorFile())).toBe(false);
  });

  it("keeps the last acknowledged cursor when a later batch fails", async () => {
    const pages = [
      { events: [ev(1)], nextCursor: "a", hasMore: true },
      { events: [ev(2)], nextCursor: "b", hasMore: false },
    ];
    const adapter: EventAdapter = {
      fetchEvents: async (_d, { cursor }) => pages[cursor === "a" ? 1 : 0]!,
    };
    handler = (_r, n) => new Response("", { status: n === 1 ? 200 : 400 });
    const { io, errors } = harness(adapter);
    expect(await runForward(options(), io)).toBe(1);
    expect(received.map(versions)).toEqual([["1"], ["2"]]);
    expect(errors).toEqual(["streamd: delivery failed: HTTP 400"]);
    expect(readFileSync(cursorFile(), "utf8")).toBe("a\n");
  });

  it("exits 1 at once on a 400 for the first batch, leaving no cursor", async () => {
    handler = () => new Response("", { status: 400 });
    const { io, errors } = harness(new MemoryAdapter([ev(1)]));
    expect(await runForward(options(), io)).toBe(1);
    expect(received).toHaveLength(1);
    expect(errors).toEqual(["streamd: delivery failed: HTTP 400"]);
    expect(existsSync(cursorFile())).toBe(false);
  });

  it("aborts during a retry wait without advancing", async () => {
    handler = () => new Response("", { status: 500 });
    const { io, errors, controller } = harness(new MemoryAdapter([ev(1)]));
    io.retryDelay = 60_000;
    const run = runForward(options(), io);
    await until(() => errors.length === 1);
    controller.abort();
    expect(await run).toBe(0);
    expect(received).toHaveLength(1);
    expect(existsSync(cursorFile())).toBe(false);
  });

  it("exits 1 when the cursor file cannot be written", async () => {
    const { io, errors } = harness(new MemoryAdapter([ev(1)]));
    const code = await runForward(options({ cursorFile: join(dir, "missing", "cursor") }), io);
    expect(code).toBe(1);
    expect(received).toHaveLength(1);
    expect(errors[0]).toMatch(/^streamd: cannot write cursor file:/);
  });

  it("refuses to write through a link planted at the temp name", async () => {
    const target = join(dir, "target");
    writeFileSync(target, "keep");
    symlinkSync(target, `${cursorFile()}.${process.pid}.tmp`);
    const { io, controller } = harness(new MemoryAdapter([ev(1)]));
    const run = runForward(options(), io);
    await until(() => existsSync(cursorFile()));
    controller.abort();
    expect(await run).toBe(0);
    expect(readFileSync(target, "utf8")).toBe("keep");
    expect(readFileSync(cursorFile(), "utf8")).toBe("1\n");
  });

  it("never prints the URL on a connection error", async () => {
    const { io, errors } = harness(new MemoryAdapter([ev(1)]));
    const code = await runForward(options({ to: `http://127.0.0.1:1/?token=${SECRET}`, retries: 0 }), io);
    expect(code).toBe(1);
    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatch(/^streamd: delivery failed: /);
    expect(errors.join("\n")).not.toContain(SECRET);
  });

  it("stops without advancing when the batch callback throws", async () => {
    writeFileSync(cursorFile(), "0\n");
    handler = () => new Response("", { status: 500 });
    const { io } = harness(new MemoryAdapter([ev(1)]));
    let reports = 0;
    io.error = () => {
      reports++;
      throw new Error("stderr closed");
    };
    expect(await runForward(options(), io)).toBe(1);
    expect(received).toHaveLength(1);
    expect(reports).toBeGreaterThan(0);
    expect(readFileSync(cursorFile(), "utf8")).toBe("0\n");
  });

  it("sends a given content-type instead of the default", async () => {
    const { io, controller } = harness(new MemoryAdapter([ev(1)]));
    const run = runForward(options({ headers: { "content-type": "text/plain" } }), io);
    await until(() => existsSync(cursorFile()));
    controller.abort();
    expect(await run).toBe(0);
    expect(received[0]!.headers.get("content-type")).toBe("text/plain");
  });

  it("does not follow redirects", async () => {
    handler = () => new Response("", { status: 307, headers: { location: "http://127.0.0.1:1/" } });
    const { io, errors } = harness(new MemoryAdapter([ev(1)]));
    expect(await runForward(options(), io)).toBe(1);
    expect(received).toHaveLength(1);
    expect(errors).toEqual(["streamd: delivery failed: HTTP 307"]);
  });

  it("times out a request and never prints the URL or headers", async () => {
    handler = () => new Promise(() => {});
    const { io, errors } = harness(new MemoryAdapter([ev(1)]));
    const code = await runForward(
      options({ timeout: 20, retries: 0, headers: { Authorization: SECRET } }),
      io,
    );
    expect(code).toBe(1);
    expect(errors).toEqual(["streamd: delivery failed: request timed out after 20ms"]);
    expect(errors.join("\n")).not.toContain(SECRET);
  });

  it("abandons an in-flight POST on abort without advancing", async () => {
    writeFileSync(cursorFile(), "0\n");
    handler = () => new Promise(() => {});
    const { io, errors, controller } = harness(new MemoryAdapter([ev(1)]));
    const run = runForward(options(), io);
    await until(() => received.length === 1);
    controller.abort();
    expect(await run).toBe(0);
    expect(readFileSync(cursorFile(), "utf8")).toBe("0\n");
    expect(errors).toEqual([]);
  });

  it("exits 1 when polling stops on an error", async () => {
    const adapter: EventAdapter = {
      fetchEvents: async () => {
        throw new StreamdError("Not a cursor this adapter issued: x");
      },
    };
    const { io, errors } = harness(adapter);
    expect(await runForward(options(), io)).toBe(1);
    expect(errors).toEqual([
      "streamd: Polling stopped on invalid input: Not a cursor this adapter issued: x",
    ]);
    expect(received).toEqual([]);
  });

  it("exits 1 on an empty cursor file", async () => {
    writeFileSync(cursorFile(), "\n");
    const { io, errors } = harness(new MemoryAdapter([ev(1)]));
    expect(await runForward(options(), io)).toBe(1);
    expect(errors).toEqual([`streamd: cannot read cursor file: cursor file is empty: ${cursorFile()}`]);
  });

  it("exits 0 at once when the signal is already aborted", async () => {
    const { io, controller } = harness(new MemoryAdapter([ev(1)]));
    controller.abort();
    expect(await runForward(options(), io)).toBe(0);
    expect(received).toEqual([]);
  });
});

describe("runForward --from-time", () => {
  it("passes the time through with any saved cursor, which the poller prefers", async () => {
    const dir = mkdtempSync(join(tmpdir(), "streamd-forward-time-"));
    const seen: Record<string, unknown>[] = [];
    const client = {
      onBatch: (_d: unknown, _cb: unknown, listener: Record<string, unknown>) => {
        seen.push(listener);
        return { unsubscribe() {}, active: true, cursor: undefined };
      },
    };
    const run = async (cursorFile: string) => {
      const controller = new AbortController();
      const running = runForward(
        {
          descriptor: { kind: "module", eventType: ["0x1::m::E"] },
          network: "mainnet",
          fromTime: 123n,
          to: "https://h.example",
          cursorFile,
          retries: 0,
          timeout: 1000,
          headers: {},
        },
        { client: client as never, error: () => {}, signal: controller.signal, headVersion: async () => 0n },
      );
      controller.abort();
      await running;
    };
    await run(join(dir, "none.cursor"));
    expect(seen[0]).toMatchObject({ fromTime: 123n });
    expect(seen[0]).not.toHaveProperty("cursor");
    const saved = join(dir, "saved.cursor");
    writeFileSync(saved, "abc");
    await run(saved);
    expect(seen[1]).toMatchObject({ cursor: "abc" });
  });
});

describe("runForward --to-version and --batch-size", () => {
  const range = (...vs: number[]) => new MemoryAdapter(vs.map(ev));
  const bounded = (toVersion: bigint, batchSize: number) => options({ fromVersion: 0n, toVersion, batchSize });

  it("POSTs at most batch-size events, then exits 0 on catching up with the cursor saved", async () => {
    const { io, errors } = harness(range(1, 2, 3, 4, 5));
    expect(await runForward(bounded(5n, 2), io)).toBe(0);
    expect(received.map(versions)).toEqual([["1", "2"], ["3", "4"], ["5"]]);
    expect(readFileSync(cursorFile(), "utf8")).toBe("5\n");
    expect(errors).toEqual([]);
  });

  it("never sends past the bound, and leaves the cursor before the page that crossed it", async () => {
    const { io } = harness(range(1, 2, 3, 4, 5));
    expect(await runForward(bounded(3n, 2), io)).toBe(0);
    expect(received.map(versions)).toEqual([["1", "2"], ["3"]]);
    expect(readFileSync(cursorFile(), "utf8")).toBe("2\n");
  });

  it("ends without a POST on a page wholly past the bound", async () => {
    const { io } = harness(range(1, 2, 10, 11));
    expect(await runForward(bounded(5n, 2), io)).toBe(0);
    expect(received.map(versions)).toEqual([["1", "2"]]);
    expect(readFileSync(cursorFile(), "utf8")).toBe("2\n");
  });

  // A scan returns whole transactions, so a page can hold more than the limit asked for.
  const overflowing = (...vs: number[]): EventAdapter => {
    const memory = range(...vs);
    return {
      fetchEvents: async (_d, o) =>
        o.cursor === undefined
          ? { events: vs.map(ev), nextCursor: String(vs.at(-1)), hasMore: false }
          : { events: [], nextCursor: o.cursor, hasMore: false },
      getHeadVersion: () => memory.getHeadVersion(),
    };
  };

  it("splits a page larger than batch-size into several POSTs", async () => {
    const { io } = harness(overflowing(1, 2, 3, 4, 5));
    expect(await runForward(bounded(5n, 2), io)).toBe(0);
    expect(received.map(versions)).toEqual([["1", "2"], ["3", "4"], ["5"]]);
    expect(readFileSync(cursorFile(), "utf8")).toBe("5\n");
  });

  it("keeps the cursor before a split page when a later POST of it fails", async () => {
    handler = (_r, n) => new Response("", { status: n === 2 ? 400 : 200 });
    const { io } = harness(overflowing(1, 2, 3, 4, 5));
    expect(await runForward(bounded(5n, 2), io)).toBe(1);
    expect(received.map(versions)).toEqual([["1", "2"], ["3", "4"]]);
    expect(existsSync(cursorFile())).toBe(false);
  });

  it("resumes a bounded run from the cursor file", async () => {
    writeFileSync(cursorFile(), "2\n");
    const { io } = harness(range(1, 2, 3, 4));
    expect(await runForward(bounded(3n, 10), io)).toBe(0);
    expect(received.map(versions)).toEqual([["3"]]);
  });

  it("refuses a bound above the head, and a failed head read, before subscribing", async () => {
    const adapter = range(1, 2);
    const calls: string[] = [];
    const watched: EventAdapter = { ...adapter, fetchEvents: (d, o) => (calls.push("fetch"), adapter.fetchEvents(d, o)) };
    const above = harness(watched, async () => 2n);
    expect(await runForward(bounded(3n, 10), above.io)).toBe(1);
    expect(above.errors).toEqual(["streamd: --to-version 3 is above the head version 2"]);
    const failing = harness(watched, async () => {
      throw new StreamdError("node down");
    });
    expect(await runForward(bounded(1n, 10), failing.io)).toBe(1);
    expect(failing.errors).toEqual(["streamd: cannot read the head version: node down"]);
    expect(calls).toEqual([]);
    expect(received).toEqual([]);
  });

  it("does not read the head without a bound", async () => {
    let reads = 0;
    const { io, controller } = harness(range(1), async () => (reads++, 0n));
    const run = runForward(options({ fromVersion: 0n }), io);
    await until(() => existsSync(cursorFile()));
    controller.abort();
    expect(await run).toBe(0);
    expect(reads).toBe(0);
  });
});
