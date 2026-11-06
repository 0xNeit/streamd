import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { existsSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { runForward } from "../../src/cli/forward.js";
import type { ForwardIO } from "../../src/cli/forward.js";
import type { ForwardOptions } from "../../src/cli/args.js";
import { StreamdClient } from "../../src/client.js";
import { MemoryAdapter } from "../../src/adapters/memory.js";
import { StreamdError } from "../../src/errors.js";
import type { EventAdapter, StreamdEvent } from "../../src/types.js";

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
      const entry = { body: await req.json(), headers: req.headers };
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

function harness(adapter: EventAdapter) {
  const controller = new AbortController();
  const errors: string[] = [];
  const io: ForwardIO = {
    client: new StreamdClient({ adapter }),
    error: (m) => void errors.push(m),
    signal: controller.signal,
    retryDelay: 1,
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
