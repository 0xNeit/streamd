import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { MemoryAdapter, StreamdClient, verifyWebhook } from "@streamd/core";
import type { StreamdEvent } from "@streamd/core";
import { runAlert } from "../src/alert.js";
import type { AlertIO } from "../src/alert.js";
import { parseRules } from "../src/alert-rules.js";
import type { AlertOptions } from "../src/args.js";

const TYPE = "0x1::transaction_fee::FeeStatement";
const MINUTE = 60_000_000n;
/** A minute boundary, so window starts are easy to read. */
const T0 = 29_386_000n * MINUTE;

function ev(version: number, timestamp: bigint | undefined, amount: unknown = "1"): StreamdEvent {
  return {
    type: TYPE,
    data: { amount },
    version: BigInt(version),
    sequenceNumber: 0n,
    eventIndex: 0,
    ...(timestamp !== undefined && { timestamp }),
  };
}

interface Alert {
  id: string;
  rule: string;
  windowStart: string;
  windowEnd: string;
  threshold: Record<string, string>;
  value: string;
  event: { version: string };
}

interface Received {
  body: Alert;
  raw: string;
  headers: Headers;
}

let server: ReturnType<typeof Bun.serve>;
let handler: (n: number) => Response;
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
      received.push({ body: JSON.parse(raw) as Alert, raw, headers: req.headers });
      return handler(received.length);
    },
  });
  dir = mkdtempSync(join(tmpdir(), "streamd-alert-"));
});

afterEach(() => {
  server.stop(true);
  rmSync(dir, { recursive: true, force: true });
});

const cursorFile = () => join(dir, "cursor");
const state = () => JSON.parse(readFileSync(cursorFile(), "utf8")) as Record<string, { cursor: string; window?: Record<string, unknown> }>;

const options = (extra: Partial<AlertOptions> = {}): AlertOptions => ({
  network: "mainnet",
  rules: "rules.json",
  to: `http://127.0.0.1:${server.port}/hook`,
  cursorFile: cursorFile(),
  retries: 3,
  timeout: 2000,
  headers: {},
  pollInterval: 1,
  fromVersion: 0n,
  ...extra,
});

const rule = (name: string, threshold: unknown, extra: Record<string, unknown> = {}) => ({
  name,
  descriptor: { eventType: TYPE },
  window: "1m",
  threshold,
  ...extra,
});
const rules = (...list: unknown[]) => parseRules(JSON.stringify({ rules: list }), "rules.json");

async function until(condition: () => boolean, ms = 3000): Promise<void> {
  const deadline = Date.now() + ms;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error("timed out waiting");
    await new Promise((r) => setTimeout(r, 2));
  }
}

function start(adapter: MemoryAdapter, list: ReturnType<typeof rules>, extra: Partial<AlertOptions> = {}) {
  const controller = new AbortController();
  const errors: string[] = [];
  const io: AlertIO = {
    client: new StreamdClient({ adapter }),
    error: (m) => void errors.push(m),
    signal: controller.signal,
    retryDelay: 1,
  };
  const run = runAlert(options(extra), list, io);
  const stop = async () => {
    controller.abort();
    return run;
  };
  return { run, stop, errors };
}

/** Runs until the cursor file holds `rule` at `version`'s page, then stops. */
async function runTo(adapter: MemoryAdapter, list: ReturnType<typeof rules>, done: () => boolean, extra: Partial<AlertOptions> = {}) {
  const h = start(adapter, list, extra);
  await until(done);
  expect(await h.stop()).toBe(0);
  return h;
}

const caughtUp = (name: string, value: string) => () =>
  existsSync(cursorFile()) && state()[name]?.window?.value === value;
const versions = () => received.map((r) => r.body.event.version);

describe("runAlert thresholds", () => {
  it("fires a count rule once per window, at the crossing event", async () => {
    const adapter = new MemoryAdapter([
      ev(1, T0 + 1n),
      ev(2, T0 + 2n),
      ev(3, T0 + 3n),
      ev(4, T0 + 4n),
      ev(5, T0 + MINUTE),
      ev(6, T0 + MINUTE + 5n),
      ev(7, T0 + 3n * MINUTE),
    ]);
    await runTo(adapter, rules(rule("burst", { count: 2 })), caughtUp("burst", "1"));
    expect(versions()).toEqual(["2", "6"]);
    expect(received.map((r) => r.body.windowStart)).toEqual([`${T0}`, `${T0 + MINUTE}`]);
    expect(received.map((r) => r.body.value)).toEqual(["2", "2"]);
    expect(state().burst!.window).toEqual({ start: `${T0 + 3n * MINUTE}`, value: "1", fired: false });
  });

  it("sends nothing below the threshold", async () => {
    const adapter = new MemoryAdapter([ev(1, T0), ev(2, T0 + MINUTE), ev(3, T0 + 2n * MINUTE)]);
    await runTo(adapter, rules(rule("burst", { count: 2 })), () => existsSync(cursorFile()) && state().burst!.window?.start === `${T0 + 2n * MINUTE}`);
    expect(received).toHaveLength(0);
  });

  it("sums at full precision past 2^64, and each rule on one descriptor alerts", async () => {
    const max = "18446744073709551615";
    const adapter = new MemoryAdapter([ev(1, T0, max), ev(2, T0 + 1n, max), ev(3, T0 + 2n, max)]);
    const list = rules(
      rule("exact", { sum: "amount", gte: "36893488147419103230" }),
      rule("above", { sum: "amount", gte: "36893488147419103231" }),
      rule("count", { count: 1 }),
    );
    await runTo(adapter, list, () => received.length === 3 && caughtUp("exact", "55340232221128654845")());
    const by = (name: string) => received.find((r) => r.body.rule === name)!.body;
    expect(by("exact")).toMatchObject({ value: "36893488147419103230", event: { version: "2" } });
    expect(by("above")).toMatchObject({ value: "55340232221128654845", event: { version: "3" } });
    expect(by("count")).toMatchObject({ value: "1", event: { version: "1" } });
  });

  it("counts only what the rule's where matches", async () => {
    const adapter = new MemoryAdapter([ev(1, T0, "5"), ev(2, T0 + 1n, "50"), ev(3, T0 + 2n, "60")]);
    await runTo(adapter, rules(rule("big", { count: 2 }, { where: ["amount>=10"] })), () => received.length === 1);
    expect(versions()).toEqual(["3"]);
  });

  it("reports and skips an event whose summed field is not an integer", async () => {
    const adapter = new MemoryAdapter([ev(1, T0, "7"), ev(2, T0 + 1n, "abc"), ev(3, T0 + 2n, "3")]);
    const h = await runTo(adapter, rules(rule("total", { sum: "amount", gte: "10" })), () => received.length === 1);
    expect(received[0]!.body).toMatchObject({ value: "10", event: { version: "3" } });
    expect(h.errors).toEqual(['streamd: rule "total": sum: field "amount" is not an integer; skipping the event at version 2']);
  });

  it("stops with exit 1 on an event with no timestamp", async () => {
    const h = start(new MemoryAdapter([ev(1, T0), ev(2, undefined)]), rules(rule("burst", { count: 5 })));
    expect(await h.run).toBe(1);
    expect(h.errors).toEqual(['streamd: rule "burst": the event at version 2 has no timestamp, so no window can be placed']);
    expect(existsSync(cursorFile())).toBe(false);
  });
});

describe("runAlert delivery", () => {
  it("POSTs the alert as signed JSON", async () => {
    const adapter = new MemoryAdapter([ev(1, T0 + 7n, "4")]);
    await runTo(adapter, rules(rule("one", { sum: "amount", gte: "4" })), () => existsSync(cursorFile()), { secret: "key" });
    const [{ raw, headers, body }] = received as [Received];
    expect(headers.get("content-type")).toBe("application/json");
    expect(await verifyWebhook(raw, headers.get("x-streamd-signature"), "key")).toBe(true);
    expect(await verifyWebhook(raw, headers.get("x-streamd-signature"), "other")).toBe(false);
    expect(body).toEqual({
      id: `one:${T0}`,
      rule: "one",
      windowStart: `${T0}`,
      windowEnd: `${T0 + MINUTE}`,
      threshold: { sum: "amount", gte: "4" },
      value: "4",
      event: { type: TYPE, data: { amount: "4" }, version: "1", sequenceNumber: "0", eventIndex: 0, timestamp: `${T0 + 7n}` } as never,
    });
  });

  it("retries a 500, and waits out a 429's Retry-After", async () => {
    handler = (n) =>
      n === 1 ? new Response("", { status: 500 }) : n === 2 ? new Response("", { status: 429, headers: { "retry-after": "1" } }) : new Response("ok");
    const adapter = new MemoryAdapter([ev(1, T0)]);
    const began = Date.now();
    const h = await runTo(adapter, rules(rule("one", { count: 1 })), () => existsSync(cursorFile()));
    expect(received).toHaveLength(3);
    expect(new Set(received.map((r) => r.raw)).size).toBe(1);
    expect(Date.now() - began).toBeGreaterThanOrEqual(1000);
    expect(h.errors[1]).toBe("streamd: delivery failed (attempt 2/4): HTTP 429; retrying in 1000ms");
  });

  it("exits 1 on a refused delivery, keeping the cursor before that page, and redelivers on the next run", async () => {
    const adapter = new MemoryAdapter([ev(1, T0), ev(2, T0 + 1n), ev(3, T0 + 2n), ev(4, T0 + 3n)]);
    const list = rules(rule("burst", { count: 2 }));
    handler = () => new Response("", { status: 400 });
    const failed = start(adapter, list);
    expect(await failed.run).toBe(1);
    expect(failed.errors).toEqual([
      "streamd: delivery failed: HTTP 400",
      `streamd: rule "burst": the alert for the window at ${T0} was not delivered`,
    ]);
    expect(existsSync(cursorFile())).toBe(false);

    handler = () => new Response("ok");
    await runTo(adapter, list, caughtUp("burst", "4"));
    expect(versions()).toEqual(["2", "2"]);
    expect(received[0]!.raw).toBe(received[1]!.raw);
  });
});

describe("runAlert cursor file", () => {
  it("resumes an open window across a restart, alerting once at the crossing event", async () => {
    const adapter = new MemoryAdapter([ev(1, T0), ev(2, T0 + 1n)]);
    const list = rules(rule("burst", { count: 3 }), rule("total", { sum: "amount", gte: "4" }));
    await runTo(adapter, list, () => caughtUp("burst", "2")() && caughtUp("total", "2")());
    expect(received).toHaveLength(0);
    const saved = state();
    expect(saved.burst!.window).toEqual({ start: `${T0}`, value: "2", fired: false });

    adapter.push(ev(3, T0 + 2n), ev(4, T0 + 3n), ev(5, T0 + 4n));
    await runTo(adapter, list, () => caughtUp("burst", "5")() && caughtUp("total", "5")());
    expect(received.map((r) => [r.body.rule, r.body.event.version])).toEqual([
      ["burst", "3"],
      ["total", "4"],
    ]);

    // A third run over nothing new sends nothing again.
    adapter.push(ev(6, T0 + MINUTE));
    await runTo(adapter, list, () => state().burst!.window?.start === `${T0 + MINUTE}` && state().total!.window?.start === `${T0 + MINUTE}`);
    expect(received).toHaveLength(2);
  });

  it("refuses a saved entry whose rule changed, and keeps entries of rules not in the file", async () => {
    const adapter = new MemoryAdapter([ev(1, T0)]);
    await runTo(adapter, rules(rule("burst", { count: 3 }), rule("gone", { count: 3 })), caughtUp("gone", "1"));
    const h = start(adapter, rules(rule("burst", { count: 4 })));
    expect(await h.run).toBe(1);
    expect(h.errors).toEqual([
      `streamd: cannot read cursor file: rule "burst" changed since its position was saved; remove its entry from ${cursorFile()} or use another --cursor-file`,
    ]);

    adapter.push(ev(2, T0 + 1n));
    await runTo(adapter, rules(rule("burst", { count: 3 })), caughtUp("burst", "2"));
    expect(state().gone!.window).toEqual({ start: `${T0}`, value: "1", fired: false });
  });

  it("exits 1 when the cursor file cannot be read", async () => {
    for (const text of ["not json", "[1]", '{"burst":{"cursor":""}}', '{"burst":{"rule":"x","cursor":"c","window":{"start":"1"}}}']) {
      writeFileSync(cursorFile(), text);
      const h = start(new MemoryAdapter([]), rules(rule("burst", { count: 3 })));
      expect(await h.run).toBe(1);
      expect(h.errors[0]).toStartWith("streamd: cannot read cursor file: ");
    }
    expect(received).toHaveLength(0);
  });
});

describe("runAlert window and cursor pairing", () => {
  interface FakeSub {
    cursor: string | undefined;
    active: boolean;
    handler: (events: StreamdEvent[]) => Promise<void>;
    unsubscribe(): void;
  }

  it("saves a page's window only once the cursor has moved past that page", async () => {
    const [burst] = rules(rule("burst", { count: 9 }));
    writeFileSync(
      cursorFile(),
      JSON.stringify({ burst: { rule: burst!.fingerprint, cursor: "c1", window: { start: `${T0}`, value: "1", fired: false } } }),
    );
    const subs: FakeSub[] = [];
    const client = {
      onBatch: (_d: unknown, handler: FakeSub["handler"], opts: { cursor?: string }) => {
        const sub: FakeSub = { cursor: opts.cursor, active: true, handler, unsubscribe: () => void (sub.active = false) };
        subs.push(sub);
        return sub;
      },
    };
    const controller = new AbortController();
    const run = runAlert(options(), [burst!], { client: client as never, error: () => {}, signal: controller.signal });
    expect(subs[0]!.cursor).toBe("c1");

    // As the poller does: the handler settles, the cursor moves, then the handler's timer runs.
    await subs[0]!.handler([ev(2, T0 + 1n), ev(3, T0 + 2n)]);
    subs[0]!.cursor = "c2";
    await new Promise((r) => setTimeout(r, 5));
    expect(state().burst).toMatchObject({ cursor: "c2", window: { value: "3" } });

    // A page whose handler returned before the cursor moved past it is not saved,
    // even when the timer runs or the command stops first.
    await subs[0]!.handler([ev(4, T0 + 3n)]);
    await new Promise((r) => setTimeout(r, 5));
    expect(state().burst).toMatchObject({ cursor: "c2", window: { value: "3" } });
    controller.abort();
    expect(await run).toBe(0);
    expect(state().burst).toMatchObject({ cursor: "c2", window: { value: "3" } });
  });
});
