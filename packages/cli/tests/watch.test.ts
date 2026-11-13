import { describe, expect, it } from "bun:test";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { MemoryAdapter, StreamdClient, StreamdError, parseEvent } from "@streamd/core";
import type { EventAdapter, EventDescriptor, FetchOptions, StreamdEvent } from "@streamd/core";
import type { WatchCommandOptions } from "../src/args.js";
import { runWatch, watchLine } from "../src/watch.js";

const ALICE = `0x${"a".padStart(64, "0")}`;
const BOB = `0x${"b".padStart(64, "0")}`;
const CAROL = `0x${"c".padStart(64, "0")}`;
const TYPE = "0x1::coin::CoinWithdraw";

function ev(version: number, amount = version * 10): StreamdEvent {
  return { type: TYPE, data: { amount: String(amount) }, version: BigInt(version), sequenceNumber: 0n, eventIndex: 0 };
}

/** One MemoryAdapter per account, so each walk sees only its own events. */
function accounts(events: Record<string, StreamdEvent[]>, fail: Record<string, Error> = {}): EventAdapter {
  const memory = Object.fromEntries(Object.entries(events).map(([a, e]) => [a, new MemoryAdapter(e)]));
  const empty = new MemoryAdapter([]);
  return {
    fetchEvents(descriptor: EventDescriptor, options: FetchOptions) {
      const address = descriptor.kind === "account" ? descriptor.accountAddress : "";
      if (fail[address] !== undefined) return Promise.reject(fail[address]);
      return (memory[address] ?? empty).fetchEvents(descriptor, options);
    },
    getHeadVersion: () => Promise.resolve(1000n),
  };
}

const options = (extra: Partial<WatchCommandOptions> = {}): WatchCommandOptions => ({
  network: "mainnet",
  addresses: [ALICE, BOB],
  fromVersion: 0n,
  pollInterval: 1,
  ...extra,
});

const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Runs until `count` lines arrive (or the run ends), then lets positions settle and aborts. */
async function until(adapter: EventAdapter, extra: Partial<WatchCommandOptions>, count: number) {
  const controller = new AbortController();
  const lines: string[] = [];
  const errors: string[] = [];
  const client = new StreamdClient({ adapter });
  let ended = false;
  const running = runWatch(options(extra), {
    client,
    write: (line) => void lines.push(line),
    error: (m) => void errors.push(m),
    signal: controller.signal,
  }).finally(() => (ended = true));
  for (let i = 0; i < 300 && lines.length < count && !ended; i++) await wait(2);
  await wait(15);
  controller.abort();
  const code = await running;
  client.destroy();
  const parsed = lines.map((l) => JSON.parse(l) as { address: string; version: string });
  return { code, lines, errors, parsed };
}

const dir = () => mkdtempSync(join(tmpdir(), "streamd-watch-"));
const tagged = (parsed: { address: string; version: string }[]) =>
  parsed.map((p) => `${p.address.slice(-1)}${p.version}`).sort();

describe("watchLine", () => {
  it("puts the address first and still parses as the event", () => {
    const line = watchLine(ev(7), ALICE);
    expect(line).toStartWith(`{"address":"${ALICE}","type":"${TYPE}"`);
    expect(line).toEndWith("\n");
    expect(parseEvent(line.trim())).toEqual(ev(7));
  });
});

describe("runWatch", () => {
  it("tags each line with the address it came from", async () => {
    const { code, parsed, errors } = await until(accounts({ [ALICE]: [ev(1), ev(3)], [BOB]: [ev(2)] }), {}, 3);
    expect(code).toBe(0);
    expect(tagged(parsed)).toEqual(["a1", "a3", "b2"]);
    expect(errors).toEqual([]);
  });

  it("exits 0 at the limit, counted across addresses", async () => {
    const adapter = accounts({ [ALICE]: [ev(1), ev(3)], [BOB]: [ev(2), ev(4)] });
    const client = new StreamdClient({ adapter });
    const lines: string[] = [];
    const io = { client, write: (l: string) => void lines.push(l), error: () => {}, signal: new AbortController().signal };
    expect(await runWatch(options({ limit: 3 }), io)).toBe(0);
    expect(lines).toHaveLength(3);
    client.destroy();
  });

  it("prints no more than the limit when writes are slow", async () => {
    const adapter = accounts({ [ALICE]: [ev(1), ev(3)], [BOB]: [ev(2), ev(4)] });
    const client = new StreamdClient({ adapter });
    const lines: string[] = [];
    const io = {
      client,
      write: async (l: string) => {
        await wait(20);
        lines.push(l);
      },
      error: () => {},
      signal: new AbortController().signal,
    };
    expect(await runWatch(options({ limit: 1 }), io)).toBe(0);
    await wait(60);
    expect(lines).toHaveLength(1);
    client.destroy();
  });

  it("applies the filter flags to every walk", async () => {
    const filter = { senders: [], functions: [], where: [{ path: "amount", op: ">=" as const, value: "30" }] };
    const { parsed } = await until(accounts({ [ALICE]: [ev(1), ev(3)], [BOB]: [ev(2), ev(4)] }), { filter }, 2);
    expect(tagged(parsed)).toEqual(["a3", "b4"]);
  });

  it("exits 0 at once when already aborted", async () => {
    const controller = new AbortController();
    controller.abort();
    let called = false;
    const client = { watch: () => ((called = true), {}) } as never;
    expect(await runWatch(options(), { client, write: () => {}, error: () => {}, signal: controller.signal })).toBe(0);
    expect(called).toBe(false);
  });

  it("exits 1 when a write fails", async () => {
    const client = new StreamdClient({ adapter: accounts({ [ALICE]: [ev(1)] }) });
    const errors: string[] = [];
    const io = {
      client,
      write: () => Promise.reject(new Error("disk full")),
      error: (m: string) => void errors.push(m),
      signal: new AbortController().signal,
    };
    expect(await runWatch(options(), io)).toBe(1);
    expect(errors).toEqual(["streamd: disk full"]);
    client.destroy();
  });

  it("exits 1 when watch refuses its options", async () => {
    const errors: string[] = [];
    const client = {
      watch: () => {
        throw new StreamdError("watch positions must be an object of address to cursor");
      },
    } as never;
    const code = await runWatch(options(), { client, write: () => {}, error: (m) => void errors.push(m), signal: new AbortController().signal });
    expect(code).toBe(1);
    expect(errors).toEqual(["streamd: watch positions must be an object of address to cursor"]);
  });

  it("reports a walk that gave up by address and keeps following the others", async () => {
    const adapter = accounts({ [ALICE]: [ev(1)] }, { [BOB]: new StreamdError("bad request") });
    const { code, parsed, errors } = await until(adapter, {}, 1);
    expect(code).toBe(0);
    expect(tagged(parsed)).toEqual(["a1"]);
    expect(errors.some((e) => e.includes("bad request"))).toBe(true);
    expect(errors).toContain(`streamd: stopped following ${BOB}`);
  });

  it("exits 1 when every walk gave up", async () => {
    const fail = new StreamdError("bad request");
    const { code, errors } = await until(accounts({}, { [ALICE]: fail, [BOB]: fail }), {}, 1);
    expect(code).toBe(1);
    expect(errors).toContain(`streamd: stopped following ${ALICE}`);
    expect(errors).toContain(`streamd: stopped following ${BOB}`);
  });
});

describe("runWatch --cursor-file", () => {
  it("saves every address's position as one JSON object and resumes each past what it printed", async () => {
    const cursorFile = join(dir(), "c");
    const events = { [ALICE]: [ev(1), ev(3)], [BOB]: [ev(2)] };
    const first = await until(accounts(events), { cursorFile }, 3);
    expect(first.code).toBe(0);
    const saved = JSON.parse(readFileSync(cursorFile, "utf8")) as Record<string, string>;
    expect(Object.keys(saved).sort()).toEqual([ALICE, BOB]);

    events[ALICE].push(ev(5));
    events[BOB].push(ev(6));
    const second = await until(accounts(events), { cursorFile }, 2);
    expect(tagged(second.parsed)).toEqual(["a5", "b6"]);
  });

  it("prefers a saved position to --from-version for that address only", async () => {
    const cursorFile = join(dir(), "c");
    const events = { [ALICE]: [ev(1), ev(3)], [BOB]: [ev(2), ev(4)] };
    await until(accounts(events), { addresses: [ALICE], cursorFile }, 2);
    const second = await until(accounts(events), { cursorFile, fromVersion: 3n }, 1);
    // alice resumes past 3 and has nothing new; bob, with no position, starts at 3.
    expect(tagged(second.parsed)).toEqual(["b4"]);
  });

  it("carries a position for an address not on the command line through to the file", async () => {
    const cursorFile = join(dir(), "c");
    writeFileSync(cursorFile, `${JSON.stringify({ [CAROL]: "keep-me" })}\n`);
    const run = await until(accounts({ [ALICE]: [ev(1)] }), { addresses: [ALICE], cursorFile }, 1);
    expect(tagged(run.parsed)).toEqual(["a1"]);
    const saved = JSON.parse(readFileSync(cursorFile, "utf8")) as Record<string, string>;
    expect(saved[CAROL]).toBe("keep-me");
    expect(saved[ALICE]).toBeDefined();
  });

  it.each([["not json"], ["[]"], ['"x"'], ["null"], [`{"${ALICE}":5}`], [`{"${ALICE}":""}`]])("exits 1 on a malformed file %j", async (text) => {
    const cursorFile = join(dir(), "c");
    writeFileSync(cursorFile, text);
    const { code, errors, lines } = await until(accounts({ [ALICE]: [ev(1)] }), { cursorFile }, 1);
    expect(code).toBe(1);
    expect(errors[0]).toContain("cannot read cursor file");
    expect(lines).toEqual([]);
  });

  it("exits 1 on an address watch refuses", async () => {
    const cursorFile = join(dir(), "c");
    writeFileSync(cursorFile, JSON.stringify({ nope: "x" }));
    const { code, errors } = await until(accounts({ [ALICE]: [ev(1)] }), { cursorFile }, 1);
    expect(code).toBe(1);
    expect(errors[0]).toContain("Not a valid Aptos address: nope");
  });

  it("never saves a position past a line the limit held back", async () => {
    const cursorFile = join(dir(), "c");
    const events = { [ALICE]: [ev(1)], [BOB]: [ev(2)] };
    const client = new StreamdClient({ adapter: accounts(events) });
    const lines: string[] = [];
    const io = {
      client,
      write: async (l: string) => {
        await wait(30);
        lines.push(l);
      },
      error: () => {},
      signal: new AbortController().signal,
    };
    expect(await runWatch(options({ limit: 1, cursorFile }), io)).toBe(0);
    client.destroy();
    const second = await until(accounts(events), { cursorFile }, 2);
    const seen = tagged([...lines.map((l) => JSON.parse(l)), ...second.parsed]);
    expect(seen).toContain("a1");
    expect(seen).toContain("b2");
  });

  it("leaves a file no walk moved as it was", async () => {
    const cursorFile = join(dir(), "c");
    const text = `${JSON.stringify({ [CAROL]: "keep-me" })}\n`;
    writeFileSync(cursorFile, text);
    await until(accounts({}), { addresses: [ALICE], cursorFile }, 1);
    expect(readFileSync(cursorFile, "utf8")).toBe(text);
  });

  it("exits 1 when the file cannot be written", async () => {
    const cursorFile = join(dir(), "no-such-dir", "c");
    const { code, errors } = await until(accounts({ [ALICE]: [ev(1), ev(2)] }), { cursorFile }, 1);
    expect(code).toBe(1);
    expect(errors.join()).toContain("cannot write cursor file");
  });
});
