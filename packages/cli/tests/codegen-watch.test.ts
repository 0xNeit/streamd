import { afterAll, describe, expect, it } from "bun:test";
import { existsSync, mkdtempSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { PackageUpgrade } from "@streamd/aptos";
import { StreamdPollingError } from "@streamd/core";
import type { StreamdEvent, Subscription, SubscriptionOptions } from "@streamd/core";
import { rewriteLine, runCodegenWatch } from "../src/codegen-watch.js";
import type { CodegenWatchIO } from "../src/codegen-watch.js";

const dir = mkdtempSync(join(tmpdir(), "streamd-codegen-watch-"));
afterAll(() => rmSync(dir, { recursive: true, force: true }));
let n = 0;
const outFile = () => join(dir, `events-${n++}.ts`);

const LONG_1 = `0x${"0".repeat(63)}1`;
const tick = () => new Promise((resolve) => setTimeout(resolve, 5));

function upgrade(version: bigint, address = LONG_1): StreamdEvent<PackageUpgrade> {
  const sig = (name: string) => ({ name, visibility: "public", typeParameters: [], params: [], returns: [] });
  return {
    type: "0x1::code::PublishPackage",
    version,
    sequenceNumber: 0n,
    eventIndex: 0,
    data: {
      address,
      isUpgrade: true,
      events: {
        added: [{ name: `${address}::coin::Minted`, abilities: ["drop", "store"], typeParameters: [], fields: [] }],
        changed: [],
        removed: [],
      },
      entryFunctions: { added: [], changed: [{ before: sig(`${address}::coin::mint`), after: sig(`${address}::coin::mint`) }], removed: [] },
      views: { added: [], changed: [], removed: [sig(`${address}::coin::supply`)] },
    },
  };
}

interface Walk {
  address: string;
  callback: (record: StreamdEvent<PackageUpgrade>) => unknown;
  options: SubscriptionOptions;
  sub: Subscription & { active: boolean };
  catchUp(): void;
  end(): void;
}

function harness(sources: string[]) {
  const walks: Walk[] = [];
  const lines: string[] = [];
  const controller = new AbortController();
  let generated = 0;
  const client: CodegenWatchIO["client"] = {
    upgrades: ((address: string, callback: Walk["callback"], options: SubscriptionOptions) => {
      let catchUp!: () => void;
      let end!: (err: Error) => void;
      const caughtUp = new Promise<void>((res, rej) => ((catchUp = res), (end = rej)));
      caughtUp.catch(() => {});
      const sub = { active: true, caughtUp, unsubscribe: () => void (sub.active = false) } as unknown as Walk["sub"];
      walks.push({
        address,
        callback,
        options,
        sub,
        catchUp,
        end: () => {
          sub.active = false;
          options.onError?.(new StreamdPollingError("gave up after 10 errors", 10, new Error("boom")));
          end(new Error("ended"));
        },
      });
      return sub;
    }) as CodegenWatchIO["client"]["upgrades"],
  };
  const io: CodegenWatchIO = {
    client,
    generate: async () => {
      const source = sources[Math.min(generated++, sources.length - 1)]!;
      if (source === "fail") throw new Error("Module 0x1::coin not found");
      return source;
    },
    report: (line) => lines.push(line),
    signal: controller.signal,
  };
  return { walks, lines, controller, io, generated: () => generated };
}

const modules = [{ address: "0x1", module: "coin" }, { address: LONG_1 }];

describe("runCodegenWatch", () => {
  it("follows each distinct address once and writes the file only after every walk caught up", async () => {
    const out = outFile();
    const h = harness(["v1\n"]);
    const done = runCodegenWatch({ network: "mainnet", modules: [...modules, { address: "0x2" }], out }, h.io);
    expect(h.walks.map((w) => w.address)).toEqual([LONG_1, `0x${"0".repeat(63)}2`]);
    expect(h.walks[0]!.options.signal).toBe(h.io.signal);
    h.walks[0]!.catchUp();
    await tick();
    expect(existsSync(out)).toBe(false);
    h.walks[1]!.catchUp();
    await tick();
    expect(readFileSync(out, "utf8")).toBe("v1\n");
    expect(h.lines).toEqual([]);
    h.controller.abort();
    expect(await done).toBe(0);
    expect(h.walks.every((w) => !w.sub.active)).toBe(true);
  });

  it("rewrites atomically and prints one line when a publish changes the output", async () => {
    const out = outFile();
    const h = harness(["v1\n", "v2\n"]);
    const done = runCodegenWatch({ network: "mainnet", modules, out }, h.io);
    h.walks[0]!.catchUp();
    await tick();
    const record = upgrade(42n);
    await h.walks[0]!.callback(record);
    await tick();
    expect(readFileSync(out, "utf8")).toBe("v2\n");
    expect(readdirSync(dir).filter((f) => f.endsWith(".tmp"))).toEqual([]);
    expect(h.lines).toEqual([rewriteLine(out, record)]);
    expect(JSON.parse(h.lines[0]!)).toEqual({
      rewrite: {
        out,
        version: "42",
        address: LONG_1,
        isUpgrade: true,
        events: { added: [`${LONG_1}::coin::Minted`], changed: [], removed: [] },
        entryFunctions: { added: [], changed: [`${LONG_1}::coin::mint`], removed: [] },
        views: { added: [], changed: [], removed: [`${LONG_1}::coin::supply`] },
      },
    });
    h.controller.abort();
    expect(await done).toBe(0);
  });

  it("leaves the file and prints nothing when a publish does not change the output", async () => {
    const out = outFile();
    writeFileSync(out, "v1\n");
    const before = statSync(out).mtimeMs;
    const h = harness(["v1\n"]);
    const done = runCodegenWatch({ network: "mainnet", modules, out }, h.io);
    h.walks[0]!.catchUp();
    await tick();
    await h.walks[0]!.callback(upgrade(7n));
    await tick();
    expect(h.generated()).toBe(2);
    expect(statSync(out).mtimeMs).toBe(before);
    expect(h.lines).toEqual([]);
    h.controller.abort();
    expect(await done).toBe(0);
  });

  it("exits 1 when a walk gives up, stopping the others", async () => {
    const h = harness(["v1\n"]);
    const done = runCodegenWatch({ network: "mainnet", modules: [{ address: "0x1" }, { address: "0x3" }], out: outFile() }, h.io);
    h.walks[0]!.end();
    expect(await done).toBe(1);
    expect(h.lines).toEqual(["streamd: gave up after 10 errors: boom"]);
    expect(h.walks[1]!.sub.active).toBe(false);
  });

  it("exits 1 when a generation fails", async () => {
    const out = outFile();
    const h = harness(["v1\n", "fail"]);
    const done = runCodegenWatch({ network: "mainnet", modules, out }, h.io);
    h.walks[0]!.catchUp();
    await tick();
    await h.walks[0]!.callback(upgrade(9n));
    expect(await done).toBe(1);
    expect(h.lines).toEqual(["streamd: Module 0x1::coin not found"]);
    expect(readFileSync(out, "utf8")).toBe("v1\n");
  });

  it("exits 0 without subscribing when already aborted", async () => {
    const h = harness(["v1\n"]);
    h.controller.abort();
    expect(await runCodegenWatch({ network: "mainnet", modules, out: outFile() }, h.io)).toBe(0);
    expect(h.walks).toEqual([]);
  });
});
