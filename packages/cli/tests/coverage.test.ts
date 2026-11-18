import { describe, expect, it } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Account, type Ed25519Account } from "@aptos-labs/ts-sdk";
import type { StreamdEvent } from "@streamd/core";
import type { Localnet } from "@streamd/testing";
import type { ModuleAbi, StructAbi } from "../src/abi.js";
import { coverageLine, coverageReport, eventStructTypes, prettyCoverageLine, runCoverage, unboundAddresses } from "../src/coverage.js";
import type { CoverageIO } from "../src/coverage.js";

const ADDR = `0x00${"ab".repeat(31)}`;

function struct(name: string, isEvent: boolean): StructAbi {
  return { name, isEvent, isEnum: false, isResource: false, typeParams: 0, fields: [], variants: [] };
}

function abi(address: string, name: string, structs: StructAbi[]): ModuleAbi {
  return { address, name, structs, views: [] };
}

function ev(type: string, version: bigint, eventIndex = 0, fn?: string): StreamdEvent {
  return {
    type,
    data: {},
    version,
    sequenceNumber: 0n,
    eventIndex,
    transactionHash: `0x${version}`,
    sender: ADDR,
    ...(fn !== undefined && { entryFunction: { function: fn, typeArguments: [], arguments: [] } }),
  };
}

describe("unboundAddresses", () => {
  it("takes the names left as _ in [addresses] only", () => {
    const toml = [
      "[package]",
      'name = "pkg"',
      "",
      "[addresses]",
      'pkg = "_"   # bound at publish',
      "single = '_'",
      '"quoted" = "_"',
      '"mismatched\' = "_"',
      'admin="_"',
      'std = "0x1"',
      '# old = "_"',
      "",
      "[dev-addresses]",
      'dev = "_"',
      "[dependencies.AptosFramework]",
      'rev = "_"',
    ].join("\r\n");
    expect(unboundAddresses(toml)).toEqual(["pkg", "single", "quoted", "admin"]);
  });

  it("returns none when there is no [addresses] section or nothing unbound", () => {
    expect(unboundAddresses('[package]\nname = "x"\n')).toEqual([]);
    expect(unboundAddresses('[addresses]\nx = "0xcafe"\n')).toEqual([]);
    expect(unboundAddresses("")).toEqual([]);
  });
});

describe("eventStructTypes", () => {
  it("keeps #[event] structs, long form, sorted and once each", () => {
    const abis = [
      abi(ADDR, "zeta", [struct("Fired", true), struct("Config", false)]),
      abi("0x1", "alpha", [struct("Started", true)]),
      abi(ADDR, "zeta", [struct("Fired", true)]),
    ];
    expect(eventStructTypes(abis)).toEqual([`0x${"0".repeat(63)}1::alpha::Started`, `${ADDR}::zeta::Fired`]);
  });

  it("is empty for modules with no events", () => {
    expect(eventStructTypes([abi(ADDR, "m", [struct("S", false)])])).toEqual([]);
  });
});

describe("coverageReport", () => {
  const bumped = `${ADDR}::m::Bumped`;
  const tagged = `${ADDR}::m::Tagged`;
  const never = `${ADDR}::m::Never`;

  it("counts per struct, keeps the earliest call and lists what was never emitted", () => {
    const short = `0x${"ab".repeat(31)}::m::Bumped`;
    const report = coverageReport([bumped, never, tagged], [
      ev(bumped, 9n, 1, `${ADDR}::m::bump`),
      ev(short, 9n, 0, `${ADDR}::m::bump_twice`),
      ev(`${tagged}<u8>`, 12n),
      ev(`${tagged}<0x1::string::String>`, 11n, 2, `${ADDR}::m::tag`),
      ev("0x1::coin::CoinDeposit", 5n),
    ]);
    expect(report).toEqual([
      { event: bumped, count: 2, first: { version: 9n, eventIndex: 0, transactionHash: "0x9", function: `${ADDR}::m::bump_twice`, sender: ADDR } },
      { event: never, count: 0 },
      { event: tagged, count: 2, first: { version: 11n, eventIndex: 2, transactionHash: "0x11", function: `${ADDR}::m::tag`, sender: ADDR } },
    ]);
  });

  it("reports a call with no entry function as null", () => {
    const [entry] = coverageReport([bumped], [ev(bumped, 3n)]);
    expect(entry!.first!.function).toBeNull();
  });
});

describe("coverage lines", () => {
  const first = { version: 12n, eventIndex: 0, transactionHash: "0xaa", function: "0x1::code::publish_package_txn", sender: ADDR };

  it("prints JSON with emitted and the version as a string", () => {
    expect(JSON.parse(coverageLine({ event: "e", count: 1, first }))).toEqual({
      event: "e",
      emitted: true,
      count: 1,
      first: { ...first, version: "12" },
    });
    expect(coverageLine({ event: "e", count: 0 })).toBe('{"event":"e","emitted":false,"count":0}\n');
  });

  it("prints pretty lines for both cases", () => {
    expect(prettyCoverageLine({ event: "e", count: 3, first })).toBe("emitted  e  x3  first at 12 by 0x1::code::publish_package_txn\n");
    expect(prettyCoverageLine({ event: "e", count: 1, first: { ...first, function: null } })).toContain("(no entry function)");
    expect(prettyCoverageLine({ event: "e", count: 0 })).toBe("missed   e\n");
  });
});

describe("runCoverage", () => {
  const PUBLISHER = Account.generate();
  const LONG = PUBLISHER.accountAddress.toStringLong();
  const moduleJson = (structs: { name: string; is_event: boolean }[]) => ({
    abi: {
      address: LONG,
      name: "m",
      structs: structs.map((s) => ({ ...s, is_enum: false, abilities: ["drop"], generic_type_params: [], fields: [] })),
      exposed_functions: [],
    },
  });

  async function packageDir(toml = '[package]\nname = "m"\n[addresses]\nm = "_"\nadmin = "_"\n'): Promise<string> {
    const dir = await mkdtemp(join(tmpdir(), "streamd-coverage-test-"));
    await writeFile(join(dir, "Move.toml"), toml);
    return dir;
  }

  function fakeLocalnet(options: { structs?: { name: string; is_event: boolean }[]; events?: StreamdEvent[] | Error; publish?: () => Promise<unknown> } = {}) {
    const calls = { published: [] as unknown[], queried: [] as unknown[], stopped: 0 };
    const localnet = {
      nodeUrl: "http://127.0.0.1:9/v1",
      faucetUrl: "http://127.0.0.1:10",
      fundedAccount: async () => PUBLISHER,
      publish: async (dir: string, opts: unknown) => {
        calls.published.push([dir, opts]);
        if (options.publish !== undefined) return options.publish();
        return { transactionHash: "0xp", version: 7n };
      },
      aptos: { getAccountModules: async () => [moduleJson(options.structs ?? [{ name: "Bumped", is_event: true }, { name: "Never", is_event: true }])] },
      client: {
        getEvents: async (descriptor: unknown, filter: unknown) => {
          calls.queried.push([descriptor, filter]);
          if (options.events instanceof Error) throw options.events;
          return options.events ?? [ev(`${LONG}::m::Bumped`, 8n, 0, `${LONG}::m::bump`)];
        },
      },
      stop: async () => void calls.stopped++,
    } as unknown as Localnet;
    return { localnet, calls };
  }

  function io(localnet: Localnet | (() => Promise<Localnet>), code: number | Error = 0, signal = new AbortController().signal) {
    const out = { lines: [] as string[], errors: [] as string[], runs: [] as [string[], Record<string, string>][] };
    const coverageIO: CoverageIO = {
      signal,
      start: typeof localnet === "function" ? localnet : async () => localnet,
      run: async (command, env) => {
        out.runs.push([command, env]);
        if (code instanceof Error) throw code;
        return code;
      },
      write: (line) => void out.lines.push(line),
      error: (message) => void out.errors.push(message),
    };
    return { coverageIO, out };
  }

  it("publishes with unbound names bound, runs the command and reports from the publish version", async () => {
    const dir = await packageDir();
    const { localnet, calls } = fakeLocalnet();
    const { coverageIO, out } = io(localnet);
    expect(await runCoverage({ packageDir: dir, command: ["bun", "t.ts"], format: "json" }, coverageIO)).toBe(0);
    expect(calls.published).toEqual([[dir, { account: PUBLISHER, namedAddresses: { m: LONG, admin: LONG } }]]);
    expect(out.runs).toEqual([[["bun", "t.ts"], {
      STREAMD_NODE_URL: "http://127.0.0.1:9/v1",
      STREAMD_FAUCET_URL: "http://127.0.0.1:10",
      STREAMD_PACKAGE_ADDRESS: LONG,
      STREAMD_PUBLISHER_KEY: (PUBLISHER as Ed25519Account).privateKey.toString(),
    }]]);
    expect(calls.queried).toEqual([
      [{ kind: "module", eventType: [`${LONG}::m::Bumped`, `${LONG}::m::Never`] }, { fromVersion: 7n, toVersion: "head", signal: coverageIO.signal }],
    ]);
    expect(out.lines.map((line) => JSON.parse(line).event)).toEqual([`${LONG}::m::Bumped`, `${LONG}::m::Never`]);
    expect(JSON.parse(out.lines[0]!).first.function).toBe(`${LONG}::m::bump`);
    expect(out.errors).toEqual(["streamd: 1 of 2 event structs emitted"]);
    expect(calls.stopped).toBe(1);
    await rm(dir, { recursive: true });
  });

  it("still reports after a failed command, and exits 1", async () => {
    const dir = await packageDir();
    const { localnet, calls } = fakeLocalnet();
    const { coverageIO, out } = io(localnet, 3);
    expect(await runCoverage({ packageDir: dir, command: ["false"], format: "pretty" }, coverageIO)).toBe(1);
    expect(out.lines[1]).toBe(`missed   ${LONG}::m::Never\n`);
    expect(out.errors).toEqual(["streamd: 1 of 2 event structs emitted; the test command exited 3"]);
    expect(calls.stopped).toBe(1);
    await rm(dir, { recursive: true });
  });

  it("reads no events when the package declares none", async () => {
    const dir = await packageDir();
    const { localnet, calls } = fakeLocalnet({ structs: [{ name: "Config", is_event: false }] });
    const { coverageIO, out } = io(localnet);
    expect(await runCoverage({ packageDir: dir, command: ["true"], format: "json" }, coverageIO)).toBe(0);
    expect(calls.queried).toEqual([]);
    expect(out.lines).toEqual([]);
    expect(out.errors).toEqual(["streamd: 0 of 0 event structs emitted"]);
  });

  it("exits 1 without running the command when the publish fails, and stops the localnet", async () => {
    const dir = await packageDir();
    const { localnet, calls } = fakeLocalnet({ publish: () => Promise.reject(new Error("Compiling failed: boom")) });
    const { coverageIO, out } = io(localnet);
    expect(await runCoverage({ packageDir: dir, command: ["true"], format: "json" }, coverageIO)).toBe(1);
    expect(out.runs).toEqual([]);
    expect(out.lines).toEqual([]);
    expect(out.errors).toEqual(["streamd: Compiling failed: boom"]);
    expect(calls.stopped).toBe(1);
  });

  it("exits 1 when the command cannot be spawned, and stops the localnet", async () => {
    const dir = await packageDir();
    const { localnet, calls } = fakeLocalnet();
    const { coverageIO, out } = io(localnet, new Error("spawn nope ENOENT"));
    expect(await runCoverage({ packageDir: dir, command: ["nope"], format: "json" }, coverageIO)).toBe(1);
    expect(out.errors).toEqual(["streamd: spawn nope ENOENT"]);
    expect(calls.stopped).toBe(1);
  });

  it("exits 1 before starting anything when Move.toml is missing", async () => {
    let started = false;
    const { coverageIO, out } = io(async () => ((started = true), fakeLocalnet().localnet));
    expect(await runCoverage({ packageDir: join(tmpdir(), "streamd-no-such-package"), command: ["true"], format: "json" }, coverageIO)).toBe(1);
    expect(started).toBe(false);
    expect(out.errors[0]).toStartWith("streamd: cannot read ");
  });

  it("exits 1 and stops the localnet when the events read fails after the command", async () => {
    const dir = await packageDir();
    const { localnet, calls } = fakeLocalnet({ events: new Error("fetch failed") });
    const { coverageIO, out } = io(localnet);
    expect(await runCoverage({ packageDir: dir, command: ["true"], format: "json" }, coverageIO)).toBe(1);
    expect(out.runs.length).toBe(1);
    expect(out.errors).toEqual(["streamd: fetch failed"]);
    expect(calls.stopped).toBe(1);
  });

  it("prints no report and exits 130 when interrupted during the command", async () => {
    const dir = await packageDir();
    const { localnet, calls } = fakeLocalnet();
    const controller = new AbortController();
    const { coverageIO, out } = io(localnet, 0, controller.signal);
    const run = coverageIO.run;
    coverageIO.run = (command, env) => (controller.abort(), run(command, env));
    expect(await runCoverage({ packageDir: dir, command: ["true"], format: "json" }, coverageIO)).toBe(130);
    expect(calls.queried).toEqual([]);
    expect(out.lines).toEqual([]);
    expect(out.errors).toEqual(["streamd: interrupted; no coverage report"]);
    expect(calls.stopped).toBe(1);
  });

  it("does not start the command when interrupted before it", async () => {
    const dir = await packageDir();
    try {
      const controller = new AbortController();
      const { localnet, calls } = fakeLocalnet();
      const { coverageIO, out } = io(async () => (controller.abort(), localnet), 0, controller.signal);
      expect(await runCoverage({ packageDir: dir, command: ["true"], format: "json" }, coverageIO)).toBe(130);
      expect(calls.published).toEqual([]);
      expect(out.runs).toEqual([]);
      expect(out.errors).toEqual(["streamd: interrupted; no coverage report"]);
      expect(calls.stopped).toBe(1);
    } finally {
      await rm(dir, { recursive: true });
    }
  });

  it("exits 130 without starting the localnet when already aborted", async () => {
    const dir = await packageDir();
    try {
      let started = false;
      const { coverageIO, out } = io(async () => ((started = true), fakeLocalnet().localnet), 0, AbortSignal.abort());
      expect(await runCoverage({ packageDir: dir, command: ["true"], format: "json" }, coverageIO)).toBe(130);
      expect(started).toBe(false);
      expect(out.errors).toEqual(["streamd: interrupted; no coverage report"]);
    } finally {
      await rm(dir, { recursive: true });
    }
  });

  it("reports an interrupt, not the failure it caused, during startup or publish", async () => {
    const dir = await packageDir();
    try {
      const startController = new AbortController();
      const startFails = io(
        () => (startController.abort(), Promise.reject(new Error("The localnet exited before it was ready"))),
        0,
        startController.signal,
      );
      expect(await runCoverage({ packageDir: dir, command: ["true"], format: "json" }, startFails.coverageIO)).toBe(130);
      expect(startFails.out.errors).toEqual(["streamd: interrupted; no coverage report"]);
      const publishController = new AbortController();
      const { localnet, calls } = fakeLocalnet({
        publish: () => (publishController.abort(), Promise.reject(new Error("Compiling failed"))),
      });
      const publishFails = io(localnet, 0, publishController.signal);
      expect(await runCoverage({ packageDir: dir, command: ["true"], format: "json" }, publishFails.coverageIO)).toBe(130);
      expect(publishFails.out.errors).toEqual(["streamd: interrupted; no coverage report"]);
      expect(calls.stopped).toBe(1);
    } finally {
      await rm(dir, { recursive: true });
    }
  });

  it("prints no report and exits 130 when interrupted as reporting begins or during event reads", async () => {
    const dir = await packageDir();
    try {
      const controller = new AbortController();
      const { localnet, calls } = fakeLocalnet();
      const getAccountModules = localnet.aptos.getAccountModules;
      localnet.aptos.getAccountModules = (async (...args: Parameters<typeof getAccountModules>) => {
        const res = await getAccountModules(...args);
        controller.abort();
        return res;
      }) as typeof getAccountModules;
      const { coverageIO, out } = io(localnet, 0, controller.signal);
      expect(await runCoverage({ packageDir: dir, command: ["true"], format: "json" }, coverageIO)).toBe(130);
      expect(calls.queried).toEqual([]);
      expect(out.lines).toEqual([]);
      expect(out.errors).toEqual(["streamd: interrupted; no coverage report"]);
      expect(calls.stopped).toBe(1);

      const eventController = new AbortController();
      const { localnet: ln2, calls: calls2 } = fakeLocalnet();
      const getEvents = ln2.client.getEvents;
      ln2.client.getEvents = (async (...args: Parameters<typeof getEvents>) => {
        eventController.abort();
        return getEvents(...args);
      }) as typeof getEvents;
      const io2 = io(ln2, 0, eventController.signal);
      expect(await runCoverage({ packageDir: dir, command: ["true"], format: "json" }, io2.coverageIO)).toBe(130);
      expect(calls2.queried).toHaveLength(1);
      expect(io2.out.lines).toEqual([]);
      expect(io2.out.errors).toEqual(["streamd: interrupted; no coverage report"]);
      expect(calls2.stopped).toBe(1);
    } finally {
      await rm(dir, { recursive: true });
    }
  });

  it("exits 1 when the localnet does not start", async () => {
    const dir = await packageDir();
    const { coverageIO, out } = io(() => Promise.reject(new Error("aptos CLI not found")));
    expect(await runCoverage({ packageDir: dir, command: ["true"], format: "json" }, coverageIO)).toBe(1);
    expect(out.runs).toEqual([]);
    expect(out.errors).toEqual(["streamd: cannot start a localnet: aptos CLI not found"]);
  });
});
