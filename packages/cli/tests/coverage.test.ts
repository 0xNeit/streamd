import { describe, expect, it } from "bun:test";
import type { StreamdEvent } from "@streamd/core";
import type { ModuleAbi, StructAbi } from "../src/abi.js";
import { coverageLine, coverageReport, eventStructTypes, prettyCoverageLine, unboundAddresses } from "../src/coverage.js";

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
      'admin="_"',
      'std = "0x1"',
      '# old = "_"',
      "",
      "[dev-addresses]",
      'dev = "_"',
      "[dependencies.AptosFramework]",
      'rev = "_"',
    ].join("\r\n");
    expect(unboundAddresses(toml)).toEqual(["pkg", "admin"]);
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
