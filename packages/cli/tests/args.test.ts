import { beforeAll, describe, expect, it } from "bun:test";
import { parseCommand, parseTime, UsageError } from "../src/args.js";
import type { StoreOptions, ForwardOptions, TailOptions } from "../src/args.js";

const ADDR = "0x1";
const LONG_ADDR = `0x${"0".repeat(63)}1`;
const TYPE = "0x1::coin::DepositEvent";

// Parsed options would carry a key from the developer's own environment.
beforeAll(() => {
  delete process.env.APTOS_API_KEY;
});

function tail(...argv: string[]): TailOptions {
  const parsed = parseCommand(["tail", ...argv]);
  if (parsed.command !== "tail") throw new Error("expected a tail command");
  return parsed.options;
}

function rejects(argv: string[], message: RegExp): void {
  expect(() => parseCommand(argv)).toThrow(UsageError);
  expect(() => parseCommand(argv)).toThrow(message);
}

describe("parseCommand help", () => {
  it.each([[[]], [["-h"]], [["--help"]], [["tail", "--help"]], [["tail", TYPE, "-h"]]])(
    "returns help for %j",
    (argv) => {
      expect(parseCommand(argv)).toEqual({ command: "help" });
    },
  );
});

describe("parseCommand descriptors", () => {
  it("builds a module descriptor from one or more event types", () => {
    expect(tail(TYPE).descriptor).toEqual({ kind: "module", eventType: [TYPE] });
    expect(tail(TYPE, "0x1::coin::WithdrawEvent").descriptor).toEqual({
      kind: "module",
      eventType: [TYPE, "0x1::coin::WithdrawEvent"],
    });
  });

  it("accepts generic event types", () => {
    const generic = "0x1::coin::CoinDeposit<0x1::aptos_coin::AptosCoin>";
    expect(tail(generic).descriptor).toEqual({ kind: "module", eventType: [generic] });
  });

  it("builds an account descriptor, with or without event types", () => {
    expect(tail("--account", ADDR).descriptor).toEqual({
      kind: "account",
      accountAddress: LONG_ADDR,
    });
    expect(tail("--account", ADDR, TYPE).descriptor).toEqual({
      kind: "account",
      accountAddress: LONG_ADDR,
      eventTypes: [TYPE],
    });
  });

  it("builds a handle descriptor", () => {
    const struct = "0x1::coin::CoinStore<0x1::aptos_coin::AptosCoin>";
    expect(
      tail("--account", ADDR, "--handle", struct, "--field", "deposit_events").descriptor,
    ).toEqual({
      kind: "handle",
      accountAddress: LONG_ADDR,
      eventHandleStruct: struct,
      fieldName: "deposit_events",
    });
  });
});

describe("parseCommand options", () => {
  it("defaults to mainnet and leaves the rest unset", () => {
    expect(tail(TYPE)).toEqual({
      descriptor: { kind: "module", eventType: [TYPE] },
      network: "mainnet",
    });
  });

  it("parses every option", () => {
    const options = tail(
      TYPE,
      "--network", "testnet",
      "--node-url", "http://127.0.0.1:8080/v1",
      "--from-version", "123456789012345678901",
      "--poll-interval", "0",
      "--limit", "5",
    );
    expect(options).toEqual({
      descriptor: { kind: "module", eventType: [TYPE] },
      network: "testnet",
      nodeUrl: "http://127.0.0.1:8080/v1",
      fromVersion: 123456789012345678901n,
      pollInterval: 0,
      limit: 5,
    });
  });

  it("accepts --flag=value form", () => {
    expect(tail(TYPE, "--network=devnet").network).toBe("devnet");
  });
});

describe("parseCommand rejects", () => {
  it.each([
    [["watch"], /unknown command: watch/],
    [["tail", TYPE, "--bogus"], /bogus/],
    [["tail", TYPE, "--cursor", "1"], /cursor/],
    [["tail"], /at least one event type/],
    [["tail", "not-a-type"], /Not a valid Move event type/],
    [["tail", "--account", "nope"], /Not a valid Aptos address/],
    [["tail", "--account", ADDR, "--handle", "0x1::a::B"], /--handle needs/],
    [["tail", "--handle", "0x1::a::B", "--field", "f"], /--handle needs/],
    [["tail", "--account", ADDR, "--field", "f"], /--handle needs/],
    [["tail", "--account", ADDR, "--handle", "0x1::a::B", "--field", "f", TYPE], /cannot be combined/],
    [["tail", TYPE, "--network", "localnet"], /--network must be one of/],
    [["tail", TYPE, "--node-url", "ftp://example.com"], /--node-url must be/],
    [["tail", TYPE, "--node-url", "not a url"], /--node-url must be/],
    [["tail", TYPE, "--from-version", "-1"], /--from-version/],
    [["tail", TYPE, "--from-version", "1.5"], /--from-version/],
    [["tail", TYPE, "--poll-interval", "-5"], /--poll-interval/],
    [["tail", TYPE, "--poll-interval", "2147483648"], /--poll-interval/],
    [["tail", TYPE, "--format", "xml"], /--format/],
    [["tail", TYPE, "--format", ""], /--format/],
    [["query", TYPE, "--from-version", "1", "--format", "pretty"], /format/],
    [["tail", TYPE, "--limit", "0"], /--limit/],
    [["tail", TYPE, "--limit", "2.5"], /--limit/],
    [["tail", TYPE, "--limit"], /--limit/],
  ])("%j", (argv, message) => {
    rejects(argv as string[], message as RegExp);
  });
});

describe("parseCommand forward", () => {
  const URL_WITH_TOKEN = "https://hooks.example.com/in?token=s3cret";

  function forward(...argv: string[]): ForwardOptions {
    const parsed = parseCommand(["forward", ...argv]);
    if (parsed.command !== "forward") throw new Error("expected a forward command");
    return parsed.options;
  }

  it("applies defaults", () => {
    expect(forward("--to", URL_WITH_TOKEN, TYPE)).toEqual({
      descriptor: { kind: "module", eventType: [TYPE] },
      network: "mainnet",
      to: URL_WITH_TOKEN,
      cursorFile: "streamd-forward.cursor",
      retries: 5,
      timeout: 10000,
      headers: {},
    });
  });

  it("reads every flag", () => {
    const options = forward(
      "--account", ADDR,
      "--to", "http://127.0.0.1:9000/hook",
      "--cursor-file", "/tmp/c",
      "--retries", "0",
      "--timeout", "250",
      "--header", "Authorization: Bearer abc:def",
      "--header", "X-Source:streamd",
      "--network", "testnet",
      "--node-url", "http://127.0.0.1:8080/v1",
      "--from-version", "7",
      "--poll-interval", "50",
    );
    expect(options).toEqual({
      descriptor: { kind: "account", accountAddress: LONG_ADDR },
      network: "testnet",
      nodeUrl: "http://127.0.0.1:8080/v1",
      fromVersion: 7n,
      pollInterval: 50,
      to: "http://127.0.0.1:9000/hook",
      cursorFile: "/tmp/c",
      retries: 0,
      timeout: 250,
      headers: { authorization: "Bearer abc:def", "x-source": "streamd" },
    });
  });

  it("reads a bounded range and a batch size", () => {
    expect(forward("--to", URL_WITH_TOKEN, TYPE, "--from-version", "7", "--to-version", "7", "--batch-size", "1")).toMatchObject({
      fromVersion: 7n,
      toVersion: 7n,
      batchSize: 1,
    });
    expect(forward("--to", URL_WITH_TOKEN, TYPE, "--from-time", "1h", "--to-version", "9")).toMatchObject({ toVersion: 9n });
    expect(forward("--to", URL_WITH_TOKEN, TYPE, "--batch-size", "500")).toMatchObject({ batchSize: 500 });
  });

  it("returns help", () => {
    expect(parseCommand(["forward", "--help"])).toEqual({ command: "help" });
  });

  it.each([
    [["forward", TYPE], /forward needs --to/],
    [["forward", "--to", URL_WITH_TOKEN], /forward needs at least one event type/],
    [["forward", "--to", URL_WITH_TOKEN, TYPE, "--limit", "1"], /limit/],
    [["tail", TYPE, "--to", URL_WITH_TOKEN], /Unknown option '--to'/],
    [["forward", "--to", URL_WITH_TOKEN, TYPE, "--retries", "101"], /--retries/],
    [["forward", "--to", URL_WITH_TOKEN, TYPE, "--timeout", "0"], /--timeout/],
    [["forward", "--to", URL_WITH_TOKEN, TYPE, "--cursor-file", ""], /--cursor-file/],
    [["forward", "--to", URL_WITH_TOKEN, TYPE, "--secret", ""], /--secret/],
    [["forward", "--to", URL_WITH_TOKEN, TYPE, "--secret", "k", "--header", "X-Streamd-Signature: x"], /x-streamd-signature/],
    [["tail", TYPE, "--secret", "k"], /secret/],
    [["forward", "--to", URL_WITH_TOKEN, TYPE, "--to-version", "9"], /--to-version needs --from-version or --from-time/],
    [["forward", "--to", URL_WITH_TOKEN, TYPE, "--from-version", "10", "--to-version", "9"], /--to-version must not be below --from-version/],
    [["forward", "--to", URL_WITH_TOKEN, TYPE, "--from-version", "1", "--to-version", "9x"], /--to-version must be a non-negative integer/],
    [["forward", "--to", URL_WITH_TOKEN, TYPE, "--batch-size", "0"], /--batch-size must be an integer from 1/],
    [["tail", TYPE, "--batch-size", "5"], /Unknown option '--batch-size'/],
  ])("rejects %j", (argv, message) => {
    rejects(argv as string[], message as RegExp);
  });

  it.each([["ftp://x.example/s3cret"], ["not a url s3cret"]])(
    "rejects a non-http --to without echoing it: %s",
    (url) => {
      expect(() => forward("--to", url, TYPE)).toThrow(/^--to must be an http or https URL$/);
    },
  );

  it.each([
    ["Authorization Bearer s3cret"],
    [": s3cret"],
    ["Bad Name: s3cret"],
    ["X: a\ns3cret"],
    ["X: s3\u20accret"],
    ["X: s3\u0007cret"],
  ])(
    "rejects a malformed --header without echoing it: %j",
    (header) => {
      expect(() => forward("--to", URL_WITH_TOKEN, TYPE, "--header", header)).toThrow(
        /^--header must be 'Name: value' with a valid name and value$/,
      );
    },
  );
});

describe("parseCommand codegen", () => {
  it("parses modules and options", () => {
    expect(parseCommand(["codegen", "0x1::coin", `${LONG_ADDR}::fungible_asset`])).toEqual({
      command: "codegen",
      options: {
        modules: [
          { address: ADDR, module: "coin" },
          { address: LONG_ADDR, module: "fungible_asset" },
        ],
        network: "mainnet",
      },
    });
    expect(
      parseCommand([
        "codegen", "--network", "testnet", "--node-url", "http://localhost:8080/v1",
        "--out", "events.ts", "0x1::coin",
      ]),
    ).toEqual({
      command: "codegen",
      options: {
        modules: [{ address: ADDR, module: "coin" }],
        network: "testnet",
        nodeUrl: "http://localhost:8080/v1",
        out: "events.ts",
      },
    });
  });

  it("takes --validators", () => {
    expect(parseCommand(["codegen", "--validators", "0x1::coin"])).toMatchObject({ options: { validators: true } });
    expect(parseCommand(["codegen", "0x1::coin"])).not.toMatchObject({ options: { validators: true } });
  });

  it("takes a bare address as every module at it", () => {
    expect(parseCommand(["codegen", "0x1", `${LONG_ADDR}::coin`])).toEqual({
      command: "codegen",
      options: { modules: [{ address: ADDR }, { address: LONG_ADDR, module: "coin" }], network: "mainnet" },
    });
  });

  it("returns help", () => {
    expect(parseCommand(["codegen", "--help"])).toEqual({ command: "help" });
  });

  it.each([
    [["codegen"], /at least one <address> or <address>::<module>/],
    [["codegen", "nothex"], /Not a valid Aptos address/],
    [["codegen", "0x1::"], /expected <address> or <address>::<module>: 0x1::/],
    [["codegen", "0x1::coin::Deposit"], /expected <address> or <address>::<module>/],
    [["codegen", "0x1::co-in"], /expected <address> or <address>::<module>/],
    [["codegen", "nothex::coin"], /Not a valid Aptos address/],
    [["codegen", "--out", "", "0x1::coin"], /--out must not be empty/],
    [["codegen", "--network", "moon", "0x1::coin"], /--network must be one of/],
    [["codegen", "--limit", "1", "0x1::coin"], /Unknown option/],
  ])("rejects %j", (argv, message) => rejects(argv, message));
});

describe("parseCommand store", () => {
  function store(...argv: string[]): StoreOptions {
    const parsed = parseCommand(["store", ...argv]);
    if (parsed.command !== "store") throw new Error("expected a store command");
    return parsed.options;
  }

  it("takes the source flags and a file", () => {
    expect(store("--db", "e.db", "--network", "testnet", "--from-version", "5", "--poll-interval", "9", TYPE)).toEqual({
      descriptor: { kind: "module", eventType: [TYPE] },
      network: "testnet",
      fromVersion: 5n,
      pollInterval: 9,
      db: "e.db",
    });
    expect(store("--db", "e.db", "--name", "swaps", TYPE).name).toBe("swaps");
    expect(store("--db", "e.db", "--account", "0x1").descriptor).toEqual({ kind: "account", accountAddress: expect.any(String) });
    expect(parseCommand(["store", "--help"])).toEqual({ command: "help" });
  });

  it.each([
    [["store", TYPE], /store needs --db/],
    [["store", "--db", "", TYPE], /--db must not be empty/],
    [["store", "--db", "e.db"], /store needs at least one event type/],
    [["store", "--db", "e.db", "--name", "", TYPE], /--name must not be empty/],
    [["store", "--db", "e.db", "--limit", "1", TYPE], /limit/],
  ] as [string[], RegExp][])("rejects %j", (argv, pattern) => {
    expect(() => parseCommand(argv)).toThrow(pattern);
  });
});

describe("parseCommand check", () => {
  it("takes event types, a network and a node URL", () => {
    expect(parseCommand(["check", "--network", "testnet", "--node-url", "https://n.example/v1", TYPE])).toEqual({
      command: "check",
      options: { descriptor: { kind: "module", eventType: [TYPE] }, network: "testnet", nodeUrl: "https://n.example/v1" },
    });
    expect(parseCommand(["check", "--help"])).toEqual({ command: "help" });
  });

  it.each([
    [["check"], /^check needs at least one event type$/],
    [["check", "nope"], /event type/i],
    [["check", "--account", "0x1", TYPE], /account/],
    [["check", "--handle", "x", TYPE], /handle/],
  ] as [string[], RegExp][])("rejects %j", (argv, pattern) => {
    expect(() => parseCommand(argv)).toThrow(pattern);
  });
});

describe("parseCommand record", () => {
  const bounds = ["--from-version", "5", "--to-version", "9"];

  it("takes a closed range, a limit and a file", () => {
    expect(parseCommand(["record", ...bounds, "--limit", "3", "--out", "r.json", TYPE])).toEqual({
      command: "record",
      options: {
        descriptor: { kind: "module", eventType: [TYPE] },
        network: "mainnet",
        fromVersion: 5n,
        toVersion: 9n,
        limit: 3,
        out: "r.json",
      },
    });
    expect(parseCommand(["record", "--help"])).toEqual({ command: "help" });
  });

  it("takes the same descriptor flags as tail", () => {
    const parsed = parseCommand(["record", ...bounds, "--account", ADDR]);
    expect(parsed).toMatchObject({ options: { descriptor: { kind: "account", accountAddress: LONG_ADDR } } });
  });

  it.each([
    [["record", TYPE], /needs --from-version and --to-version/],
    [["record", "--from-version", "5", TYPE], /needs --from-version and --to-version/],
    [["record", "--to-version", "5", TYPE], /needs --from-version and --to-version/],
    [["record", "--from-version", "9", "--to-version", "5", TYPE], /must not be below/],
    [["record", "--from-version", "5", "--to-version", "x", TYPE], /--to-version must be a non-negative integer/],
    [["record", ...bounds], /record needs at least one event type/],
    [["record", ...bounds, "--out", "", TYPE], /--out must not be empty/],
    [["record", ...bounds, "--limit", "0", TYPE], /--limit/],
  ] as [string[], RegExp][])("rejects %j", (argv, pattern) => {
    expect(() => parseCommand(argv)).toThrow(pattern);
  });
});

describe("parseCommand query", () => {
  it("defaults to ascending, ending at the head", () => {
    expect(parseCommand(["query", "--from-version", "5", TYPE])).toEqual({
      command: "query",
      options: {
        descriptor: { kind: "module", eventType: [TYPE] },
        network: "mainnet",
        fromVersion: 5n,
        toVersion: "head",
        order: "asc",
      },
    });
    expect(parseCommand(["query", "--help"])).toEqual({ command: "help" });
  });

  it("takes a closed range, a limit, an order and the descriptor flags", () => {
    expect(
      parseCommand(["query", "--from-version", "5", "--to-version", "9", "--limit", "3", "--order", "desc", "--account", ADDR]),
    ).toMatchObject({
      options: { fromVersion: 5n, toVersion: 9n, limit: 3, order: "desc", descriptor: { kind: "account", accountAddress: LONG_ADDR } },
    });
  });

  it.each([
    [["query", TYPE], /query needs --from-version or --from-time/],
    [["query", "--from-version", "9", "--to-version", "5", TYPE], /must not be below/],
    [["query", "--from-version", "5", "--order", "up", TYPE], /--order must be asc or desc/],
    [["query", "--from-version", "5", "--limit", "0", TYPE], /--limit/],
    [["query", "--from-version", "5"], /query needs at least one event type/],
  ] as [string[], RegExp][])("rejects %j", (argv, pattern) => {
    expect(() => parseCommand(argv)).toThrow(pattern);
  });
});

describe("parseTime", () => {
  const now = Date.UTC(2026, 10, 10, 12);

  it.each([
    ["90s", now - 90_000],
    ["30m", now - 1_800_000],
    ["2h", now - 7_200_000],
    ["7d", now - 604_800_000],
    ["0s", now],
  ] as [string, number][])("reads the duration %s", (raw, ms) => {
    expect(parseTime(raw, now)).toBe(BigInt(ms) * 1000n);
  });

  it.each([
    ["2026-11-10", Date.UTC(2026, 10, 10)],
    ["2026-11-10T09:30", Date.UTC(2026, 10, 10, 9, 30)],
    ["2026-11-10T09:30:15", Date.UTC(2026, 10, 10, 9, 30, 15)],
    ["2026-11-10T09:30:15.250Z", Date.UTC(2026, 10, 10, 9, 30, 15, 250)],
    ["2026-11-10T09:30:00+01:00", Date.UTC(2026, 10, 10, 8, 30)],
  ] as [string, number][])("reads %s, UTC unless it names an offset", (raw, ms) => {
    expect(parseTime(raw, now)).toBe(BigInt(ms) * 1000n);
  });

  it("reads a date-time the same in any zone", () => {
    const zone = process.env.TZ;
    process.env.TZ = "America/New_York";
    try {
      expect(parseTime("2026-11-10T09:30", now)).toBe(BigInt(Date.UTC(2026, 10, 10, 9, 30)) * 1000n);
    } finally {
      process.env.TZ = zone;
    }
  });

  it("clamps a duration reaching before the epoch", () => {
    expect(parseTime("99999999999d", now)).toBe(0n);
  });

  it.each(["", "2", "2x", "h", "-2h", "1.5h", "2026", "2026-13-45", "2026-11-10 09:30", "yesterday", "1969-01-01"])(
    "refuses %j",
    (raw) => {
      expect(() => parseTime(raw, now)).toThrow(UsageError);
    },
  );
});

describe("--from-time", () => {
  it("is taken by tail, forward and query", () => {
    const at = ["--from-time", "2026-11-10"];
    const micros = BigInt(Date.UTC(2026, 10, 10)) * 1000n;
    expect(parseCommand(["tail", ...at, TYPE])).toMatchObject({ options: { fromTime: micros } });
    expect(parseCommand(["forward", "--to", "https://h.example", ...at, TYPE])).toMatchObject({ options: { fromTime: micros } });
    const query = parseCommand(["query", ...at, TYPE]);
    expect(query).toMatchObject({ options: { fromTime: micros, toVersion: "head" } });
    expect((query as { options: { fromVersion?: bigint } }).options.fromVersion).toBeUndefined();
  });

  it.each([
    [["tail", "--from-time", "2h", "--from-version", "5", TYPE], /not both/],
    [["query", "--from-time", "2h", "--from-version", "5", TYPE], /not both/],
    [["tail", "--from-time", "soon", TYPE], /--from-time must be/],
    [["query", TYPE], /query needs --from-version or --from-time/],
    [["store", "--db", "e.db", "--from-time", "2h", TYPE], /from-time/],
    [["record", "--from-time", "2h", "--to-version", "9", TYPE], /from-time/],
  ] as [string[], RegExp][])("rejects %j", (argv, pattern) => {
    expect(() => parseCommand(argv)).toThrow(pattern);
  });
});

describe("every descriptor kind", () => {
  const RESOURCE = "0x1::block::BlockResource";
  const bounds = ["--from-version", "1", "--to-version", "2"];
  const commands: [string, string[]][] = [
    ["tail", []],
    ["forward", ["--to", "https://h.example"]],
    ["store", ["--db", "e.db"]],
    ["query", bounds],
    ["record", bounds],
  ];

  it.each(commands)("%s follows a resource", (command, extra) => {
    expect(parseCommand([command, ...extra, "--account", ADDR, "--resource", RESOURCE])).toMatchObject({
      options: { descriptor: { kind: "resource", accountAddress: LONG_ADDR, resourceType: RESOURCE } },
    });
  });

  it.each(commands)("%s follows a handle by creation number", (command, extra) => {
    expect(parseCommand([command, ...extra, "--account", ADDR, "--creation-number", "7"])).toMatchObject({
      options: { descriptor: { kind: "handle", accountAddress: LONG_ADDR, creationNumber: 7n } },
    });
  });

  it("takes the largest u64 creation number exactly", () => {
    expect(parseCommand(["tail", "--account", ADDR, "--creation-number", "18446744073709551615"])).toMatchObject({
      options: { descriptor: { creationNumber: 18446744073709551615n } },
    });
  });

  it.each([
    [["tail", "--resource", RESOURCE], /--resource needs --account/],
    [["tail", "--creation-number", "7"], /--creation-number needs --account/],
    [["tail", "--account", ADDR, "--resource", RESOURCE, "--creation-number", "7"], /not both/],
    [["tail", "--account", ADDR, "--resource", RESOURCE, "--handle", "s", "--field", "f"], /cannot be combined/],
    [["tail", "--account", ADDR, "--creation-number", "7", "--field", "f"], /cannot be combined/],
    [["tail", "--account", ADDR, "--resource", RESOURCE, TYPE], /event types cannot be combined/],
    [["tail", "--account", ADDR, "--creation-number", "7", TYPE], /event types cannot be combined/],
    [["tail", "--account", ADDR, "--resource", "nope"], /resource|type/i],
    [["tail", "--account", ADDR, "--creation-number", "x"], /must be a u64/],
    [["tail", "--account", ADDR, "--creation-number", "18446744073709551616"], /must be a u64/],
    [["check", "--account", ADDR, "--resource", RESOURCE], /account/],
  ] as [string[], RegExp][])("rejects %j", (argv, pattern) => {
    expect(() => parseCommand(argv)).toThrow(pattern);
  });
});

describe("Movement networks", () => {
  it.each(["movement-mainnet", "movement-testnet"])("%s reaches every command", (name) => {
    const net = ["--network", name];
    const range = ["--from-version", "1", "--to-version", "2"];
    for (const argv of [
      ["tail", ...net, TYPE],
      ["forward", "--to", "https://h.example", ...net, TYPE],
      ["store", "--db", "e.db", ...net, TYPE],
      ["query", ...range, ...net, TYPE],
      ["record", ...range, ...net, TYPE],
      ["check", ...net, TYPE],
      ["codegen", ...net, "0x1::coin"],
    ]) {
      expect(parseCommand(argv)).toMatchObject({ options: { network: name } });
    }
  });

  it("still refuses an unknown network, naming the choices", () => {
    expect(() => parseCommand(["tail", "--network", "moon", TYPE])).toThrow(/movement-mainnet/);
  });
});

describe("tail --cursor-file", () => {
  it("is optional, and carried when given", () => {
    expect(parseCommand(["tail", TYPE])).not.toHaveProperty("options.cursorFile");
    expect(parseCommand(["tail", "--cursor-file", "c", TYPE])).toMatchObject({ options: { cursorFile: "c" } });
    expect(() => parseCommand(["tail", "--cursor-file", "", TYPE])).toThrow(/--cursor-file must not be empty/);
  });
});

describe("parseCommand tx", () => {
  const HASH = `0x${"ab".repeat(32)}`;

  it("takes a hash, a network, a node URL and a timeout", () => {
    expect(parseCommand(["tx", HASH])).toEqual({ command: "tx", options: { hash: HASH, network: "mainnet" } });
    expect(parseCommand(["tx", "--network", "testnet", "--node-url", "https://n.example/v1", "--timeout", "500", HASH])).toEqual({
      command: "tx",
      options: { hash: HASH, network: "testnet", nodeUrl: "https://n.example/v1", timeout: 500 },
    });
    expect(parseCommand(["tx", "--help"])).toEqual({ command: "help" });
  });

  it.each([
    [["tx"], /tx needs one transaction hash/],
    [["tx", HASH, HASH], /tx needs one transaction hash/],
    [["tx", "0x12"], /not a transaction hash/],
    [["tx", `${"ab".repeat(32)}`], /not a transaction hash/],
    [["tx", `0x${"zz".repeat(32)}`], /not a transaction hash/],
    [["tx", HASH, "--timeout", "soon"], /--timeout/],
    [["tx", HASH, "--limit", "1"], /limit/],
  ] as [string[], RegExp][])("rejects %j", (argv, pattern) => {
    expect(() => parseCommand(argv)).toThrow(pattern);
  });
});

describe("forward secret", () => {
  const ENV = "STREAMD_WEBHOOK_SECRET";
  const secretOf = (...extra: string[]) => {
    const parsed = parseCommand(["forward", "--to", "https://h.example", TYPE, ...extra]);
    if (parsed.command !== "forward") throw new Error("expected a forward command");
    return parsed.options.secret;
  };
  const withEnv = (value: string | undefined, run: () => void) => {
    const before = process.env[ENV];
    if (value === undefined) delete process.env[ENV];
    else process.env[ENV] = value;
    try {
      run();
    } finally {
      if (before === undefined) delete process.env[ENV];
      else process.env[ENV] = before;
    }
  };

  it("takes the flag", () => withEnv(undefined, () => expect(secretOf("--secret", "k")).toBe("k")));
  it("reads the environment", () => withEnv("e", () => expect(secretOf()).toBe("e")));
  it("prefers the flag", () => withEnv("e", () => expect(secretOf("--secret", "k")).toBe("k")));
  it("treats an empty environment variable as unset", () => withEnv("", () => expect(secretOf()).toBeUndefined()));
  it("is unset by default", () => withEnv(undefined, () => expect(secretOf()).toBeUndefined()));
  it("allows the header when not signing", () =>
    withEnv(undefined, () => expect(secretOf("--header", "x-streamd-signature: mine")).toBeUndefined()));
});

describe("parseCommand --api-key and APTOS_API_KEY", () => {
  const COMMANDS: string[][] = [
    ["tail", TYPE],
    ["forward", "--to", "https://h.example", TYPE],
    ["store", "--db", "e.db", TYPE],
    ["query", "--from-version", "1", TYPE],
    ["tx", `0x${"ab".repeat(32)}`],
    ["check", TYPE],
    ["record", "--from-version", "1", "--to-version", "2", TYPE],
    ["codegen", "0x1::coin"],
  ];
  const keyOf = (argv: string[]) => (parseCommand(argv) as { options: { apiKey?: string } }).options.apiKey;
  const withEnv = <T>(value: string | undefined, run: () => T): T => {
    const saved = process.env.APTOS_API_KEY;
    if (value === undefined) delete process.env.APTOS_API_KEY;
    else process.env.APTOS_API_KEY = value;
    try {
      return run();
    } finally {
      if (saved === undefined) delete process.env.APTOS_API_KEY;
      else process.env.APTOS_API_KEY = saved;
    }
  };

  it.each(COMMANDS)("takes --api-key on %s", (...argv) => {
    withEnv(undefined, () => {
      expect(keyOf([...argv, "--api-key", "k1"])).toBe("k1");
      expect(keyOf([...argv, "--api-key", "k1", "--node-url", "http://127.0.0.1:8080/v1"])).toBe("k1");
      expect(keyOf([...argv, "--api-key", "k1", "--network", "movement-mainnet"])).toBe("k1");
      expect(keyOf(argv)).toBeUndefined();
    });
  });

  it.each(COMMANDS)("reads APTOS_API_KEY on %s only for an Aptos network without --node-url", (...argv) => {
    withEnv("envkey", () => {
      expect(keyOf(argv)).toBe("envkey");
      expect(keyOf([...argv, "--network", "testnet"])).toBe("envkey");
      expect(keyOf([...argv, "--network", "devnet"])).toBe("envkey");
      expect(keyOf([...argv, "--node-url", "http://127.0.0.1:8080/v1"])).toBeUndefined();
      expect(keyOf([...argv, "--network", "movement-mainnet"])).toBeUndefined();
      expect(keyOf([...argv, "--network", "movement-testnet"])).toBeUndefined();
      expect(keyOf([...argv, "--api-key", "flag"])).toBe("flag");
    });
    withEnv("", () => expect(keyOf(argv)).toBeUndefined());
  });

  it("refuses an empty --api-key without echoing anything", () => {
    expect(() => parseCommand(["tail", TYPE, "--api-key", ""])).toThrow("--api-key must not be empty");
  });
});
