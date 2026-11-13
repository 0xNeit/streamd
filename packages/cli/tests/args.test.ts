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
    [["follow"], /unknown command: follow/],
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
    [["codegen", "--chain", "evm", "0x1::coin"], /--chain must be aptos or sui: evm/],
    [["codegen", "--chain", "sui", "nothex"], /Not a valid Sui address: nothex/],
    [["codegen", "--chain", "sui", "0x2::co-in"], /expected <address> or <address>::<module>/],
    [["codegen", "--chain", "sui", "--network", "movement-mainnet", "0x2"], /--network with --chain sui must be one of mainnet, testnet, devnet/],
    [["codegen", "--chain", "sui", "--api-key", "k", "0x2"], /--api-key is not sent to Sui nodes/],
  ])("rejects %j", (argv, message) => rejects(argv, message));

  it("takes --chain sui with Sui networks and a gRPC URL, sending no key", () => {
    const saved = process.env.APTOS_API_KEY;
    process.env.APTOS_API_KEY = "from-env";
    try {
      expect(parseCommand(["codegen", "--chain", "sui", "--validators", "0x2::kiosk", "0x3"])).toEqual({
        command: "codegen",
        options: { chain: "sui", modules: [{ address: "0x2", module: "kiosk" }, { address: "0x3" }], network: "mainnet", validators: true },
      });
      expect(
        parseCommand(["codegen", "--chain", "sui", "--network", "testnet", "--node-url", "https://sui.example:443", "0x2"]),
      ).toEqual({
        command: "codegen",
        options: { chain: "sui", modules: [{ address: "0x2" }], network: "testnet", nodeUrl: "https://sui.example:443" },
      });
      expect(parseCommand(["codegen", "--chain", "aptos", "0x1::coin"])).toEqual({
        command: "codegen",
        options: { modules: [{ address: ADDR, module: "coin" }], network: "mainnet", apiKey: "from-env" },
      });
    } finally {
      if (saved === undefined) delete process.env.APTOS_API_KEY;
      else process.env.APTOS_API_KEY = saved;
    }
  });
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

  it("takes a Postgres URL in place of a file, with every other store flag", () => {
    const url = "postgres://me:pw@db.example:5432/events?sslmode=require";
    const options = store("--postgres", url, "--name", "swaps", "--sender", ADDR, "--decode", "amount", TYPE);
    expect(options).toMatchObject({ postgres: url, name: "swaps", decode: ["amount"], filter: { senders: [LONG_ADDR] } });
    expect("db" in options).toBe(false);
    expect(store("--postgres", "postgresql:///events", TYPE).postgres).toBe("postgresql:///events");
    expect(store("--chain", "sui", "--postgres", url, "0x2::coin::CoinEvent")).toMatchObject({ chain: "sui", postgres: url });
  });

  it.each([
    ["postgres://me:hunter2@:::bad"],
    ["mysql://me:hunter2@db/events"],
    ["postgres:hunter2"],
    ["hunter2"],
    [""],
  ])("refuses --postgres %j without repeating it", (url) => {
    let thrown: unknown;
    try {
      parseCommand(["store", "--postgres", url, TYPE]);
    } catch (err) {
      thrown = err;
    }
    expect(String(thrown)).toContain("--postgres must be a postgres:// or postgresql:// URL");
    expect(String(thrown)).not.toContain("hunter2");
  });

  it.each([
    [["store", TYPE], /store needs --db <file> or --postgres <url>/],
    [["store", "--db", "e.db", "--postgres", "postgres://h/db", TYPE], /--db and --postgres cannot be used together/],
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

describe("parseCommand compare", () => {
  const AGAINST = "https://archive.example/v1";
  const base = ["compare", "--against", AGAINST, "--from-version", "10", "--to-version", "20"];

  it("reads the flags", () => {
    expect(parseCommand([...base, "--max-pages", "3", "--limit", "50", "--node-url", "http://127.0.0.1:8080/v1", TYPE])).toEqual({
      command: "compare",
      options: {
        descriptor: { kind: "module", eventType: [TYPE] },
        network: "mainnet",
        nodeUrl: "http://127.0.0.1:8080/v1",
        against: AGAINST,
        fromVersion: 10n,
        toVersion: 20n,
        maxPages: 3,
        limit: 50,
      },
    });
    expect(parseCommand([...base, "--account", ADDR])).toMatchObject({ options: { descriptor: { kind: "account" } } });
    expect(parseCommand(["compare", "--help"])).toEqual({ command: "help" });
  });

  it.each([
    [["compare", "--from-version", "1", "--to-version", "2", TYPE], /compare needs --against/],
    [["compare", "--against", "ftp://x", "--from-version", "1", "--to-version", "2", TYPE], /--against must be an http or https URL/],
    [["compare", "--against", AGAINST, "--to-version", "2", TYPE], /needs --from-version and --to-version/],
    [["compare", "--against", AGAINST, "--from-version", "5", "--to-version", "2", TYPE], /must not be below/],
    [["compare", "--against", AGAINST, "--from-version", "0", "--to-version", "100000", TYPE], /at most 100000 versions/],
    [[...base, "--max-pages", "0", TYPE], /--max-pages/],
    [[...base, "--max-pages", "1001", TYPE], /--max-pages/],
    [[...base, "--limit", "0", TYPE], /--limit/],
    [base, /at least one event type/],
  ])("rejects %j", (argv, message) => {
    rejects(argv as string[], message as RegExp);
  });

  it("takes exactly 100000 versions", () => {
    expect(parseCommand(["compare", "--against", AGAINST, "--from-version", "1", "--to-version", "100000", TYPE])).toMatchObject({
      command: "compare",
    });
  });
});

describe("--chain sui on every command", () => {
  const SUI_TYPE = "0x3::validator::StakingRequestEvent";
  const DIGEST = "Da2zqzrgB4Yis2qEgjVYh5N4Go1tXXDesyo4BKBkonqS";
  const GRPC = "https://sui.example:443";
  const SUI_ADDR = `0x${"0".repeat(62)}a1`;
  const commands: [string, string[]][] = [
    ["tail", [SUI_TYPE]],
    ["forward", ["--to", "https://hook.example", SUI_TYPE]],
    ["store", ["--db", "e.db", SUI_TYPE]],
    ["query", ["--from-version", "1", SUI_TYPE]],
    ["record", ["--from-version", "1", "--to-version", "2", SUI_TYPE]],
    ["compare", ["--against", GRPC, "--from-version", "1", "--to-version", "2", SUI_TYPE]],
    ["check", [SUI_TYPE]],
    ["tx", [DIGEST]],
  ];

  function withKey<T>(fn: () => T): T {
    const saved = process.env.APTOS_API_KEY;
    process.env.APTOS_API_KEY = "from-env";
    try {
      return fn();
    } finally {
      if (saved === undefined) delete process.env.APTOS_API_KEY;
      else process.env.APTOS_API_KEY = saved;
    }
  }

  it.each(commands)("%s takes it, with a Sui network and gRPC URL and no key from the environment", (command, rest) => {
    const parsed = withKey(() => parseCommand([command, "--chain", "sui", "--network", "testnet", "--node-url", GRPC, ...rest]));
    expect(parsed.command).toBe(command as typeof parsed.command);
    const options = (parsed as unknown as { options: Record<string, unknown> }).options;
    expect(options).toMatchObject({ chain: "sui", network: "testnet", nodeUrl: GRPC });
    expect(options.apiKey).toBeUndefined();
  });

  it.each(commands)("%s leaves chain unset for --chain aptos and by default", (command, rest) => {
    expect((parseCommand([command, ...rest.map((a) => (a === DIGEST ? `0x${"ab".repeat(32)}` : a))]) as { options: object }).options).not.toHaveProperty("chain");
    expect((parseCommand([command, "--chain", "aptos", ...rest.map((a) => (a === DIGEST ? `0x${"ab".repeat(32)}` : a))]) as { options: object }).options).not.toHaveProperty("chain");
  });

  it.each(commands)("%s refuses a Movement network, an API key and an unknown chain", (command, rest) => {
    rejects([command, "--chain", "sui", "--network", "movement-mainnet", ...rest], /--network with --chain sui must be one of mainnet, testnet, devnet/);
    rejects([command, "--chain", "sui", "--api-key", "k", ...rest], /--api-key is not sent to Sui nodes/);
    rejects([command, "--chain", "evm", ...rest], /--chain must be aptos or sui: evm/);
  });

  it("builds module and account descriptors, the address in long form", () => {
    expect(tail("--chain", "sui", SUI_TYPE).descriptor).toEqual({ kind: "module", eventType: [SUI_TYPE] });
    expect(tail("--chain", "sui", "--account", "0xa1").descriptor).toEqual({ kind: "account", accountAddress: SUI_ADDR });
    expect(tail("--chain", "sui", "--account", "0xa1", SUI_TYPE).descriptor).toEqual({ kind: "account", accountAddress: SUI_ADDR, eventTypes: [SUI_TYPE] });
  });

  it.each([
    [["tail", "--chain", "sui", "--account", ADDR, "--handle", "0x1::m::S", "--field", "f"], /--handle is not available with --chain sui: Sui has no event handles/],
    [["tail", "--chain", "sui", "--account", ADDR, "--field", "f"], /--field is not available with --chain sui/],
    [["query", "--chain", "sui", "--from-version", "1", "--account", ADDR, "--creation-number", "3"], /--creation-number is not available with --chain sui/],
    [["forward", "--chain", "sui", "--to", "https://h.example", "--account", ADDR, "--resource", "0x2::m::R"], /--resource is not available with --chain sui/],
    [["record", "--chain", "sui", "--from-version", "1", "--to-version", "2", "--account", ADDR, "--resource", "0x2::m::R"], /--resource is not available with --chain sui/],
    [["tail", "--chain", "sui", "--account", "nothex"], /Not a valid Sui address: nothex/],
    [["tx", "--chain", "sui", `0x${"ab".repeat(32)}`], /not a Sui transaction digest/],
    [["tx", "--chain", "sui", "0OIl0OIl0OIl0OIl0OIl0OIl0OIl0OIl"], /not a Sui transaction digest/],
    [["tx", DIGEST], /not a transaction hash/],
  ])("refuses %j", (argv, message) => {
    rejects(argv as string[], message as RegExp);
  });

  it("checks the API key after the other flags, as before", () => {
    rejects(["tail", "--api-key", "", "--from-version", "x", TYPE], /--from-version must be a non-negative integer/);
    rejects(["query", "--api-key", "", "--from-version", "1", "--limit", "0", TYPE], /--limit/);
    rejects(["record", "--api-key", "", "--from-version", "1", "--to-version", "2", "--limit", "0", TYPE], /--limit/);
    rejects(["tx", "--api-key", "", "--timeout", "x", `0x${"ab".repeat(32)}`], /--timeout/);
  });

  it("takes --from-time and a tx timeout under sui", () => {
    expect(tail("--chain", "sui", "--from-time", "1970-01-02", SUI_TYPE).fromTime).toBe(86_400_000_000n);
    expect(parseCommand(["tx", "--chain", "sui", "--timeout", "5", DIGEST])).toEqual({
      command: "tx",
      options: { chain: "sui", hash: DIGEST, network: "mainnet", timeout: 5 },
    });
  });
});

describe("filter flags", () => {
  const SENDER = `0x${"0".repeat(62)}ab`;
  const flags = ["--sender", "0xab", "--function", "0x1::coin::transfer", "--where", "amount>=5", "--where", "kind==burn"];
  const expected = {
    senders: [SENDER],
    functions: ["0x1::coin::transfer"],
    where: [
      { path: "amount", op: ">=", value: "5" },
      { path: "kind", op: "==", value: "burn" },
    ],
  };

  it.each([
    ["tail", []],
    ["forward", ["--to", "https://example.com"]],
    ["store", ["--db", "x.db"]],
    ["query", ["--from-version", "1"]],
  ])("sets filter on %s", (command, extra) => {
    const parsed = parseCommand([command, ...extra, ...flags, TYPE]);
    expect((parsed as { options: { filter?: unknown } }).options.filter).toEqual(expected);
  });

  it("leaves filter unset without the flags", () => {
    expect("filter" in tail(TYPE)).toBe(false);
  });

  it("works under --chain sui with a long Sui address", () => {
    const sui = `0x${"cd".repeat(32)}`;
    expect(tail("--chain", "sui", "--sender", sui, "0x2::coin::CoinEvent").filter?.senders).toEqual([sui]);
  });

  it("is refused on commands that do not filter", () => {
    rejects(["record", "--from-version", "1", "--to-version", "2", "--sender", "0x1", TYPE], /Unknown option '--sender'/);
  });

  it.each([
    [["--sender", "nope"], /--sender must be an address/],
    [["--function", "transfer"], /--function:/],
    [["--where", "amount~5"], /--where must be <field><op><value>/],
    [["--where", "amount<x"], /--where amount<x: < needs an integer value/],
  ])("rejects %j as a usage error", (args, message) => {
    rejects(["tail", ...args, TYPE], message);
  });
});

describe("decode flag", () => {
  it.each([
    ["tail", []],
    ["forward", ["--to", "https://example.com"]],
    ["store", ["--db", "x.db"]],
    ["query", ["--from-version", "1"]],
  ])("sets decode on %s, beside the filter", (command, extra) => {
    const parsed = parseCommand([command, ...extra, "--decode", "amount,legs.*.amount", "--decode", "fee", TYPE]);
    const options = (parsed as { options: { decode?: unknown; filter?: unknown } }).options;
    expect(options.decode).toEqual(["amount", "legs.*.amount", "fee"]);
    expect("filter" in options).toBe(false);
  });

  it("leaves decode unset without the flag", () => {
    expect("decode" in tail(TYPE)).toBe(false);
  });

  it("is refused on commands that do not decode", () => {
    rejects(["record", "--from-version", "1", "--to-version", "2", "--decode", "amount", TYPE], /Unknown option '--decode'/);
  });

  it.each([[["--decode", "amount,"]], [["--decode", "*.x"]], [["--decode", ""]]])("rejects %j as a usage error", (args) => {
    rejects(["tail", ...args, TYPE], /--decode: /);
  });
});

describe("parseCommand transfers", () => {
  const transfers = (...argv: string[]) => {
    const parsed = parseCommand(["transfers", ...argv]);
    if (parsed.command !== "transfers") throw new Error("expected a transfers command");
    return parsed.options;
  };

  it("follows by default, with only the flags given", () => {
    expect(transfers()).toEqual({ network: "mainnet" });
    expect(
      transfers("--asset", "0xa", "--account", "alice.apt", "--with-asset-info", "--from-version", "5", "--limit", "3",
        "--poll-interval", "200", "--batch-size", "500", "--cursor-file", "t.cursor"),
    ).toEqual({
      network: "mainnet",
      asset: "0xa",
      account: "alice.apt",
      withAssetInfo: true,
      fromVersion: 5n,
      limit: 3,
      pollInterval: 200,
      batchSize: 500,
      cursorFile: "t.cursor",
    });
  });

  it("reads a range with --to-version, a version or head, ascending by default", () => {
    expect(transfers("--from-version", "5", "--to-version", "9")).toEqual({ network: "mainnet", fromVersion: 5n, toVersion: 9n, order: "asc" });
    const head = transfers("--from-time", "1h", "--to-version", "head", "--order", "desc");
    expect(head.toVersion).toBe("head");
    expect(head.order).toBe("desc");
    expect(typeof head.fromTime).toBe("bigint");
  });

  it("takes --chain sui with no key", () => {
    expect(transfers("--chain", "sui", "--asset", "0x2::sui::SUI")).toEqual({ network: "mainnet", chain: "sui", asset: "0x2::sui::SUI" });
    rejects(["transfers", "--chain", "sui", "--api-key", "k"], /--api-key is not sent to Sui nodes/);
  });

  it("sends --api-key on Aptos", () => {
    expect(transfers("--api-key", "k").apiKey).toBe("k");
  });

  it.each([
    [["0x1::coin::CoinDeposit"], /transfers takes no event types/],
    [["--asset", ""], /--asset must not be empty/],
    [["--account", ""], /--account must not be empty/],
    [["--cursor-file", ""], /--cursor-file must not be empty/],
    [["--order", "desc"], /--order needs --to-version/],
    [["--to-version", "9"], /--to-version needs --from-version or --from-time/],
    [["--from-version", "9", "--to-version", "5"], /--to-version must not be below --from-version/],
    [["--from-version", "1", "--to-version", "x"], /--to-version must be a non-negative integer/],
    [["--from-version", "1", "--to-version", "head", "--order", "up"], /--order must be asc or desc/],
    [["--from-version", "1", "--to-version", "head", "--cursor-file", "c"], /--cursor-file follows live/],
    [["--from-version", "1", "--to-version", "head", "--poll-interval", "5"], /--poll-interval follows live/],
    [["--from-version", "1", "--to-version", "head", "--batch-size", "5"], /--batch-size follows live/],
    [["--from-version", "1", "--from-time", "1h"], /set --from-time or --from-version, not both/],
    [["--batch-size", "0"], /--batch-size must be an integer from 1 to 1000/],
    [["--sender", "0x1"], /Unknown option '--sender'/],
  ])("rejects %j as a usage error", (args, message) => {
    rejects(["transfers", ...args], message);
  });
});

describe("parseCommand balances", () => {
  const balances = (...argv: string[]) => {
    const parsed = parseCommand(["balances", ...argv]);
    if (parsed.command !== "balances") throw new Error("expected a balances command");
    return parsed.options;
  };

  it("takes one address or name, with only the flags given", () => {
    expect(balances("alice.apt")).toEqual({ network: "mainnet", address: "alice.apt" });
    expect(balances("0x1", "--asset", "0xa", "--poll-interval", "200", "--limit", "3")).toEqual({
      network: "mainnet",
      address: "0x1",
      asset: ["0xa"],
      pollInterval: 200,
      limit: 3,
    });
  });

  it("collects a repeated --asset into a list in order", () => {
    expect(balances("0x1", "--asset", "0xa", "--asset", "0x1::aptos_coin::AptosCoin").asset).toEqual([
      "0xa",
      "0x1::aptos_coin::AptosCoin",
    ]);
  });

  it("takes --chain sui with no key, and sends --api-key on Aptos", () => {
    expect(balances("--chain", "sui", "alice.sui")).toEqual({ network: "mainnet", chain: "sui", address: "alice.sui" });
    rejects(["balances", "--chain", "sui", "--api-key", "k", "0x1"], /--api-key is not sent to Sui nodes/);
    expect(balances("0x1", "--api-key", "k").apiKey).toBe("k");
  });

  it.each([
    [[], /balances needs one address or name/],
    [[""], /balances needs one address or name/],
    [["0x1", "0x2"], /balances needs one address or name/],
    [["0x1", "--asset", ""], /--asset must not be empty/],
    [["0x1", "--asset", "0xa", "--asset", ""], /--asset must not be empty/],
    [["0x1", "--limit", "0"], /--limit must be an integer from 1/],
    [["0x1", "--poll-interval", "x"], /--poll-interval must be an integer from 0/],
    [["0x1", "--from-version", "5"], /Unknown option '--from-version'/],
  ])("rejects %j as a usage error", (args, message) => {
    rejects(["balances", ...args], message);
  });
});

describe("parseCommand watch", () => {
  const ALICE = `0x${"a".padStart(64, "0")}`;
  const BOB = `0x${"b".padStart(64, "0")}`;
  const watch = (...argv: string[]) => {
    const parsed = parseCommand(["watch", ...argv]);
    if (parsed.command !== "watch") throw new Error("expected a watch command");
    return parsed.options;
  };

  it("takes one or more addresses in long form, with only the flags given", () => {
    expect(watch("0xa")).toEqual({ network: "mainnet", addresses: [ALICE] });
    expect(watch("0xa", "0xB", "0xa")).toEqual({ network: "mainnet", addresses: [ALICE, BOB, ALICE] });
    expect(
      watch("0xa", "--from-version", "5", "--poll-interval", "200", "--limit", "3", "--cursor-file", "w.json"),
    ).toEqual({ network: "mainnet", addresses: [ALICE], fromVersion: 5n, pollInterval: 200, limit: 3, cursorFile: "w.json" });
  });

  it("takes --from-time and the filter flags", () => {
    expect(watch("0xa", "--from-time", "2026-01-01T00:00:00Z").fromTime).toBe(1767225600000000n);
    const options = watch("0xa", "--sender", "0xb", "--function", "0x1::coin::transfer", "--where", "amount>=5", "--decode", "amount");
    expect(options.filter).toEqual({
      senders: [BOB],
      functions: ["0x1::coin::transfer"],
      where: [{ path: "amount", op: ">=", value: "5" }],
    });
    expect(options.decode).toEqual(["amount"]);
  });

  it("takes --chain sui with no key, and sends --api-key on Aptos", () => {
    expect(watch("--chain", "sui", "0xa")).toEqual({ network: "mainnet", chain: "sui", addresses: [ALICE] });
    rejects(["watch", "--chain", "sui", "--api-key", "k", "0xa"], /--api-key is not sent to Sui nodes/);
    expect(watch("0xa", "--api-key", "k").apiKey).toBe("k");
  });

  it.each([
    [[], /watch needs at least one address/],
    [[""], /Not a valid Aptos address/],
    [["0xa", "alice.apt"], /Not a valid Aptos address: alice.apt/],
    [["--chain", "sui", "nope"], /Not a valid Sui address: nope/],
    [["0xa", "--from-version", "1", "--from-time", "1h"], /set --from-time or --from-version, not both/],
    [["0xa", "--cursor-file", ""], /--cursor-file must not be empty/],
    [["0xa", "--limit", "0"], /--limit must be an integer from 1/],
    [["0xa", "--where", "amount"], /--where must be/],
    [["0xa", "--resource", "0x1::a::B"], /Unknown option '--resource'/],
  ])("rejects %j as a usage error", (args, message) => {
    rejects(["watch", ...args], message);
  });
});

describe("parseCommand handles", () => {
  const handles = (...argv: string[]) => {
    const parsed = parseCommand(["handles", ...argv]);
    if (parsed.command !== "handles") throw new Error("expected a handles command");
    return parsed.options;
  };

  it("takes one address or name as given, with the endpoint flags", () => {
    expect(handles("0x1")).toEqual({ network: "mainnet", address: "0x1" });
    expect(handles("alice.apt", "--network", "testnet", "--node-url", "https://n.example/v1", "--api-key", "k")).toEqual({
      network: "testnet",
      nodeUrl: "https://n.example/v1",
      apiKey: "k",
      address: "alice.apt",
    });
  });

  it.each([
    [[], /handles needs one address or name/],
    [[""], /handles needs one address or name/],
    [["0x1", "0x2"], /handles needs one address or name/],
    [["--chain", "sui", "0x1"], /handles is not available with --chain sui: Sui has no event handles/],
    [["0x1", "--limit", "1"], /Unknown option '--limit'/],
  ])("rejects %j as a usage error", (args, message) => {
    rejects(["handles", ...args], message);
  });
});

describe("parseCommand name", () => {
  const name = (...argv: string[]) => {
    const parsed = parseCommand(["name", ...argv]);
    if (parsed.command !== "name") throw new Error("expected a name command");
    return parsed.options;
  };

  it("takes one name or address as given, on either chain", () => {
    expect(name("alice.apt")).toEqual({ network: "mainnet", value: "alice.apt" });
    expect(name("0x1", "--api-key", "k")).toEqual({ network: "mainnet", value: "0x1", apiKey: "k" });
    expect(name("--chain", "sui", "--network", "testnet", "@alice")).toEqual({ network: "testnet", chain: "sui", value: "@alice" });
  });

  it.each([
    [[], /name needs one name or address/],
    [[""], /name needs one name or address/],
    [["alice.apt", "bob.apt"], /name needs one name or address/],
    [["--chain", "sui", "--api-key", "k", "alice.sui"], /--api-key is not sent to Sui nodes/],
    [["--chain", "sui", "--network", "movement-mainnet", "alice.sui"], /--network with --chain sui must be one of/],
    [["alice.apt", "--limit", "1"], /Unknown option '--limit'/],
  ])("rejects %j as a usage error", (args, message) => {
    rejects(["name", ...args], message);
  });
});

describe("several nodes", () => {
  const A = "https://a.example/v1";
  const B = "https://b.example/v1";
  const C = "http://127.0.0.1:8080/v1";
  const HASH = `0x${"ab".repeat(32)}`;
  const READING: string[][] = [
    ["tail", TYPE],
    ["forward", "--to", "https://hook.example", TYPE],
    ["store", "--db", "events.db", TYPE],
    ["query", "--from-version", "1", TYPE],
    ["record", "--from-version", "1", "--to-version", "2", TYPE],
    ["tx", HASH],
    ["transfers"],
    ["balances", ADDR],
    ["watch", ADDR],
  ];
  const options = (argv: string[]) => (parseCommand(argv) as unknown as { options: Record<string, unknown> }).options;

  it.each(READING)("%s pools every --node-url after the first, the key going with the first", (...argv) => {
    const parsed = options([...argv, "--node-url", A, "--node-url", B, "--node-url", C, "--api-key", "k"]);
    expect(parsed).toMatchObject({ nodeUrl: A, extraNodeUrls: [B, C], apiKey: "k" });
  });

  it.each(READING)("%s with one --node-url parses as before", (...argv) => {
    const parsed = options([...argv, "--node-url", A]);
    expect(parsed.nodeUrl).toBe(A);
    expect("extraNodeUrls" in parsed).toBe(false);
  });

  it("pools Sui gRPC URLs too", () => {
    expect(options(["tail", "--chain", "sui", "--node-url", "https://a.example:443", "--node-url", "https://b.example:443", TYPE]))
      .toMatchObject({ chain: "sui", nodeUrl: "https://a.example:443", extraNodeUrls: ["https://b.example:443"] });
  });

  it("checks every URL", () => {
    rejects(["tail", TYPE, "--node-url", A, "--node-url", "ftp://b.example"], /--node-url must be an http or https URL: ftp:/);
  });

  it.each([
    ["check", [TYPE]],
    ["codegen", ["0x1::coin"]],
    ["handles", [ADDR]],
    ["name", ["alice.apt"]],
    ["compare", ["--against", B, "--from-version", "1", "--to-version", "2", TYPE]],
  ])("%s refuses a second --node-url", (command, rest) => {
    rejects([command, ...rest, "--node-url", A, "--node-url", B], new RegExp(`--node-url is given once for ${command}: it reads one node`));
    expect(options([command, ...rest, "--node-url", A]).nodeUrl).toBe(A);
  });
});

describe("--archive", () => {
  const HASH = `0x${"ab".repeat(32)}`;
  const READING: string[][] = [
    ["tail", TYPE],
    ["forward", "--to", "https://hook.example", TYPE],
    ["store", "--db", "events.db", TYPE],
    ["query", "--from-version", "1", TYPE],
    ["record", "--from-version", "1", "--to-version", "2", TYPE],
    ["tx", HASH],
    ["transfers"],
    ["balances", ADDR],
    ["watch", ADDR],
  ];
  const options = (argv: string[]) => (parseCommand(argv) as unknown as { options: Record<string, unknown> }).options;

  it.each(READING)("%s takes a URL or default on mainnet, beside several nodes", (...argv) => {
    expect(options([...argv, "--archive", "https://archive.example/v1"]).archive).toBe("https://archive.example/v1");
    expect(options([...argv, "--archive", "default"]).archive).toBe("default");
    expect(options([...argv, "--node-url", "https://a.example/v1", "--node-url", "https://b.example/v1", "--archive", "default"]))
      .toMatchObject({ archive: "default", extraNodeUrls: ["https://b.example/v1"] });
    expect("archive" in options(argv)).toBe(false);
  });

  it.each(["testnet", "devnet", "movement-mainnet", "movement-testnet"])("refuses default on %s, but takes a URL", (network) => {
    rejects(["tail", TYPE, "--network", network, "--archive", "default"], new RegExp(`--archive default is Aptos mainnet's archive; ${network} has none`));
    expect(options(["tail", TYPE, "--network", network, "--archive", "https://archive.example/v1"]).archive).toBe("https://archive.example/v1");
  });

  it("refuses a value that is not a URL", () => {
    rejects(["query", "--from-version", "1", TYPE, "--archive", "archive.example"], /--archive must be an http or https URL, or default: archive.example/);
    rejects(["query", "--from-version", "1", TYPE, "--archive", "ftp://archive.example"], /--archive must be an http or https URL/);
  });

  it("is refused with --chain sui", () => {
    rejects(["tail", "--chain", "sui", TYPE, "--archive", "default"], /--archive is not available with --chain sui: Sui's archive needs a native gRPC transport/);
  });

  const SINGLE: string[][] = [
    ["check", TYPE],
    ["codegen", "0x1::coin"],
    ["handles", ADDR],
    ["name", "alice.apt"],
    ["compare", "--against", "https://b.example/v1", "--from-version", "1", "--to-version", "2", TYPE],
  ];
  it.each(SINGLE)("is not an option of %s", (...argv) => {
    rejects([...argv, "--archive", "default"], /--archive/);
  });
});

describe("--cache", () => {
  const COMMANDS: string[][] = [
    ["query", "--from-version", "1", TYPE],
    ["record", "--from-version", "1", "--to-version", "2", TYPE],
  ];
  const options = (argv: string[]) => (parseCommand(argv) as unknown as { options: Record<string, unknown> }).options;

  it.each(COMMANDS)("%s takes a file, and leaves it unset without one", (...argv) => {
    expect(options([...argv, "--cache", "pages.db"]).cache).toBe("pages.db");
    expect("cache" in options(argv)).toBe(false);
  });

  it.each(COMMANDS)("%s refuses an empty file and --chain sui", (...argv) => {
    rejects([...argv, "--cache", ""], /--cache must not be empty/);
    rejects([...argv, "--chain", "sui", "--cache", "pages.db"], /--cache does nothing with --chain sui: Sui pages are never marked final/);
  });

  it.each([["tail", TYPE], ["store", "--db", "e.db", TYPE], ["transfers"]] as string[][])("is not an option of %s", (...argv) => {
    rejects([...argv, "--cache", "pages.db"], /--cache/);
  });
});
