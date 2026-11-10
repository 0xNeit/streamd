import { describe, expect, it } from "bun:test";
import { parseCommand, UsageError } from "../src/args.js";
import type { StoreOptions, ForwardOptions, TailOptions } from "../src/args.js";

const ADDR = "0x1";
const LONG_ADDR = `0x${"0".repeat(63)}1`;
const TYPE = "0x1::coin::DepositEvent";

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

  it("returns help", () => {
    expect(parseCommand(["codegen", "--help"])).toEqual({ command: "help" });
  });

  it.each([
    [["codegen"], /at least one <address>::<module>/],
    [["codegen", "0x1"], /expected <address>::<module>: 0x1/],
    [["codegen", "0x1::coin::Deposit"], /expected <address>::<module>/],
    [["codegen", "0x1::co-in"], /expected <address>::<module>/],
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
