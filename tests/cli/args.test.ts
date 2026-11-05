import { describe, expect, it } from "bun:test";
import { parseCommand, UsageError } from "../../src/cli/args.js";
import type { TailOptions } from "../../src/cli/args.js";

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
      "--cursor", "abc",
      "--poll-interval", "0",
      "--limit", "5",
    );
    expect(options).toEqual({
      descriptor: { kind: "module", eventType: [TYPE] },
      network: "testnet",
      nodeUrl: "http://127.0.0.1:8080/v1",
      fromVersion: 123456789012345678901n,
      cursor: "abc",
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
