import { beforeAll, describe, expect, it } from "bun:test";
import { parseCommand, parseTime, UsageError } from "../src/args.js";
import { parseKafkaUrl } from "../src/kafka.js";
import type { AlertOptions, MonitorOptions, ServeOptions, StoreOptions, ForwardOptions, TailOptions } from "../src/args.js";

const ADDR = "0x1";
const LONG_ADDR = `0x${"0".repeat(63)}1`;
const TYPE = "0x1::coin::DepositEvent";

// Parsed options would carry a key from the developer's own environment.
beforeAll(() => {
  delete process.env.APTOS_API_KEY;
  delete process.env.STREAMD_GATEWAY_TOKEN;
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
  describe("to Redis", () => {
    const REDIS = "redis://streamd:pw-s3cret@127.0.0.1:6379/2";

    it("reads a Redis URL and its stream", () => {
      expect(forward("--to", REDIS, "--stream", "aptos:deposits", TYPE)).toEqual({
        descriptor: { kind: "module", eventType: [TYPE] },
        network: "mainnet",
        to: REDIS,
        stream: "aptos:deposits",
        cursorFile: "streamd-forward.cursor",
        retries: 5,
        timeout: 10000,
        headers: {},
      });
      expect(forward("--to", "rediss://redis.example", "--stream", "s", "--secret", "k", TYPE)).toMatchObject({
        to: "rediss://redis.example",
        stream: "s",
        secret: "k",
      });
      expect(forward("--to", "REDISS://h:6380/", "--stream", "s", TYPE).to).toBe("rediss://h:6380/");
    });

    it.each([
      [["--to", REDIS, TYPE], /^--to redis:\/\/ needs --stream <key>$/],
      [["--to", REDIS, "--stream", "", TYPE], /^--to redis:\/\/ needs --stream <key>$/],
      [["--to", "https://hooks.example.com/in", "--stream", "s", TYPE], /^--stream needs a redis:\/\/ or rediss:\/\/ --to$/],
      [["--to", REDIS, "--stream", "s", "--header", "X: pw-s3cret", TYPE], /^--header is for webhooks/],
      [["--to", "redis://:pw-s3cret@/", "--stream", "s", TYPE], /^--to must be a valid redis:\/\/ or rediss:\/\/ URL with a host$/],
      [["--to", "redis://h/streams/pw-s3cret", "--stream", "s", TYPE], /^--to redis:\/\/ takes only a database number/],
      [["--to", "redis://h/0?password=pw-s3cret", "--stream", "s", TYPE], /^--to redis:\/\/ takes only a database number/],
      [["--to", "redis://h/0#pw-s3cret", "--stream", "s", TYPE], /^--to redis:\/\/ takes only a database number/],
      [["--to", "redis-pw-s3cret", "--stream", "s", TYPE], /^--to must be an http or https URL$/],
    ])("rejects %j without echoing the URL", (argv, pattern) => {
      let error: unknown;
      try {
        forward(...(argv as string[]));
      } catch (err) {
        error = err;
      }
      expect((error as Error).message).toMatch(pattern as RegExp);
      expect(String(error)).not.toContain("pw-s3cret");
    });

    it("keeps alert webhook-only", () => {
      rejects(["alert", "--rules", "r.json", "--to", REDIS], /^--to must be an http or https URL$/);
      rejects(["alert", "--rules", "r.json", "--to", "https://h.example", "--stream", "s"], /Unknown option '--stream'/);
    });
  });

  describe("to NATS", () => {
    const NATS = "nats://streamd:pw-s3cret@127.0.0.1:4222";

    it("reads a NATS URL and its subject", () => {
      expect(forward("--to", NATS, "--subject", "aptos.deposits", TYPE)).toEqual({
        descriptor: { kind: "module", eventType: [TYPE] },
        network: "mainnet",
        to: NATS,
        subject: "aptos.deposits",
        cursorFile: "streamd-forward.cursor",
        retries: 5,
        timeout: 10000,
        headers: {},
      });
      expect(forward("--to", "nats://tok-s3cret@nats.example", "--subject", "s", "--secret", "k", TYPE)).toMatchObject({
        to: "nats://tok-s3cret@nats.example",
        subject: "s",
        secret: "k",
      });
      expect(forward("--to", "NATS://h:4223/", "--subject", "a.b", TYPE).to).toBe("nats://h:4223/");
    });

    it.each([
      [["--to", NATS, TYPE], /^--to nats:\/\/ needs --subject <name>$/],
      [["--to", NATS, "--subject", "", TYPE], /^--to nats:\/\/ needs --subject <name>$/],
      [["--to", NATS, "--subject", "a.*", TYPE], /^--subject must be dot-separated tokens/],
      [["--to", NATS, "--subject", "a.>", TYPE], /^--subject must be dot-separated tokens/],
      [["--to", NATS, "--subject", "a b", TYPE], /^--subject must be dot-separated tokens/],
      [["--to", NATS, "--subject", "a..b", TYPE], /^--subject must be dot-separated tokens/],
      [["--to", NATS, "--subject", ".a", TYPE], /^--subject must be dot-separated tokens/],
      [["--to", NATS, "--subject", "a\tb", TYPE], /^--subject must be dot-separated tokens/],
      [["--to", "https://hooks.example.com/in", "--subject", "s", TYPE], /^--subject needs a nats:\/\/ --to$/],
      [["--to", "redis://h", "--stream", "s", "--subject", "s", TYPE], /^--subject needs a nats:\/\/ --to$/],
      [["--to", NATS, "--subject", "s", "--stream", "s", TYPE], /^--stream needs a redis:\/\/ or rediss:\/\/ --to$/],
      [["--to", NATS, "--subject", "s", "--header", "X: pw-s3cret", TYPE], /^--header is for webhooks/],
      [["--to", "tls://u:pw-s3cret@h:4222", "--subject", "s", TYPE], /^--to tls:\/\/ is not supported/],
      [["--to", "nats+tls://u:pw-s3cret@h:4222", "--subject", "s", TYPE], /^--to tls:\/\/ is not supported/],
      [["--to", "nats://:pw-s3cret@/", "--subject", "s", TYPE], /^--to must be a valid nats:\/\/ URL with a host$/],
      [["--to", "nats://h/pw-s3cret", "--subject", "s", TYPE], /^--to nats:\/\/ takes no path, query or fragment$/],
      [["--to", "nats://h?pass=pw-s3cret", "--subject", "s", TYPE], /^--to nats:\/\/ takes no path, query or fragment$/],
      [["--to", "nats://h#pw-s3cret", "--subject", "s", TYPE], /^--to nats:\/\/ takes no path, query or fragment$/],
      [["--to", "nats://u:pw-s3cret%zz@h", "--subject", "s", TYPE], /^--to nats:\/\/ has a malformed percent-escape/],
      [["--to", "nats://pw-s3cret%zz@h", "--subject", "s", TYPE], /^--to nats:\/\/ has a malformed percent-escape/],
    ])("rejects %j without echoing the URL", (argv, pattern) => {
      let error: unknown;
      try {
        forward(...(argv as string[]));
      } catch (err) {
        error = err;
      }
      expect((error as Error).message).toMatch(pattern as RegExp);
      expect(String(error)).not.toContain("pw-s3cret");
    });

    it("keeps alert webhook-only", () => {
      rejects(["alert", "--rules", "r.json", "--to", NATS], /^--to must be an http or https URL$/);
      rejects(["alert", "--rules", "r.json", "--to", "https://h.example", "--subject", "s"], /Unknown option '--subject'/);
    });
  });

  describe("to Kafka", () => {
    const KAFKA = "kafka://streamd:pw-s3cret@10.0.0.1:9093,b2.example?sasl=scram-sha-512";

    it("reads a Kafka URL and its topic", () => {
      expect(forward("--to", KAFKA, "--topic", "aptos.deposits", TYPE)).toEqual({
        descriptor: { kind: "module", eventType: [TYPE] },
        network: "mainnet",
        to: KAFKA,
        topic: "aptos.deposits",
        cursorFile: "streamd-forward.cursor",
        retries: 5,
        timeout: 10000,
        headers: {},
      });
      expect(forward("--to", "KAFKAS://10.0.0.2:9092", "--topic", "a_b-C.9", "--secret", "k", TYPE)).toMatchObject({
        to: "kafkas://10.0.0.2:9092",
        topic: "a_b-C.9",
        secret: "k",
      });
      expect(forward("--to", "kafka://h", "--topic", "t".repeat(249), TYPE).topic).toHaveLength(249);
    });

    it.each([
      [["--to", KAFKA, TYPE], /^--to kafka:\/\/ needs --topic <name>$/],
      [["--to", KAFKA, "--topic", "", TYPE], /^--to kafka:\/\/ needs --topic <name>$/],
      [["--to", KAFKA, "--topic", "..", TYPE], /^--topic must be 1 to 249/],
      [["--to", KAFKA, "--topic", ".", TYPE], /^--topic must be 1 to 249/],
      [["--to", KAFKA, "--topic", "a b", TYPE], /^--topic must be 1 to 249/],
      [["--to", KAFKA, "--topic", "a/b", TYPE], /^--topic must be 1 to 249/],
      [["--to", KAFKA, "--topic", "t".repeat(250), TYPE], /^--topic must be 1 to 249/],
      [["--to", "https://hooks.example.com/in", "--topic", "t", TYPE], /^--topic needs a kafka:\/\/ or kafkas:\/\/ --to$/],
      [["--to", "nats://h", "--subject", "s", "--topic", "t", TYPE], /^--topic needs a kafka:\/\/ or kafkas:\/\/ --to$/],
      [["--to", KAFKA, "--topic", "t", "--subject", "s", TYPE], /^--subject needs a nats:\/\/ --to$/],
      [["--to", KAFKA, "--topic", "t", "--stream", "s", TYPE], /^--stream needs a redis:\/\/ or rediss:\/\/ --to$/],
      [["--to", KAFKA, "--topic", "t", "--header", "X: pw-s3cret", TYPE], /^--header is for webhooks/],
      [["--to", "kafka://u:pw-s3cret@h", "--topic", "t", TYPE], /^--to kafka:\/\/ with credentials needs \?sasl=plain/],
      [["--to", "kafka://u:pw-s3cret@h?sasl=gssapi", "--topic", "t", TYPE], /^--to kafka:\/\/ with credentials needs \?sasl=plain/],
      [["--to", "kafka://pw-s3cret@h?sasl=plain", "--topic", "t", TYPE], /^--to kafka:\/\/ credentials must be user:password@$/],
      [["--to", "kafka://h?sasl=plain", "--topic", "t", TYPE], /^--to kafka:\/\/ \?sasl= needs user:password@/],
      [["--to", "kafka://u:pw-s3cret@h?sasl=plain&pass=pw-s3cret", "--topic", "t", TYPE], /^--to kafka:\/\/ takes no query but/],
      [["--to", "kafka://u:pw-s3cret@h/pw-s3cret", "--topic", "t", TYPE], /^--to kafka:\/\/ takes no path or fragment/],
      [["--to", "kafka://u:pw-s3cret@h#pw-s3cret", "--topic", "t", TYPE], /^--to kafka:\/\/ takes no path or fragment/],
      [["--to", "kafka://u:pw-s3cret@?sasl=plain", "--topic", "t", TYPE], /^--to must be a valid kafka:\/\/ URL with host\[:port\] brokers$/],
      [["--to", "kafka://u:pw-s3cret@h:?sasl=plain", "--topic", "t", TYPE], /^--to must be a valid kafka:\/\/ URL/],
      [["--to", "kafka://u:pw-s3cret@h:70000?sasl=plain", "--topic", "t", TYPE], /^--to must be a valid kafka:\/\/ URL/],
      [["--to", "kafka://u:pw-s3cret@h,,h2?sasl=plain", "--topic", "t", TYPE], /^--to must be a valid kafka:\/\/ URL/],
      [["--to", "kafka://u:pw-s3cret%zz@h?sasl=plain", "--topic", "t", TYPE], /^--to kafka:\/\/ has a malformed percent-escape/],
    ])("rejects %j without echoing the URL", (argv, pattern) => {
      let error: unknown;
      try {
        forward(...(argv as string[]));
      } catch (err) {
        error = err;
      }
      expect((error as Error).message).toMatch(pattern as RegExp);
      expect(String(error)).not.toContain("pw-s3cret");
    });

    it("keeps alert webhook-only", () => {
      rejects(["alert", "--rules", "r.json", "--to", KAFKA], /^--to must be an http or https URL$/);
      rejects(["alert", "--rules", "r.json", "--to", "https://h.example", "--topic", "t"], /Unknown option '--topic'/);
    });
  });
});

describe("parseKafkaUrl", () => {
  it("reads brokers, TLS and SASL", () => {
    expect(parseKafkaUrl("kafka://h")).toEqual({ brokers: ["h:9092"], ssl: false });
    expect(() => parseKafkaUrl("kafka://[::1]:9092")).toThrow("--to must be a valid kafka:// URL with host[:port] brokers");
    expect(parseKafkaUrl("kafkas://a:1,h,b.example:9094/")).toEqual({
      brokers: ["a:1", "h:9092", "b.example:9094"],
      ssl: true,
    });
    for (const mechanism of ["plain", "scram-sha-256", "scram-sha-512"] as const) {
      expect(parseKafkaUrl(`kafka://us%40er:p%3Aw@h?sasl=${mechanism.toUpperCase()}`).sasl).toEqual({
        mechanism,
        username: "us@er",
        password: "p:w",
      });
    }
  });
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

  it("takes --arbitraries on both chains and beside --check", () => {
    expect(parseCommand(["codegen", "--arbitraries", "0x1::coin"])).toMatchObject({ options: { arbitraries: true } });
    expect(parseCommand(["codegen", "0x1::coin"])).not.toMatchObject({ options: { arbitraries: true } });
    expect(parseCommand(["codegen", "--chain", "sui", "--arbitraries", "--validators", "0x2::kiosk"])).toEqual({
      command: "codegen",
      options: { chain: "sui", modules: [{ address: "0x2", module: "kiosk" }], network: "mainnet", validators: true, arbitraries: true },
    });
    expect(parseCommand(["codegen", "--check", "--out", "events.ts", "--arbitraries", "0x1"])).toEqual({
      command: "codegen",
      options: { modules: [{ address: ADDR }], network: "mainnet", out: "events.ts", arbitraries: true, check: true },
    });
  });

  it("takes --sql sqlite or postgres on both chains and beside --check", () => {
    expect(parseCommand(["codegen", "--sql", "sqlite", "0x1::coin"])).toMatchObject({ options: { sql: "sqlite" } });
    expect(parseCommand(["codegen", "0x1::coin"])).not.toMatchObject({ options: { sql: expect.anything() } });
    expect(parseCommand(["codegen", "--chain", "sui", "--sql", "postgres", "0x2::kiosk"])).toEqual({
      command: "codegen",
      options: { chain: "sui", modules: [{ address: "0x2", module: "kiosk" }], network: "mainnet", sql: "postgres" },
    });
    expect(parseCommand(["codegen", "--check", "--out", "events.ts", "--sql", "postgres", "0x1"])).toEqual({
      command: "codegen",
      options: { modules: [{ address: ADDR }], network: "mainnet", out: "events.ts", sql: "postgres", check: true },
    });
    expect(() => parseCommand(["codegen", "--sql", "mysql", "0x1"])).toThrow("--sql must be sqlite or postgres: mysql");
    expect(() => parseCommand(["codegen", "--sql", "0x1"])).toThrow(UsageError);
  });

  it("takes --check with --out, beside the other flags", () => {
    expect(parseCommand(["codegen", "--check", "--out", "events.ts", "--validators", "0x1"])).toEqual({
      command: "codegen",
      options: { modules: [{ address: ADDR }], network: "mainnet", out: "events.ts", validators: true, check: true },
    });
    expect(parseCommand(["codegen", "--chain", "sui", "--out", "events.ts", "--check", "0x2::kiosk"])).toEqual({
      command: "codegen",
      options: { chain: "sui", modules: [{ address: "0x2", module: "kiosk" }], network: "mainnet", out: "events.ts", check: true },
    });
    expect(parseCommand(["codegen", "--out", "events.ts", "0x1"])).not.toMatchObject({ options: { check: true } });
  });

  it("takes --watch with --out, refusing it without --out, with --check and on Sui", () => {
    expect(parseCommand(["codegen", "--watch", "--out", "events.ts", "--validators", "0x1"])).toEqual({
      command: "codegen",
      options: { modules: [{ address: ADDR }], network: "mainnet", out: "events.ts", validators: true, watch: true },
    });
    expect(parseCommand(["codegen", "--out", "events.ts", "0x1"])).not.toMatchObject({ options: { watch: true } });
    expect(() => parseCommand(["codegen", "--watch", "0x1"])).toThrow(new UsageError("--watch needs --out <file> to rewrite"));
    expect(() => parseCommand(["codegen", "--watch", "--check", "--out", "events.ts", "0x1"])).toThrow(
      new UsageError("--watch cannot be combined with --check"),
    );
    expect(() => parseCommand(["codegen", "--chain", "sui", "--watch", "--out", "events.ts", "0x2::kiosk"])).toThrow(
      new UsageError("--watch reads Aptos package publishes only, not --chain sui"),
    );
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
    [["codegen", "--check", "0x1::coin"], /^--check needs --out <file> to compare with$/],
    [["codegen", "--chain", "sui", "--check", "0x2"], /^--check needs --out <file> to compare with$/],
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

describe("parseCommand explain", () => {
  const HASH = `0x${"ab".repeat(32)}`;

  it("takes a hash, --json, a timeout and the read options", () => {
    expect(parseCommand(["explain", HASH])).toEqual({ command: "explain", options: { hash: HASH, network: "mainnet" } });
    expect(parseCommand(["explain", HASH, "--json"])).toEqual({ command: "explain", options: { hash: HASH, network: "mainnet", json: true } });
    expect(parseCommand(["explain", "--network", "testnet", "--node-url", "https://n.example/v1", "--timeout", "500", HASH])).toEqual({
      command: "explain",
      options: { hash: HASH, network: "testnet", nodeUrl: "https://n.example/v1", timeout: 500 },
    });
    expect(parseCommand(["explain", "--help"])).toEqual({ command: "help" });
  });

  it.each([
    [["explain"], /explain needs one transaction hash/],
    [["explain", HASH, HASH], /explain needs one transaction hash/],
    [["explain", "0x12"], /not a transaction hash/],
    [["explain", `0x${"zz".repeat(32)}`], /not a transaction hash/],
    [["explain", HASH, "--timeout", "soon"], /--timeout/],
    [["explain", HASH, "--json=yes"], /json/],
    [["explain", HASH, "--limit", "1"], /limit/],
    [["explain", "--chain", "sui", "Da2zqzrgB4Yis2qEgjVYh5N4Go1tXXDesyo4BKBkonqS"], /explain is not available with --chain sui/],
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
    ["explain", `0x${"ab".repeat(32)}`],
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

describe("parseCommand swaps and sales", () => {
  const options = (...argv: string[]) => {
    const parsed = parseCommand(argv);
    if (parsed.command !== "swaps" && parsed.command !== "sales") throw new Error("expected swaps or sales");
    return parsed.options;
  };

  it("follows by default, with repeatable names and only the flags given", () => {
    expect(options("swaps")).toEqual({ network: "mainnet" });
    expect(options("swaps", "--protocol", "hyperion", "--protocol", "tapp", "--asset", "0xa")).toEqual({
      network: "mainnet", protocol: ["hyperion", "tapp"], asset: "0xa",
    });
    expect(options("sales", "--marketplace", "wapal", "--token", "0xk", "--from-version", "5", "--limit", "3",
      "--poll-interval", "200", "--batch-size", "500", "--cursor-file", "s.cursor")).toEqual({
      network: "mainnet", marketplace: ["wapal"], token: "0xk", fromVersion: 5n, limit: 3, pollInterval: 200, batchSize: 500, cursorFile: "s.cursor",
    });
  });

  it("reads a range with --to-version, and takes the read options", () => {
    expect(options("swaps", "--from-version", "5", "--to-version", "9", "--order", "desc"))
      .toEqual({ network: "mainnet", fromVersion: 5n, toVersion: 9n, order: "desc" });
    expect(options("sales", "--from-version", "5", "--to-version", "head").toVersion).toBe("head");
    expect(options("sales", "--api-key", "k", "--archive", "default").apiKey).toBe("k");
  });

  it.each(["swaps", "sales"])("%s refuses --chain sui", (command) => {
    rejects([command, "--chain", "sui"], new RegExp(`${command} is not available with --chain sui: SuiClient has no ${command} yet`));
  });

  it.each([
    [["swaps", "0x1::coin::CoinDeposit"], /swaps takes no event types/],
    [["swaps", "--protocol", ""], /--protocol must not be empty/],
    [["swaps", "--asset", ""], /--asset must not be empty/],
    [["sales", "--marketplace", "wapal", "--marketplace", ""], /--marketplace must not be empty/],
    [["sales", "--token", ""], /--token must not be empty/],
    [["sales", "--cursor-file", ""], /--cursor-file must not be empty/],
    [["swaps", "--order", "desc"], /--order needs --to-version/],
    [["sales", "--to-version", "9"], /--to-version needs --from-version or --from-time/],
    [["swaps", "--from-version", "9", "--to-version", "5"], /--to-version must not be below --from-version/],
    [["sales", "--from-version", "1", "--to-version", "head", "--cursor-file", "c"], /--cursor-file follows live/],
    [["swaps", "--from-version", "1", "--from-time", "1h"], /set --from-time or --from-version, not both/],
    [["swaps", "--token", "0xk"], /Unknown option '--token'/],
    [["sales", "--asset", "0xa"], /Unknown option '--asset'/],
    [["swaps", "--account", "0x1"], /Unknown option '--account'/],
  ])("rejects %j as a usage error", (args, message) => {
    rejects(args, message);
  });
});

describe("parseCommand activity and trace", () => {
  const options = (...argv: string[]) => {
    const parsed = parseCommand(argv);
    if (parsed.command !== "activity" && parsed.command !== "trace") throw new Error("expected activity or trace");
    return parsed.options;
  };

  it("activity takes one address or name and the walk flags", () => {
    expect(options("activity", "alice.apt")).toEqual({ network: "mainnet", address: "alice.apt" });
    expect(options("activity", "0x1", "--with-asset-info", "--indexer", "https://i.example/v1/graphql",
      "--from-version", "5", "--to-version", "head", "--order", "desc", "--limit", "2")).toEqual({
      network: "mainnet", address: "0x1", withAssetInfo: true, indexer: "https://i.example/v1/graphql",
      fromVersion: 5n, toVersion: "head", order: "desc", limit: 2,
    });
    expect(options("activity", "0x1", "--cursor-file", "a.cursor", "--poll-interval", "200", "--api-key", "k", "--stats"))
      .toMatchObject({ cursorFile: "a.cursor", pollInterval: 200, apiKey: "k", stats: true });
  });

  it("trace defaults to JSON and takes every bound", () => {
    expect(options("trace", "0x1")).toEqual({ network: "mainnet", address: "0x1", format: "json" });
    expect(options("trace", "alice.apt", "--format", "dot", "--hops", "6", "--direction", "out", "--from-version", "5",
      "--to-version", "9", "--min-amount", "1000000", "--asset", "0xa", "--max-nodes", "10", "--max-requests", "50",
      "--stop", "0x2", "--stop", "0x3", "--target", "0x4", "--archive", "default", "--node-url", "https://a.example/v1")).toEqual({
      network: "mainnet", address: "alice.apt", format: "dot", hops: 6, direction: "out", fromVersion: 5n, toVersion: 9n,
      minAmount: 1000000n, asset: "0xa", maxNodes: 10, maxRequests: 50, stop: ["0x2", "0x3"], target: "0x4",
      archive: "default", nodeUrl: "https://a.example/v1",
    });
    expect(options("trace", "0x1", "--to-version", "head").toVersion).toBe("head");
  });

  it.each(["activity", "trace"])("%s refuses --chain sui", (command) => {
    rejects([command, "--chain", "sui", "0x1"], new RegExp(`${command} is not available with --chain sui: SuiClient has no ${command} yet`));
  });

  it.each([
    [["activity"], /activity needs one address or name/],
    [["activity", "0x1", "0x2"], /activity needs one address or name/],
    [["trace", ""], /trace needs one address or name/],
    [["activity", "0x1", "--indexer", "ftp://i.example"], /--indexer must be an http or https URL/],
    [["trace", "0x1", "--indexer", "nope"], /--indexer must be an http or https URL/],
    [["activity", "0x1", "--order", "desc"], /--order needs --to-version/],
    [["activity", "0x1", "--to-version", "9"], /--to-version needs --from-version or --from-time/],
    [["activity", "0x1", "--from-version", "1", "--to-version", "2", "--cursor-file", "c"], /--cursor-file follows live/],
    [["activity", "0x1", "--asset", "0xa"], /Unknown option '--asset'/],
    [["trace", "0x1", "--hops", "0"], /--hops must be an integer from 1 to 6/],
    [["trace", "0x1", "--hops", "7"], /--hops must be an integer from 1 to 6/],
    [["trace", "0x1", "--direction", "up"], /--direction must be out, in or both/],
    [["trace", "0x1", "--format", "svg"], /--format must be json or dot/],
    [["trace", "0x1", "--min-amount", "1.5"], /--min-amount must be a non-negative integer/],
    [["trace", "0x1", "--max-nodes", "0"], /--max-nodes must be an integer from 1/],
    [["trace", "0x1", "--max-requests", "x"], /--max-requests must be an integer from 1/],
    [["trace", "0x1", "--from-version", "9", "--to-version", "5"], /--to-version must not be below --from-version/],
    [["trace", "0x1", "--stop", ""], /--stop must not be empty/],
    [["trace", "0x1", "--target", ""], /--target must not be empty/],
    [["trace", "0x1", "--asset", ""], /--asset must not be empty/],
    [["trace", "0x1", "--limit", "1"], /Unknown option '--limit'/],
    [["trace", "0x1", "--from-time", "1h"], /Unknown option '--from-time'/],
  ])("rejects %j as a usage error", (args, message) => {
    rejects(args, message);
  });
});

describe("parseCommand holders", () => {
  const holders = (...argv: string[]) => {
    const parsed = parseCommand(["holders", ...argv]);
    if (parsed.command !== "holders") throw new Error("expected a holders command");
    return parsed.options;
  };

  it("defaults to JSON and leaves the checks to the client", () => {
    expect(holders("0xabc", "--at", "8120000000")).toEqual({ network: "mainnet", asset: "0xabc", at: 8120000000n, format: "json" });
  });

  it("takes every flag", () => {
    expect(holders("0xabc", "--at", "0", "--format", "csv", "--verify", "stores", "--max-requests", "40",
      "--indexer", "https://i.example/v1/graphql", "--api-key", "k", "--archive", "default", "--stats")).toEqual({
      network: "mainnet", asset: "0xabc", at: 0n, format: "csv", verify: "stores", maxRequests: 40,
      indexer: "https://i.example/v1/graphql", apiKey: "k", archive: "default", stats: true,
    });
  });

  it("refuses --chain sui", () => {
    rejects(["holders", "--chain", "sui", "0xabc", "--at", "1"], /holders is not available with --chain sui/);
  });

  it.each([
    [["holders", "--at", "1"], /holders needs one asset/],
    [["holders", "0xa", "0xb", "--at", "1"], /holders needs one asset/],
    [["holders", "0xabc"], /holders needs --at <version>/],
    [["holders", "0xabc", "--at", "head"], /--at must be a non-negative integer/],
    [["holders", "0xabc", "--at", "1", "--verify", "all"], /--verify must be none, supply or stores/],
    [["holders", "0xabc", "--at", "1", "--format", "tsv"], /--format must be json or csv/],
    [["holders", "0xabc", "--at", "1", "--max-requests", "0"], /--max-requests must be an integer from 1/],
    [["holders", "0xabc", "--at", "1", "--indexer", "ftp://i.example"], /--indexer must be an http or https URL/],
    [["holders", "0xabc", "--at", "1", "--limit", "5"], /Unknown option '--limit'/],
  ])("rejects %j as a usage error", (args, message) => {
    rejects(args, message);
  });
});

describe("parseCommand positions", () => {
  const A = `0x${"a".repeat(64)}`;
  const positions = (...argv: string[]) => {
    const parsed = parseCommand(["positions", ...argv]);
    if (parsed.command !== "positions") throw new Error("expected a positions command");
    return parsed.options;
  };

  it("defaults to no webhook and leaves the rest to the client", () => {
    expect(positions("aave")).toEqual({ network: "mainnet", protocol: "aave", retries: 5, timeout: 10_000, headers: {} });
  });

  it("takes every flag, the threshold exactly and accounts in long form", () => {
    expect(positions("echelon", "--account", "0xa", "--account", A, "--threshold", "1.05", "--refresh-interval", "0",
      "--from-version", "7", "--to", "https://hook.example/p", "--retries", "2", "--timeout", "500",
      "--header", "X-Team: risk", "--secret", "s", "--node-url", "https://n.example/v1", "--api-key", "k", "--stats")).toEqual({
      network: "mainnet", protocol: "echelon", account: [`0x${"0".repeat(63)}a`, A],
      threshold: { numerator: 105n, denominator: 100n }, refreshInterval: 0, fromVersion: 7n,
      to: "https://hook.example/p", retries: 2, timeout: 500, headers: { "x-team": "risk" }, secret: "s",
      nodeUrl: "https://n.example/v1", apiKey: "k", stats: true,
    });
    expect(positions("aave", "--threshold", "2").threshold).toEqual({ numerator: 2n, denominator: 1n });
  });

  it("signs from the environment only with a webhook", () => {
    process.env.STREAMD_WEBHOOK_SECRET = "from-env";
    try {
      expect(positions("aave", "--to", "http://127.0.0.1:9/").secret).toBe("from-env");
      expect(positions("aave").secret).toBeUndefined();
    } finally {
      delete process.env.STREAMD_WEBHOOK_SECRET;
    }
  });

  it.each([
    [["positions"], /positions needs one protocol/],
    [["positions", "aave", "echelon"], /positions needs one protocol/],
    [["positions", "aries"], /positions reads aave or echelon: aries/],
    [["positions", "aave", "--account", "alice.apt"], /Not a valid Aptos address/],
    [["positions", "aave", "--threshold", "1e3"], /--threshold must be a non-negative decimal/],
    [["positions", "aave", "--threshold=-1"], /--threshold must be a non-negative decimal/],
    [["positions", "aave", "--threshold", ".5"], /--threshold must be a non-negative decimal/],
    [["positions", "aave", "--refresh-interval=-1"], /--refresh-interval must be an integer from 0/],
    [["positions", "aave", "--from-version", "0"], /--from-version must be at least 1/],
    [["positions", "aave", "--from-version", "x"], /--from-version must be a non-negative integer/],
    [["positions", "aave", "--secret", "s"], /--secret needs --to <url>/],
    [["positions", "aave", "--retries", "1"], /--retries needs --to <url>/],
    [["positions", "aave", "--header", "A: b"], /--header needs --to <url>/],
    [["positions", "aave", "--to", "ftp://hook.example"], /--to must be an http or https URL/],
    [["positions", "aave", "--cursor-file", "c"], /Unknown option '--cursor-file'/],
    [["positions", "--chain", "sui", "aave"], /positions is not available with --chain sui/],
  ])("rejects %j as a usage error", (args, message) => {
    rejects(args, message);
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

describe("parseCommand inspect", () => {
  const RESOURCE = "0x1::coin::CoinStore<0x1::aptos_coin::AptosCoin>";
  const inspect = (...argv: string[]) => {
    const parsed = parseCommand(["inspect", ...argv]);
    if (parsed.command !== "inspect") throw new Error("expected an inspect command");
    return parsed.options;
  };

  it("takes a range, to the head by default", () => {
    expect(inspect("0x1", "--resource", RESOURCE, "--from-version", "10")).toEqual({
      address: LONG_ADDR,
      resourceType: RESOURCE,
      network: "mainnet",
      format: "json",
      fromVersion: 10n,
      toVersion: "head",
    });
    const bounded = inspect("0x1", "--resource", RESOURCE, "--from-version", "10", "--to-version", "20", "--limit", "3");
    expect([bounded.toVersion, bounded.limit]).toEqual([20n, 3]);
    expect(inspect("0x1", "--resource", RESOURCE, "--from-version", "10", "--to-version", "head").toVersion).toBe("head");
    expect(inspect("0x1", "--resource", RESOURCE, "--from-version", "10", "--to-version", "10").toVersion).toBe(10n);
  });

  it("takes --at, pretty output and the read flags", () => {
    expect(
      inspect("0x1", "--resource", RESOURCE, "--at", "5", "--format", "pretty", "--archive", "default", "--stats", "--api-key", "k"),
    ).toEqual({
      address: LONG_ADDR,
      resourceType: RESOURCE,
      network: "mainnet",
      archive: "default",
      stats: true,
      apiKey: "k",
      format: "pretty",
      at: 5n,
    });
    expect(inspect("0x1", "--resource", RESOURCE, "--at", "0", "--node-url", "https://a.example/v1", "--node-url", "https://b.example/v1")).toMatchObject({
      at: 0n,
      nodeUrl: "https://a.example/v1",
      extraNodeUrls: ["https://b.example/v1"],
    });
  });

  it.each([
    [[], /inspect needs one address/],
    [["0x1", "0x2", "--resource", RESOURCE, "--at", "1"], /inspect needs one address/],
    [["nope", "--resource", RESOURCE, "--at", "1"], /address/i],
    [["0x1", "--at", "1"], /inspect needs --resource/],
    [["0x1", "--resource", "not a type", "--at", "1"], /type/i],
    [["0x1", "--resource", RESOURCE], /inspect needs --at or --from-version/],
    [["0x1", "--resource", RESOURCE, "--to-version", "5"], /inspect needs --at or --from-version/],
    [["0x1", "--resource", RESOURCE, "--at", "1", "--from-version", "1"], /set --at or --from-version, not both/],
    [["0x1", "--resource", RESOURCE, "--at", "1", "--to-version", "2"], /set --at or --to-version, not both/],
    [["0x1", "--resource", RESOURCE, "--at", "1", "--limit", "2"], /set --at or --limit, not both/],
    [["0x1", "--resource", RESOURCE, "--at", "1.5"], /--at must be a non-negative integer/],
    [["0x1", "--resource", RESOURCE, "--from-version", "5", "--to-version", "4"], /--to-version must not be below --from-version/],
    [["0x1", "--resource", RESOURCE, "--from-version", "5", "--to-version", "later"], /--to-version must be a non-negative integer/],
    [["0x1", "--resource", RESOURCE, "--from-version", "5", "--limit", "0"], /--limit must be an integer/],
    [["0x1", "--resource", RESOURCE, "--at", "1", "--format", "yaml"], /--format must be json or pretty/],
    [["0x1", "--resource", RESOURCE, "--at", "1", "--chain", "sui"], /inspect is not available with --chain sui/],
    [["0x1", "--resource", RESOURCE, "--at", "1", "--where", "a==1"], /Unknown option '--where'/],
  ])("rejects %j as a usage error", (args, message) => {
    rejects(["inspect", ...args], message);
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
    ["explain", HASH],
    ["transfers"],
    ["swaps"],
    ["sales"],
    ["activity", ADDR],
    ["trace", ADDR],
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
    ["explain", HASH],
    ["transfers"],
    ["swaps"],
    ["sales"],
    ["activity", ADDR],
    ["trace", ADDR],
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

describe("--stats", () => {
  const READS: string[][] = [
    ["tail", TYPE],
    ["forward", "--to", "https://hook.example", TYPE],
    ["store", "--db", "e.db", TYPE],
    ["query", "--from-version", "1", TYPE],
    ["record", "--from-version", "1", "--to-version", "2", TYPE],
    ["transfers"],
    ["swaps"],
    ["sales"],
    ["activity", "0x1"],
    ["trace", "0x1"],
    ["balances", "0x1"],
    ["watch", "0x1"],
    ["tx", `0x${"a".repeat(64)}`],
    ["explain", `0x${"a".repeat(64)}`],
  ];
  const options = (argv: string[]) => (parseCommand(argv) as unknown as { options: Record<string, unknown> }).options;

  it.each(READS)("%s takes it, and leaves it unset without it", (...argv) => {
    expect(options([...argv, "--stats"]).stats).toBe(true);
    expect("stats" in options(argv)).toBe(false);
  });

  it.each([
    ["compare", "--against", "https://b.example/v1", "--from-version", "1", "--to-version", "2", TYPE],
    ["check", TYPE],
    ["handles", "0x1"],
    ["name", "alice.apt"],
    ["codegen", "0x1::coin"],
  ] as string[][])("is not an option of %s", (...argv) => {
    rejects([...argv, "--stats"], /--stats/);
  });

  it("takes no value", () => {
    rejects(["tail", TYPE, "--stats=yes"], /--stats/);
  });
});

describe("parseCommand alert", () => {
  const HOOK = "https://hooks.example.com/in?token=s3cret";

  function alert(...argv: string[]): AlertOptions {
    const parsed = parseCommand(["alert", ...argv]);
    if (parsed.command !== "alert") throw new Error("expected an alert command");
    return parsed.options;
  }

  it("applies defaults", () => {
    expect(alert("--rules", "rules.json", "--to", HOOK)).toEqual({
      network: "mainnet",
      rules: "rules.json",
      to: HOOK,
      cursorFile: "streamd-alert.cursor",
      retries: 5,
      timeout: 10000,
      headers: {},
    });
  });

  it("reads every flag", () => {
    expect(
      alert(
        "--rules", "r.json",
        "--to", "http://127.0.0.1:9000/hook",
        "--cursor-file", "/tmp/a",
        "--retries", "0",
        "--timeout", "500",
        "--header", "Authorization: Bearer x",
        "--secret", "k",
        "--from-version", "12",
        "--poll-interval", "250",
        "--network", "testnet",
        "--node-url", "http://127.0.0.1:8080/v1",
        "--node-url", "http://127.0.0.1:8081/v1",
        "--archive", "http://127.0.0.1:8082/v1",
        "--api-key", "key",
        "--stats",
      ),
    ).toEqual({
      network: "testnet",
      nodeUrl: "http://127.0.0.1:8080/v1",
      extraNodeUrls: ["http://127.0.0.1:8081/v1"],
      archive: "http://127.0.0.1:8082/v1",
      apiKey: "key",
      stats: true,
      rules: "r.json",
      to: "http://127.0.0.1:9000/hook",
      cursorFile: "/tmp/a",
      retries: 0,
      timeout: 500,
      headers: { authorization: "Bearer x" },
      secret: "k",
      fromVersion: 12n,
      pollInterval: 250,
    });
  });

  it("takes the secret from the environment, --from-time, and --chain sui", () => {
    process.env.STREAMD_WEBHOOK_SECRET = "from-env";
    try {
      expect(alert("--rules", "r", "--to", HOOK).secret).toBe("from-env");
    } finally {
      delete process.env.STREAMD_WEBHOOK_SECRET;
    }
    expect(alert("--rules", "r", "--to", HOOK, "--from-time", "2026-01-01").fromTime).toBe(1767225600000000n);
    expect(alert("--rules", "r", "--to", HOOK, "--chain", "sui")).toMatchObject({ chain: "sui", network: "mainnet" });
    expect(parseCommand(["alert", "--help"])).toEqual({ command: "help" });
  });

  it.each([
    [["alert", "--to", HOOK], /alert needs --rules <file>/],
    [["alert", "--rules", "", "--to", HOOK], /alert needs --rules <file>/],
    [["alert", "--rules", "r"], /alert needs --to <url>/],
    [["alert", "--rules", "r", "--to", HOOK, TYPE], /Unexpected argument/],
    [["alert", "--rules", "r", "--to", HOOK, "--account", ADDR], /Unknown option '--account'/],
    [["alert", "--rules", "r", "--to", HOOK, "--where", "a>1"], /Unknown option '--where'/],
    [["alert", "--rules", "r", "--to", HOOK, "--from-version", "1", "--from-time", "1h"], /--from-time or --from-version, not both/],
    [["alert", "--rules", "r", "--to", HOOK, "--secret", "k", "--header", "X-Streamd-Signature: x"], /x-streamd-signature/],
    [["alert", "--rules", "r", "--to", HOOK, "--cursor-file", ""], /--cursor-file must not be empty/],
    [["alert", "--rules", "r", "--to", "https://u:p@hooks.example.com/"], /--to must not carry credentials/],
    [["alert", "--rules", "r", "--to", HOOK, "--chain", "sui", "--api-key", "k"], /--api-key is not sent to Sui nodes/],
  ])("rejects %j", (argv, message) => {
    rejects(argv as string[], message as RegExp);
  });
});

describe("parseCommand monitor", () => {
  const A = "https://a.example/v1";
  const B = "https://b.example/v1";
  const base = ["monitor", "--node-url", A, "--node-url", B];
  const monitor = (...argv: string[]) => (parseCommand([...base, ...argv]) as { options: MonitorOptions }).options;

  it("defaults to FeeStatement, 10s rounds, a 5m window, 100 versions and JSON", () => {
    expect(parseCommand(base)).toEqual({
      command: "monitor",
      options: {
        descriptor: { kind: "module", eventType: ["0x1::transaction_fee::FeeStatement"] },
        network: "mainnet",
        nodeUrl: A,
        extraNodeUrls: [B],
        interval: 10_000,
        window: 300_000,
        range: 100,
        format: "json",
      },
    });
  });

  it("reads every flag", () => {
    expect(
      monitor("--node-url", A, "--interval", "2000", "--window", "1h", "--range", "500", "--format", "prometheus", "--listen", "9464", "--limit", "3", "--api-key", "k", TYPE),
    ).toEqual({
      descriptor: { kind: "module", eventType: [TYPE] },
      network: "mainnet",
      nodeUrl: A,
      extraNodeUrls: [B, A],
      apiKey: "k",
      interval: 2000,
      window: 3_600_000,
      range: 500,
      format: "prometheus",
      listen: { host: "127.0.0.1", port: 9464 },
      limit: 3,
    });
    expect(monitor("--listen", "0.0.0.0:9464").listen).toEqual({ host: "0.0.0.0", port: 9464 });
    expect(monitor("--listen", "[::1]:9464").listen).toEqual({ host: "::1", port: 9464 });
    expect(monitor("--window", "30s").window).toBe(30_000);
    expect(monitor("--account", ADDR).descriptor).toEqual({ kind: "account", accountAddress: LONG_ADDR });
    expect(parseCommand(["monitor", "--help"])).toEqual({ command: "help" });
  });

  it("never takes APTOS_API_KEY, since every node is a custom URL", () => {
    process.env.APTOS_API_KEY = "from-env";
    try {
      expect("apiKey" in monitor()).toBe(false);
    } finally {
      delete process.env.APTOS_API_KEY;
    }
  });

  it("asks Sui for a descriptor and takes one", () => {
    rejects([...base, "--chain", "sui"], /monitor needs at least one event type, or --account/);
    expect(monitor("--chain", "sui", "0x3::validator::StakingRequestEvent")).toMatchObject({ chain: "sui", descriptor: { kind: "module" } });
  });

  it.each([
    [["monitor", TYPE], /--node-url at least twice/],
    [["monitor", "--node-url", A, TYPE], /--node-url at least twice/],
    [[...base, "--window", "0m"], /--window must be a duration/],
    [[...base, "--window", "5"], /--window must be a duration/],
    [[...base, "--interval", "0"], /--interval/],
    [[...base, "--range", "0"], /--range/],
    [[...base, "--range", "10001"], /--range/],
    [[...base, "--format", "pretty"], /--format must be json or prometheus/],
    [[...base, "--listen", "0"], /--listen must be a port/],
    [[...base, "--listen", "70000"], /--listen must be a port/],
    [[...base, "--listen", "host:"], /--listen must be a port/],
    [[...base, "--limit", "0"], /--limit/],
    [[...base, "--archive", "default"], /--archive/],
    [[...base, "--stats"], /--stats/],
    [[...base, "--cache", "c.db"], /--cache/],
  ])("rejects %j", (argv, message) => {
    rejects(argv as string[], message as RegExp);
  });
});

describe("parseCommand coverage", () => {
  const coverage = (...argv: string[]) => {
    const parsed = parseCommand(["coverage", ...argv]);
    if (parsed.command !== "coverage") throw new Error("expected a coverage command");
    return parsed.options;
  };

  it("takes the package, then everything after -- as the command", () => {
    expect(coverage("./move/pkg", "--", "bun", "test", "--timeout", "5000")).toEqual({
      packageDir: "./move/pkg",
      command: ["bun", "test", "--timeout", "5000"],
      format: "json",
    });
    expect(coverage("--format", "pretty", "pkg", "--", "sh", "-c", "x -- y").command).toEqual(["sh", "-c", "x -- y"]);
    expect(coverage("pkg", "--format", "pretty", "--", "t").format).toBe("pretty");
  });

  it("refuses a missing package, a missing command and a bad format", () => {
    expect(() => parseCommand(["coverage", "--", "bun", "test"])).toThrow(UsageError);
    expect(() => parseCommand(["coverage", "a", "b", "--", "t"])).toThrow(UsageError);
    expect(() => parseCommand(["coverage", "pkg"])).toThrow("coverage needs a test command after --");
    expect(() => parseCommand(["coverage", "pkg", "--"])).toThrow(UsageError);
    expect(() => parseCommand(["coverage", "pkg", "--", ""])).toThrow(UsageError);
    expect(() => parseCommand(["coverage", "pkg", "--format", "yaml", "--", "t"])).toThrow("--format must be json or pretty");
    expect(() => parseCommand(["coverage", "pkg", "--network", "testnet", "--", "t"])).toThrow(UsageError);
  });

  it("shows help", () => {
    expect(parseCommand(["coverage", "--help"])).toEqual({ command: "help" });
  });
});

describe("parseCommand serve", () => {
  function serve(...argv: string[]): ServeOptions {
    const parsed = parseCommand(["serve", ...argv]);
    if (parsed.command !== "serve") throw new Error("expected a serve command");
    return parsed.options;
  }

  it("listens on loopback port 8787 with no token by default", () => {
    const saved = process.env.STREAMD_GATEWAY_TOKEN;
    delete process.env.STREAMD_GATEWAY_TOKEN;
    try {
      expect(serve()).toEqual({ network: "mainnet", listen: { host: "127.0.0.1", port: 8787 } });
    } finally {
      if (saved !== undefined) process.env.STREAMD_GATEWAY_TOKEN = saved;
    }
  });

  it("takes the listen address, token, origins, poll interval and limits", () => {
    expect(
      serve(
        "--listen", "0.0.0.0:9000", "--token", "t", "--allow-origin", "https://app.example", "--allow-origin", "*",
        "--poll-interval", "500", "--max-topics", "8", "--max-connections", "50", "--network", "testnet",
      ),
    ).toEqual({
      network: "testnet",
      listen: { host: "0.0.0.0", port: 9000 },
      token: "t",
      allowOrigins: ["https://app.example", "*"],
      pollInterval: 500,
      maxTopics: 8,
      maxConnections: 50,
    });
    expect(serve("--listen", "localhost:1").listen).toEqual({ host: "localhost", port: 1 });
    expect(serve("--listen", "[::1]:1").listen).toEqual({ host: "::1", port: 1 });
  });

  it("reads the token from STREAMD_GATEWAY_TOKEN", () => {
    const saved = process.env.STREAMD_GATEWAY_TOKEN;
    process.env.STREAMD_GATEWAY_TOKEN = "from-env";
    try {
      expect(serve("--listen", "0.0.0.0:1").token).toBe("from-env");
    } finally {
      if (saved === undefined) delete process.env.STREAMD_GATEWAY_TOKEN;
      else process.env.STREAMD_GATEWAY_TOKEN = saved;
    }
  });

  it.each([
    [["serve", "--listen", "0.0.0.0:9000"], /serve needs --token/],
    [["serve", "--listen", "10.0.0.1:9000"], /serve needs --token/],
    [["serve", "--token", ""], /--token must not be empty/],
    [["serve", "--allow-origin", "https://app.example/"], /--allow-origin must be/],
    [["serve", "--allow-origin", "app.example"], /--allow-origin must be/],
    [["serve", "--max-topics", "0"], /--max-topics must be an integer/],
    [["serve", TYPE], /serve takes no event types/],
    [["serve", "--retries", "2"], /--retries needs --webhooks/],
    [["serve", "--webhook-allow-private"], /--webhook-allow-private needs --webhooks/],
    [["serve", "--webhooks", ""], /--webhooks must not be empty/],
    [["serve", "--webhooks", "w.json", "--batch-size", "0"], /--batch-size must be an integer/],
  ])("rejects %j", (argv, message) => rejects(argv, message));

  it("takes the webhook file and its delivery options", () => {
    expect(serve("--webhooks", "w.json").webhooks).toEqual({ file: "w.json" });
    expect(
      serve("--webhooks", "w.json", "--webhook-allow-private", "--retries", "0", "--timeout", "500", "--batch-size", "10").webhooks,
    ).toEqual({ file: "w.json", allowPrivateTargets: true, retries: 0, timeout: 500, batchSize: 10 });
  });
});
