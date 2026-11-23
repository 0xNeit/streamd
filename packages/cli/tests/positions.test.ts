import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import type { LendingPosition, PositionsOptions } from "@streamd/aptos";
import { StreamdError, verifyWebhook, WEBHOOK_SIGNATURE_HEADER } from "@streamd/core";
import type { PositionsCommandOptions } from "../src/args.js";
import { alertBody, positionLine, runPositions } from "../src/positions.js";

const A = `0x${"a".repeat(64)}`;
const B = `0x${"b".repeat(64)}`;
const USDC = `0x${"c".repeat(64)}`;

function position(account: string, version: bigint, health?: [bigint, bigint], below?: boolean): LendingPosition {
  return {
    protocol: "aave",
    account,
    version,
    assets: [{ asset: USDC, decimals: 6, supplied: 10n ** 20n, borrowed: 5n, collateral: true }],
    collateralValue: { numerator: 3n, denominator: 2n },
    debtValue: { numerator: 1n, denominator: 1n },
    liquidationThreshold: { numerator: 63n, denominator: 100n },
    ...(health !== undefined && { health: { numerator: health[0], denominator: health[1] } }),
    ...(below !== undefined && { below }),
  };
}

let server: ReturnType<typeof Bun.serve>;
let status: () => number;
let received: { raw: string; signature: string | null }[];

beforeEach(() => {
  received = [];
  status = () => 200;
  server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch: async (req) => {
      received.push({ raw: await req.text(), signature: req.headers.get(WEBHOOK_SIGNATURE_HEADER) });
      return new Response(null, { status: status() });
    },
  });
});

afterEach(() => server.stop(true));

/** A fake `positions()` the test drives through the options it was given. */
function harness(start: () => Promise<LendingPosition[]> = async () => [position(A, 10n, [3n, 2n], false)]) {
  const calls: { protocol: string; options: PositionsOptions }[] = [];
  const out: string[] = [];
  const errors: string[] = [];
  const controller = new AbortController();
  const state = { active: true, unsubscribed: false };
  let given: PositionsOptions = {};
  const io = {
    client: {
      positions: (protocol: string, options: PositionsOptions) => {
        calls.push({ protocol, options });
        given = options;
        const ready = start().then((list) => new Map(list.map((p) => [p.account, p])));
        return {
          ready,
          get active() {
            return state.active;
          },
          unsubscribe: () => void (state.unsubscribed = true),
        };
      },
    },
    write: (line: string) => void out.push(line),
    error: (m: string) => errors.push(m),
    signal: controller.signal,
    retryDelay: 1,
  };
  const run = (options: Partial<PositionsCommandOptions> = {}) =>
    runPositions({ network: "mainnet", protocol: "aave", retries: 0, timeout: 5000, headers: {}, ...options }, io as never);
  const settle = () => new Promise((r) => setTimeout(r, 20));
  return { run, calls, out, errors, controller, state, settle, options: () => given };
}

describe("positionLine", () => {
  it("writes bigints as decimal strings and fractions as n/d", () => {
    expect(positionLine(position(A, 10n, [3n, 2n], false))).toBe(
      `{"protocol":"aave","account":"${A}","version":"10","assets":[{"asset":"${USDC}","decimals":6,"supplied":"100000000000000000000","borrowed":"5","collateral":true}],"collateralValue":"3/2","debtValue":"1/1","liquidationThreshold":"63/100","health":"3/2","below":false}\n`,
    );
  });
});

describe("alertBody", () => {
  it("names the account, side and version in a stable id", () => {
    const body = JSON.parse(alertBody(position(A, 12n, [9n, 10n], true), { numerator: 1n, denominator: 1n }));
    expect(body).toMatchObject({ id: `${A}:below:12`, protocol: "aave", account: A, side: "below", threshold: "1/1", version: "12" });
    expect(body.position.health).toBe("9/10");
    expect(JSON.parse(alertBody(position(A, 13n, [3n, 2n], false), { numerator: 11n, denominator: 10n })).id).toBe(`${A}:above:13`);
  });
});

describe("runPositions", () => {
  it("passes only the options given, then prints the start read and each change", async () => {
    const h = harness(async () => [position(A, 10n, [3n, 2n], false), position(B, 10n)]);
    const done = h.run({ account: [A, B], threshold: { numerator: 11n, denominator: 10n }, refreshInterval: 5000, fromVersion: 7n });
    await h.settle();
    const { onChange, onCross, onError, signal, ...rest } = h.options();
    expect(h.calls[0]!.protocol).toBe("aave");
    expect(rest).toEqual({ account: [A, B], threshold: { numerator: 11n, denominator: 10n }, refreshInterval: 5000, fromVersion: 7n });
    expect(signal).toBe(h.controller.signal);
    expect(typeof onChange).toBe("function");
    expect(onCross).toBeUndefined();
    expect(typeof onError).toBe("function");
    onChange!(position(A, 11n, [5n, 4n], false));
    await h.settle();
    expect(h.out.map((l) => JSON.parse(l).account)).toEqual([A, B, A]);
    expect(JSON.parse(h.out[2]!)).toMatchObject({ version: "11", health: "5/4" });
    h.controller.abort();
    expect(await done).toBe(0);
    expect(h.state.unsubscribed).toBe(true);
  });

  it("POSTs a signed alert per crossing, after the change's line", async () => {
    const h = harness();
    const done = h.run({ to: server.url.href, secret: "s3cret" });
    await h.settle();
    const crossed = position(A, 11n, [9n, 10n], true);
    h.options().onChange!(crossed);
    h.options().onCross!(crossed);
    await h.settle();
    expect(received).toHaveLength(1);
    const { raw, signature } = received[0]!;
    expect(await verifyWebhook(raw, signature, "s3cret")).toBe(true);
    expect(JSON.parse(raw)).toMatchObject({ id: `${A}:below:11`, side: "below", threshold: "1/1", version: "11" });
    expect(h.out).toHaveLength(2);
    h.controller.abort();
    expect(await done).toBe(0);
  });

  it("sends nothing without a webhook", async () => {
    const h = harness();
    const done = h.run();
    await h.settle();
    expect(h.options().onCross).toBeUndefined();
    h.controller.abort();
    expect(await done).toBe(0);
    expect(received).toHaveLength(0);
  });

  it("exits 1 when an alert fails past its retries", async () => {
    status = () => 503;
    const h = harness();
    const done = h.run({ to: server.url.href, retries: 1 });
    await h.settle();
    h.options().onCross!(position(A, 11n, [9n, 10n], true));
    expect(await done).toBe(1);
    expect(received).toHaveLength(2);
    expect(h.errors.at(-1)).toBe(`streamd: the alert for ${A} at 11 was not delivered`);
    expect(h.state.unsubscribed).toBe(true);
  });

  it("exits 1 when the start read fails, reporting it once", async () => {
    const failure = new StreamdError("view refused");
    const h = harness(async () => {
      h.options().onError!(failure);
      throw failure;
    });
    expect(await h.run()).toBe(1);
    expect(h.errors).toEqual(["streamd: view refused"]);
  });

  it("exits 1 once the monitor stops, but not on an error it retries", async () => {
    const h = harness();
    const done = h.run();
    await h.settle();
    h.options().onError!(new StreamdError("one poll failed"));
    await h.settle();
    h.state.active = false;
    h.options().onError!(new StreamdError("gave up"));
    expect(await done).toBe(1);
    expect(h.errors).toEqual(["streamd: one poll failed", "streamd: gave up"]);
  });

  it("exits 1 when the client refuses the call", async () => {
    const h = harness();
    h.calls.push = () => {
      throw new StreamdError("positions needs an adapter that implements view and getHeadVersion");
    };
    expect(await h.run()).toBe(1);
    expect(h.errors[0]).toContain("implements view");
  });

  it("exits 1 when a write fails, and 0 when aborted first", async () => {
    const errors: string[] = [];
    const io = {
      client: { positions: () => ({ ready: Promise.resolve(new Map([[A, position(A, 1n)]])), active: true, unsubscribe() {} }) },
      write: () => {
        throw new Error("EIO");
      },
      error: (m: string) => errors.push(m),
      signal: new AbortController().signal,
    };
    expect(await runPositions({ network: "mainnet", protocol: "aave", retries: 0, timeout: 1, headers: {} }, io as never)).toBe(1);
    expect(errors).toEqual(["streamd: EIO"]);
    const h = harness();
    h.controller.abort();
    expect(await h.run()).toBe(0);
    expect(h.calls).toHaveLength(0);
  });
});
