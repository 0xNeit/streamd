import "./dom.js";
import { describe, expect, it } from "bun:test";
import { createElement } from "react";
import { renderToString } from "react-dom/server";
import { AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { MemoryAdapter, StreamdError } from "@streamd/core";
import { AptosClient, type LendingPosition, type LendingProtocol, type Positions, type PositionsOptions } from "@streamd/aptos";
import { StreamdProvider, usePositions, type UsePositionsOptions, type UsePositionsResult } from "../src/index.js";
import { render, rerender, unmount, waitFor } from "./render.js";

const long = (hex: string) => `0x${hex.padStart(64, "0")}`;
const [ALICE, BOB] = [long("a11ce"), long("b0b")];
const position = (account: string, version: bigint): LendingPosition => ({ protocol: "aave", account, version, assets: [] });

/** One started monitor as the stub hands it out, driven by hand. */
interface Started {
  protocol: LendingProtocol;
  options: PositionsOptions;
  value: Map<string, LendingPosition>;
  active: boolean;
  unsubscribed: number;
  resolve: () => void;
  reject: (err: Error) => void;
}

/** An `AptosClient` whose `positions` returns a hand-driven handle, or throws `refuse`. */
function setup() {
  const client = new AptosClient({
    adapter: new MemoryAdapter([]),
    aptos: new AptosConfig({ network: Network.DEVNET, clientConfig: { http2: false } }),
  });
  const started: Started[] = [];
  const stub = { refuse: undefined as Error | undefined };
  client.positions = (protocol, options = {}) => {
    if (stub.refuse) throw stub.refuse;
    const value = new Map<string, LendingPosition>();
    let resolve!: () => void;
    let reject!: (err: Error) => void;
    const ready = new Promise<ReadonlyMap<string, LendingPosition>>((res, rej) => {
      resolve = () => res(value);
      reject = rej;
    });
    const s: Started = { protocol, options, value, active: true, unsubscribed: 0, resolve, reject };
    started.push(s);
    const handle: Positions = {
      value,
      get: (account) => value.get(account),
      ready,
      get active() {
        return s.active;
      },
      unsubscribe: () => void s.unsubscribed++,
      [Symbol.dispose]: () => void s.unsubscribed++,
    };
    return handle;
  };
  const results: UsePositionsResult[] = [];
  function Monitor({ protocol, options }: { protocol: LendingProtocol | null | undefined; options: UsePositionsOptions }) {
    results.push(usePositions(protocol, { ...options }));
    return null;
  }
  const el = (protocol: LendingProtocol | null | undefined, options: UsePositionsOptions = {}) =>
    createElement(StreamdProvider, { client }, createElement(Monitor, { protocol, options }));
  return { el, started, stub, results, last: () => results.at(-1)! };
}

describe("usePositions", () => {
  it("waits on null and undefined", async () => {
    const t = setup();
    const root = await render(t.el(null));
    await rerender(root, t.el(undefined));
    expect(t.last()).toEqual({ positions: new Map(), error: undefined, status: "idle" });
    expect(t.started).toHaveLength(0);
    await unmount(root);
  });

  it("goes from loading to live on ready, then shows each change in a fresh map", async () => {
    const t = setup();
    const seen: string[] = [];
    const root = await render(t.el("aave", { account: ALICE, onChange: (p) => seen.push(`first ${p.version}`) }));
    await waitFor(() => expect(t.last().status).toBe("loading"));
    const s = t.started[0]!;
    expect(s.protocol).toBe("aave");
    expect(s.options.account).toBe(ALICE);
    s.value.set(ALICE, position(ALICE, 1n));
    s.resolve();
    await waitFor(() => expect(t.last().status).toBe("live"));
    const first = t.last().positions;
    expect(first).toEqual(new Map([[ALICE, position(ALICE, 1n)]]));
    expect(first).not.toBe(s.value);

    await rerender(root, t.el("aave", { account: ALICE, onChange: (p) => seen.push(`latest ${p.version}`) }));
    s.value.set(BOB, position(BOB, 2n));
    s.options.onChange!(position(BOB, 2n));
    await waitFor(() => expect(t.last().positions.size).toBe(2));
    expect(first.size).toBe(1);
    expect(seen).toEqual(["latest 2"]);
    expect(t.started).toHaveLength(1);
    await unmount(root);
    expect(s.unsubscribed).toBe(1);
  });

  it("restarts on a changed protocol or options by value", async () => {
    const t = setup();
    const root = await render(t.el("aave", { account: [ALICE] }));
    await rerender(root, t.el("aave", { account: [ALICE] }));
    expect(t.started).toHaveLength(1);
    await rerender(root, t.el("aave", { account: [BOB] }));
    await rerender(root, t.el("echelon", { account: [BOB] }));
    expect(t.started.map((s) => s.protocol)).toEqual(["aave", "aave", "echelon"]);
    expect(t.started.map((s) => s.unsubscribed)).toEqual([1, 1, 0]);
    await unmount(root);
  });

  it("stops when ready rejects, and when an error arrives with the monitor inactive", async () => {
    const t = setup();
    const root = await render(t.el("aave"));
    await waitFor(() => expect(t.started).toHaveLength(1));
    t.started[0]!.reject(new StreamdError("start read failed"));
    await waitFor(() => expect(t.last().status).toBe("stopped"));
    expect(t.last().error?.message).toBe("start read failed");

    await rerender(root, t.el("echelon"));
    const s = t.started[1]!;
    s.resolve();
    await waitFor(() => expect(t.last().status).toBe("live"));
    s.options.onError!(new StreamdError("one poll failed"));
    await waitFor(() => expect(t.last().error?.message).toBe("one poll failed"));
    expect(t.last().status).toBe("live");
    s.active = false;
    s.options.onError!(new StreamdError("gave up"));
    await waitFor(() => expect(t.last().status).toBe("stopped"));
    await unmount(root);
  });

  it("shows a refused start as stopped with its error, passing it to onError", async () => {
    const t = setup();
    t.stub.refuse = new StreamdError("positions needs an adapter that implements view and getHeadVersion");
    const errors: Error[] = [];
    const root = await render(t.el("aave", { onError: (err) => errors.push(err) }));
    await waitFor(() => expect(t.last().status).toBe("stopped"));
    expect(t.last().error).toBe(t.stub.refuse);
    expect(errors).toEqual([t.stub.refuse]);
    await unmount(root);
  });

  it("starts nothing during server rendering", () => {
    const t = setup();
    renderToString(t.el("aave"));
    expect(t.started).toHaveLength(0);
    expect(t.last().status).toBe("idle");
  });
});
