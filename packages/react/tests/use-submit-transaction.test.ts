import "./dom.js";
import { describe, expect, it } from "bun:test";
import { StrictMode, createElement } from "react";
import {
  MemoryAdapter,
  StreamdError,
  StreamdNetworkError,
  StreamdTimeoutError,
  StreamdTransactionError,
  type StreamdEvent,
  type TransactionEvents,
} from "@streamd/core";
import { AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { AptosClient } from "@streamd/aptos";
import {
  StreamdProvider,
  useSubmitTransaction,
  type SubmittedTransaction,
  type UseSubmitTransactionOptions,
  type UseSubmitTransactionResult,
} from "../src/index.js";
import { OTHER, TYPE, ev } from "./fixtures.js";
import { render, rerender, unmount, waitFor } from "./render.js";

const A = `0x${"a".repeat(64)}`;
const FAILED = `0x${"c".repeat(64)}`;

/** Passes the Aptos client's built-transaction check; `fail` makes the fake simulation fail. */
interface Tx {
  rawTransaction: object;
  fail?: boolean;
  throws?: boolean;
}
const TX: Tx = { rawTransaction: {} };

function tagged(version: bigint, hash: string, type = TYPE): StreamdEvent {
  return { ...ev(version, type), transactionHash: hash };
}

function setup() {
  const memory = new MemoryAdapter([tagged(1n, A), { ...tagged(1n, A, OTHER), eventIndex: 1 }]);
  const simulations: unknown[] = [];
  const held = { simulation: undefined as Promise<void> | undefined, lookup: undefined as Promise<void> | undefined };
  const adapter = {
    fetchEvents: memory.fetchEvents.bind(memory),
    async getTransactionEvents(hash: string): Promise<TransactionEvents | undefined> {
      await held.lookup;
      if (hash === FAILED) return { version: 3n, success: false, vmStatus: "Move abort: 0x1", events: [] };
      return memory.getTransactionEvents(hash);
    },
    async simulateTransactionEvents(simulation: { transaction: Tx }): Promise<TransactionEvents> {
      simulations.push(simulation);
      await held.simulation;
      if (simulation.transaction.throws) throw new StreamdNetworkError("rate limited");
      if (simulation.transaction.fail) return { version: 9n, success: false, vmStatus: "EINSUFFICIENT_BALANCE", events: [] };
      return { version: 9n, success: true, vmStatus: "Executed successfully", events: [ev(9n), { ...ev(9n, OTHER), eventIndex: 1 }] };
    },
  };
  const client = new AptosClient({ adapter, aptos: new AptosConfig({ network: Network.DEVNET, clientConfig: { http2: false } }) });
  const signals: AbortSignal[] = [];
  const eventsOf = client.eventsOf.bind(client);
  client.eventsOf = ((h: string, d: undefined, o: { signal?: AbortSignal }) => {
    signals.push(o.signal!);
    return eventsOf(h, d, o);
  }) as typeof client.eventsOf;
  const signed: Tx[] = [];
  const walletSignals: AbortSignal[] = [];
  let answer: () => Promise<SubmittedTransaction> = async () => A;
  const results: UseSubmitTransactionResult<Tx, unknown>[] = [];
  function Sender({ options }: { options?: Partial<UseSubmitTransactionOptions<Tx>> }) {
    results.push(
      useSubmitTransaction<Tx>({
        signAndSubmit: (tx, { signal }) => {
          signed.push(tx);
          walletSignals.push(signal);
          return answer();
        },
        ...options,
      }),
    );
    return null;
  }
  const el = (options?: Partial<UseSubmitTransactionOptions<Tx>>) =>
    createElement(StreamdProvider, { client }, createElement(Sender, { options }));
  return {
    client,
    simulations,
    signed,
    signals,
    walletSignals,
    results,
    el,
    last: () => results.at(-1)!,
    answer: (fn: () => Promise<SubmittedTransaction>) => (answer = fn),
    /** Holds the next simulation or lookups until the returned function is called. */
    hold: (what: keyof typeof held) => {
      let release!: () => void;
      held[what] = new Promise<void>((r) => (release = r));
      return () => {
        held[what] = undefined;
        release();
      };
    },
  };
}

async function submitted(t: ReturnType<typeof setup>, tx: Tx = TX) {
  let promise!: ReturnType<UseSubmitTransactionResult<Tx, unknown>["submit"]>;
  await waitFor(() => {
    promise = t.last().submit(tx);
  });
  const final = await promise;
  await waitFor(() => expect(t.last().status).toBe(final.status));
  return final;
}

describe("useSubmitTransaction", () => {
  it("previews, signs, then returns the committed transaction's events", async () => {
    const t = setup();
    const root = await render(t.el());
    expect(t.last().status).toBe("idle");
    const releaseSimulation = t.hold("simulation");
    const releaseLookup = t.hold("lookup");
    let releaseWallet!: () => void;
    t.answer(() => new Promise((r) => (releaseWallet = () => r(A))));
    const promise = t.last().submit(TX);
    await waitFor(() => expect(t.last().status).toBe("previewing"));
    releaseSimulation();
    await waitFor(() => expect(t.last().status).toBe("signing"));
    expect(t.last().preview).toHaveLength(2);
    releaseWallet();
    await waitFor(() => expect(t.last().status).toBe("confirming"));
    expect(t.last().hash).toBe(A);
    releaseLookup();
    const final = await promise;
    await waitFor(() => expect(t.last().status).toBe("confirmed"));
    expect(final.preview!.map((e) => e.version)).toEqual([9n, 9n]);
    expect(final.hash).toBe(A);
    expect(final.events!.map((e) => e.type)).toEqual([TYPE, OTHER]);
    expect(final.error).toBeUndefined();
    expect(t.signed).toEqual([TX]);
    const { submit, reset, ...shown } = t.last();
    expect(shown).toEqual(final);
    await unmount(root);
  });

  it("narrows the preview and the events by descriptor", async () => {
    const t = setup();
    const root = await render(t.el({ descriptor: { kind: "module", eventType: OTHER } }));
    const final = await submitted(t);
    expect(final.preview!.map((e) => e.type)).toEqual([OTHER]);
    expect(final.events!.map((e) => e.type)).toEqual([OTHER]);
    await unmount(root);
  });

  it("reports a transaction that failed on chain as failed with its status", async () => {
    const t = setup();
    t.answer(async () => FAILED);
    const root = await render(t.el());
    const final = await submitted(t);
    expect(final.status).toBe("failed");
    expect(final.error).toBeInstanceOf(StreamdTransactionError);
    expect(final.vmStatus).toBe("Move abort: 0x1");
    expect(final.hash).toBe(FAILED);
    expect(final.events).toBeUndefined();
    await unmount(root);
  });

  it("stops before signing when the preview fails", async () => {
    const t = setup();
    const root = await render(t.el());
    const final = await submitted(t, { ...TX, fail: true });
    expect(final.status).toBe("failed");
    expect(final.error).toBeInstanceOf(StreamdTransactionError);
    expect(final.previewError).toBe(final.error);
    expect(final.vmStatus).toBe("EINSUFFICIENT_BALANCE");
    expect(final.hash).toBeUndefined();
    expect(t.signed).toEqual([]);
    await unmount(root);
  });

  it("submits after a failed preview with submitOnPreviewFailure, keeping the reason", async () => {
    const t = setup();
    const root = await render(t.el({ submitOnPreviewFailure: true }));
    const final = await submitted(t, { ...TX, throws: true });
    expect(final.status).toBe("confirmed");
    expect(final.previewError).toBeInstanceOf(StreamdNetworkError);
    expect(final.preview).toBeUndefined();
    expect(final.error).toBeUndefined();
    expect(final.vmStatus).toBeUndefined();
    expect(t.signed).toHaveLength(1);
    await unmount(root);
  });

  it("skips the simulation with preview: false", async () => {
    const t = setup();
    const root = await render(t.el({ preview: false }));
    const final = await submitted(t);
    expect(final.status).toBe("confirmed");
    expect(t.simulations).toEqual([]);
    expect(t.results.some((r) => r.status === "previewing")).toBe(false);
    await unmount(root);
  });

  it("takes a hash, a { hash } or a { digest }, and fails on anything else", async () => {
    for (const answer of [A, { hash: A }, { digest: A }]) {
      const t = setup();
      t.answer(async () => answer);
      const root = await render(t.el());
      expect((await submitted(t)).hash).toBe(A);
      await unmount(root);
    }
    for (const answer of [undefined, "", { hash: 1 }, { transactionHash: A }]) {
      const t = setup();
      t.answer(async () => answer as SubmittedTransaction);
      const root = await render(t.el());
      const final = await submitted(t);
      expect(final.status).toBe("failed");
      expect(final.error).toBeInstanceOf(StreamdError);
      expect(final.error!.message).toContain("hash or digest");
      expect(final.hash).toBeUndefined();
      await unmount(root);
    }
  });

  it("fails the run when signAndSubmit rejects or throws", async () => {
    const t = setup();
    t.answer(async () => {
      throw new Error("User rejected the request");
    });
    const root = await render(t.el());
    let final = await submitted(t);
    expect(final.status).toBe("failed");
    expect(final.error!.message).toBe("User rejected the request");
    expect(final.vmStatus).toBeUndefined();
    t.answer(() => {
      throw "no wallet";
    });
    final = await submitted(t);
    expect(final.error!.message).toBe("no wallet");
    await unmount(root);
  });

  it("fails on a malformed hash and on a wait that times out", async () => {
    const t = setup();
    t.answer(async () => "not-a-hash");
    const root = await render(t.el({ timeout: 1 }));
    let final = await submitted(t);
    expect(final.status).toBe("failed");
    expect(final.error).toBeInstanceOf(StreamdError);
    t.answer(async () => `0x${"e".repeat(64)}`);
    final = await submitted(t);
    expect(final.error).toBeInstanceOf(StreamdTimeoutError);
    expect(final.vmStatus).toBeUndefined();
    await unmount(root);
  });

  it("refuses a client without previewEvents unless preview is false", async () => {
    const t = setup();
    const bare = Object.assign(Object.create(t.client), { previewEvents: undefined });
    const errors: unknown[] = [];
    function Probe({ preview }: { preview: boolean }) {
      try {
        useSubmitTransaction<Tx>({ client: bare, preview, signAndSubmit: async () => A });
      } catch (err) {
        errors.push(err);
      }
      return null;
    }
    const root = await render(createElement(Probe, { preview: false }));
    expect(errors).toEqual([]);
    await unmount(root);
    const again = await render(createElement(Probe, { preview: true }));
    expect(errors[0]).toBeInstanceOf(StreamdError);
    expect((errors[0] as Error).message).toContain("previewEvents");
    await unmount(again);
  });

  describe("lifetime", () => {
    it("ignores a second submit while one runs, signing once", async () => {
      const t = setup();
      const root = await render(t.el());
      const release = t.hold("lookup");
      const first = t.last().submit(TX);
      const second = t.last().submit({ rawTransaction: {} });
      expect(second).toBe(first);
      release();
      expect((await first).status).toBe("confirmed");
      expect(t.signed).toEqual([TX]);
      const third = await submitted(t);
      expect(third.status).toBe("confirmed");
      expect(t.signed).toHaveLength(2);
      await unmount(root);
    });

    it("never signs when unmounted during the preview", async () => {
      const t = setup();
      const root = await render(t.el());
      const release = t.hold("simulation");
      const promise = t.last().submit(TX);
      await waitFor(() => expect(t.last().status).toBe("previewing"));
      const renders = t.results.length;
      await unmount(root);
      release();
      const final = await promise;
      expect(final.status).toBe("previewing");
      expect(t.signed).toEqual([]);
      expect(t.results.length).toBe(renders);
    });

    it("aborts the wait when unmounted while confirming, still resolving with the hash", async () => {
      const t = setup();
      const root = await render(t.el());
      t.hold("lookup");
      const promise = t.last().submit(TX);
      await waitFor(() => expect(t.last().status).toBe("confirming"));
      const renders = t.results.length;
      await unmount(root);
      expect(t.signals[0]!.aborted).toBe(true);
      expect(t.walletSignals[0]!.aborted).toBe(true);
      const final = await promise;
      expect(final.status).toBe("confirming");
      expect(final.hash).toBe(A);
      expect(t.results.length).toBe(renders);
    });

    it("keeps a hash the wallet returns after unmount, without waiting for it", async () => {
      const t = setup();
      let releaseWallet!: () => void;
      t.answer(() => new Promise((r) => (releaseWallet = () => r({ hash: A }))));
      const root = await render(t.el());
      const promise = t.last().submit(TX);
      await waitFor(() => expect(t.last().status).toBe("signing"));
      await unmount(root);
      releaseWallet();
      const final = await promise;
      expect(final).toMatchObject({ status: "confirming", hash: A });
      expect(t.signals).toEqual([]);
    });

    it("reset abandons a run, returns to idle and ignores the abandoned result", async () => {
      const t = setup();
      const root = await render(t.el());
      const release = t.hold("lookup");
      const abandoned = t.last().submit(TX);
      await waitFor(() => expect(t.last().status).toBe("confirming"));
      t.last().reset();
      await waitFor(() => expect(t.last().status).toBe("idle"));
      expect(t.signals[0]!.aborted).toBe(true);
      expect(t.last().hash).toBeUndefined();
      release();
      expect((await abandoned).status).toBe("confirming");
      const next = await submitted(t);
      expect(next.status).toBe("confirmed");
      expect(t.last().status).toBe("confirmed");
      await unmount(root);
    });

    it("keeps submit and reset stable across renders, and reads options at submit time", async () => {
      const t = setup();
      const root = await render(createElement(StrictMode, null, t.el()));
      const { submit, reset } = t.last();
      await rerender(root, createElement(StrictMode, null, t.el({ descriptor: { kind: "module", eventType: OTHER } })));
      expect(t.last().submit).toBe(submit);
      expect(t.last().reset).toBe(reset);
      const final = await submitted(t);
      expect(final.events!.map((e) => e.type)).toEqual([OTHER]);
      await unmount(root);
    });
  });
});
