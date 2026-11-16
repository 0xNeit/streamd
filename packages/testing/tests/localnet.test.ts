import { afterEach, describe, expect, it } from "bun:test";
import { existsSync } from "node:fs";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { Account } from "@aptos-labs/ts-sdk";
import { AptosClient } from "@streamd/aptos";
import { StreamdError, StreamdNetworkError, StreamdTimeoutError, StreamdTransactionError } from "@streamd/core";
import { attachLocalnet, startLocalnet, type Localnet } from "../src/index.js";

const FAKE = new URL("./fixtures/fake-aptos.ts", import.meta.url).pathname;

/** A node that answers the ledger read and nothing else. */
function fakeNode() {
  return Bun.serve({
    port: 0,
    hostname: "127.0.0.1",
    fetch: (request) =>
      new URL(request.url).pathname === "/v1"
        ? Response.json({ chain_id: 4, epoch: "1", ledger_version: "1", oldest_ledger_version: "0", ledger_timestamp: "1", node_role: "validator", oldest_block_height: "0", block_height: "1" })
        : new Response("not found", { status: 404 }),
  });
}

async function answers(url: string): Promise<boolean> {
  return fetch(url).then(() => true, () => false);
}

/** The arguments and override file the fake CLI was started with. */
async function withLog<T>(run: (log: string) => Promise<T>): Promise<T> {
  const dir = await mkdtemp(join(tmpdir(), "streamd-testing-log-"));
  process.env.FAKE_APTOS_LOG = join(dir, "log.json");
  try {
    return await run(process.env.FAKE_APTOS_LOG);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

afterEach(() => {
  delete process.env.FAKE_APTOS;
  delete process.env.FAKE_APTOS_LOG;
});

describe("startLocalnet", () => {
  it("starts the node in its own directory with the instance's ports, and stop() removes it", async () => {
    await withLog(async (log) => {
      const localnet = await startLocalnet({ cli: FAKE });
      let testDir: string;
      try {
        const ports = localnet.ports!;
        expect(localnet.nodeUrl).toBe(`http://127.0.0.1:${ports.api}/v1`);
        expect(localnet.faucetUrl).toBe(`http://127.0.0.1:${ports.faucet}`);
        expect(localnet.client).toBeInstanceOf(AptosClient);
        const { args, override } = JSON.parse(await readFile(log, "utf8"));
        testDir = args[args.indexOf("--test-dir") + 1];
        expect(dirname(testDir)).toBe(dirname(args[args.indexOf("--test-config-override") + 1]));
        expect(args[args.indexOf("--faucet-port") + 1]).toBe(String(ports.faucet));
        expect(args[args.indexOf("--ready-server-listen-port") + 1]).toBe(String(ports.ready));
        expect(override).toContain(`api: { address: "127.0.0.1:${ports.api}" }`);
        expect(override).toContain(`storage: { backup_service_address: "127.0.0.1:${ports.backup}" }`);
        expect(existsSync(dirname(testDir))).toBe(true);
      } finally {
        await localnet.stop();
      }
      await localnet.stop();
      expect(existsSync(dirname(testDir))).toBe(false);
      expect(await answers(`http://127.0.0.1:${localnet.ports!.ready}/`)).toBe(false);
    });
  });

  it("removes its directory when the node never becomes ready", async () => {
    process.env.FAKE_APTOS = "never";
    await withLog(async (log) => {
      await expect(startLocalnet({ cli: FAKE, readyTimeout: 800 })).rejects.toThrow(StreamdTimeoutError);
      const { args } = JSON.parse(await readFile(log, "utf8"));
      expect(existsSync(dirname(args[args.indexOf("--test-dir") + 1]))).toBe(false);
    });
  });

  it("stops the node and removes its directory when the client config is refused", async () => {
    await withLog(async (log) => {
      await expect(startLocalnet({ cli: FAKE, client: { defaults: [] as never } })).rejects.toThrow(StreamdError);
      const { args } = JSON.parse(await readFile(log, "utf8"));
      expect(existsSync(dirname(args[args.indexOf("--test-dir") + 1]))).toBe(false);
      expect(await answers(`http://127.0.0.1:${args[args.indexOf("--ready-server-listen-port") + 1]}/`)).toBe(false);
    });
  });

  it("stops through await using", async () => {
    let url: string;
    {
      await using localnet = await startLocalnet({ cli: FAKE });
      url = `http://127.0.0.1:${localnet.ports!.ready}/`;
      expect(await answers(url)).toBe(true);
    }
    expect(await answers(url)).toBe(false);
  });

  it("refuses a readyTimeout that is not a positive number", async () => {
    for (const readyTimeout of [0, -1, Number.NaN]) {
      await expect(startLocalnet({ cli: FAKE, readyTimeout })).rejects.toThrow(StreamdError);
    }
  });
});

describe("attachLocalnet", () => {
  it("rejects when no node answers", async () => {
    const err = await attachLocalnet({ nodeUrl: "http://127.0.0.1:1/v1" }).catch((e) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect(err.message).toBe("No Aptos node answered at http://127.0.0.1:1/v1");
  });

  it("attaches to a node that answers, with no ports of its own", async () => {
    const node = fakeNode();
    const localnet = await attachLocalnet({ nodeUrl: `http://127.0.0.1:${node.port}/v1` });
    expect(localnet.ports).toBeUndefined();
    expect(localnet.faucetUrl).toBeUndefined();
    await localnet.stop();
    node.stop(true);
  });

  it("polls for commits every 100 ms unless the config sets a poll interval", async () => {
    const node = fakeNode();
    const nodeUrl = `http://127.0.0.1:${node.port}/v1`;
    const interval = (localnet: Localnet) => (localnet.client as unknown as { defaults: { pollInterval: number } }).defaults.pollInterval;
    const cases: [Localnet, number][] = [
      [await attachLocalnet({ nodeUrl }), 100],
      [await attachLocalnet({ nodeUrl, client: { defaults: { pollInterval: 250 } } }), 250],
      [await attachLocalnet({ nodeUrl, client: { defaultPollInterval: 400 } }), 400],
    ];
    for (const [localnet, expected] of cases) {
      expect(interval(localnet)).toBe(expected);
      await localnet.stop();
    }
    node.stop(true);
  });

  it("refuses fundedAccount without a faucet, and a bad amount, before any request", async () => {
    const node = fakeNode();
    const bare = await attachLocalnet({ nodeUrl: `http://127.0.0.1:${node.port}/v1` });
    await expect(bare.fundedAccount()).rejects.toThrow("fundedAccount needs a localnet with a faucet");
    const funded = await attachLocalnet({ nodeUrl: `http://127.0.0.1:${node.port}/v1`, faucetUrl: "http://127.0.0.1:1" });
    for (const amount of [0, -5, 1.5, 2 ** 60]) {
      await expect(funded.fundedAccount(amount)).rejects.toThrow("amount must be a positive whole number of octas");
    }
    await expect(funded.publish("/pkg", { account: undefined as unknown as Account })).rejects.toThrow("publish needs `account`");
    await bare.stop();
    await funded.stop();
    node.stop(true);
  });
});

describe("Localnet over a stubbed SDK", () => {
  async function stubbed(overrides: Record<string, unknown>) {
    const node = fakeNode();
    const localnet = await attachLocalnet({ nodeUrl: `http://127.0.0.1:${node.port}/v1`, faucetUrl: "http://127.0.0.1:1", cli: FAKE });
    Object.assign(localnet.aptos, overrides);
    return { localnet, done: async () => (await localnet.stop(), node.stop(true)) };
  }
  const submitted = {
    publishPackageTransaction: async () => ({}),
    signAndSubmitTransaction: async () => ({ hash: "0xabc" }),
  };

  it("funds the account without waiting on an Indexer", async () => {
    const calls: unknown[] = [];
    const { localnet, done } = await stubbed({
      fundAccount: async (args: unknown) => (calls.push(args), { success: true }),
    });
    const account = await localnet.fundedAccount(5);
    expect(calls).toEqual([{ accountAddress: account.accountAddress, amount: 5, options: { waitForIndexer: false } }]);
    await done();
  });

  it("rejects a failed or unreachable faucet", async () => {
    const failing = await stubbed({ fundAccount: async () => ({ success: false, vm_status: "OUT_OF_GAS" }) });
    await expect(failing.localnet.fundedAccount()).rejects.toThrow("The faucet's funding transaction failed: OUT_OF_GAS");
    await failing.done();
    const down = await stubbed({ fundAccount: async () => Promise.reject(new Error("ECONNREFUSED")) });
    await expect(down.localnet.fundedAccount()).rejects.toThrow(StreamdNetworkError);
    await down.done();
  });

  it("resolves a committed publish and rejects a failed one with its VM status", async () => {
    const account = Account.generate();
    const ok = await stubbed({ ...submitted, waitForTransaction: async () => ({ success: true, hash: "0xabc", version: "55", vm_status: "Executed successfully" }) });
    expect(await ok.localnet.publish("/pkg", { account })).toEqual({ transactionHash: "0xabc", version: 55n });
    await ok.done();

    const failed = await stubbed({ ...submitted, waitForTransaction: async () => ({ success: false, hash: "0xabc", version: "56", vm_status: "Move abort: EMODULE" }) });
    const err = await failed.localnet.publish("/pkg", { account }).catch((e) => e);
    expect(err).toBeInstanceOf(StreamdTransactionError);
    expect(err.transactionHash).toBe("0xabc");
    expect(err.version).toBe(56n);
    expect(err.vmStatus).toBe("Move abort: EMODULE");
    await failed.done();

    const refused = await stubbed({ ...submitted, signAndSubmitTransaction: async () => Promise.reject(new Error("INSUFFICIENT_BALANCE")) });
    await expect(refused.localnet.publish("/pkg", { account })).rejects.toThrow(StreamdNetworkError);
    await refused.done();
  });
});
