import { afterEach, describe, expect, it } from "bun:test";
import { readdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import { Account } from "@aptos-labs/ts-sdk";
import { AptosClient } from "@streamd/aptos";
import { StreamdError, StreamdTimeoutError } from "@streamd/core";
import { attachLocalnet, startLocalnet } from "../src/index.js";

const FAKE = new URL("./fixtures/fake-aptos.ts", import.meta.url).pathname;

async function localnetDirs(): Promise<number> {
  return (await readdir(tmpdir())).filter((name) => name.startsWith("streamd-localnet-")).length;
}

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

afterEach(() => {
  delete process.env.FAKE_APTOS;
});

describe("startLocalnet", () => {
  it("serves the node and faucet on the instance's ports, and stop() removes its directory", async () => {
    const before = await localnetDirs();
    const localnet = await startLocalnet({ cli: FAKE, basePort: 23400 });
    expect(localnet.ports?.ready).toBe(23400);
    expect(localnet.nodeUrl).toBe("http://127.0.0.1:23401/v1");
    expect(localnet.faucetUrl).toBe("http://127.0.0.1:23402");
    expect(localnet.client).toBeInstanceOf(AptosClient);
    expect(await localnetDirs()).toBe(before + 1);
    await localnet.stop();
    await localnet.stop();
    expect(await localnetDirs()).toBe(before);
    expect(await fetch("http://127.0.0.1:23400/").then(() => true, () => false)).toBe(false);
  });

  it("removes its directory when the node never becomes ready", async () => {
    process.env.FAKE_APTOS = "never";
    const before = await localnetDirs();
    await expect(startLocalnet({ cli: FAKE, readyTimeout: 800 })).rejects.toThrow(StreamdTimeoutError);
    expect(await localnetDirs()).toBe(before);
  });

  it("stops through await using", async () => {
    let url: string;
    {
      await using localnet = await startLocalnet({ cli: FAKE });
      url = `http://127.0.0.1:${localnet.ports!.ready}/`;
      expect(await fetch(url).then(() => true, () => false)).toBe(true);
    }
    expect(await fetch(url).then(() => true, () => false)).toBe(false);
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
