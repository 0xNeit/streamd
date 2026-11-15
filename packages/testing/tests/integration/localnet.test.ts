import { describe, expect, it } from "bun:test";
import { connect } from "node:net";
import { StreamdAssertionError, StreamdError, whereField } from "@streamd/core";
import { startLocalnet, type Localnet } from "../../src/index.js";

/**
 * Starts real localnets with the installed `aptos` CLI. Run with
 * `bun run test:localnet`; skipped otherwise, so the unit suite needs no CLI.
 */
const enabled = process.env.STREAMD_LOCALNET === "1";
const COUNTER = new URL("../fixtures/counter", import.meta.url).pathname;

async function bumpAndAssert(localnet: Localnet) {
  const account = await localnet.fundedAccount();
  const address = account.accountAddress.toString();
  const published = await localnet.publish(COUNTER, { account, namedAddresses: { counter: address } });
  expect(published.version).toBeGreaterThan(0n);

  const transaction = await localnet.aptos.transaction.build.simple({
    sender: account.accountAddress,
    data: { function: `${address}::counter::bump`, functionArguments: [42] },
  });
  const { hash } = await localnet.aptos.signAndSubmitTransaction({ signer: account, transaction });
  const bumped = { kind: "module", eventType: `${address}::counter::Bumped` } as const;
  const tx = localnet.client.expectEvents(hash, { decode: ["value"] });
  const [event] = await tx.toEmit(bumped, whereField("value", 42n));
  expect(event.sender).toBe(address);
  expect(event.entryFunction?.function).toBe(`${address}::counter::bump`);
  await tx.notToEmit({ kind: "module", eventType: "0x1::coin::CoinDeposit" });
  await expect(tx.notToEmit(bumped)).rejects.toThrow(StreamdAssertionError);
  expect(() => localnet.client.on(bumped, () => {}, { withNames: true })).toThrow(StreamdError);
}

function listening(port: number): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = connect(port, "127.0.0.1");
    socket.once("connect", () => (socket.destroy(), resolve(true)));
    socket.once("error", () => resolve(false));
  });
}

describe.skipIf(!enabled)("startLocalnet (aptos CLI)", () => {
  it("runs two localnets side by side, publishing to each and asserting its events", async () => {
    const localnets = await Promise.all([startLocalnet(), startLocalnet()]);
    try {
      expect(localnets[0].nodeUrl).not.toBe(localnets[1].nodeUrl);
      await Promise.all(localnets.map(bumpAndAssert));
    } finally {
      await Promise.all(localnets.map((localnet) => localnet.stop()));
    }
    for (const localnet of localnets) {
      for (const port of Object.values(localnet.ports!)) expect(await listening(port)).toBe(false);
    }
  }, 180_000);
});
