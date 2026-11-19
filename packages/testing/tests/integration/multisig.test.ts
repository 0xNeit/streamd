import { describe, expect, it } from "bun:test";
import { AccountAddress, EntryFunction, Hex, MultiSigTransactionPayload, U64, hashValues } from "@aptos-labs/ts-sdk";
import type { Account, InputEntryFunctionData } from "@aptos-labs/ts-sdk";
import type { Multisig, MultisigState } from "@streamd/aptos";
import { startLocalnet, type Localnet } from "../../src/index.js";

/**
 * A 2-of-3 `0x1::multisig_account` on a real localnet, followed by
 * `AptosClient.multisig` through creation, votes, previews, an execution and
 * a rejection. The localnet leaves the multisig timelock feature off, so the
 * timelock views read as none here. Run with `bun run test:localnet`.
 */
const enabled = process.env.STREAMD_LOCALNET === "1";
const CAFE = `0x${"cafe".padStart(64, "0")}`;

const transfer = (amount: number) =>
  new MultiSigTransactionPayload(EntryFunction.build("0x1::aptos_account", "transfer", [], [AccountAddress.from(CAFE), new U64(amount)])).bcsToBytes();

async function submit(localnet: Localnet, signer: Account, data: InputEntryFunctionData) {
  const transaction = await localnet.aptos.transaction.build.simple({ sender: signer.accountAddress, data });
  const { hash } = await localnet.aptos.signAndSubmitTransaction({ signer, transaction });
  const done = await localnet.aptos.waitForTransaction({ transactionHash: hash });
  if (!done.success) throw new Error(`${data.function} failed: ${done.vm_status}`);
}

async function createMultisig(localnet: Localnet, creator: Account, others: Account[]): Promise<string> {
  const [address] = await localnet.aptos.view<[string]>({
    payload: { function: "0x1::multisig_account::get_next_multisig_account_address", functionArguments: [creator.accountAddress] },
  });
  const owners = others.map((o) => o.accountAddress);
  await submit(localnet, creator, { function: "0x1::multisig_account::create_with_owners", functionArguments: [owners, 2, [], []] });
  await submit(localnet, creator, { function: "0x1::aptos_account::transfer", functionArguments: [address, 100_000_000] });
  return AccountAddress.from(address).toStringLong();
}

async function until(multisig: Multisig, check: (state: MultisigState) => boolean) {
  for (let i = 0; i < 100 && !check(multisig.value); i++) await new Promise((r) => setTimeout(r, 100));
  expect(check(multisig.value)).toBe(true);
}

const types = (events: { type: string }[]) => events.map((e) => e.type.split("::").slice(1).join("::"));
const long = (account: Account) => account.accountAddress.toStringLong();

describe.skipIf(!enabled)("AptosClient.multisig (aptos CLI localnet)", () => {
  it("follows a 2-of-3 multisig through votes, previews, an execution and a rejection", async () => {
    const localnet = await startLocalnet();
    try {
      const [a, b, c] = await Promise.all([localnet.fundedAccount(), localnet.fundedAccount(), localnet.fundedAccount()]);
      const m = await createMultisig(localnet, a, [b, c]);
      const seen: string[] = [];
      const multisig = localnet.client.multisig(m, { onChange: (_, e) => seen.push(e.type.split("::")[2]!) });
      const start = await multisig.ready;
      expect(start).toMatchObject({ address: m, owners: [long(b), long(c), long(a)], threshold: 2n, lastResolved: 0n, nextSequenceNumber: 1n });
      expect(start.pending.size).toBe(0);
      expect(start.timelock).toBeUndefined();

      const [first, second] = [transfer(1000), transfer(2000)];
      await submit(localnet, a, { function: "0x1::multisig_account::create_transaction", functionArguments: [m, first] });
      await submit(localnet, a, { function: "0x1::multisig_account::create_transaction_with_hash", functionArguments: [m, hashValues([second])] });
      await until(multisig, (s) => s.pending.size === 2);
      expect(multisig.get(1n)).toMatchObject({ creator: long(a), approvals: [long(a)], call: { function: "0x1::aptos_account::transfer", arguments: [CAFE, "0xe803000000000000"] } });
      expect(multisig.get(2n)).toMatchObject({ payloadHash: Hex.fromHexInput(hashValues([second])).toString() });

      // Owners are listed creator last; b has not voted, so b's approval makes the second.
      const wrapped = await multisig.preview(1n);
      expect(wrapped).toMatchObject({ sender: long(b), partial: false });
      expect(types(wrapped.events)).toEqual(["fungible_asset::Withdraw", "fungible_asset::Deposit", "transaction_fee::FeeStatement"]);
      expect(types(wrapped.executorEvents)).toEqual(["multisig_account::Vote", "multisig_account::TransactionExecutionSucceeded"]);
      const later = await multisig.preview(2n, { payload: second });
      expect(later).toMatchObject({ sender: m, partial: true, executorEvents: [] });
      expect(types(later.events)).toEqual(["fungible_asset::Withdraw", "fungible_asset::Deposit", "transaction_fee::FeeStatement"]);
      await expect(multisig.preview(2n, { payload: first })).rejects.toThrow("does not match");

      await submit(localnet, b, { function: "0x1::multisig_account::approve_transaction", functionArguments: [m, 1] });
      await until(multisig, (s) => s.pending.get(1n)?.approvals.length === 2);
      const approved = await multisig.preview(1n);
      // Now the first owner listed can execute with the two approvals recorded.
      expect(approved).toMatchObject({ sender: long(b), partial: false });
      expect(types(approved.executorEvents)).toEqual(["multisig_account::TransactionExecutionSucceeded"]);

      const execute = await localnet.aptos.transaction.build.simple({
        sender: a.accountAddress,
        data: { multisigAddress: m, function: "0x1::aptos_account::transfer", functionArguments: [CAFE, 1000] },
      });
      await localnet.aptos.waitForTransaction({ transactionHash: (await localnet.aptos.signAndSubmitTransaction({ signer: a, transaction: execute })).hash });
      await until(multisig, (s) => s.lastResolved === 1n);
      expect([...multisig.value.pending.keys()]).toEqual([2n]);

      await submit(localnet, b, { function: "0x1::multisig_account::reject_transaction", functionArguments: [m, 2] });
      await submit(localnet, c, { function: "0x1::multisig_account::reject_transaction", functionArguments: [m, 2] });
      await until(multisig, (s) => s.pending.get(2n)?.rejections.length === 2);
      expect(multisig.get(2n)!.approvals).toEqual([long(a)]);
      await submit(localnet, a, { function: "0x1::multisig_account::execute_rejected_transaction", functionArguments: [m] });
      await until(multisig, (s) => s.lastResolved === 2n && s.pending.size === 0);
      // The executor's own rejection is recorded as a vote before the rejected transaction resolves.
      expect(seen).toEqual(["CreateTransaction", "CreateTransaction", "Vote", "TransactionExecutionSucceeded", "Vote", "Vote", "Vote", "ExecuteRejectedTransaction"]);

      // More than the multisig holds: the execution fails inside, as a real one would.
      await submit(localnet, a, { function: "0x1::multisig_account::create_transaction", functionArguments: [m, transfer(50_000_000_000)] });
      await until(multisig, (s) => s.pending.has(3n));
      const failing = await multisig.preview(3n);
      expect(types(failing.events)).toEqual(["transaction_fee::FeeStatement"]);
      expect(types(failing.executorEvents)).toEqual(["multisig_account::Vote", "multisig_account::TransactionExecutionFailed"]);
      multisig.unsubscribe();

    } finally {
      await localnet.stop();
    }
  }, 240_000);
});
