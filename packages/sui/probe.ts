import { SuiGrpcClient } from "@mysten/sui/grpc";
const g = new SuiGrpcClient({ network: "mainnet", baseUrl: "https://fullnode.mainnet.sui.io:443" });
const SUI = "0x0000000000000000000000000000000000000000000000000000000000000002::sui::SUI";
const { response: info } = await g.ledgerService.getServiceInfo({});
const call = g.ledgerService.listTransactions({ readMask: { paths: ["digest", "balance_changes", "effects.gas_used", "effects.status", "transaction.gas_payment.owner", "transaction.sender"] }, startCheckpoint: info.checkpointHeight! - 60n, options: { limit: 500, ordering: 0 } });
let n = 0, gasOnly = 0, noPayerEntry = 0, sumZero = 0, sumNonZero = 0, failed = 0, failedClean = 0, missingGas = 0, sponsored = 0; const examples: string[] = [];
for await (const f of call.responses) {
  const t = f.transaction; if (!t) continue; n++;
  const gu = t.effects?.gasUsed; const payer = t.transaction?.gasPayment?.owner;
  if (!gu || !payer) { missingGas++; continue; }
  if (payer !== t.transaction?.sender) sponsored++;
  const gas = gu.computationCost! + gu.storageCost! - gu.storageRebate!;
  const sui = t.balanceChanges.filter(b => b.coinType === SUI).map(b => ({ a: b.address!, v: BigInt(b.amount!) }));
  const p = sui.find(b => b.a === payer); if (!p) noPayerEntry++;
  const adj = sui.map(b => b.a === payer ? { ...b, v: b.v + gas } : b); if (!p) adj.push({ a: payer, v: gas });
  const nz = adj.filter(b => b.v !== 0n);
  if (t.effects?.status?.success === false) { failed++; if (nz.length === 0) failedClean++; }
  if (nz.length === 0) gasOnly++;
  const s = nz.reduce((x, b) => x + b.v, 0n); if (nz.length) { if (s === 0n) sumZero++; else { sumNonZero++; if (examples.length < 3) examples.push(t.digest + " " + nz.map(b => b.v).join(",")); } }
}
console.log({ n, missingGas, sponsored, gasOnly, noPayerEntry, failed, failedClean, sumZero, sumNonZero }); console.log(examples);
