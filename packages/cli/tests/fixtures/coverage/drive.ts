import { Account, Aptos, AptosConfig, Ed25519PrivateKey, Network } from "@aptos-labs/ts-sdk";

// The test command `streamd coverage` runs: two calls as the publisher, from the environment it sets.
const aptos = new Aptos(new AptosConfig({ network: Network.LOCAL, fullnode: process.env.STREAMD_NODE_URL, clientConfig: { http2: false } }));
const signer = Account.fromPrivateKey({ privateKey: new Ed25519PrivateKey(process.env.STREAMD_PUBLISHER_KEY!) });
const pkg = process.env.STREAMD_PACKAGE_ADDRESS!;

async function call(fn: `${string}::${string}::${string}`, typeArguments: string[], functionArguments: number[]) {
  const transaction = await aptos.transaction.build.simple({ sender: signer.accountAddress, data: { function: fn, typeArguments, functionArguments } });
  const { hash } = await aptos.signAndSubmitTransaction({ signer, transaction });
  await aptos.waitForTransaction({ transactionHash: hash });
  console.log(`called ${fn}`);
}

await call(`${pkg}::coverage::bump`, [], [42]);
await call(`${pkg}::coverage::tag`, ["u8"], []);
await call(`${pkg}::coverage::never`, [], []);
