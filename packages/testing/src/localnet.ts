import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Aptos, AptosConfig, Account, Network } from "@aptos-labs/ts-sdk";
import { AptosClient, type AptosClientConfig } from "@streamd/aptos";
import { StreamdError, StreamdTransactionError, toStreamdError } from "@streamd/core";
import { configOverride, localnetPorts, runLocalnetArgs, type LocalnetPorts } from "./config.js";
import { startNode, type NodeProcess } from "./process.js";
import { buildPublishPayload } from "./publish.js";

/** `AptosClient`'s config without `aptos`, which the localnet supplies. */
export type LocalnetClientConfig = Omit<AptosClientConfig, "aptos">;

export interface LocalnetOptions {
  /** The `aptos` CLI to run (default `"aptos"`, found on `PATH`). */
  cli?: string;
  /** Take eight consecutive ports from here instead of eight free ones the OS picks. */
  basePort?: number;
  /** How long to wait for the node and faucet, in ms (default 60000). */
  readyTimeout?: number;
  /** Config for `client`; `defaults.pollInterval` is 100 unless given. */
  client?: LocalnetClientConfig;
}

export interface AttachOptions {
  /** The node's REST API, with its `/v1`, such as `http://127.0.0.1:8080/v1`. */
  nodeUrl: string;
  /** The faucet, such as `http://127.0.0.1:8081`; `fundedAccount` needs it. */
  faucetUrl?: string;
  /** The `aptos` CLI `publish` compiles with (default `"aptos"`). */
  cli?: string;
  client?: LocalnetClientConfig;
}

export interface PublishOptions {
  /** The account that publishes, and pays for it. */
  account: Account;
  /** Values for the package's named addresses, such as `{ counter: account.accountAddress.toString() }`. */
  namedAddresses?: Readonly<Record<string, string>>;
}

export interface PublishResult {
  transactionHash: string;
  version: bigint;
}

/** A localnet to test against, started by `startLocalnet` or attached by `attachLocalnet`. */
export interface Localnet {
  readonly nodeUrl: string;
  readonly faucetUrl: string | undefined;
  /** The ports a started localnet took; `undefined` when attached. */
  readonly ports: LocalnetPorts | undefined;
  readonly aptos: Aptos;
  readonly client: AptosClient;
  /** A new account funded through the faucet with `amount` octas (default 10 APT). */
  fundedAccount(amount?: number): Promise<Account>;
  /** Compiles the package with the CLI and publishes it from `account`, resolving once committed. */
  publish(packageDir: string, options: PublishOptions): Promise<PublishResult>;
  /** Destroys `client`; a started localnet's node is stopped and its directory removed. Idempotent. */
  stop(): Promise<void>;
  [Symbol.asyncDispose](): Promise<void>;
}

const DEFAULT_FUNDING = 1_000_000_000;
const DEFAULT_READY_TIMEOUT = 60_000;
const ASYNC_DISPOSE: typeof Symbol.asyncDispose =
  Symbol.asyncDispose ?? (Symbol.for("Symbol.asyncDispose") as typeof Symbol.asyncDispose);

/**
 * Starts an Aptos localnet with the installed `aptos` CLI, without Docker,
 * in a fresh temporary directory, and resolves once its node and faucet are
 * ready. Every port is moved per instance, so several can run at once.
 */
export async function startLocalnet(options: LocalnetOptions = {}): Promise<Localnet> {
  const cli = options.cli ?? "aptos";
  const readyTimeout = options.readyTimeout ?? DEFAULT_READY_TIMEOUT;
  if (!Number.isFinite(readyTimeout) || readyTimeout <= 0) {
    throw new StreamdError(`readyTimeout must be a positive number of ms, got ${String(readyTimeout)}`);
  }
  const ports = await localnetPorts(options.basePort);
  const dir = await mkdtemp(join(tmpdir(), "streamd-localnet-"));
  try {
    const override = join(dir, "override.yaml");
    await writeFile(override, configOverride(ports));
    const node = await startNode(cli, runLocalnetArgs(join(dir, "node"), override, ports), `http://127.0.0.1:${ports.ready}/`, readyTimeout);
    const urls = { nodeUrl: `http://127.0.0.1:${ports.api}/v1`, faucetUrl: `http://127.0.0.1:${ports.faucet}` };
    return new RunningLocalnet(urls.nodeUrl, urls.faucetUrl, cli, options.client, ports, async () => {
      await node.stop();
      await rm(dir, { recursive: true, force: true });
    });
  } catch (err) {
    await rm(dir, { recursive: true, force: true });
    throw err;
  }
}

/** A `Localnet` over a node already running; one ledger read checks it answers. */
export async function attachLocalnet(options: AttachOptions): Promise<Localnet> {
  if (typeof options?.nodeUrl !== "string") throw new StreamdError("attachLocalnet needs `nodeUrl`");
  const localnet = new RunningLocalnet(options.nodeUrl, options.faucetUrl, options.cli ?? "aptos", options.client, undefined, async () => {});
  try {
    await localnet.aptos.getLedgerInfo();
  } catch (err) {
    await localnet.stop();
    throw new StreamdError(`No Aptos node answered at ${options.nodeUrl}`, err);
  }
  return localnet;
}

class RunningLocalnet implements Localnet {
  readonly aptos: Aptos;
  readonly client: AptosClient;
  private stopped: Promise<void> | undefined;

  constructor(
    readonly nodeUrl: string,
    readonly faucetUrl: string | undefined,
    private readonly cli: string,
    config: LocalnetClientConfig | undefined,
    readonly ports: LocalnetPorts | undefined,
    private readonly shutdown: () => Promise<void>,
  ) {
    this.aptos = new Aptos(
      new AptosConfig({ network: Network.LOCAL, fullnode: nodeUrl, faucet: faucetUrl, clientConfig: { http2: false } }),
    );
    this.client = new AptosClient({ ...config, aptos: this.aptos, defaults: { pollInterval: 100, ...config?.defaults } });
  }

  async fundedAccount(amount: number = DEFAULT_FUNDING): Promise<Account> {
    if (this.faucetUrl === undefined) throw new StreamdError("fundedAccount needs a localnet with a faucet");
    if (!Number.isSafeInteger(amount) || amount <= 0) {
      throw new StreamdError(`amount must be a positive whole number of octas, got ${String(amount)}`);
    }
    const account = Account.generate();
    // Network.LOCAL points the Indexer at a port a Docker-free localnet does not serve.
    const funded = await this.aptos
      .fundAccount({ accountAddress: account.accountAddress, amount, options: { waitForIndexer: false } })
      .catch((err) => Promise.reject(toStreamdError(err, "faucet funding")));
    if (!funded.success) throw new StreamdError(`The faucet's funding transaction failed: ${funded.vm_status}`);
    return account;
  }

  async publish(packageDir: string, options: PublishOptions): Promise<PublishResult> {
    const account = options?.account;
    if (account?.accountAddress === undefined) throw new StreamdError("publish needs `account`, an Account to publish from");
    const { metadataBytes, moduleBytecode } = await buildPublishPayload(this.cli, packageDir, options.namedAddresses);
    const committed = await (async () => {
      const transaction = await this.aptos.publishPackageTransaction({ account: account.accountAddress, metadataBytes, moduleBytecode });
      const { hash } = await this.aptos.signAndSubmitTransaction({ signer: account, transaction });
      return this.aptos.waitForTransaction({ transactionHash: hash, options: { checkSuccess: false } });
    })().catch((err) => Promise.reject(toStreamdError(err, `publishing ${packageDir}`)));
    const version = BigInt(committed.version);
    if (!committed.success) {
      throw new StreamdTransactionError(`Publishing ${packageDir} failed: ${committed.vm_status}`, committed.hash, version, committed.vm_status);
    }
    return { transactionHash: committed.hash, version };
  }

  stop(): Promise<void> {
    this.stopped ??= (async () => {
      this.client.destroy();
      await this.shutdown();
    })();
    return this.stopped;
  }

  [ASYNC_DISPOSE](): Promise<void> {
    return this.stop();
  }
}
