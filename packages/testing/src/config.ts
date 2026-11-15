import { createServer, type Server } from "node:net";
import { StreamdError } from "@streamd/core";

/** The loopback ports one localnet listens on, every one of them moved off the CLI's defaults. */
export interface LocalnetPorts {
  ready: number;
  api: number;
  faucet: number;
  inspection: number;
  admin: number;
  backup: number;
  validator: number;
  public: number;
}

const PORT_NAMES = ["ready", "api", "faucet", "inspection", "admin", "backup", "validator", "public"] as const;

/**
 * Eight consecutive ports from `basePort`, or eight free loopback ports the OS
 * picks. The picked ports are released before the node binds them, so another
 * process could take one in between; a node that cannot bind exits, and the
 * start rejects with its output.
 */
export async function localnetPorts(basePort?: number): Promise<LocalnetPorts> {
  if (basePort !== undefined) {
    if (!Number.isInteger(basePort) || basePort < 1024 || basePort + PORT_NAMES.length - 1 > 65535) {
      throw new StreamdError(`basePort must be an integer from 1024 to ${65536 - PORT_NAMES.length}, got ${String(basePort)}`);
    }
    return portMap(PORT_NAMES.map((_, i) => basePort + i));
  }
  const servers = await Promise.all(PORT_NAMES.map(() => listenOnFreePort()));
  const ports = servers.map((server) => (server.address() as { port: number }).port);
  await Promise.all(servers.map((server) => new Promise((done) => server.close(done))));
  return portMap(ports);
}

function portMap(ports: number[]): LocalnetPorts {
  return Object.fromEntries(PORT_NAMES.map((name, i) => [name, ports[i]])) as unknown as LocalnetPorts;
}

function listenOnFreePort(): Promise<Server> {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve(server));
  });
}

/**
 * The node config override. The CLI's own flags move only the faucet and the
 * ready server; everything else is fixed in the generated config, so a second
 * instance would collide on it without these.
 */
export function configOverride(ports: LocalnetPorts): string {
  return [
    `api: { address: "127.0.0.1:${ports.api}" }`,
    `inspection_service: { port: ${ports.inspection} }`,
    `admin_service: { port: ${ports.admin} }`,
    `validator_network: { network_id: validator, listen_address: /ip4/127.0.0.1/tcp/${ports.validator}, mutual_authentication: true }`,
    `full_node_networks: [{ network_id: public, listen_address: /ip4/127.0.0.1/tcp/${ports.public} }]`,
    `storage: { backup_service_address: "127.0.0.1:${ports.backup}" }`,
    "",
  ].join("\n");
}

/** The `aptos` arguments that start a Docker-free localnet in `testDir`. */
export function runLocalnetArgs(testDir: string, overridePath: string, ports: LocalnetPorts): string[] {
  return [
    "node",
    "run-localnet",
    "--test-dir",
    testDir,
    "--no-txn-stream",
    "--force-restart",
    "--assume-yes",
    "--faucet-port",
    String(ports.faucet),
    "--ready-server-listen-port",
    String(ports.ready),
    "--test-config-override",
    overridePath,
  ];
}
