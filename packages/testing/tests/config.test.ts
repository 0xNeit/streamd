import { describe, expect, it } from "bun:test";
import { StreamdError } from "@streamd/core";
import { configOverride, localnetPorts, runLocalnetArgs } from "../src/config.js";

describe("localnetPorts", () => {
  it("takes eight consecutive ports from basePort", async () => {
    expect(await localnetPorts(20000)).toEqual({
      ready: 20000,
      api: 20001,
      faucet: 20002,
      inspection: 20003,
      admin: 20004,
      backup: 20005,
      validator: 20006,
      public: 20007,
    });
    expect((await localnetPorts(65528)).public).toBe(65535);
  });

  it("refuses a basePort whose block holds a port already taken", async () => {
    const { createServer } = await import("node:net");
    const server = createServer();
    await new Promise<void>((done) => server.listen(0, "127.0.0.1", done));
    const taken = (server.address() as { port: number }).port;
    try {
      await expect(localnetPorts(taken - 3)).rejects.toThrow("A localnet port is not free");
    } finally {
      server.close();
    }
  });

  it("refuses a basePort that is not an integer or leaves the range", async () => {
    for (const bad of [1023, 65529, 20000.5, Number.NaN, -1]) {
      await expect(localnetPorts(bad)).rejects.toThrow(StreamdError);
    }
  });

  it("picks eight distinct free ports when no basePort is given", async () => {
    const ports = Object.values(await localnetPorts());
    expect(ports).toHaveLength(8);
    expect(new Set(ports).size).toBe(8);
    for (const port of ports) expect(port).toBeGreaterThan(0);
  });
});

describe("configOverride", () => {
  it("moves every fixed listener onto the instance's loopback ports", async () => {
    expect(configOverride(await localnetPorts(20000))).toBe(
      [
        'api: { address: "127.0.0.1:20001" }',
        "inspection_service: { port: 20003 }",
        "admin_service: { port: 20004 }",
        "validator_network: { network_id: validator, listen_address: /ip4/127.0.0.1/tcp/20006, mutual_authentication: true }",
        "full_node_networks: [{ network_id: public, listen_address: /ip4/127.0.0.1/tcp/20007 }]",
        'storage: { backup_service_address: "127.0.0.1:20005" }',
        "",
      ].join("\n"),
    );
  });
});

describe("runLocalnetArgs", () => {
  it("starts without Docker or the transaction stream, on the faucet and ready ports given", async () => {
    expect(runLocalnetArgs("/tmp/net", "/tmp/override.yaml", await localnetPorts(20000))).toEqual([
      "node", "run-localnet", "--test-dir", "/tmp/net", "--no-txn-stream", "--force-restart", "--assume-yes",
      "--faucet-port", "20002", "--ready-server-listen-port", "20000", "--test-config-override", "/tmp/override.yaml",
    ]);
  });
});
