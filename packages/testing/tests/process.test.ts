import { afterEach, describe, expect, it } from "bun:test";
import { StreamdError, StreamdTimeoutError } from "@streamd/core";
import { localnetPorts } from "../src/config.js";
import { startNode } from "../src/process.js";

const FAKE = new URL("./fixtures/fake-aptos.ts", import.meta.url).pathname;

async function start(mode: string, readyTimeout = 5000, stopGrace?: number) {
  process.env.FAKE_APTOS = mode;
  const { ready } = await localnetPorts();
  const url = `http://127.0.0.1:${ready}/`;
  const node = await startNode(FAKE, ["node", "run-localnet", "--ready-server-listen-port", String(ready)], url, readyTimeout, stopGrace);
  return { node, url };
}

async function listening(url: string): Promise<boolean> {
  return fetch(url).then(() => true, () => false);
}

afterEach(() => {
  delete process.env.FAKE_APTOS;
});

describe("startNode", () => {
  it("resolves once the ready server reports nothing left, and stop() ends the process", async () => {
    const { node, url } = await start("ready");
    expect(await listening(url)).toBe(true);
    await node.stop();
    expect(await listening(url)).toBe(false);
    await node.stop();
  });

  it("rejects with the node's last output when it exits before ready", async () => {
    const err = await start("exit").catch((e) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect(err.message).toContain("exited before it was ready");
    expect(err.message).toContain("Address already in use");
  });

  it("kills a node not ready in time and rejects with StreamdTimeoutError", async () => {
    process.env.FAKE_APTOS = "never";
    const { ready } = await localnetPorts();
    const url = `http://127.0.0.1:${ready}/`;
    const err = await startNode(FAKE, ["node", "run-localnet", "--ready-server-listen-port", String(ready)], url, 1000).catch((e) => e);
    expect(err).toBeInstanceOf(StreamdTimeoutError);
    expect(err.message).toContain("not ready after 1000ms");
    expect(await listening(url)).toBe(false);
  });

  it("says the CLI must be installed when it is missing", async () => {
    const err = await startNode("/nonexistent/aptos", [], "http://127.0.0.1:1/", 1000).catch((e) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect(err.message).toContain('The aptos CLI was not found at "/nonexistent/aptos"');
  });

  it("kills a node that ignores SIGINT once the grace period ends", async () => {
    const { node, url } = await start("stubborn", 5000, 300);
    const began = Date.now();
    await node.stop();
    expect(Date.now() - began).toBeGreaterThanOrEqual(250);
    expect(await listening(url)).toBe(false);
  });
});
