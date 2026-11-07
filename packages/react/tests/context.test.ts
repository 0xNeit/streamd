import "./dom.js";
import { describe, expect, it } from "bun:test";
import { createElement } from "react";
import { MemoryAdapter, StreamdClient, StreamdError } from "@streamd/core";
import { StreamdProvider, useStreamdClient } from "../src/index.js";
import { render, unmount } from "./render.js";

function capture(explicit?: StreamdClient) {
  const seen: { client?: StreamdClient; error?: unknown } = {};
  function Probe() {
    try {
      seen.client = useStreamdClient(explicit);
    } catch (err) {
      seen.error = err;
    }
    return null;
  }
  return { seen, Probe };
}

describe("useStreamdClient", () => {
  const a = new StreamdClient({ adapter: new MemoryAdapter() });
  const b = new StreamdClient({ adapter: new MemoryAdapter() });

  it("reads the provider's client", async () => {
    const { seen, Probe } = capture();
    const root = await render(createElement(StreamdProvider, { client: a }, createElement(Probe)));
    expect(seen.client).toBe(a);
    await unmount(root);
  });

  it("prefers an explicit client over the provider's", async () => {
    const { seen, Probe } = capture(b);
    const root = await render(createElement(StreamdProvider, { client: a }, createElement(Probe)));
    expect(seen.client).toBe(b);
    await unmount(root);
  });

  it("throws a StreamdError with neither", async () => {
    const { seen, Probe } = capture();
    const root = await render(createElement(Probe));
    expect(seen.error).toBeInstanceOf(StreamdError);
    expect((seen.error as Error).message).toContain("StreamdProvider");
    await unmount(root);
  });
});
