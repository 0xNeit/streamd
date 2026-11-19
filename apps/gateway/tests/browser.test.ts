import { describe, expect, it } from "bun:test";

describe("@streamd/gateway/adapter", () => {
  it("bundles for the browser with no node builtins", async () => {
    const result = await Bun.build({ entrypoints: [`${import.meta.dir}/../src/adapter.ts`], target: "browser", external: ["node:*"] });
    expect(result.success).toBe(true);
    const code = await result.outputs[0]!.text();
    expect(code).toContain("GatewayAdapter");
    expect(code).not.toMatch(/["']node:/);
  });
});
