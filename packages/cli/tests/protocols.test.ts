import { describe, expect, it } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { generateProtocols, PROTOCOL_MODULES, PROTOCOLS_PATH } from "../scripts/protocols.js";

describe("protocol catalog", () => {
  it("matches the generator's output over the committed ABI snapshots", async () => {
    expect(readFileSync(PROTOCOLS_PATH, "utf8")).toBe(await generateProtocols());
  });

  it("qualifies names that two protocol modules share, by module and then by address", async () => {
    const source = await generateProtocols();
    expect(source).toContain(
      'export const Pool_SwapEvent_0x75b4890de3e312d9425408c43d9a9752b64ab3562a30e89a55bdc568c645920 = defineEvent<Pool_SwapEvent_0x75b4890de3e312d9425408c43d9a9752b64ab3562a30e89a55bdc568c645920>("0x75b4890de3e312d9425408c43d9a9752b64ab3562a30e89a55bdc568c645920::pool::SwapEvent");',
    );
    expect(source).toContain("export const Pool_SwapEvent_0x7730cd28ee1cdc9e999336cbc430f99e7c44397c0aa77516f6f23a78559bb5 =");
    expect(source).toContain("export const LiquidityPool_SwapEvent = defineEvent<LiquidityPool_SwapEvent>(");
    expect(source).toContain("export const PoolV3_SwapEvent = defineEvent<PoolV3_SwapEvent>(");
    expect(source).toContain('export const SwapEventV3 = defineEvent<SwapEventV3>("0x8b4a2c4bb53857c718a04c020b98f8c2e1f99a68b0f57389a8bf5434cd22e05c::pool_v3::SwapEventV3");');
    expect(source).not.toMatch(/^export const SwapEvent /m);
    expect(source).toContain("export const poolEvents_0x75b4890de3e312d9425408c43d9a9752b64ab3562a30e89a55bdc568c645920 = defineEvents({");
    expect(source).toContain("export const lendingEvents = defineEvents({");
  });

  it("gives every export one name", async () => {
    const source = await generateProtocols();
    const consts = [...source.matchAll(/^export const (\w+)/gm)].map((m) => m[1]);
    expect(new Set(consts).size).toBe(consts.length);
  });

  it("has a descriptor for every event in the snapshots of every listed module", async () => {
    const source = await generateProtocols();
    const dir = new URL("../scripts/protocols-abi/", import.meta.url);
    const listed = Object.entries(PROTOCOL_MODULES).flatMap(([a, ms]) => ms.map((m) => `${a}-${m}.json`));
    expect(readdirSync(dir)).toEqual(expect.arrayContaining(listed));
    let events = 0;
    for (const file of listed) {
      const { abi } = JSON.parse(readFileSync(new URL(file, dir), "utf8"));
      for (const s of abi.structs.filter((s: { is_event: boolean }) => s.is_event)) {
        expect(source).toContain(`("${abi.address}::${abi.name}::${s.name}")`);
        events++;
      }
    }
    expect(events).toBeGreaterThan(80);
  });
});
