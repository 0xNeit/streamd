import { describe, expect, it } from "bun:test";
import { StreamdNetworkError } from "@streamd/core";
import type { DescriptorCheck, EventTypeStatus } from "@streamd/aptos";
import type { CheckOptions } from "../src/args.js";
import { runCheck } from "../src/check.js";

const options: CheckOptions = {
  descriptor: { kind: "module", eventType: ["0x1::m::E"] },
  network: "mainnet",
};

async function run(check: () => Promise<DescriptorCheck>) {
  const lines: string[] = [];
  const errors: string[] = [];
  const code = await runCheck(options, { check, write: (l) => lines.push(l), error: (m) => errors.push(m) });
  return { code, lines, errors };
}

const report = (ok: boolean, ...statuses: EventTypeStatus[]): DescriptorCheck => ({
  ok,
  types: statuses.map((status, i) => ({ eventType: `0x1::m::T${i}`, status })),
});

describe("runCheck", () => {
  it("prints one aligned line per type and exits 0 when all can be emitted", async () => {
    const { code, lines, errors } = await run(async () => report(true, "event", "legacy"));
    expect(code).toBe(0);
    expect(errors).toEqual([]);
    expect(lines).toEqual(["event          0x1::m::T0\n", "legacy         0x1::m::T1\n"]);
  });

  it("exits 1 when a type cannot be emitted, still printing every line", async () => {
    const { code, lines } = await run(async () => report(false, "event", "not-event", "missing-module", "missing-struct"));
    expect(code).toBe(1);
    expect(lines).toHaveLength(4);
    expect(lines[3]).toBe("missing-struct 0x1::m::T3\n");
  });

  it("reports a failed request and exits 1 with nothing on stdout", async () => {
    const { code, lines, errors } = await run(async () => {
      throw new StreamdNetworkError("fullnode down", 503);
    });
    expect(code).toBe(1);
    expect(lines).toEqual([]);
    expect(errors).toEqual(["streamd: fullnode down"]);
  });

  it("passes the parsed descriptor through", async () => {
    let seen: unknown;
    await runCheck(options, {
      check: async (d) => ((seen = d), report(true)),
      write: () => {},
      error: () => {},
    });
    expect(seen).toEqual(options.descriptor);
  });
});
