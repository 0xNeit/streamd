import { describe, expect, it } from "bun:test";
import { parseAlert, serializeAlert } from "../src/alert.js";
import type { AlertPayload } from "../src/alert.js";
import { StreamdError } from "../src/errors.js";

const event = {
  type: "0x1::transaction_fee::FeeStatement",
  data: { total_charge_gas_units: "7" },
  version: 3480218811n,
  sequenceNumber: 0n,
  eventIndex: 2,
  timestamp: 1763130000500000n,
};

const base = { rule: "fee", windowStart: 1763130000000000n, windowEnd: 1763130060000000n, event };

const countAlert: AlertPayload = { id: "fee:1763130000000000", ...base, threshold: { count: 50n }, value: 50n };
const sumAlert: AlertPayload = { id: "fee:1763130000000000", ...base, threshold: { sum: "amount", gte: -5n }, value: -3n };
const anomalyAlert: AlertPayload = {
  id: "fee:1763130000000000",
  ...base,
  threshold: { anomaly: { metric: "sum", sum: "amount", k: "2.5", baseline: 30, minBaseline: 10, direction: "both" } },
  value: 412n,
  anomaly: { side: "above", median: "201.5", mad: "18", low: "-121.4396", high: "281.5604", baselineWindows: 30 },
};

const wire = (alert: AlertPayload) => JSON.parse(serializeAlert(alert)) as Record<string, any>;
const parseWith = (alert: AlertPayload, edit: (body: Record<string, any>) => void) => {
  const body = wire(alert);
  edit(body);
  return () => parseAlert(JSON.stringify(body));
};

describe("serializeAlert / parseAlert", () => {
  it("round-trips count, sum and anomaly alerts", () => {
    for (const alert of [countAlert, sumAlert, anomalyAlert]) {
      const text = serializeAlert(alert);
      expect(parseAlert(text)).toEqual(alert);
      expect(serializeAlert(parseAlert(text))).toBe(text);
    }
  });

  it("writes integers as decimal strings in the documented key order", () => {
    expect(serializeAlert(anomalyAlert)).toStartWith(
      '{"id":"fee:1763130000000000","rule":"fee","windowStart":"1763130000000000","windowEnd":"1763130060000000",' +
        '"threshold":{"anomaly":{"metric":"sum","sum":"amount","k":"2.5","baseline":"30","minBaseline":"10","direction":"both"}},' +
        '"value":"412","anomaly":{"side":"above","median":"201.5","mad":"18","low":"-121.4396","high":"281.5604","baselineWindows":"30"},' +
        '"event":{"type":"0x1::transaction_fee::FeeStatement"',
    );
    expect(serializeAlert(countAlert)).toContain('"threshold":{"count":"50"},"value":"50","event":');
  });

  const refused: [string, AlertPayload, (b: Record<string, any>) => void][] = [
    ["alert", countAlert, (b) => (b.extra = 1)],
    ["id", countAlert, (b) => (b.id = "fee:1")],
    ["rule", countAlert, (b) => (b.rule = "")],
    ["windowStart", countAlert, (b) => (b.windowStart = 1763130000000000)],
    ["windowEnd", countAlert, (b) => (b.windowEnd = b.windowStart)],
    ["value", countAlert, (b) => (b.value = "1.5")],
    ["threshold.count", countAlert, (b) => (b.threshold.count = "0")],
    ["threshold.gte", sumAlert, (b) => (b.threshold.gte = "01")],
    ["threshold", sumAlert, (b) => delete b.threshold.gte],
    ["threshold.anomaly.k", anomalyAlert, (b) => (b.threshold.anomaly.k = "2.50")],
    ["threshold.anomaly.sum", anomalyAlert, (b) => (b.threshold.anomaly.metric = "count")],
    ["threshold.anomaly.minBaseline", anomalyAlert, (b) => (b.threshold.anomaly.minBaseline = "31")],
    ["threshold.anomaly.direction", anomalyAlert, (b) => (b.threshold.anomaly.direction = "up")],
    ["threshold.anomaly.extra", anomalyAlert, (b) => (b.threshold.anomaly.extra = "1")],
    ["anomaly", anomalyAlert, (b) => delete b.anomaly],
    ["anomaly", countAlert, (b) => (b.anomaly = wire(anomalyAlert).anomaly)],
    ["anomaly.side", anomalyAlert, (b) => (b.anomaly.side = "both")],
    ["anomaly.median", anomalyAlert, (b) => (b.anomaly.median = "201.50")],
    ["anomaly.baselineWindows", anomalyAlert, (b) => (b.anomaly.baselineWindows = "9")],
    ["event", countAlert, (b) => delete b.event],
    ["event", countAlert, (b) => (b.event.version = "x")],
  ];
  for (const [field, alert, edit] of refused) {
    it(`refuses a malformed ${field}, naming it`, () => {
      const run = parseWith(alert, edit);
      expect(run).toThrow(StreamdError);
      expect(run).toThrow(`"${field}`);
    });
  }

  it("refuses a body that is not a JSON object", () => {
    expect(() => parseAlert("not json")).toThrow("not valid JSON");
    expect(() => parseAlert("[]")).toThrow('"alert" must be an object');
  });
});
