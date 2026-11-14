import { describe, expect, it } from "bun:test";
import { adapterMetrics } from "../src/adapters/metrics.js";
import type { AdapterMetricsSnapshot } from "../src/adapters/metrics.js";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { StreamdError } from "../src/errors.js";
import { toPrometheus } from "../src/prometheus.js";

const LATENCY_HELP =
  "# HELP streamd_adapter_latency_seconds Adapter call latency, nearest rank over the most recent 1024 settled calls per method; quantile 1 is the maximum.";

const snapshot: AdapterMetricsSnapshot = {
  fetchEvents: { requests: 12, errors: 3, rateLimited: 1, latency: { p50: 120, p95: 480, p99: 950, max: 1500 } },
  view: { requests: 1, errors: 0, rateLimited: 0 },
};

describe("toPrometheus", () => {
  it("renders every family for a known snapshot", () => {
    expect(toPrometheus(snapshot)).toBe(
      [
        "# HELP streamd_adapter_requests_total Adapter calls started, in-flight ones included.",
        "# TYPE streamd_adapter_requests_total counter",
        'streamd_adapter_requests_total{method="fetchEvents"} 12',
        'streamd_adapter_requests_total{method="view"} 1',
        "# HELP streamd_adapter_errors_total Adapter calls that failed, rate-limited ones included.",
        "# TYPE streamd_adapter_errors_total counter",
        'streamd_adapter_errors_total{method="fetchEvents"} 3',
        'streamd_adapter_errors_total{method="view"} 0',
        "# HELP streamd_adapter_rate_limited_total Failed adapter calls with status 429 or a retry hint.",
        "# TYPE streamd_adapter_rate_limited_total counter",
        'streamd_adapter_rate_limited_total{method="fetchEvents"} 1',
        'streamd_adapter_rate_limited_total{method="view"} 0',
        LATENCY_HELP,
        "# TYPE streamd_adapter_latency_seconds gauge",
        'streamd_adapter_latency_seconds{method="fetchEvents",quantile="0.5"} 0.12',
        'streamd_adapter_latency_seconds{method="fetchEvents",quantile="0.95"} 0.48',
        'streamd_adapter_latency_seconds{method="fetchEvents",quantile="0.99"} 0.95',
        'streamd_adapter_latency_seconds{method="fetchEvents",quantile="1"} 1.5',
        "",
      ].join("\n"),
    );
  });

  it("merges labelled snapshots under one HELP and TYPE per family", () => {
    const text = toPrometheus(
      [
        { snapshot: { fetchEvents: { requests: 2, errors: 0, rateLimited: 0 } }, labels: { node: "0" } },
        { snapshot: { fetchEvents: { requests: 5, errors: 1, rateLimited: 1 } }, labels: { node: "1" } },
        { snapshot: { view: { requests: 1, errors: 0, rateLimited: 0 } }, labels: { role: "archive" } },
      ],
      { labels: { chain: "aptos" } },
    );
    expect(text.split("\n").slice(0, 5)).toEqual([
      "# HELP streamd_adapter_requests_total Adapter calls started, in-flight ones included.",
      "# TYPE streamd_adapter_requests_total counter",
      'streamd_adapter_requests_total{chain="aptos",node="0",method="fetchEvents"} 2',
      'streamd_adapter_requests_total{chain="aptos",node="1",method="fetchEvents"} 5',
      'streamd_adapter_requests_total{chain="aptos",role="archive",method="view"} 1',
    ]);
    expect(text.match(/^# TYPE /gm)).toHaveLength(3);
    expect(text.match(/^# HELP /gm)).toHaveLength(3);
    expect(text).not.toContain("latency");
  });

  it("lets a source's label win over a constant one of the same name, in place", () => {
    const text = toPrometheus([{ snapshot: { view: snapshot.view }, labels: { node: "1", env: "test" } }], {
      labels: { node: "0", app: "x" },
    });
    expect(text).toContain('streamd_adapter_requests_total{node="1",app="x",env="test",method="view"} 1\n');
  });

  it("starts every name with a custom prefix", () => {
    const text = toPrometheus(snapshot, { prefix: "my:app_rpc" });
    expect(text.match(/^my:app_rpc_(requests_total|errors_total|rate_limited_total|latency_seconds)\{/gm)).toHaveLength(10);
    expect(text.match(/^# TYPE my:app_rpc_/gm)).toHaveLength(4);
    expect(text).not.toContain("streamd_adapter");
  });

  it("escapes backslash, double quote and newline in label values", () => {
    const text = toPrometheus([{ snapshot: { view: snapshot.view }, labels: { node: 'a\\b"c\nd' } }]);
    expect(text).toContain('streamd_adapter_requests_total{node="a\\\\b\\"c\\nd",method="view"} 1\n');
    expect(text.split("\n")).toHaveLength(10);
  });

  it("renders nothing when there is nothing to render", () => {
    expect(toPrometheus({})).toBe("");
    expect(toPrometheus([])).toBe("");
    expect(toPrometheus([{ snapshot: {}, labels: { node: "0" } }, { snapshot: {}, labels: { node: "1" } }])).toBe("");
  });

  it("renders a real adapterMetrics snapshot, latency in seconds", async () => {
    const metrics = adapterMetrics(new MemoryAdapter([]));
    await metrics.adapter.fetchEvents({ kind: "module", eventType: "0x1::m::E" }, { fromVersion: 0n });
    const text = toPrometheus(metrics.snapshot());
    expect(text).toContain('streamd_adapter_requests_total{method="fetchEvents"} 1\n');
    const max = Number(/quantile="1"\} (\S+)\n/.exec(text)![1]);
    expect(max).toBe(metrics.snapshot().fetchEvents!.latency!.max / 1000);
  });

  describe("refuses", () => {
    const refuses = (run: () => unknown, message: RegExp) => {
      expect(run).toThrow(StreamdError);
      expect(run).toThrow(message);
    };

    it("an invalid prefix", () => {
      for (const prefix of ["", "1abc", "has-dash", "sp ace"]) refuses(() => toPrometheus(snapshot, { prefix }), /prefix/);
    });

    it("an invalid or reserved label name", () => {
      for (const name of ["1node", "no-de", "__name", "method", "quantile", ""]) {
        refuses(() => toPrometheus(snapshot, { labels: { [name]: "x" } }), /label name/);
        refuses(() => toPrometheus([{ snapshot, labels: { [name]: "x" } }]), /label name/);
      }
    });

    it("a label value that is not a string, and labels that are not an object", () => {
      refuses(() => toPrometheus(snapshot, { labels: { node: 0 as unknown as string } }), /label node must be a string/);
      refuses(() => toPrometheus([{ snapshot, labels: ["x"] as unknown as Record<string, string> }]), /entry 0 labels/);
      refuses(() => toPrometheus(snapshot, { labels: null as unknown as Record<string, string> }), /options.labels/);
    });

    it("input that is not a snapshot or a list of sources", () => {
      refuses(() => toPrometheus(null as unknown as AdapterMetricsSnapshot), /expected a snapshot/);
      refuses(() => toPrometheus("x" as unknown as AdapterMetricsSnapshot), /expected a snapshot/);
      refuses(() => toPrometheus([snapshot] as unknown as []), /entry 0/);
      refuses(() => toPrometheus([{ snapshot: null }] as unknown as []), /entry 0/);
    });

    it("two sources with the same labels, in any order", () => {
      refuses(() => toPrometheus([{ snapshot }, { snapshot }]), /entry 1 repeats/);
      refuses(
        () =>
          toPrometheus([
            { snapshot, labels: { a: "1", b: "2" } },
            { snapshot, labels: { b: "2", a: "1" } },
          ]),
        /entry 1 repeats/,
      );
      refuses(
        () => toPrometheus([{ snapshot }, { snapshot, labels: { node: "0" } }], { labels: { node: "0" } }),
        /entry 1 repeats/,
      );
    });
  });
});
