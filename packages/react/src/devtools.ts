import { createElement as h, useEffect, useRef, useState, type CSSProperties, type ReactNode } from "react";
import {
  StreamdError,
  type AdapterMetrics,
  type AdapterMetricsSnapshot,
  type EventDescriptor,
  type StreamdClient,
  type SubscriptionInfo,
} from "@streamd/core";
import { useStreamdClient } from "./context.js";

export interface StreamdDevtoolsProps {
  /** Use this client instead of the nearest `StreamdProvider`'s. */
  client?: StreamdClient;
  /** One `adapterMetrics` wrapper, or several by a label of your choosing. */
  metrics?: AdapterMetrics | Readonly<Record<string, AdapterMetrics>>;
  /** How often the panel re-reads the client and metrics, in ms. Default 1000. */
  interval?: number;
  /** Whether the panel starts open. Default false: only the toggle shows. */
  defaultOpen?: boolean;
}

interface View {
  rows: SubscriptionInfo[];
  metrics: [string, AdapterMetricsSnapshot][];
  at: number;
}

const CURSOR_CHARS = 24;

function sources(metrics: StreamdDevtoolsProps["metrics"]): [string, AdapterMetrics][] {
  if (metrics === undefined) return [];
  if (typeof (metrics as AdapterMetrics).snapshot === "function") return [["adapter", metrics as AdapterMetrics]];
  return Object.entries(metrics as Record<string, AdapterMetrics>);
}

function summary(d: EventDescriptor): string {
  switch (d.kind) {
    case "module":
      return [d.eventType].flat().join(", ");
    case "handle":
      return d.creationNumber !== undefined
        ? `${d.accountAddress} handle #${d.creationNumber}`
        : `${d.accountAddress} ${d.eventHandleStruct}.${d.fieldName}`;
    case "account":
      return `account ${d.accountAddress}${d.received ? " received" : ""}${d.eventTypes?.length ? ` ${d.eventTypes.join(", ")}` : ""}`;
    case "resource":
      return `${d.accountAddress} ${d.resourceType}`;
    case "object":
      return `object ${d.objectId}`;
    default:
      return String((d as { kind?: unknown }).kind);
  }
}

function state(info: SubscriptionInfo): string {
  if (info.ended) return "ended";
  if (info.paused) return "paused";
  return info.consecutiveErrors > 0 ? "retrying" : "live";
}

function short(text: string | undefined): string {
  if (text === undefined) return "-";
  return text.length > CURSOR_CHARS ? `${text.slice(0, CURSOR_CHARS - 3)}...` : text;
}

function errorText(info: SubscriptionInfo): string {
  const e = info.lastError;
  if (e === undefined) return String(info.consecutiveErrors);
  return `${info.consecutiveErrors} ${e.name}: ${e.message}${e.cause === undefined ? "" : ` (cause: ${e.cause})`}`;
}

function age(at: number | undefined, now: number): string {
  return at === undefined ? "-" : `${Math.max(0, Math.round((now - at) / 1000))}s ago`;
}

const PANEL: CSSProperties = {
  position: "fixed",
  right: 12,
  bottom: 12,
  zIndex: 2147483000,
  font: "12px/1.4 ui-monospace, SFMono-Regular, Menlo, monospace",
  color: "#e5e7eb",
  textAlign: "left",
};
const BUTTON: CSSProperties = {
  font: "inherit",
  color: "inherit",
  background: "#111827",
  border: "1px solid #374151",
  borderRadius: 6,
  padding: "4px 10px",
  cursor: "pointer",
};
const BODY: CSSProperties = {
  marginBottom: 6,
  maxHeight: "60vh",
  maxWidth: "min(960px, calc(100vw - 24px))",
  boxSizing: "border-box",
  overflow: "auto",
  background: "#111827",
  border: "1px solid #374151",
  borderRadius: 6,
  padding: 8,
};
const TABLE: CSSProperties = { borderCollapse: "collapse", width: "100%", marginBottom: 8 };
const CELL: CSSProperties = { padding: "2px 6px", borderBottom: "1px solid #1f2937", verticalAlign: "top" };
const CAPTION: CSSProperties = { textAlign: "left", fontWeight: "bold", padding: "2px 6px" };

function table(caption: string, head: string[], rows: ReactNode[][], empty: string) {
  return h(
    "table",
    { style: TABLE },
    h("caption", { style: CAPTION }, caption),
    h("thead", null, h("tr", null, ...head.map((c) => h("th", { key: c, scope: "col", style: CELL }, c)))),
    h(
      "tbody",
      null,
      rows.length === 0
        ? h("tr", null, h("td", { colSpan: head.length, style: CELL }, empty))
        : rows.map((cells, i) => h("tr", { key: i }, ...cells.map((c, j) => h("td", { key: j, style: CELL }, c)))),
    ),
  );
}

function subscriptionRows(rows: SubscriptionInfo[], now: number): ReactNode[][] {
  return rows.map((info) => [
    info.id,
    summary(info.descriptor),
    state(info),
    h("span", { title: info.cursor }, short(info.cursor)),
    info.lastVersion === undefined ? "-" : String(info.lastVersion),
    errorText(info),
    age(info.lastSuccessAt, now),
  ]);
}

function metricRows(metrics: View["metrics"]): ReactNode[][] {
  const ms = (n: number | undefined) => (n === undefined ? "-" : `${Math.round(n)}ms`);
  return metrics.flatMap(([label, snapshot]) =>
    Object.entries(snapshot).map(([method, m]) => [
      label,
      method,
      String(m!.requests),
      String(m!.errors),
      String(m!.rateLimited),
      ms(m!.latency?.p50),
      ms(m!.latency?.p95),
      ms(m!.latency?.max),
    ]),
  );
}

/**
 * A floating panel listing the client's subscriptions, running and recently
 * ended, through `client.listSubscriptions()`, and the `metrics` given, re-read every
 * `interval`. Read-only: it shows cursors, errors and pause state and offers
 * no control over them. Renders nothing during server rendering; mount it in
 * development builds only.
 */
export function StreamdDevtools({ client: explicit, metrics, interval = 1000, defaultOpen = false }: StreamdDevtoolsProps) {
  if (typeof interval !== "number" || !Number.isFinite(interval) || interval <= 0) {
    throw new StreamdError(`Invalid interval: ${String(interval)} (expected a positive number of ms)`);
  }
  const client = useStreamdClient(explicit);
  const latestMetrics = useRef(metrics);
  latestMetrics.current = metrics;
  const [open, setOpen] = useState(defaultOpen);
  const [view, setView] = useState<View | undefined>(undefined);

  useEffect(() => {
    const read = () =>
      setView({
        rows: client.listSubscriptions({ ended: true }),
        metrics: sources(latestMetrics.current).map(([label, m]) => [label, m.snapshot()]),
        at: Date.now(),
      });
    read();
    const timer = setInterval(read, interval);
    return () => clearInterval(timer);
  }, [client, interval]);

  if (view === undefined) return null;
  const running = view.rows.filter((r) => !r.ended).length;

  return h(
    "div",
    { style: PANEL, "data-streamd-devtools": "" },
    open &&
      h(
        "div",
        { style: BODY },
        table(
          "Subscriptions",
          ["id", "descriptor", "state", "cursor", "last version", "errors", "last success"],
          subscriptionRows(view.rows, view.at),
          "No subscriptions",
        ),
        view.metrics.length > 0 &&
          table(
            "Adapter metrics",
            ["source", "method", "requests", "errors", "rate limited", "p50", "p95", "max"],
            metricRows(view.metrics),
            "No calls yet",
          ),
      ),
    h(
      "button",
      { type: "button", style: BUTTON, "aria-expanded": open, onClick: () => setOpen((o) => !o) },
      `streamd (${running} running)`,
    ),
  );
}
