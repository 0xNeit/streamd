export type SaslMechanism = "plain" | "scram-sha-256" | "scram-sha-512";

/** Where a `kafka://` or `kafkas://` URL points: its brokers as `host:port`, TLS, and SASL credentials. */
export interface KafkaTarget {
  brokers: string[];
  ssl: boolean;
  sasl?: { mechanism: SaslMechanism; username: string; password: string };
}

const DEFAULT_PORT = 9092;
const MECHANISMS = new Set<string>(["plain", "scram-sha-256", "scram-sha-512"]);
// Kafka's own URL has no standard form, and several brokers make it no valid URL either, so it is split by hand.
const KAFKA_URL = /^(kafkas?):\/\/(?:([^@/?#]*)@)?([^@/?#]*)\/?(?:\?([^#]*))?$/i;
const BROKER = /^(\[[0-9A-Fa-f:.]+\]|[A-Za-z0-9._-]+)(?::(\d{1,5}))?$/;

function decode(part: string): string {
  try {
    return decodeURIComponent(part);
  } catch {
    throw new Error("--to kafka:// has a malformed percent-escape in its user or password");
  }
}

/**
 * Reads a Kafka URL. The messages never echo it, since it may carry a
 * password: credentials mean SASL, and need `?sasl=<mechanism>`.
 */
export function parseKafkaUrl(raw: string): KafkaTarget {
  const match = KAFKA_URL.exec(raw);
  if (match === null) throw new Error("--to kafka:// takes no path or fragment, and no query but ?sasl=<mechanism>");
  const [, scheme, userinfo, hosts, query] = match;
  const brokers = hosts!.split(",").map((broker) => {
    const parts = BROKER.exec(broker);
    const port = Number(parts?.[2] ?? DEFAULT_PORT);
    if (parts === null || port < 1 || port > 65535) throw new Error("--to must be a valid kafka:// URL with host[:port] brokers");
    return `${parts[1]}:${port}`;
  });
  const params = new URLSearchParams(query ?? "");
  for (const key of params.keys()) {
    if (key !== "sasl") throw new Error("--to kafka:// takes no query but ?sasl=<mechanism>");
  }
  const mechanism = params.get("sasl")?.toLowerCase();
  const target: KafkaTarget = { brokers, ssl: scheme!.toLowerCase() === "kafkas" };
  if (userinfo === undefined) {
    if (mechanism !== undefined) throw new Error("--to kafka:// ?sasl= needs user:password@ in the URL");
    return target;
  }
  const colon = userinfo.indexOf(":");
  if (colon <= 0) throw new Error("--to kafka:// credentials must be user:password@");
  if (mechanism === undefined || !MECHANISMS.has(mechanism)) {
    throw new Error("--to kafka:// with credentials needs ?sasl=plain, ?sasl=scram-sha-256 or ?sasl=scram-sha-512");
  }
  target.sasl = {
    mechanism: mechanism as SaslMechanism,
    username: decode(userinfo.slice(0, colon)),
    password: decode(userinfo.slice(colon + 1)),
  };
  return target;
}
