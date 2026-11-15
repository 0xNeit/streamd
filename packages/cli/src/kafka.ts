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

/** One record: `value` the webhook body, `key` the same for every batch of a forwarder. */
export interface KafkaMessage {
  key: string;
  value: string;
  headers: Record<string, string>;
}

/** The one call the Kafka sink makes: resolves once the leader acknowledges the record with acks=all. */
export interface KafkaLike {
  send(topic: string, message: KafkaMessage): Promise<void>;
  close(): unknown;
}

interface Producer {
  connect(): Promise<void>;
  send(record: Record<string, unknown>): Promise<unknown>;
  disconnect(): Promise<void>;
}
type KafkaConstructor = new (config: Record<string, unknown>) => { producer(config: Record<string, unknown>): Producer };
type KafkaJsModule = { Kafka?: KafkaConstructor; default?: { Kafka?: KafkaConstructor } };
type ConfluentModule = { KafkaJS?: { Kafka?: KafkaConstructor }; default?: { KafkaJS?: { Kafka?: KafkaConstructor } } };

/** Imports a client package by name; a parameter so tests can hand over a fake. */
export type KafkaLoader = (name: string) => Promise<unknown>;

// Variables, so the bundle never inlines either client.
const CONFLUENT = "@confluentinc/kafka-javascript";
const KAFKAJS = "kafkajs";
// logLevel.NOTHING in both clients: their own logs would print broker addresses and retry noise.
const NOTHING = 0;

async function optional(load: KafkaLoader, name: string): Promise<unknown> {
  try {
    return await load(name);
  } catch (err) {
    // Only the package itself missing: a broken install of it, such as a native build that fails to load, is reported as it is.
    if (String(err).includes(`Cannot find package '${name}'`) || String(err).includes(`Cannot find module '${name}'`)) return undefined;
    throw err;
  }
}

/**
 * Opens a Kafka producer through `@confluentinc/kafka-javascript` (its
 * KafkaJS-compatible API) when installed, else `kafkajs`, so the CLI ships no
 * client. The producer is idempotent with acks=all and connects on the first
 * send, and again after `close()` or a failed connect. The client's own
 * retries are the fewest an idempotent producer allows, so `deliver` owns
 * the backoff and an unknown topic fails inside `--timeout`.
 */
export async function connectKafka(url: string, timeout: number, load: KafkaLoader = (name) => import(name)): Promise<KafkaLike> {
  const { brokers, ssl, sasl } = parseKafkaUrl(url);
  const client = {
    clientId: "streamd",
    brokers,
    ssl,
    ...(sasl !== undefined && { sasl }),
    connectionTimeout: timeout,
    requestTimeout: timeout,
    retry: { retries: 1 },
    logLevel: NOTHING,
  };
  const producerConfig = { idempotent: true, maxInFlightRequests: 1, allowAutoTopicCreation: false, retry: { retries: 1 } };
  let build: () => Producer;
  let record: (topic: string, message: KafkaMessage) => Record<string, unknown>;
  const confluent = (await optional(load, CONFLUENT)) as ConfluentModule | undefined;
  const Confluent = confluent?.KafkaJS?.Kafka ?? confluent?.default?.KafkaJS?.Kafka;
  if (Confluent !== undefined) {
    const kafka = new Confluent({ kafkaJS: client });
    // Confluent's send refuses acks; it belongs in the producer config there.
    build = () => kafka.producer({ kafkaJS: { ...producerConfig, acks: -1 } });
    record = (topic, message) => ({ topic, messages: [message] });
  } else {
    const kafkajs = (await optional(load, KAFKAJS)) as KafkaJsModule | undefined;
    const KafkaJs = kafkajs?.Kafka ?? kafkajs?.default?.Kafka;
    if (KafkaJs === undefined) {
      throw new Error(`--to kafka:// needs a Kafka client installed: npm i ${CONFLUENT} (or ${KAFKAJS})`);
    }
    const kafka = new KafkaJs(client);
    build = () => kafka.producer(producerConfig);
    record = (topic, message) => ({ topic, acks: -1, messages: [message] });
  }
  let current: Promise<Producer> | undefined;
  const open = (): Promise<Producer> => {
    const producer = build();
    const ready = producer.connect().then(() => producer);
    ready.catch(() => {
      if (current === ready) current = undefined;
      producer.disconnect().catch(() => {});
    });
    return ready;
  };
  return {
    async send(topic, message) {
      const producer = await (current ??= open());
      await producer.send(record(topic, message));
    },
    close() {
      const closing = current;
      current = undefined;
      closing?.then((producer) => producer.disconnect()).catch(() => {});
    },
  };
}
