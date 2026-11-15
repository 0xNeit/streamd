import { WEBHOOK_SIGNATURE_HEADER, raceAbort } from "@streamd/core";
import { message, scrubber, signature } from "./format.js";
import type { Attempt, Sink } from "./forward.js";

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
// No bracketed IPv6: kafkajs splits a broker on every colon, so it could never connect to one.
const BROKER = /^([A-Za-z0-9._-]+)(?::(\d{1,5}))?$/;

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
  // A third of --timeout, so the client's one retry of a stalled request fits inside it: that
  // retry carries the same producer id and sequence, and the broker drops it if the first landed.
  const attempt = Math.max(1, Math.floor(timeout / 3));
  const client = {
    clientId: "streamd",
    brokers,
    ssl,
    ...(sasl !== undefined && { sasl }),
    connectionTimeout: attempt,
    requestTimeout: attempt,
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
      const opening = (current ??= open());
      const producer = await opening;
      // A close() while connecting is disconnecting this producer; the retry sends on a fresh one.
      if (current !== opening) throw new Error("the Kafka producer was closed");
      await producer.send(record(topic, message));
    },
    close() {
      const closing = current;
      current = undefined;
      closing?.then((producer) => producer.disconnect()).catch(() => {});
    },
  };
}

export interface KafkaSinkOptions {
  /** Only for scrubbing messages: the client was opened with it. */
  to: string;
  topic: string;
  /** Every record's key, so the default partitioner keeps a forwarder's batches on one partition, in order. */
  key: string;
  secret?: string;
  /** Per-send timeout in ms, connecting included. */
  timeout: number;
}

// Refusals that will not change on a retry, by kafkajs's error type and by
// librdkafka's (Confluent's client) without its ERR_ prefix. Every other
// error, leader elections, too few in-sync replicas and network failures
// included, is retried under backoff.
const FINAL_TYPES = new Set([
  "UNKNOWN_TOPIC_OR_PARTITION",
  "UNKNOWN_TOPIC_OR_PART",
  "UNKNOWN_TOPIC",
  "TOPIC_AUTHORIZATION_FAILED",
  "CLUSTER_AUTHORIZATION_FAILED",
  "TRANSACTIONAL_ID_AUTHORIZATION_FAILED",
  "SASL_AUTHENTICATION_FAILED",
  "AUTHENTICATION",
  "MESSAGE_TOO_LARGE",
  "MSG_SIZE_TOO_LARGE",
  "RECORD_LIST_TOO_LARGE",
  "INVALID_TOPIC_EXCEPTION",
  "INVALID_RECORD",
  "INVALID_REQUIRED_ACKS",
  "POLICY_VIOLATION",
  "INVALID_ARG",
]);
const FINAL_NAMES = new Set(["KafkaJSSASLAuthenticationError", "KafkaJSServerDoesNotSupportApiKey"]);

/** The error and the causes it wraps, as kafkajs wraps a protocol error once its retries run out. */
function chain(err: unknown): { name?: unknown; type?: unknown }[] {
  const links: { name?: unknown; type?: unknown; cause?: unknown }[] = [];
  for (let link = err; typeof link === "object" && link !== null && links.length < 8; link = (link as { cause?: unknown }).cause) {
    links.push(link);
  }
  return links;
}

/** The error types along the chain, without librdkafka's ERR_ prefix. */
function errorTypes(err: unknown): string[] {
  return chain(err).flatMap(({ type }) => (typeof type === "string" ? [type.replace(/^ERR_+/, "")] : []));
}

/**
 * Produces each body to `options.topic` as one record, keyed by
 * `options.key`, with the signature header when signing; the send resolving,
 * acknowledged by every in-sync replica, is the acknowledgement.
 */
export function kafkaSink(client: KafkaLike, options: KafkaSinkOptions): Sink {
  const scrub = scrubber(options.to);
  return {
    async send(body, signal): Promise<Attempt> {
      const headers: Record<string, string> = {};
      if (options.secret !== undefined) headers[WEBHOOK_SIGNATURE_HEADER] = signature(body, options.secret);
      const timeout = AbortSignal.timeout(options.timeout);
      try {
        await raceAbort(client.send(options.topic, { key: options.key, value: body, headers }), AbortSignal.any([signal, timeout]));
        return { ok: true };
      } catch (err) {
        if (signal.aborted) return { ok: false, failure: "aborted" };
        if (timeout.aborted) {
          // A wedged producer would time out every retry; the next attempt opens a fresh one.
          client.close();
          return { ok: false, failure: `send timed out after ${options.timeout}ms`, retryable: true };
        }
        const types = errorTypes(err);
        // Any final type along the chain decides, so a wrapper's own type cannot hide a final cause.
        const type = types.find((t) => FINAL_TYPES.has(t)) ?? types[0];
        const final = (type !== undefined && FINAL_TYPES.has(type)) || chain(err).some(({ name }) => FINAL_NAMES.has(String(name)));
        const failure = scrub(type === undefined ? message(err) : `${message(err)} (${type})`);
        return { ok: false, failure, retryable: !final };
      }
    },
    close: () => client.close(),
  };
}
