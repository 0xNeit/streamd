import { describe, expect, it } from "bun:test";
import { connectKafka, type KafkaLoader } from "../src/kafka.js";

interface Recorded {
  client?: Record<string, unknown>;
  producers: Record<string, unknown>[];
  connects: number;
  disconnects: number;
  sends: Record<string, unknown>[];
}

/** A client module recording what it is built with; `failConnects` rejects that many connects first. */
function fakeKafka(recorded: Recorded, failConnects = 0) {
  return class {
    constructor(config: Record<string, unknown>) {
      recorded.client = config;
    }
    producer(config: Record<string, unknown>) {
      recorded.producers.push(config);
      return {
        connect: async () => {
          recorded.connects++;
          if (failConnects-- > 0) throw new Error("Connection error: connect ECONNREFUSED");
        },
        send: async (record: Record<string, unknown>) => void recorded.sends.push(record),
        disconnect: async () => void recorded.disconnects++,
      };
    }
  };
}

function fresh(): Recorded {
  return { producers: [], connects: 0, disconnects: 0, sends: [] };
}

function missing(name: string): Error {
  return new Error(`Cannot find package '${name}' from '/x'`);
}

function loader(modules: Record<string, unknown>): KafkaLoader {
  return async (name) => {
    if (name in modules) return modules[name];
    throw missing(name);
  };
}

const MESSAGE = { key: "k", value: "[]", headers: { "x-streamd-signature": "sha256=ab" } };
const URL = "kafkas://u:pw@a:9093,b?sasl=scram-sha-256";
const CLIENT = {
  clientId: "streamd",
  brokers: ["a:9093", "b:9092"],
  ssl: true,
  sasl: { mechanism: "scram-sha-256", username: "u", password: "pw" },
  connectionTimeout: 1666,
  requestTimeout: 1666,
  retry: { retries: 1 },
  logLevel: 0,
};
const PRODUCER = { idempotent: true, maxInFlightRequests: 1, allowAutoTopicCreation: false, retry: { retries: 1 } };

describe("connectKafka", () => {
  it("configures kafkajs with acks in each send", async () => {
    const recorded = fresh();
    const kafka = await connectKafka(URL, 5000, loader({ kafkajs: { Kafka: fakeKafka(recorded) } }));
    expect(recorded.client).toEqual(CLIENT);
    expect(recorded.connects).toBe(0);
    await kafka.send("t", MESSAGE);
    await kafka.send("t", MESSAGE);
    expect(recorded.producers).toEqual([PRODUCER]);
    expect(recorded.connects).toBe(1);
    expect(recorded.sends).toEqual([
      { topic: "t", acks: -1, messages: [MESSAGE] },
      { topic: "t", acks: -1, messages: [MESSAGE] },
    ]);
  });

  it("prefers Confluent's client, with acks in the producer config", async () => {
    const confluent = fresh();
    const kafkajs = fresh();
    const kafka = await connectKafka(
      "kafka://h",
      100,
      loader({
        "@confluentinc/kafka-javascript": { default: { KafkaJS: { Kafka: fakeKafka(confluent) } } },
        kafkajs: { Kafka: fakeKafka(kafkajs) },
      }),
    );
    await kafka.send("t", MESSAGE);
    expect(confluent.client).toEqual({
      kafkaJS: { clientId: "streamd", brokers: ["h:9092"], ssl: false, connectionTimeout: 33, requestTimeout: 33, retry: { retries: 1 }, logLevel: 0 },
    });
    expect(confluent.producers).toEqual([{ kafkaJS: { ...PRODUCER, acks: -1 } }]);
    expect(confluent.sends).toEqual([{ topic: "t", messages: [MESSAGE] }]);
    expect(kafkajs.client).toBeUndefined();
  });

  it("opens a fresh producer after close and after a failed connect", async () => {
    const recorded = fresh();
    const kafka = await connectKafka("kafka://h", 100, loader({ kafkajs: { default: { Kafka: fakeKafka(recorded, 1) } } }));
    await expect(kafka.send("t", MESSAGE)).rejects.toThrow("ECONNREFUSED");
    await Bun.sleep(0);
    expect(recorded.disconnects).toBe(1);
    await kafka.send("t", MESSAGE);
    expect(recorded.producers).toHaveLength(2);
    kafka.close();
    await Bun.sleep(0);
    expect(recorded.disconnects).toBe(2);
    await kafka.send("t", MESSAGE);
    expect(recorded.producers).toHaveLength(3);
    expect(recorded.sends).toHaveLength(2);
  });

  it("never sends on a producer closed while it was connecting", async () => {
    const recorded = fresh();
    let release!: () => void;
    const Kafka = class {
      producer() {
        recorded.producers.push({});
        const gate = recorded.producers.length === 1 ? new Promise<void>((r) => (release = r)) : Promise.resolve();
        return {
          connect: () => gate,
          send: async (record: Record<string, unknown>) => void recorded.sends.push(record),
          disconnect: async () => void recorded.disconnects++,
        };
      }
    };
    const kafka = await connectKafka("kafka://h", 100, loader({ kafkajs: { Kafka } }));
    const first = kafka.send("t", MESSAGE);
    kafka.close();
    release();
    await expect(first).rejects.toThrow("the Kafka producer was closed");
    expect(recorded.sends).toEqual([]);
    expect(recorded.disconnects).toBe(1);
    await kafka.send("t", MESSAGE);
    expect(recorded.sends).toHaveLength(1);
  });

  it("names both clients when neither is installed", async () => {
    await expect(connectKafka("kafka://h", 100, loader({}))).rejects.toThrow(
      "--to kafka:// needs a Kafka client installed: npm i @confluentinc/kafka-javascript (or kafkajs)",
    );
  });

  it("reports a broken install rather than falling back", async () => {
    const broken: KafkaLoader = async (name) => {
      if (name === "@confluentinc/kafka-javascript") {
        throw new Error("Cannot find module '../build/Release/confluent-kafka-javascript.node' from '/n/node_modules/@confluentinc/kafka-javascript/lib/binding.js'");
      }
      return { Kafka: fakeKafka(fresh()) };
    };
    await expect(connectKafka("kafka://h", 100, broken)).rejects.toThrow("confluent-kafka-javascript.node");
  });
});
