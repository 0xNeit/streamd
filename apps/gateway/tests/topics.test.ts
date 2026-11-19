import { afterEach, describe, expect, it } from "bun:test";
import { StreamdClient, StreamdNetworkError, parseEvent } from "@streamd/core";
import type { EventAdapter } from "@streamd/core";
import { Topics } from "../src/topics.js";
import type { Entry, Subscriber } from "../src/topics.js";
import { CountingAdapter, PING, PINGS, PONG, ev, waitFor } from "./helpers.js";

const clients: StreamdClient[] = [];

function setup(adapter: EventAdapter, options: { buffer?: number; linger?: number; maxTopics?: number } = {}) {
  const client = new StreamdClient({ adapter });
  clients.push(client);
  return new Topics({ client, maxTopics: 4, buffer: 100, linger: 1000, pollInterval: 10, ...options });
}

function collector() {
  const seen: Entry[] = [];
  const ended: string[] = [];
  const subscriber: Subscriber = { deliver: (entries) => seen.push(...entries), end: (m) => ended.push(m) };
  return { seen, ended, subscriber };
}

afterEach(() => {
  for (const client of clients.splice(0)) client.destroy();
});

describe("Topics", () => {
  it("serves every client on one descriptor from one walk", async () => {
    const adapter = new CountingAdapter();
    const topics = setup(adapter);
    const a = collector();
    const b = collector();
    const topic = topics.open(PINGS);
    topics.join(topic, a.subscriber);
    expect(topics.open({ kind: "module", eventType: [PING] })).toBe(topic);
    topics.join(topic, b.subscriber);
    adapter.push(ev(1), ev(2));
    await waitFor(() => b.seen.length === 2);
    expect(a.seen.map((e) => parseEvent(e.line).version)).toEqual([1n, 2n]);
    expect(b.seen.map((e) => topic.id(e.seq))).toEqual([topic.id(1), topic.id(2)]);
    expect(clients.at(-1)!.listSubscriptions()).toHaveLength(1);
    expect(topics.size).toBe(1);
    topics.close();
  });

  it("resumes after a held id and refuses one no longer held", async () => {
    const adapter = new CountingAdapter();
    const topics = setup(adapter, { buffer: 2 });
    const topic = topics.open(PINGS);
    topics.join(topic, collector().subscriber);
    adapter.push(ev(1));
    await waitFor(() => topic.seq === 1);
    expect(topic.after(topic.id(0))!.map((e) => e.seq)).toEqual([1]);
    expect(topic.after(topic.id(1))).toEqual([]);
    adapter.push(ev(2), ev(3));
    await waitFor(() => topic.seq === 3);
    expect(topic.after(topic.id(1))!.map((e) => e.seq)).toEqual([2, 3]);
    expect(topic.after(topic.id(0))).toBeUndefined();
    expect(topic.after(topic.id(4))).toBeUndefined();
    expect(topic.after("00000000:1")).toBeUndefined();
    expect(topic.after("junk")).toBeUndefined();
    expect(topic.buffered).toBe(2);
    topics.close();
  });

  it("stops a topic its last client left once the linger passes, and restarts it with a new epoch", async () => {
    const topics = setup(new CountingAdapter(), { linger: 300 });
    const topic = topics.open(PINGS);
    const a = collector();
    topics.join(topic, a.subscriber);
    topics.leave(topic, a.subscriber);
    await Bun.sleep(100);
    topics.touch(topic);
    await Bun.sleep(250);
    expect(topics.get(PINGS)).toBe(topic);
    await waitFor(() => topics.size === 0);
    expect(topic.subscription!.active).toBe(false);
    const again = topics.open(PINGS);
    expect(again).not.toBe(topic);
    expect(again.after(topic.id(0))).toBeUndefined();
    topics.close();
  });

  it("refuses a topic past maxTopics as a retryable 503", () => {
    const topics = setup(new CountingAdapter(), { maxTopics: 1 });
    topics.open(PINGS);
    let error: unknown;
    try {
      topics.open({ kind: "module", eventType: PONG });
    } catch (err) {
      error = err;
    }
    expect(error).toBeInstanceOf(StreamdNetworkError);
    expect((error as StreamdNetworkError).statusCode).toBe(503);
    topics.close();
  });

  it("ends a topic whose poller gives up, telling its clients with the message redacted", async () => {
    const reported: Error[] = [];
    const client = new StreamdClient({
      adapter: {
        fetchEvents: async () => {
          throw new StreamdNetworkError("down at https://node.example/v1?api_key=secret");
        },
      },
      defaults: { maxErrors: 2, maxBackoff: 1 },
    });
    clients.push(client);
    const topics = new Topics({ client, maxTopics: 4, buffer: 10, linger: 1000, pollInterval: 1, onError: (e) => reported.push(e) });
    const topic = topics.open(PINGS);
    const a = collector();
    topics.join(topic, a.subscriber);
    await waitFor(() => a.ended.length === 1);
    expect(a.ended[0]).toContain("Polling failed (2/2): down at <url>");
    expect(a.ended[0]).not.toContain("secret");
    expect(reported.length).toBe(2);
    expect(topics.size).toBe(0);
    expect(topic.subscribers.size).toBe(0);
  });

  it("reports status per topic", async () => {
    const adapter = new CountingAdapter();
    const topics = setup(adapter);
    const topic = topics.open(PINGS);
    topics.join(topic, collector().subscriber);
    adapter.push(ev(1));
    await waitFor(() => topic.seq === 1);
    expect(topics.status()).toEqual([{ descriptor: PINGS, clients: 1, lastEventId: topic.id(1), buffered: 1 }]);
    topics.close();
  });
});
