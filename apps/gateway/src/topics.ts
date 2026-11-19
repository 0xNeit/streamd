import { StreamdNetworkError, descriptorKey, redactSecrets, serializeEvent } from "@streamd/core";
import type { EventDescriptor, StreamdClient, Subscription } from "@streamd/core";

/** One event as a topic holds it: its place in the topic and its `serializeEvent` line. */
export interface Entry {
  seq: number;
  line: string;
}

/** A stream following a topic. Neither method may throw. */
export interface Subscriber {
  deliver(entries: readonly Entry[]): void;
  /** The topic stopped for good, its poller having given up. */
  end(message: string): void;
}

export interface TopicStatus {
  descriptor: EventDescriptor;
  clients: number;
  lastEventId: string;
  buffered: number;
}

export interface TopicsOptions {
  client: StreamdClient;
  maxTopics: number;
  /** Events kept per topic for resuming streams and topic cursors. */
  buffer: number;
  /** How long, in ms, a topic outlives its last client or topic-cursor read. */
  linger: number;
  pollInterval?: number;
  batchSize?: number;
  onError?: (error: Error, descriptor: EventDescriptor) => void;
}

/**
 * One descriptor's live walk from the head, shared by every client on it.
 * Event ids are `<epoch>:<seq>`: the epoch is new each time the topic starts,
 * so an id from an earlier run of the same descriptor is never taken as held.
 */
export class Topic {
  readonly epoch = crypto.randomUUID().slice(0, 8);
  readonly subscribers = new Set<Subscriber>();
  seq = 0;
  /** Every entry above this seq is still held. */
  private floor = 0;
  private readonly entries: Entry[] = [];
  subscription?: Subscription;
  timer?: ReturnType<typeof setTimeout>;

  constructor(
    readonly key: string,
    readonly descriptor: EventDescriptor,
    private readonly buffer: number,
  ) {}

  id(seq: number): string {
    return `${this.epoch}:${seq}`;
  }

  /** The entries after `id`, or `undefined` when they are no longer all held. */
  after(id: string): Entry[] | undefined {
    const match = /^([0-9a-f]{8}):(\d{1,15})$/.exec(id);
    if (match === null || match[1] !== this.epoch) return undefined;
    const seq = Number(match[2]);
    if (seq < this.floor || seq > this.seq) return undefined;
    return this.entries.slice(this.entries.length - (this.seq - seq));
  }

  /** Numbers and holds `lines`, evicting the oldest past `buffer`, and returns the new entries. */
  add(lines: readonly string[]): Entry[] {
    const added = lines.map((line) => ({ seq: ++this.seq, line }));
    this.entries.push(...added);
    // ponytail: shift is O(buffer) per evicted event; a ring index if buffers grow large.
    while (this.entries.length > this.buffer) this.floor = this.entries.shift()!.seq;
    return added;
  }

  get buffered(): number {
    return this.entries.length;
  }
}

function describe(error: Error): string {
  return error.cause instanceof Error ? `${error.message}: ${error.cause.message}` : error.message;
}

/** The gateway's topics: at most `maxTopics` live walks, keyed by descriptor value. */
export class Topics {
  private readonly topics = new Map<string, Topic>();

  constructor(private readonly options: TopicsOptions) {}

  /** The running topic on `descriptor`, if any. */
  get(descriptor: EventDescriptor): Topic | undefined {
    const key = descriptorKey(descriptor);
    return key === undefined ? undefined : this.topics.get(key);
  }

  /**
   * The topic on `descriptor`, started at the head when there is none. A new
   * topic with no client stops after `linger`, as an idle one does.
   */
  open(descriptor: EventDescriptor): Topic {
    const existing = this.get(descriptor);
    if (existing !== undefined) return existing;
    if (this.topics.size >= this.options.maxTopics) {
      throw new StreamdNetworkError(`The gateway follows at most ${this.options.maxTopics} descriptors`, 503);
    }
    const key = descriptorKey(descriptor)!;
    const topic = new Topic(key, descriptor, this.options.buffer);
    const { client, pollInterval, batchSize, onError } = this.options;
    topic.subscription = client.onBatch(
      descriptor,
      (events) => {
        const added = topic.add(events.map(serializeEvent));
        for (const subscriber of topic.subscribers) subscriber.deliver(added);
      },
      {
        pollInterval,
        batchSize,
        onError: (error) => {
          onError?.(error, descriptor);
          if (topic.subscription?.active === false) this.end(topic, redactSecrets(describe(error)));
        },
      },
    );
    this.topics.set(key, topic);
    this.idle(topic);
    return topic;
  }

  join(topic: Topic, subscriber: Subscriber): void {
    clearTimeout(topic.timer);
    topic.timer = undefined;
    topic.subscribers.add(subscriber);
  }

  leave(topic: Topic, subscriber: Subscriber): void {
    topic.subscribers.delete(subscriber);
    this.idle(topic);
  }

  /** Restarts an idle topic's linger, as a topic-cursor read does. */
  touch(topic: Topic): void {
    this.idle(topic);
  }

  private idle(topic: Topic): void {
    if (topic.subscribers.size > 0 || this.topics.get(topic.key) !== topic) return;
    clearTimeout(topic.timer);
    topic.timer = setTimeout(() => this.stop(topic), this.options.linger);
  }

  private stop(topic: Topic): void {
    clearTimeout(topic.timer);
    topic.subscription?.unsubscribe();
    if (this.topics.get(topic.key) === topic) this.topics.delete(topic.key);
  }

  private end(topic: Topic, message: string): void {
    this.stop(topic);
    for (const subscriber of topic.subscribers) subscriber.end(message);
    topic.subscribers.clear();
  }

  status(): TopicStatus[] {
    return [...this.topics.values()].map((topic) => ({
      descriptor: topic.descriptor,
      clients: topic.subscribers.size,
      lastEventId: topic.id(topic.seq),
      buffered: topic.buffered,
    }));
  }

  get size(): number {
    return this.topics.size;
  }

  /** Stops every topic; their subscribers are the caller's to close. */
  close(): void {
    for (const topic of [...this.topics.values()]) this.stop(topic);
  }
}
