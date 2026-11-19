import { readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { StreamdError, StreamdNetworkError, deliver, isObject, redactSecrets, serializeEvent, webhookSink } from "@streamd/core";
import type { DeliverySink, EventDescriptor, StreamdClient, Subscription } from "@streamd/core";
import { checkTarget, parseTarget } from "./targets.js";
import type { Lookup } from "./targets.js";
import { parseDescriptor, toJson } from "./wire.js";

export type WebhookState = "active" | "failed";

/** A registered webhook as the API reports it: everything but its secret. */
export interface WebhookView {
  id: string;
  url: string;
  descriptor: EventDescriptor;
  status: WebhookState;
  /** ISO 8601. */
  createdAt: string;
  /** The position after the last acknowledged page; absent until the first is saved. */
  cursor?: string;
  /** Events acknowledged, redeliveries included. */
  delivered: number;
  /** Why the webhook failed, while it is `failed`. */
  lastError?: string;
}

/** What the webhook file holds per webhook. */
interface WebhookRecord extends WebhookView {
  secret: string;
}

export interface WebhooksOptions {
  client: StreamdClient;
  /** The JSON file subscriptions and their cursors are kept in, across restarts. */
  file: string;
  /** Let targets resolve to loopback, private and other non-public addresses. */
  allowPrivateTargets?: boolean;
  /** Registered webhooks at once (default: 100). */
  maxWebhooks?: number;
  /** Retries per batch before the webhook is marked failed (default: 5). */
  retries?: number;
  /** Per-request timeout in ms (default: 10000). */
  timeout?: number;
  /** Events per POST; a page larger than this is split (default: the page). */
  batchSize?: number;
  pollInterval?: number;
  /** First retry wait in ms, doubling per attempt (default: 1000). */
  retryDelay?: number;
  lookup?: Lookup;
  /** Delivery failures, polling errors and file write failures, by webhook id. */
  onError?: (message: string, id: string) => void;
}

interface Running {
  record: WebhookRecord;
  sub?: Subscription;
  controller?: AbortController;
}

const ID = /^[0-9a-f]{16}$/;
const SECRET = /^[0-9a-f]{64}$/;

function hex(bytes: number): string {
  return Array.from(crypto.getRandomValues(new Uint8Array(bytes)), (b) => b.toString(16).padStart(2, "0")).join("");
}

function message(err: unknown): string {
  if (!(err instanceof Error)) return String(err);
  return err.cause instanceof Error ? `${err.message}: ${err.cause.message}` : err.message;
}

function view({ secret: _secret, ...rest }: WebhookRecord): WebhookView {
  return { ...rest };
}

/** One stored webhook, validated, so a hand-edited or torn file stops the gateway rather than misbehaving. */
function parseRecord(value: unknown, index: number): WebhookRecord {
  const bad = (field: string): never => {
    throw new StreamdError(`webhook ${index}: "${field}" is missing or invalid`);
  };
  if (!isObject(value)) bad("webhook");
  const v = value as Record<string, unknown>;
  if (typeof v.id !== "string" || !ID.test(v.id)) bad("id");
  if (typeof v.secret !== "string" || !SECRET.test(v.secret)) bad("secret");
  if (v.status !== "active" && v.status !== "failed") bad("status");
  if (typeof v.createdAt !== "string") bad("createdAt");
  if (!Number.isSafeInteger(v.delivered) || (v.delivered as number) < 0) bad("delivered");
  if (v.cursor !== undefined && (typeof v.cursor !== "string" || v.cursor === "")) bad("cursor");
  if (v.lastError !== undefined && typeof v.lastError !== "string") bad("lastError");
  const record: WebhookRecord = {
    id: v.id as string,
    url: parseTarget(v.url).href,
    descriptor: parseDescriptor(v.descriptor),
    secret: v.secret as string,
    status: v.status as WebhookState,
    createdAt: v.createdAt as string,
    delivered: v.delivered as number,
  };
  if (v.cursor !== undefined) record.cursor = v.cursor as string;
  if (v.lastError !== undefined) record.lastError = v.lastError as string;
  return record;
}

function load(file: string): WebhookRecord[] {
  let text: string;
  try {
    text = readFileSync(file, "utf8");
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === "ENOENT") return [];
    throw err;
  }
  try {
    const body: unknown = JSON.parse(text);
    if (!isObject(body) || !Array.isArray(body.webhooks)) throw new StreamdError('expected { "webhooks": [...] }');
    return body.webhooks.map(parseRecord);
  } catch (err) {
    throw new StreamdError(`webhook file ${file}: ${message(err)}`);
  }
}

/**
 * The gateway's registered webhooks. Each follows its descriptor with its own
 * cursor and POSTs every page through `deliver`, signed with its secret; the
 * file only ever holds a cursor whose events the target acknowledged, so
 * delivery is at least once across restarts.
 */
export class Webhooks {
  private readonly running = new Map<string, Running>();
  private readonly maxWebhooks: number;
  private closed = false;

  constructor(private readonly options: WebhooksOptions) {
    for (const name of ["maxWebhooks", "timeout", "batchSize"] as const) {
      const value = options[name];
      if (value !== undefined && !(Number.isSafeInteger(value) && value > 0)) {
        throw new StreamdError(`${name} must be a positive integer: ${value}`);
      }
    }
    if (options.retries !== undefined && !(Number.isSafeInteger(options.retries) && options.retries >= 0)) {
      throw new StreamdError(`retries must be a non-negative integer: ${options.retries}`);
    }
    this.maxWebhooks = options.maxWebhooks ?? 100;
    for (const record of load(options.file)) this.running.set(record.id, { record });
    for (const entry of this.running.values()) if (entry.record.status === "active") this.start(entry);
  }

  get size(): number {
    return this.running.size;
  }

  /** Registers `{ url, descriptor }` and starts it at the head; the result is the only place its secret appears. */
  async create(body: unknown): Promise<WebhookView & { secret: string }> {
    if (!isObject(body)) throw new StreamdError("The request body must be an object");
    const url = parseTarget(body.url);
    const descriptor = parseDescriptor(body.descriptor);
    await checkTarget(url, { allowPrivate: this.options.allowPrivateTargets, lookup: this.options.lookup });
    if (this.running.size >= this.maxWebhooks) {
      throw new StreamdNetworkError(`The gateway holds at most ${this.maxWebhooks} webhooks`, 503);
    }
    const record: WebhookRecord = {
      id: hex(8),
      url: url.href,
      descriptor,
      secret: hex(32),
      status: "active",
      createdAt: new Date().toISOString(),
      delivered: 0,
    };
    const entry: Running = { record };
    this.running.set(record.id, entry);
    try {
      this.save();
    } catch (err) {
      this.running.delete(record.id);
      throw err;
    }
    this.start(entry);
    return { ...view(record), secret: record.secret };
  }

  list(): WebhookView[] {
    return [...this.running.values()].map((entry) => view(entry.record));
  }

  get(id: string): WebhookView | undefined {
    const entry = this.running.get(id);
    return entry === undefined ? undefined : view(entry.record);
  }

  /** Stops and forgets the webhook; false when there is none. */
  delete(id: string): boolean {
    const entry = this.running.get(id);
    if (entry === undefined) return false;
    this.stop(entry);
    this.running.delete(id);
    try {
      this.save();
    } catch (err) {
      this.running.set(id, entry);
      if (entry.record.status === "active") this.start(entry);
      throw err;
    }
    return true;
  }

  /** Restarts a failed webhook from its saved cursor; an active one is left as it is. */
  resume(id: string): WebhookView | undefined {
    const entry = this.running.get(id);
    if (entry === undefined) return undefined;
    if (entry.record.status === "failed") {
      const { lastError } = entry.record;
      entry.record.status = "active";
      delete entry.record.lastError;
      try {
        this.save();
      } catch (err) {
        entry.record.status = "failed";
        entry.record.lastError = lastError;
        throw err;
      }
      this.start(entry);
    }
    return view(entry.record);
  }

  /** Stops every webhook, abandoning in-flight posts, and saves each cursor. */
  close(): void {
    if (this.closed) return;
    this.closed = true;
    for (const entry of this.running.values()) this.stop(entry);
    this.trySave("");
  }

  private start(entry: Running): void {
    const { record } = entry;
    const { client, retries = 5, timeout = 10_000, batchSize, pollInterval, retryDelay } = this.options;
    const controller = new AbortController();
    const target = parseTarget(record.url);
    const post = webhookSink({ to: record.url, secret: record.secret, timeout });
    // Checked on every attempt, so a name re-pointed at a private address after registration is refused when next used.
    const sink: DeliverySink = {
      send: async (body, signal) => {
        try {
          await checkTarget(target, { allowPrivate: this.options.allowPrivateTargets, lookup: this.options.lookup });
        } catch (err) {
          return { ok: false, failure: message(err), retryable: true };
        }
        return post.send(body, signal);
      },
    };
    let lastFailure = "delivery failed";
    const io = {
      signal: controller.signal,
      retryDelay,
      error: (text: string) => {
        lastFailure = text.replace(/^streamd: /, "");
        this.options.onError?.(text, record.id);
      },
    };
    const sub = client.onBatch(
      record.descriptor,
      async (events) => {
        const size = batchSize ?? events.length;
        let delivered = true;
        try {
          for (let i = 0; delivered && i < events.length; i += size) {
            const chunk = events.slice(i, i + size);
            delivered = await deliver(`[${chunk.map(serializeEvent).join(",")}]`, sink, retries, io);
            if (delivered) record.delivered += chunk.length;
          }
        } catch (err) {
          delivered = false;
          lastFailure = message(err);
        } finally {
          // Stopping before returning keeps the poller's cursor on this undelivered page.
          if (!delivered && !controller.signal.aborted) this.fail(entry, sub, lastFailure);
        }
        if (!delivered) return;
        // The poller moves its cursor past the page once this callback's promise settles, on a microtask.
        setTimeout(() => this.checkpoint(entry, sub), 0);
      },
      {
        ...(record.cursor !== undefined && { cursor: record.cursor }),
        ...(pollInterval !== undefined && { pollInterval }),
        ...(batchSize !== undefined && { batchSize }),
        // A start at the head saves its position once there, so a restart before any event does not skip ahead.
        onCaughtUp: () => this.checkpoint(entry, sub),
        onError: (err) => {
          this.options.onError?.(`streamd: ${message(err)}`, record.id);
          if (!sub.active) this.fail(entry, sub, message(err));
        },
      },
    );
    entry.sub = sub;
    entry.controller = controller;
  }

  private current(entry: Running, sub: Subscription): boolean {
    return !this.closed && entry.sub === sub && this.running.get(entry.record.id) === entry;
  }

  private checkpoint(entry: Running, sub: Subscription): void {
    if (!this.current(entry, sub)) return;
    const cursor = sub.cursor;
    if (cursor === undefined || cursor === entry.record.cursor) return;
    entry.record.cursor = cursor;
    this.trySave(entry.record.id);
  }

  private fail(entry: Running, sub: Subscription, reason: string): void {
    if (!this.current(entry, sub) || entry.record.status === "failed") return;
    this.stop(entry);
    entry.record.status = "failed";
    entry.record.lastError = redactSecrets(reason);
    this.trySave(entry.record.id);
  }

  /** Stops the walk and records where it stopped; the cursor is safe to keep at any moment. */
  private stop(entry: Running): void {
    const { sub, controller } = entry;
    entry.sub = undefined;
    entry.controller = undefined;
    if (sub === undefined) return;
    sub.unsubscribe();
    if (sub.cursor !== undefined) entry.record.cursor = sub.cursor;
    controller?.abort();
  }

  private trySave(id: string): void {
    try {
      this.save();
    } catch (err) {
      this.options.onError?.(`streamd: cannot write the webhook file: ${message(err)}`, id);
    }
  }

  // A rename replaces the file whole, so a crash mid-write never leaves a torn file.
  private save(): void {
    const { file } = this.options;
    const temp = `${file}.${process.pid}.tmp`;
    const body = toJson({ webhooks: [...this.running.values()].map((entry) => entry.record) });
    // Exclusive create: a link planted at the predictable name fails the write instead of redirecting it.
    rmSync(temp, { force: true });
    writeFileSync(temp, `${body}\n`, { flag: "wx", mode: 0o600 });
    renameSync(temp, file);
  }
}
