import type { EventAdapter, EventDescriptor, EventPage, FetchOptions } from "../types.js";
import { StreamdError, StreamdNetworkError } from "../errors.js";
import { SharedFetches } from "../shared.js";
import { fetchKey, generateId } from "../utils.js";
import { isObject, requireAdapter } from "./validate.js";

/** The part of `navigator.locks` the election uses. */
export interface LockManagerLike {
  request(
    name: string,
    options: { signal?: AbortSignal },
    callback: (lock: unknown) => Promise<void>,
  ): Promise<unknown>;
}

/** The part of `BroadcastChannel` the requests and replies use. */
export interface BroadcastChannelLike {
  onmessage: ((event: { data: unknown }) => void) | null;
  postMessage(message: unknown): void;
  close(): void;
}

export interface TabSharedAdapterOptions {
  /**
   * Names the lock and the channel. Every tab using one name must wrap the
   * same source on the same network, since any of them may answer the rest.
   */
  name: string;
  /** How long, in ms, the leader reuses a page for an identical request (default: 1000). */
  shareFor?: number;
  /** After this many ms the leader stops joining a source request that has not settled. */
  requestTimeout?: number;
  /** Defaults to `navigator.locks`. */
  locks?: LockManagerLike;
  /** Defaults to `new BroadcastChannel(name)`. */
  channel?: BroadcastChannelLike;
}

const DEFAULT_SHARE_FOR = 1000;

const OPTIONAL = [
  "getHeadVersion",
  "getEarliestVersion",
  "getBlock",
  "getBlockHeights",
  "getTransactionEvents",
  "getTransactionEventsByVersion",
  "simulateTransactionEvents",
  "view",
  "readObjects",
] as const;

interface Pending {
  descriptor: EventDescriptor;
  options: FetchOptions;
  resolve(page: EventPage): void;
  reject(reason: unknown): void;
}

interface WireError {
  name: string;
  message: string;
  statusCode?: number;
  retryAfter?: number;
}

type Message =
  | { kind: "leader" }
  | { kind: "request"; from: string; id: number; descriptor: EventDescriptor; options: FetchOptions }
  | { kind: "reply"; to: string; id: number; page?: EventPage; error?: WireError };

function toWire(err: unknown): WireError {
  if (err instanceof StreamdNetworkError) {
    return { name: err.name, message: err.message, statusCode: err.statusCode, retryAfter: err.retryAfter };
  }
  return { name: "StreamdError", message: err instanceof Error ? err.message : String(err) };
}

function fromWire(err: WireError): StreamdError {
  return err.name === "StreamdNetworkError"
    ? new StreamdNetworkError(err.message, err.statusCode, undefined, err.retryAfter)
    : new StreamdError(err.message);
}

/**
 * The key a walk's pages share whatever their cursor, for a request that
 * starts at the head or continues a walk; anything bounded has none.
 */
function walkKey(descriptor: EventDescriptor, options: FetchOptions): string | undefined {
  const { fromVersion, toVersion, offset, orderBy } = options;
  if (fromVersion !== undefined || toVersion !== undefined || offset !== undefined) return undefined;
  if (orderBy === "desc") return undefined;
  return fetchKey(descriptor, { ...options, cursor: undefined });
}

function openChannel(name: string): BroadcastChannelLike | undefined {
  const Channel = (globalThis as { BroadcastChannel?: new (name: string) => BroadcastChannelLike })
    .BroadcastChannel;
  return Channel === undefined ? undefined : new Channel(name);
}

/**
 * Wraps `adapter` so that, across the browser tabs of one origin, only one
 * tab sends `fetchEvents` requests to it. The others post each request to
 * that tab over a `BroadcastChannel` and get its page back, and each keeps
 * its own cursors, so whichever tab leads next resumes every walk where its
 * own subscriptions left off. A start at the head joins the newest caught-up
 * page of the same walk, so tabs opened at different times share one walk. Without Web Locks or
 * `BroadcastChannel` every call goes straight to `adapter`, so each tab polls
 * on its own. The other adapter methods always run in the calling tab.
 */
export class TabSharedAdapter implements EventAdapter {
  declare getHeadVersion?: EventAdapter["getHeadVersion"];
  declare getEarliestVersion?: EventAdapter["getEarliestVersion"];
  declare getBlock?: EventAdapter["getBlock"];
  declare getBlockHeights?: EventAdapter["getBlockHeights"];
  declare getTransactionEvents?: EventAdapter["getTransactionEvents"];
  declare getTransactionEventsByVersion?: EventAdapter["getTransactionEventsByVersion"];
  declare simulateTransactionEvents?: EventAdapter["simulateTransactionEvents"];
  declare view?: EventAdapter["view"];
  declare readObjects?: EventAdapter["readObjects"];

  private readonly source: EventAdapter;
  private readonly shared: EventAdapter;
  private readonly channel?: BroadcastChannelLike;
  private readonly tab = generateId();
  private readonly pending = new Map<number, Pending>();
  private readonly walks = new Map<string, { at: number; page: EventPage }>();
  private readonly shareFor: number;
  private readonly abort = new AbortController();
  private nextId = 0;
  private leading = false;
  private local = false;
  private closed = false;
  private release?: () => void;

  constructor(adapter: EventAdapter, options: TabSharedAdapterOptions) {
    this.source = requireAdapter(adapter, "TabSharedAdapter");
    if (typeof options?.name !== "string" || options.name === "") {
      throw new StreamdError("TabSharedAdapter needs a name");
    }
    const shareFor = options.shareFor ?? DEFAULT_SHARE_FOR;
    if (!(Number.isFinite(shareFor) && shareFor > 0)) {
      throw new StreamdError(`Invalid shareFor: ${String(shareFor)} (expected a positive number of ms)`);
    }
    this.shareFor = shareFor;
    this.shared = new SharedFetches(this.source, options.requestTimeout).view(shareFor);
    for (const name of OPTIONAL) {
      const method = this.source[name];
      if (method !== undefined) Object.assign(this, { [name]: method.bind(this.source) });
    }

    const locks =
      options.locks ?? (globalThis as { navigator?: { locks?: LockManagerLike } }).navigator?.locks;
    const channel = locks === undefined ? undefined : (options.channel ?? openChannel(options.name));
    if (locks === undefined || channel === undefined) {
      this.local = true;
      return;
    }
    this.channel = channel;
    channel.onmessage = (event) => this.receive(event.data);
    locks
      .request(options.name, { signal: this.abort.signal }, () => this.lead())
      .catch(() => {
        if (!this.closed) this.goLocal();
      });
  }

  /** True while this tab makes the requests for every tab. */
  get isLeader(): boolean {
    return this.leading;
  }

  fetchEvents(descriptor: EventDescriptor, options: FetchOptions = {}): Promise<EventPage> {
    if (this.closed) return Promise.reject(new StreamdError("TabSharedAdapter is closed"));
    if (this.local) return this.source.fetchEvents(descriptor, options);
    if (this.leading) return this.serve(descriptor, options);
    return new Promise((resolve, reject) => {
      const id = ++this.nextId;
      const request = { descriptor, options, resolve, reject };
      this.pending.set(id, request);
      this.ask(id, request);
    });
  }

  /** Give up leadership or a place in the queue, and fail every request still waiting. */
  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.leading = false;
    this.abort.abort();
    this.release?.();
    this.channel?.close();
    for (const p of this.pending.values()) p.reject(new StreamdError("TabSharedAdapter is closed"));
    this.pending.clear();
    this.walks.clear();
  }

  private lead(): Promise<void> {
    if (this.closed) return Promise.resolve();
    this.leading = true;
    this.post({ kind: "leader" });
    this.servePending((d, o) => this.serve(d, o));
    return new Promise((resolve) => (this.release = resolve));
  }

  private goLocal(): void {
    this.local = true;
    this.servePending((d, o) => this.source.fetchEvents(d, o));
  }

  private servePending(fetch: (d: EventDescriptor, o: FetchOptions) => Promise<EventPage>): void {
    const waiting = [...this.pending.values()];
    this.pending.clear();
    for (const p of waiting) fetch(p.descriptor, p.options).then(p.resolve, p.reject);
  }

  private async serve(descriptor: EventDescriptor, options: FetchOptions): Promise<EventPage> {
    const walk = walkKey(descriptor, options);
    if (walk !== undefined && options.cursor === undefined) {
      const newest = this.walks.get(walk);
      if (newest !== undefined && performance.now() - newest.at <= this.shareFor) return newest.page;
    }
    const page = await this.shared.fetchEvents(descriptor, options);
    // Only a caught-up page is at the head; a backfill's would start a joiner in the past.
    if (walk !== undefined && page.hasMore !== true) this.remember(walk, page);
    return page;
  }

  private remember(walk: string, page: EventPage): void {
    const now = performance.now();
    for (const [key, entry] of this.walks) {
      if (now - entry.at > this.shareFor) this.walks.delete(key);
    }
    this.walks.set(walk, { at: now, page });
  }

  private ask(id: number, request: Pending): void {
    try {
      this.channel!.postMessage({
        kind: "request",
        from: this.tab,
        id,
        descriptor: request.descriptor,
        options: request.options,
      } satisfies Message);
    } catch (err) {
      this.pending.delete(id);
      request.reject(new StreamdError("A request could not be sent to the leading tab", err));
    }
  }

  private post(message: Message): boolean {
    try {
      this.channel!.postMessage(message);
      return true;
    } catch {
      return false;
    }
  }

  // Any script on the origin can post here, so shapes are checked, not trusted.
  private receive(data: unknown): void {
    if (this.closed || !isObject(data)) return;
    const message = data as Partial<Message> & Record<string, unknown>;
    if (message.kind === "leader") {
      // The request the old leader took with it is asked again; a reply to both is taken once.
      if (!this.leading) for (const [id, request] of this.pending) this.ask(id, request);
    } else if (message.kind === "request") {
      if (this.leading) this.answer(message);
    } else if (message.kind === "reply" && message.to === this.tab) {
      this.settle(message);
    }
  }

  private answer(message: Record<string, unknown>): void {
    const { from: to, id, descriptor, options } = message;
    if (typeof to !== "string" || typeof id !== "number" || !isObject(descriptor)) return;
    const fetchOptions = isObject(options) ? (options as FetchOptions) : {};
    this.serve(descriptor as unknown as EventDescriptor, fetchOptions).then(
      (page) => {
        if (!this.post({ kind: "reply", to, id, page })) {
          const error = toWire(new StreamdError("The page could not be sent between tabs"));
          this.post({ kind: "reply", to, id, error });
        }
      },
      (err: unknown) => this.post({ kind: "reply", to, id, error: toWire(err) }),
    );
  }

  private settle(message: Record<string, unknown>): void {
    const request = typeof message.id === "number" ? this.pending.get(message.id) : undefined;
    if (request === undefined) return;
    this.pending.delete(message.id as number);
    const { page, error } = message;
    if (isObject(error)) {
      request.reject(
        fromWire({
          name: String(error.name),
          message: String(error.message),
          statusCode: num(error.statusCode),
          retryAfter: num(error.retryAfter),
        }),
      );
    } else if (isObject(page) && Array.isArray(page.events)) {
      request.resolve(page as unknown as EventPage);
    } else {
      request.reject(new StreamdError("The leading tab sent a malformed page"));
    }
  }
}

function num(value: unknown): number | undefined {
  return typeof value === "number" ? value : undefined;
}
