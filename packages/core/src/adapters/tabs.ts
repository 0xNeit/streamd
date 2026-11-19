import type { EventAdapter, EventDescriptor, EventPage, FetchOptions } from "../types.js";
import { StreamdError } from "../errors.js";
import { SharedFetches } from "../shared.js";
import { generateId } from "../utils.js";
import { requireAdapter } from "./validate.js";

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

function openChannel(name: string): BroadcastChannelLike | undefined {
  const Channel = (globalThis as { BroadcastChannel?: new (name: string) => BroadcastChannelLike })
    .BroadcastChannel;
  return Channel === undefined ? undefined : new Channel(name);
}

/**
 * Wraps `adapter` so that, across the browser tabs of one origin, only one
 * tab sends `fetchEvents` requests to it. Without Web Locks or
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
      this.pending.set(++this.nextId, { descriptor, options, resolve, reject });
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
  }

  private lead(): Promise<void> {
    if (this.closed) return Promise.resolve();
    this.leading = true;
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

  private serve(descriptor: EventDescriptor, options: FetchOptions): Promise<EventPage> {
    return this.shared.fetchEvents(descriptor, options);
  }
}
