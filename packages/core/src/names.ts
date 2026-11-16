import { StreamdError } from "./errors.js";
import { normalizeAddress } from "./filter.js";
import { RequestLimiter } from "./limiter.js";
import type { StreamdEvent } from "./types.js";
import { raceAbort } from "./utils.js";

/** A chain's primary-name lookup for a long-form address, `undefined` for none. */
export type NameLookup = (address: string) => Promise<string | undefined>;

export const DEFAULT_NAME_TTL = 300_000;
const MAX_NAMES = 10_000;

/**
 * The per-client cache behind `withNames`, keyed by long-form address. Misses
 * are kept as `null`, failures are not; concurrent lookups for one address
 * share one request whatever expiry each caller asked for.
 */
export class SenderNames {
  private lookup?: NameLookup;
  private refusal = "withNames needs a client that looks up names, such as AptosClient or SuiClient";
  private readonly cache = new Map<string, { name: string | null; at: number }>();
  private readonly pending = new Map<string, Promise<string | null>>();

  constructor(
    private readonly limiter = new RequestLimiter(),
    private readonly now: () => number = Date.now,
  ) {}

  /** A chain client's lookup, or the reason this client cannot serve names. */
  serve(lookup: NameLookup | string): void {
    if (typeof lookup === "string") this.refusal = lookup;
    else this.lookup = lookup;
  }

  /** The expiry `withNames` asks for, `undefined` when off; throws on a bad value or when names are not served. */
  ttl(withNames: unknown): number | undefined {
    if (withNames === undefined || withNames === false) return undefined;
    let ttl: unknown = DEFAULT_NAME_TTL;
    if (withNames !== true) {
      const options = withNames as Record<string, unknown> | null;
      if (typeof options !== "object" || options === null || Array.isArray(options) || Object.keys(options).some((k) => k !== "ttl")) {
        throw new StreamdError("withNames must be true, false or { ttl }");
      }
      ttl = options.ttl ?? DEFAULT_NAME_TTL;
    }
    if (typeof ttl !== "number" || !(ttl >= 0)) {
      throw new StreamdError(`withNames ttl must be a non-negative number of ms, got ${String(ttl)}`);
    }
    if (this.lookup === undefined) throw new StreamdError(this.refusal);
    return ttl;
  }

  /** A copy of `event` with `senderName`, or `event` itself when it has no hex `sender`. */
  async attach(event: StreamdEvent, ttl: number, signal?: AbortSignal): Promise<StreamdEvent> {
    const address = event.sender === undefined ? undefined : normalizeAddress(event.sender);
    if (address === undefined) return event;
    return { ...event, senderName: await this.name(address, ttl, signal) };
  }

  /** The primary name of a long-form `address`, `null` for none, from the cache while younger than `ttl`. */
  async name(address: string, ttl: number, signal?: AbortSignal): Promise<string | null> {
    const hit = this.cache.get(address);
    if (hit !== undefined && this.now() - hit.at < ttl) return hit.name;
    return raceAbort(this.fetch(address, true), signal);
  }

  /**
   * `name`, for a caller already inside one of the limiter's slots, such as
   * a page fetch: going through the limiter again would wait on that slot,
   * forever under `maxConcurrentRequests: 1`.
   */
  async nameWithinSlot(address: string, ttl: number): Promise<string | null> {
    const hit = this.cache.get(address);
    if (hit !== undefined && this.now() - hit.at < ttl) return hit.name;
    return this.fetch(address, false);
  }

  private fetch(address: string, limited: boolean): Promise<string | null> {
    // Shared per mode: a lookup inside a slot must never wait on one queued for that slot.
    const key = `${limited ? "queued" : "in-slot"}:${address}`;
    let pending = this.pending.get(key);
    if (pending === undefined) {
      const lookup = () => this.lookup!(address);
      pending = (limited ? this.limiter.run(lookup) : Promise.resolve().then(lookup))
        .then((name) => {
          const found = name ?? null;
          this.cache.delete(address);
          if (this.cache.size >= MAX_NAMES) this.cache.delete(this.cache.keys().next().value!);
          this.cache.set(address, { name: found, at: this.now() });
          return found;
        })
        .finally(() => this.pending.delete(key));
      this.pending.set(key, pending);
    }
    return pending;
  }
}

const UNSERVED = new SenderNames();

/** `names.ttl(withNames)`, refusing `withNames` where no cache was given. */
export function nameTtl(withNames: unknown, names: SenderNames | undefined): number | undefined {
  return (names ?? UNSERVED).ttl(withNames);
}
