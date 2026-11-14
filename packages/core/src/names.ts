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
    const hit = this.cache.get(address);
    if (hit !== undefined && this.now() - hit.at <= ttl) return { ...event, senderName: hit.name };
    return { ...event, senderName: await raceAbort(this.fetch(address), signal) };
  }

  private fetch(address: string): Promise<string | null> {
    let pending = this.pending.get(address);
    if (pending === undefined) {
      pending = this.limiter
        .run(() => this.lookup!(address))
        .then((name) => {
          const found = name ?? null;
          this.cache.delete(address);
          if (this.cache.size >= MAX_NAMES) this.cache.delete(this.cache.keys().next().value!);
          this.cache.set(address, { name: found, at: this.now() });
          return found;
        })
        .finally(() => this.pending.delete(address));
      this.pending.set(address, pending);
    }
    return pending;
  }
}
