import { getAptosFullNode, type Aptos } from "@aptos-labs/ts-sdk";
import type {
  AccountEventDescriptor,
  EventAdapter,
  EventDescriptor,
  EventPage,
  FetchOptions,
  HandleEventDescriptor,
  ModuleEventDescriptor,
  StreamdEvent,
} from "../types.js";
import { StreamdError, StreamdNetworkError } from "../errors.js";
import {
  DEFAULT_LIMIT,
  moduleTypeFilter,
  parseCursor,
  requireAddress,
  validateOffset,
} from "./validate.js";

interface RawEvent {
  guid?: { creation_number: string; account_address: string };
  sequence_number: string;
  type: string;
  data: unknown;
}

interface TransactionWithEvents {
  version?: string;
  hash?: string;
  timestamp?: string;
  sender?: string;
  type: string;
  events?: RawEvent[];
}

/** Transactions requested per underlying REST call while scanning. */
const DEFAULT_SCAN_BATCH = 100;
/**
 * Ceiling on transactions scanned in a single fetchEvents call. Bounds the
 * work done before returning, so a sparse event type yields control (with
 * hasMore set) instead of blocking indefinitely.
 */
const DEFAULT_MAX_SCAN = 1000;

/** Events of one transaction whose type passes `keep`, indexed by position in it. */
function toStreamdEvents(
  txn: TransactionWithEvents,
  version: bigint,
  keep: (type: string) => boolean,
): StreamdEvent[] {
  const out: StreamdEvent[] = [];
  let timestamp: bigint | undefined;
  txn.events?.forEach((event, eventIndex) => {
    if (!keep(event.type)) return;
    const item: StreamdEvent = {
      type: event.type,
      data: event.data,
      version,
      sequenceNumber: BigInt(event.sequence_number ?? "0"),
      eventIndex,
    };
    if (txn.hash !== undefined) item.transactionHash = txn.hash;
    if (txn.sender !== undefined) item.sender = txn.sender;
    if (txn.timestamp !== undefined) {
      timestamp ??= BigInt(txn.timestamp);
      item.timestamp = timestamp;
    }
    out.push(item);
  });
  return out;
}

function toStreamdError(err: unknown, what: string): StreamdError {
  if (err instanceof StreamdError) return err;
  const status = (err as { status?: unknown } | null)?.status;
  return new StreamdNetworkError(
    `Fullnode request for ${what} failed: ${
      err instanceof Error ? err.message : String(err)
    }`,
    typeof status === "number" ? status : undefined,
    err,
  );
}

export interface FullnodeScanAdapterOptions {
  /** Transactions per underlying REST call (default: 100). */
  scanBatch?: number;
  /** Max transactions scanned per fetchEvents call (default: 1000). */
  maxScan?: number;
}

/**
 * Reads events from an Aptos fullnode.
 *
 * Handle events use the dedicated `/accounts/{addr}/events/{handle}/{field}`
 * endpoint and paginate by sequence number — this path is exact and cheap.
 *
 * Module events have no by-type endpoint and no hosted index (the Indexer's
 * `events` table was removed), so they are found by scanning transactions and
 * filtering client-side. That is workable for tailing the chain head, which is
 * what `StreamdClient.on()` does. It is *not* suitable for deep historical
 * queries: fullnodes prune, and a rare event type costs a full scan of every
 * intervening transaction. For deep history, implement `EventAdapter` against
 * your own index and pass it as `StreamdConfig.adapter`.
 *
 * Account events walk one account's transaction history via
 * `/accounts/{addr}/transactions`, paginating by that account's sequence
 * number. Because it is indexed per-account it stays cheap over deep history,
 * unlike the chain-wide module scan.
 *
 * The limit to understand before relying on it: that endpoint returns
 * transactions the account *sent*. An event where the account is only a
 * recipient — someone else's transfer depositing into it — belongs to the
 * sender's transaction and will never appear. Send-side activity is exact;
 * "everything touching this account" is not expressible against a fullnode
 * and needs a real index.
 */
export class FullnodeScanAdapter implements EventAdapter {
  private readonly scanBatch: number;
  private readonly maxScan: number;

  constructor(
    private readonly aptos: Aptos,
    options: FullnodeScanAdapterOptions = {},
  ) {
    this.scanBatch = options.scanBatch ?? DEFAULT_SCAN_BATCH;
    this.maxScan = options.maxScan ?? DEFAULT_MAX_SCAN;
  }

  /** Only ever rejects with a `StreamdError`, whichever path failed. */
  async fetchEvents(
    descriptor: EventDescriptor,
    options: FetchOptions,
  ): Promise<EventPage> {
    try {
      if (options.cursor !== undefined) parseCursor(options.cursor);
      validateOffset(options.offset);
      return await this.dispatch(descriptor, options);
    } catch (err) {
      throw toStreamdError(err, `${descriptor.kind} events`);
    }
  }

  /** The fullnode's ledger version, whatever the descriptor. */
  async getHeadVersion(): Promise<bigint> {
    try {
      return await this.ledgerVersion();
    } catch (err) {
      throw toStreamdError(err, "ledger version");
    }
  }

  /** The fullnode's oldest unpruned ledger version, whatever the descriptor. */
  async getEarliestVersion(): Promise<bigint> {
    try {
      const info = await this.aptos.getLedgerInfo();
      return BigInt(info.oldest_ledger_version);
    } catch (err) {
      throw toStreamdError(err, "oldest ledger version");
    }
  }

  private dispatch(
    descriptor: EventDescriptor,
    options: FetchOptions,
  ): Promise<EventPage> {
    switch (descriptor.kind) {
      case "handle":
        return this.fetchHandleEvents(descriptor, options);
      case "account":
        return this.fetchAccountEvents(descriptor, options);
      default:
        return this.fetchModuleEvents(descriptor, options);
    }
  }

  /**
   * Scans forward from the cursor collecting matching events.
   *
   * The returned cursor tracks the last transaction *scanned*, not the last
   * event *matched*, so callers advance past windows with no matches rather
   * than re-reading them forever.
   */
  private async fetchModuleEvents(
    descriptor: ModuleEventDescriptor,
    options: FetchOptions,
  ): Promise<EventPage> {
    const keep = moduleTypeFilter(descriptor.eventType);
    const wanted = options.limit ?? DEFAULT_LIMIT;
    if (wanted <= 0) return { events: [], hasMore: false };

    const head = await this.ledgerVersion();
    let next = this.resolveStart(options, head);
    const events: StreamdEvent[] = [];
    let scanned = 0;
    let hasMore = false;

    while (events.length < wanted && next <= head) {
      if (options.toVersion !== undefined && next > options.toVersion) break;
      if (scanned >= this.maxScan) {
        hasMore = true; // stopped on budget, not on data
        break;
      }

      const limit = Math.min(this.scanBatch, this.maxScan - scanned);
      const txns = await this.getTransactions(next, limit);
      if (txns.length === 0) break;

      let stoppedAtToVersion = false;
      for (const txn of txns) {
        if (txn.version === undefined) continue;
        const version = BigInt(txn.version);
        if (options.toVersion !== undefined && version > options.toVersion) {
          stoppedAtToVersion = true;
          break;
        }
        next = version + 1n;
        events.push(...toStreamdEvents(txn, version, keep));
      }

      scanned += txns.length;
      if (stoppedAtToVersion) break;
    }

    // A single transaction can push us past `wanted`; keep the overflow rather
    // than dropping events the cursor has already moved past.
    if (options.orderBy === "desc") events.reverse();

    return {
      events,
      nextCursor: next.toString(),
      hasMore: hasMore || events.length >= wanted,
    };
  }

  /**
   * Walks one account's transactions, collecting their events.
   *
   * The cursor is the account's transaction sequence number, not a ledger
   * version — `fromVersion`/`toVersion` therefore act as client-side filters
   * here rather than as seek positions. Passing `fromVersion` without a cursor
   * starts the walk at sequence 0, since there is no way to seek an account's
   * history by ledger version.
   */
  private async fetchAccountEvents(
    descriptor: AccountEventDescriptor,
    options: FetchOptions,
  ): Promise<EventPage> {
    requireAddress(descriptor.accountAddress);
    const wanted = options.limit ?? DEFAULT_LIMIT;
    if (wanted <= 0) return { events: [], hasMore: false };

    const types =
      descriptor.eventTypes !== undefined
        ? new Set(descriptor.eventTypes)
        : undefined;

    let seq = await this.resolveAccountStart(descriptor, options);

    const events: StreamdEvent[] = [];
    let scanned = 0;
    let hasMore = false;
    let exhausted = false;

    while (events.length < wanted) {
      if (scanned >= this.maxScan) {
        hasMore = true; // stopped on budget, not on data
        break;
      }

      const limit = Math.min(this.scanBatch, this.maxScan - scanned);
      const txns = await this.getAccountTransactions(
        descriptor.accountAddress,
        seq,
        limit,
      );
      if (txns.length === 0) {
        exhausted = true;
        break;
      }

      seq += BigInt(txns.length);
      scanned += txns.length;

      let pastToVersion = false;

      for (const txn of txns) {
        if (txn.version === undefined) continue;
        const version = BigInt(txn.version);

        // An account's transactions ascend in version, so once we pass
        // toVersion nothing later can qualify.
        if (options.toVersion !== undefined && version > options.toVersion) {
          pastToVersion = true;
          break;
        }
        if (options.fromVersion !== undefined && version < options.fromVersion) {
          continue;
        }

        events.push(
          ...toStreamdEvents(txn, version, (t) => types === undefined || types.has(t)),
        );
      }

      if (pastToVersion) {
        exhausted = true;
        break;
      }
    }

    return {
      events,
      nextCursor: seq.toString(),
      hasMore: exhausted ? false : hasMore || events.length >= wanted,
    };
  }

  /**
   * Handle events paginate by sequence number, so the cursor is a sequence
   * number rather than a ledger version.
   */
  private async fetchHandleEvents(
    descriptor: HandleEventDescriptor,
    options: FetchOptions,
  ): Promise<EventPage> {
    const limit = options.limit ?? DEFAULT_LIMIT;
    if (limit <= 0) return { events: [], hasMore: false };

    const start =
      options.cursor !== undefined
        ? parseCursor(options.cursor)
        : BigInt(options.offset ?? 0);

    // The address lands in the URL path, so it is validated before use.
    const address = requireAddress(descriptor.accountAddress);

    // Through the SDK, not bare fetch, so the config's headers and API key apply.
    const { data: raw } = await getAptosFullNode<
      {},
      Array<RawEvent & { version: string }>
    >({
      aptosConfig: this.aptos.config,
      originMethod: "fetchHandleEvents",
      path:
        `accounts/${address}/events/` +
        `${encodeURIComponent(descriptor.eventHandleStruct)}/` +
        `${encodeURIComponent(descriptor.fieldName)}`,
      params: { start: start.toString(), limit },
    });

    // This endpoint does not say where an event sits in its transaction, and a
    // page position restarts at 0 every page. The handle's sequence number is
    // unique and stable, so it stands in as the index.
    let events: StreamdEvent[] = raw.map((e) => ({
      type: e.type,
      data: e.data,
      version: BigInt(e.version),
      sequenceNumber: BigInt(e.sequence_number),
      eventIndex: Number(e.sequence_number),
    }));

    if (options.fromVersion !== undefined) {
      events = events.filter((e) => e.version >= options.fromVersion!);
    }
    if (options.toVersion !== undefined) {
      events = events.filter((e) => e.version <= options.toVersion!);
    }

    // Advance by what the node returned, not by what survived filtering.
    const nextCursor = (start + BigInt(raw.length)).toString();
    if (options.orderBy === "desc") events.reverse();

    return { events, nextCursor, hasMore: raw.length >= limit };
  }

  /** Cursor wins over fromVersion; neither means "start at the head". */
  private resolveStart(options: FetchOptions, head: bigint): bigint {
    if (options.cursor !== undefined) return parseCursor(options.cursor);
    if (options.fromVersion !== undefined) return options.fromVersion;
    return head;
  }

  /**
   * Cursor, then offset, then sequence 0 when a version filter is set (no way
   * to seek by version), otherwise the account's latest sequence number so a
   * fresh subscription tails rather than replaying the whole history.
   */
  private async resolveAccountStart(
    descriptor: AccountEventDescriptor,
    options: FetchOptions,
  ): Promise<bigint> {
    if (options.cursor !== undefined) return parseCursor(options.cursor);
    if (options.offset !== undefined) return BigInt(options.offset);
    if (options.fromVersion !== undefined || options.toVersion !== undefined) {
      return 0n;
    }
    const info = await this.aptos.getAccountInfo({
      accountAddress: descriptor.accountAddress,
    });
    return BigInt(info.sequence_number);
  }

  private async getAccountTransactions(
    accountAddress: string,
    start: bigint,
    limit: number,
  ): Promise<TransactionWithEvents[]> {
    return (await this.aptos.getAccountTransactions({
      accountAddress,
      options: { offset: Number(start), limit },
    })) as unknown as TransactionWithEvents[];
  }

  private async ledgerVersion(): Promise<bigint> {
    const info = await this.aptos.getLedgerInfo();
    return BigInt(info.ledger_version);
  }

  private async getTransactions(
    start: bigint,
    limit: number,
  ): Promise<TransactionWithEvents[]> {
    return (await this.aptos.getTransactions({
      options: { offset: Number(start), limit },
    })) as unknown as TransactionWithEvents[];
  }
}
