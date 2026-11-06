import { getAptosFullNode, type Aptos } from "@aptos-labs/ts-sdk";
import type {
  AccountEventDescriptor,
  BlockHeights,
  BlockInfo,
  CreationNumberEventDescriptor,
  EventAdapter,
  EventDescriptor,
  EventGuid,
  EventPage,
  FetchOptions,
  HandleEventDescriptor,
  ModuleEventDescriptor,
  StreamdEvent,
} from "../types.js";
import { StreamdError, StreamdNetworkError } from "../errors.js";
import {
  DEFAULT_LIMIT,
  handlePath,
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

/**
 * The raw guid, unless it is missing, malformed, or the zero guid module
 * events report. A bad guid drops only the field, never the page.
 */
function toGuid(raw: RawEvent): EventGuid | undefined {
  const guid = raw.guid;
  if (
    guid === undefined ||
    !/^\d+$/.test(String(guid.creation_number)) ||
    !/^0x[0-9a-fA-F]+$/.test(String(guid.account_address))
  ) {
    return undefined;
  }
  const creationNumber = BigInt(guid.creation_number);
  if (creationNumber === 0n && BigInt(guid.account_address) === 0n) return undefined;
  return { creationNumber, accountAddress: guid.account_address };
}

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
    const guid = toGuid(event);
    if (guid !== undefined) item.guid = guid;
    if (txn.timestamp !== undefined) {
      timestamp ??= BigInt(txn.timestamp);
      item.timestamp = timestamp;
    }
    out.push(item);
  });
  return out;
}

export function toStreamdError(err: unknown, what: string): StreamdError {
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

  /** The fullnode's oldest unpruned and newest block heights, whatever the descriptor. */
  async getBlockHeights(): Promise<BlockHeights> {
    try {
      const info = await this.aptos.getLedgerInfo();
      return { oldest: BigInt(info.oldest_block_height), newest: BigInt(info.block_height) };
    } catch (err) {
      throw toStreamdError(err, "block heights");
    }
  }

  /** The block at `height`, whatever the descriptor; a pruned block fails with status 410. */
  async getBlock(_descriptor: EventDescriptor, height: bigint): Promise<BlockInfo> {
    try {
      const block = await this.aptos.getBlockByHeight({
        blockHeight: height,
        options: { withTransactions: false },
      });
      return {
        height: BigInt(block.block_height),
        firstVersion: BigInt(block.first_version),
        lastVersion: BigInt(block.last_version),
        timestamp: BigInt(block.block_timestamp),
      };
    } catch (err) {
      throw toStreamdError(err, `block ${height}`);
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
    let ranDry = false;

    while (events.length < wanted && next <= head) {
      if (options.toVersion !== undefined && next > options.toVersion) break;
      if (scanned >= this.maxScan) {
        hasMore = true; // stopped on budget, not on data
        break;
      }

      const limit = Math.min(this.scanBatch, this.maxScan - scanned);
      const txns = await this.getTransactions(next, limit);
      if (txns.length === 0) {
        ranDry = true;
        break;
      }

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

    const page: EventPage = {
      events,
      nextCursor: next.toString(),
      hasMore: hasMore || events.length >= wanted,
    };
    // Everything scanned sits strictly below the head read before scanning, so
    // later transactions cannot change it. A start taken from the head can, and
    // so can a scan that ran dry below a head this node has not reached (the
    // SDK caches ledger info per network, not per node).
    const fixedStart = options.cursor !== undefined || options.fromVersion !== undefined;
    if (fixedStart && !ranDry && next <= head) page.final = true;
    return page;
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
    // A full batch by sequence number never changes; a short one ended at the
    // account's latest transaction, which a later request reads past.
    let batchesFull = false;

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
      batchesFull = (scanned === 0 || batchesFull) && txns.length === limit;
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

    const page: EventPage = {
      events,
      nextCursor: seq.toString(),
      hasMore: exhausted ? false : hasMore || events.length >= wanted,
    };
    const fixedStart =
      options.cursor !== undefined ||
      options.offset !== undefined ||
      options.fromVersion !== undefined ||
      options.toVersion !== undefined;
    if (fixedStart && batchesFull) page.final = true;
    return page;
  }

  /**
   * Handle events paginate by sequence number, so the cursor is a sequence
   * number rather than a ledger version. Both descriptor forms read the same
   * stream, by struct and field or by creation number.
   */
  private async fetchHandleEvents(
    descriptor: HandleEventDescriptor | CreationNumberEventDescriptor,
    options: FetchOptions,
  ): Promise<EventPage> {
    const limit = options.limit ?? DEFAULT_LIMIT;
    if (limit <= 0) return { events: [], hasMore: false };

    const start =
      options.cursor !== undefined
        ? parseCursor(options.cursor)
        : BigInt(options.offset ?? 0);

    const path = handlePath(descriptor);

    // Through the SDK, not bare fetch, so the config's headers and API key apply.
    const { data: raw } = await getAptosFullNode<
      {},
      Array<RawEvent & { version: string }>
    >({
      aptosConfig: this.aptos.config,
      originMethod: "fetchHandleEvents",
      path,
      params: { start: start.toString(), limit },
    });

    // This endpoint does not say where an event sits in its transaction, and a
    // page position restarts at 0 every page. The handle's sequence number is
    // unique and stable, so it stands in as the index.
    let events: StreamdEvent[] = raw.map((e) => {
      const event: StreamdEvent = {
        type: e.type,
        data: e.data,
        version: BigInt(e.version),
        sequenceNumber: BigInt(e.sequence_number),
        eventIndex: Number(e.sequence_number),
      };
      const guid = toGuid(e);
      if (guid !== undefined) event.guid = guid;
      return event;
    });

    if (options.fromVersion !== undefined) {
      events = events.filter((e) => e.version >= options.fromVersion!);
    }
    if (options.toVersion !== undefined) {
      events = events.filter((e) => e.version <= options.toVersion!);
    }

    // Advance by what the node returned, not by what survived filtering.
    const nextCursor = (start + BigInt(raw.length)).toString();
    if (options.orderBy === "desc") events.reverse();

    // The start comes from the request, and a full page of a handle's events
    // by sequence number never changes.
    const full = raw.length >= limit;
    return full
      ? { events, nextCursor, hasMore: true, final: true }
      : { events, nextCursor, hasMore: false };
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
