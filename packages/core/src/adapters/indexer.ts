import { AptosApiType, type Aptos } from "@aptos-labs/ts-sdk";
import type {
  AccountEventDescriptor,
  EventAdapter,
  EventDescriptor,
  EventPage,
  FetchOptions,
  StreamdEvent,
} from "../types.js";
import { StreamdError, StreamdNetworkError, parseRetryAfter } from "../errors.js";
import { toStreamdError, toStreamdEvents, type TransactionWithEvents } from "./fullnode.js";
import { DEFAULT_LIMIT, isObject, parseCursor, requireAddress } from "./validate.js";

const PROCESSOR = "account_transactions_processor";
const DEFAULT_MAX_SCAN = 100;
/** Rows per `account_transactions` request. */
const INDEXER_PAGE = 100;

const INDEXED_QUERY = `query IndexedVersion($processor: String!) {
  processor_status(where: { processor: { _eq: $processor } }) { last_success_version }
}`;

const TRANSACTIONS_QUERY = `query AccountTransactions($address: String!, $from: bigint!, $to: bigint!, $limit: Int!) {
  account_transactions(
    where: { account_address: { _eq: $address }, transaction_version: { _gte: $from, _lte: $to } }
    order_by: { transaction_version: asc }
    limit: $limit
  ) { transaction_version }
}`;

export interface IndexerAccountAdapterOptions {
  /** GraphQL endpoint (default: the Indexer of the `Aptos` instance's network). */
  indexerUrl?: string;
  /** Sent as `Authorization: Bearer <apiKey>`; anonymous when omitted. */
  apiKey?: string;
  /** The `fetch` the Indexer is called with (default: `globalThis.fetch`). */
  fetch?: (url: string, init: RequestInit) => Promise<Response>;
  /** Most transactions read per `fetchEvents` call (default: 100). */
  maxScan?: number;
}

/** A ledger version as the Indexer (number) or fullnode (decimal string) sends it. */
function toVersion(value: unknown): bigint | undefined {
  if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) return BigInt(value);
  if (typeof value === "string" && /^\d+$/.test(value)) return BigInt(value);
  return undefined;
}

function resolveUrl(aptos: Aptos, indexerUrl: unknown): string {
  let url: string;
  if (indexerUrl !== undefined) {
    if (typeof indexerUrl !== "string") throw new StreamdError("indexerUrl must be a string");
    url = indexerUrl;
  } else {
    try {
      url = aptos.config.getRequestUrl(AptosApiType.INDEXER);
    } catch {
      throw new StreamdError("This network has no default Indexer; pass indexerUrl");
    }
  }
  try {
    if (/^https?:$/.test(new URL(url).protocol)) return url;
  } catch {}
  throw new StreamdError("indexerUrl must be an http(s) URL");
}

/** Checks the fields `toStreamdEvents` reads, so a malformed transaction is refused rather than mapped. */
function requireTransaction(txn: unknown, version: bigint): TransactionWithEvents {
  const malformed = new StreamdError(`Fullnode returned a malformed transaction at version ${version}`);
  if (!isObject(txn) || toVersion(txn.version) !== version) throw malformed;
  for (const key of ["hash", "sender"]) {
    if (txn[key] !== undefined && typeof txn[key] !== "string") throw malformed;
  }
  if (txn.timestamp !== undefined && toVersion(txn.timestamp) === undefined) throw malformed;
  const events = txn.events;
  if (events !== undefined && !Array.isArray(events)) throw malformed;
  for (const event of events ?? []) {
    if (!isObject(event) || typeof event.type !== "string") throw malformed;
    if (event.guid !== undefined && !isObject(event.guid)) throw malformed;
    if (event.sequence_number !== undefined && toVersion(event.sequence_number) === undefined) throw malformed;
  }
  return txn as unknown as TransactionWithEvents;
}

/** Why a GraphQL `errors` entry failed, kept short since the text is the server's. */
function graphqlFailure(errors: unknown[], retryAfter: number | undefined): StreamdNetworkError {
  const first = isObject(errors[0]) ? errors[0] : {};
  const code = isObject(first.extensions) ? first.extensions.code : undefined;
  const message = typeof first.message === "string" ? first.message.slice(0, 200) : "unknown error";
  const limited = code === 429 || code === "429";
  return new StreamdNetworkError(
    `Indexer query failed: ${message}`,
    limited ? 429 : undefined,
    undefined,
    retryAfter,
  );
}

/**
 * Serves `account` descriptors with every transaction touching the account,
 * received ones included, which the fullnode cannot list: the Aptos Indexer's
 * `account_transactions` table names the versions, then each one's events are
 * read from the fullnode, one read per transaction.
 *
 * Reads never pass the Indexer's processed version (`getHeadVersion`): a
 * listing above it is incomplete, and paging past a gap would skip it for
 * good. The cursor is the next ledger version to read. Without one,
 * `fromVersion` is raised to the fullnode's oldest version, since older
 * events cannot be read, and with neither the walk starts at the processed
 * version. Pages are never `final`.
 */
export class IndexerAccountAdapter implements EventAdapter {
  private readonly url: string;
  private readonly apiKey: string | undefined;
  private readonly fetch: (url: string, init: RequestInit) => Promise<Response>;
  private readonly maxScan: number;

  constructor(
    private readonly aptos: Aptos,
    options: IndexerAccountAdapterOptions = {},
  ) {
    this.url = resolveUrl(aptos, options.indexerUrl);
    if (options.apiKey !== undefined && (typeof options.apiKey !== "string" || options.apiKey === "")) {
      throw new StreamdError("apiKey must be a non-empty string");
    }
    this.apiKey = options.apiKey;
    if (options.fetch !== undefined && typeof options.fetch !== "function") {
      throw new StreamdError("fetch must be a function");
    }
    this.fetch = options.fetch ?? ((url, init) => globalThis.fetch(url, init));
    const maxScan = options.maxScan ?? DEFAULT_MAX_SCAN;
    if (!Number.isSafeInteger(maxScan) || maxScan < 1) {
      throw new StreamdError("maxScan must be a positive integer");
    }
    this.maxScan = maxScan;
  }

  /** Only `account` descriptors; anything else is refused before any request. */
  async fetchEvents(descriptor: EventDescriptor, options: FetchOptions): Promise<EventPage> {
    if (descriptor?.kind !== "account") {
      throw new StreamdError(
        `IndexerAccountAdapter serves account descriptors only, not ${String(descriptor?.kind)}`,
      );
    }
    const address = requireAddress(descriptor.accountAddress);
    const cursor = options.cursor === undefined ? undefined : parseCursor(options.cursor);
    if (options.offset !== undefined) {
      throw new StreamdError("IndexerAccountAdapter takes no offset; start at fromVersion or a cursor");
    }
    const wanted = options.limit ?? DEFAULT_LIMIT;
    if (!(wanted > 0)) return { events: [], hasMore: false };

    const indexed = await this.getHeadVersion();
    let start = cursor ?? indexed;
    if (cursor === undefined && options.fromVersion !== undefined) {
      const earliest = await this.getEarliestVersion();
      start = options.fromVersion > earliest ? options.fromVersion : earliest;
    }
    const end =
      options.toVersion !== undefined && options.toVersion < indexed ? options.toVersion : indexed;
    const { events, next } = await this.walk(descriptor, address, start, end, wanted);

    if (options.orderBy === "desc") events.reverse();
    const page: EventPage = { events, nextCursor: next.toString(), hasMore: next <= end };
    if (next > start) page.throughVersion = next - 1n;
    return page;
  }

  /** Whole transactions from `start` until `wanted` events, `maxScan` reads, or past `end`. */
  private async walk(
    descriptor: AccountEventDescriptor,
    address: string,
    start: bigint,
    end: bigint,
    wanted: number,
  ): Promise<{ events: StreamdEvent[]; next: bigint }> {
    const types = descriptor.eventTypes === undefined ? undefined : new Set(descriptor.eventTypes);
    const keep = (type: string) => types === undefined || types.has(type);
    const events: StreamdEvent[] = [];
    let next = start;
    let scanned = 0;
    while (next <= end && events.length < wanted && scanned < this.maxScan) {
      const limit = Math.min(INDEXER_PAGE, this.maxScan - scanned);
      const versions = await this.listVersions(address, next, end, limit);
      let read = 0;
      for (const version of versions) {
        if (events.length >= wanted) break;
        // ponytail: one sequential fullnode read per transaction; batch or parallelise if latency matters.
        events.push(...toStreamdEvents(await this.readTransaction(version), version, keep));
        next = version + 1n;
        read++;
      }
      scanned += read;
      // A short listing, read in full, covered everything up to `end`.
      if (read === versions.length && versions.length < limit) next = end + 1n;
    }
    return { events, next };
  }

  /** Versions touching `address` in `[from, to]`, ascending, checked to be just that. */
  private async listVersions(address: string, from: bigint, to: bigint, limit: number): Promise<bigint[]> {
    const data = await this.query(TRANSACTIONS_QUERY, {
      address,
      from: from.toString(),
      to: to.toString(),
      limit,
    });
    const rows = data.account_transactions;
    if (!Array.isArray(rows) || rows.length > limit) {
      throw new StreamdError("Indexer returned a malformed account_transactions listing");
    }
    let last = from - 1n;
    return rows.map((row) => {
      const version = isObject(row) ? toVersion(row.transaction_version) : undefined;
      if (version === undefined || version <= last || version > to) {
        throw new StreamdError("Indexer returned a malformed account_transactions listing");
      }
      last = version;
      return version;
    });
  }

  private async readTransaction(version: bigint): Promise<TransactionWithEvents> {
    let txn: unknown;
    try {
      txn = await this.aptos.getTransactionByVersion({ ledgerVersion: version });
    } catch (err) {
      throw toStreamdError(err, `transaction ${version}`);
    }
    return requireTransaction(txn, version);
  }

  /**
   * The newest version the Indexer has processed account transactions
   * through, whatever the descriptor. Above it, listings are incomplete.
   */
  async getHeadVersion(): Promise<bigint> {
    const data = await this.query(INDEXED_QUERY, { processor: PROCESSOR });
    const rows = data.processor_status;
    const row = Array.isArray(rows) && rows.length === 1 && isObject(rows[0]) ? rows[0] : undefined;
    const version = toVersion(row?.last_success_version);
    if (version === undefined) {
      throw new StreamdError(`Indexer reported no processed version for ${PROCESSOR}`);
    }
    return version;
  }

  /**
   * The fullnode's oldest unpruned version, whatever the descriptor. The
   * Indexer lists older transactions, but their events can no longer be read.
   */
  async getEarliestVersion(): Promise<bigint> {
    let oldest: unknown;
    try {
      oldest = (await this.aptos.getLedgerInfo()).oldest_ledger_version;
    } catch (err) {
      throw toStreamdError(err, "oldest ledger version");
    }
    const version = toVersion(oldest);
    if (version === undefined) throw new StreamdError("Fullnode returned a malformed oldest ledger version");
    return version;
  }

  /** One GraphQL POST; the API key never reaches an error. */
  private async query(
    query: string,
    variables: Record<string, unknown>,
  ): Promise<Record<string, unknown>> {
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (this.apiKey !== undefined) headers.Authorization = `Bearer ${this.apiKey}`;
    let response: Response;
    try {
      response = await this.fetch(this.url, {
        method: "POST",
        headers,
        body: JSON.stringify({ query, variables }),
      });
    } catch (err) {
      throw new StreamdNetworkError(
        `Indexer request failed: ${err instanceof Error ? err.message : "unknown error"}`,
      );
    }
    const retryAfter = parseRetryAfter(response.headers.get("retry-after"));
    if (!response.ok) {
      throw new StreamdNetworkError(
        `Indexer answered HTTP ${response.status}`,
        response.status,
        undefined,
        retryAfter,
      );
    }
    let body: unknown;
    try {
      body = await response.json();
    } catch {
      // A gateway's error page sent as 200 is a passing fault, so it is retried like one.
      throw new StreamdNetworkError(
        "Indexer returned a body that is not JSON",
        response.status,
        undefined,
        retryAfter,
      );
    }
    if (isObject(body) && Array.isArray(body.errors) && body.errors.length > 0) {
      throw graphqlFailure(body.errors, retryAfter);
    }
    if (!isObject(body) || !isObject(body.data)) throw new StreamdError("Indexer returned no data");
    return body.data;
  }
}
