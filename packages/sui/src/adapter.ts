import { normalizeStructTag, normalizeSuiAddress } from "@mysten/sui/utils";
import { StreamdError, StreamdNetworkError } from "@streamd/core";
import type { EventAdapter, EventDescriptor, EventPage, FetchOptions, StreamdEvent } from "@streamd/core";
import { toJson, type ProtoValue } from "./json.js";

/** The `ListEvents` filter: terms are ORed, the literals inside one are ANDed. */
interface Literal {
  negated: boolean;
  predicate:
    | { oneofKind: "eventType"; eventType: { eventType: string } }
    | { oneofKind: "sender"; sender: { address: string } };
}
interface EventFilter {
  terms: { literals: Literal[] }[];
}

interface SuiEvent {
  eventType?: string;
  sender?: string;
  json?: ProtoValue;
  checkpoint?: bigint;
  transactionDigest?: string;
  transactionIndex?: bigint;
  eventIndex?: number;
}
interface ListEventsFrame {
  event?: SuiEvent;
  watermark?: { cursor?: Uint8Array; checkpoint?: bigint };
  end?: { reason?: number };
}
interface CheckpointResponse {
  checkpoint?: {
    sequenceNumber?: bigint;
    summary?: { totalNetworkTransactions?: bigint; timestamp?: { seconds: bigint; nanos: number } };
  };
}

/** The part of `@mysten/sui`'s `SuiGrpcClient` the adapter uses. */
export interface SuiLedgerClient {
  ledgerService: {
    listEvents(
      input: {
        readMask: { paths: string[] };
        filter?: EventFilter;
        startCheckpoint?: bigint;
        endCheckpoint?: bigint;
        options: { limit: number; ordering: number; after?: Uint8Array };
      },
      options?: { abort?: AbortSignal },
    ): { responses: AsyncIterable<ListEventsFrame> };
    getCheckpoint(input: {
      checkpointId: { oneofKind: "sequenceNumber"; sequenceNumber: bigint } | { oneofKind: undefined };
      readMask: { paths: string[] };
    }): PromiseLike<{ response: CheckpointResponse }>;
    getServiceInfo(input: object): PromiseLike<{ response: { checkpointHeight?: bigint; lowestAvailableCheckpoint?: bigint } }>;
  };
}

const ASCENDING = 0;
const ITEM_LIMIT = 1;
const SCAN_LIMIT = 2;
const DEFAULT_LIMIT = 25;
const KEPT_SUMMARIES = 4096;
const EVENT_FIELDS = ["event_type", "sender", "json", "checkpoint", "transaction_digest", "transaction_index", "event_index"];
const SUMMARY_FIELDS = ["sequence_number", "summary.total_network_transactions", "summary.timestamp"];

interface Summary {
  /** Transactions on the chain up to and including this checkpoint. */
  total: bigint;
  /** Microseconds since the epoch. */
  timestamp: bigint | undefined;
}

/** A gRPC failure as the poller and queries expect one: a network error, 429 when rate-limited, 410 when pruned. */
function networkError(err: unknown): never {
  if (err instanceof StreamdError) throw err;
  const code = (err as { code?: unknown } | null)?.code;
  // A checkpoint the node no longer has is pruned history, as a 410 is on the Aptos fullnode.
  const text = err instanceof Error ? err.message : String(err);
  // OUT_OF_RANGE also covers a range above the indexed tip, which a lagging replica can give; only "below" is pruned.
  const below = code === "OUT_OF_RANGE" && /below earliest/i.test(text);
  const status = code === "RESOURCE_EXHAUSTED" ? 429 : code === "NOT_FOUND" || below ? 410 : undefined;
  throw new StreamdNetworkError(`Sui gRPC request failed: ${text}`, status);
}

function eventType(type: unknown): string {
  try {
    return normalizeStructTag(type as string);
  } catch {
    throw new StreamdError(`Not a valid Sui event type: ${String(type)}`);
  }
}

function filterOf(descriptor: EventDescriptor): EventFilter {
  const byType = (t: string): Literal => ({ negated: false, predicate: { oneofKind: "eventType", eventType: { eventType: eventType(t) } } });
  switch (descriptor?.kind) {
    case "module": {
      const types = Array.isArray(descriptor.eventType) ? descriptor.eventType : [descriptor.eventType];
      if (types.length === 0) throw new StreamdError("Module descriptor requires at least one eventType");
      return { terms: types.map((t) => ({ literals: [byType(t as string)] })) };
    }
    case "account": {
      let address: string;
      try {
        address = normalizeSuiAddress(descriptor.accountAddress);
      } catch {
        throw new StreamdError(`Not a valid Sui address: ${String(descriptor.accountAddress)}`);
      }
      const sender: Literal = { negated: false, predicate: { oneofKind: "sender", sender: { address } } };
      if (descriptor.eventTypes === undefined) return { terms: [{ literals: [sender] }] };
      // An empty list matches nothing, which no filter term can say.
      return { terms: descriptor.eventTypes.map((t) => ({ literals: [sender, byType(t)] })) };
    }
    case "handle":
    case "resource":
      throw new StreamdError(`SuiAdapter cannot serve ${descriptor.kind} descriptors: Sui has no event handles or account resources`);
    default:
      throw new StreamdError(`Unknown descriptor kind: ${String((descriptor as { kind?: unknown })?.kind)}`);
  }
}

const pruned = (err: unknown) => err instanceof StreamdNetworkError && err.statusCode === 410;

const toBase64 = (bytes: Uint8Array) => Buffer.from(bytes).toString("base64");
const fromBase64 = (text: string) => {
  if (!/^[A-Za-z0-9+/]+={0,2}$/.test(text)) throw new StreamdError(`Not a cursor this adapter issued: ${text}`);
  return new Uint8Array(Buffer.from(text, "base64"));
};

/**
 * Serves Sui events through the `EventAdapter` contract, over the Sui gRPC
 * `LedgerService` of the `SuiGrpcClient` passed in. A `version` is Sui's
 * global transaction number, the previous checkpoint's running total plus
 * the transaction's index in its checkpoint, so relative starts count
 * transactions as on Aptos. Serves `module` and `account` descriptors.
 */
export class SuiAdapter implements EventAdapter {
  private readonly ledger: SuiLedgerClient["ledgerService"];
  private readonly summaries = new Map<bigint, Promise<Summary>>();

  constructor(client: SuiLedgerClient) {
    if (typeof client?.ledgerService?.listEvents !== "function") {
      throw new StreamdError("SuiAdapter needs a SuiGrpcClient from @mysten/sui/grpc");
    }
    this.ledger = client.ledgerService;
  }

  /** The last transaction of the newest checkpoint. */
  async getHeadVersion(): Promise<bigint> {
    return (await this.latest()).total - 1n;
  }

  /**
   * The first transaction the node can still place. The node's reported
   * lowest checkpoint can trail what it has kept, so the oldest readable one
   * is searched for; its own transactions need the one before it, which may
   * be gone, so the answer is then the start of the next.
   */
  async getEarliestVersion(): Promise<bigint> {
    let low = await this.lowest();
    let high = (await this.latest()).sequence;
    while (low < high) {
      const mid = (low + high) / 2n;
      if ((await this.readable(mid)) !== undefined) high = mid;
      else low = mid + 1n;
    }
    try {
      return await this.base(low);
    } catch (err) {
      if (!pruned(err)) throw err;
      return (await this.summary(low)).total;
    }
  }

  /** The checkpoint's summary, or `undefined` when the node has pruned it. */
  private async readable(checkpoint: bigint): Promise<Summary | undefined> {
    try {
      return await this.summary(checkpoint);
    } catch (err) {
      if (pruned(err)) return undefined;
      throw err;
    }
  }

  async fetchEvents(descriptor: EventDescriptor, options: FetchOptions = {}): Promise<EventPage> {
    const filter = filterOf(descriptor);
    if (descriptor.kind === "account" && descriptor.eventTypes?.length === 0) {
      return { events: [], nextCursor: options.cursor, hasMore: false };
    }
    const limit = options.limit ?? DEFAULT_LIMIT;
    if (!Number.isSafeInteger(limit) || limit < 0) throw new StreamdError(`Not a valid limit: ${limit}`);
    const after = options.cursor === undefined ? undefined : fromBase64(options.cursor);
    if (limit === 0) return { events: [], nextCursor: options.cursor, hasMore: false };
    const { fromVersion, toVersion } = options;
    // Without a cursor or a start, a walk starts at the head, as on the Aptos fullnode.
    const startCheckpoint =
      after !== undefined ? undefined : fromVersion !== undefined ? await this.startOf(fromVersion) : (await this.latest()).sequence;
    // The node's end bound is exclusive: stop after the checkpoint holding toVersion.
    const endCheckpoint = toVersion === undefined ? undefined : (await this.checkpointOf(toVersion)) + 1n;

    const frames: ListEventsFrame[] = [];
    try {
      const call = this.ledger.listEvents({
        readMask: { paths: EVENT_FIELDS },
        filter,
        ...(startCheckpoint !== undefined && { startCheckpoint }),
        ...(endCheckpoint !== undefined && { endCheckpoint }),
        options: { limit, ordering: ASCENDING, ...(after !== undefined && { after }) },
      });
      for await (const frame of call.responses) frames.push(frame);
    } catch (err) {
      networkError(err);
    }

    const events: StreamdEvent[] = [];
    let watermark: ListEventsFrame["watermark"];
    let reason: number | undefined;
    for (const frame of frames) {
      if (frame.watermark?.cursor !== undefined) watermark = frame.watermark;
      if (frame.end?.reason !== undefined) reason = frame.end.reason;
      if (frame.event === undefined) continue;
      const event = await this.toEvent(frame.event);
      if (fromVersion !== undefined && after === undefined && event.version < fromVersion) continue;
      if (toVersion !== undefined && event.version > toVersion) continue;
      events.push(event);
    }
    const page: EventPage = {
      events,
      nextCursor: watermark?.cursor === undefined ? options.cursor : toBase64(watermark.cursor),
      hasMore: reason === ITEM_LIMIT || reason === SCAN_LIMIT,
    };
    if (watermark?.checkpoint !== undefined) {
      const through = (await this.summary(watermark.checkpoint)).total - 1n;
      page.throughVersion = toVersion !== undefined && through > toVersion ? toVersion : through;
    }
    return page;
  }

  private async toEvent(raw: SuiEvent): Promise<StreamdEvent> {
    const { checkpoint, transactionIndex, eventIndex, eventType: type } = raw;
    if (checkpoint === undefined || transactionIndex === undefined || eventIndex === undefined || type === undefined) {
      throw new StreamdError("Sui node returned an event without its position or type");
    }
    const [base, own] = await Promise.all([this.base(checkpoint), this.summary(checkpoint)]);
    const event: StreamdEvent = {
      type: eventType(type),
      data: toJson(raw.json),
      version: base + transactionIndex,
      sequenceNumber: 0n,
      eventIndex,
    };
    if (raw.transactionDigest !== undefined) event.transactionHash = raw.transactionDigest;
    if (raw.sender !== undefined && raw.sender !== "") event.sender = normalizeSuiAddress(raw.sender);
    if (own.timestamp !== undefined) event.timestamp = own.timestamp;
    return event;
  }

  /** Transactions before `checkpoint`: the version of its first one. */
  private async base(checkpoint: bigint): Promise<bigint> {
    return checkpoint === 0n ? 0n : (await this.summary(checkpoint - 1n)).total;
  }

  /**
   * The checkpoint to start a walk from `version` at. The lowest checkpoint
   * the node keeps cannot place its own versions, which need the one before,
   * so a start that lands there is pruned history.
   */
  private async startOf(version: bigint): Promise<bigint> {
    const checkpoint = await this.checkpointOf(version);
    if (checkpoint > 0n && (await this.readable(checkpoint - 1n)) === undefined) {
      throw new StreamdNetworkError(`Version ${version} is older than this Sui node keeps`, 410);
    }
    return checkpoint;
  }

  /** The earliest checkpoint whose running total passes `version`, found by bisection. */
  private async checkpointOf(version: bigint): Promise<bigint> {
    if (typeof version !== "bigint" || version < 0n) throw new StreamdError(`Not a valid version: ${String(version)}`);
    let low = await this.lowest();
    let high = (await this.latest()).sequence;
    if ((await this.summary(high)).total <= version) return high;
    while (low < high) {
      const mid = (low + high) / 2n;
      // A pruned checkpoint lies below every one the node still has.
      const summary = await this.readable(mid);
      if (summary !== undefined && summary.total > version) high = mid;
      else low = mid + 1n;
    }
    return low;
  }

  private async lowest(): Promise<bigint> {
    try {
      const { response } = await this.ledger.getServiceInfo({});
      return response.lowestAvailableCheckpoint ?? 0n;
    } catch (err) {
      networkError(err);
    }
  }

  private async latest(): Promise<{ sequence: bigint; total: bigint }> {
    const { sequence, summary } = await this.readCheckpoint({ oneofKind: undefined });
    return { sequence, total: summary.total };
  }

  /** A checkpoint's summary, read once; a failed read is not kept. */
  private summary(checkpoint: bigint): Promise<Summary> {
    let found = this.summaries.get(checkpoint);
    if (found === undefined) {
      found = this.readCheckpoint({ oneofKind: "sequenceNumber", sequenceNumber: checkpoint }).then((c) => c.summary);
      found.catch(() => this.summaries.delete(checkpoint));
      this.summaries.set(checkpoint, found);
      if (this.summaries.size > KEPT_SUMMARIES) this.summaries.delete(this.summaries.keys().next().value!);
    }
    return found;
  }

  private async readCheckpoint(
    checkpointId: { oneofKind: "sequenceNumber"; sequenceNumber: bigint } | { oneofKind: undefined },
  ): Promise<{ sequence: bigint; summary: Summary }> {
    let response: CheckpointResponse;
    try {
      ({ response } = await this.ledger.getCheckpoint({ checkpointId, readMask: { paths: SUMMARY_FIELDS } }));
    } catch (err) {
      networkError(err);
    }
    const sequence = response.checkpoint?.sequenceNumber;
    const total = response.checkpoint?.summary?.totalNetworkTransactions;
    if (sequence === undefined || total === undefined) throw new StreamdError("Sui node returned a checkpoint without its totals");
    const stamp = response.checkpoint?.summary?.timestamp;
    const timestamp = stamp === undefined ? undefined : stamp.seconds * 1_000_000n + BigInt(Math.floor(stamp.nanos / 1000));
    return { sequence, summary: { total, timestamp } };
  }
}
