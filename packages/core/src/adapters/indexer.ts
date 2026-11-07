import { AptosApiType, type Aptos } from "@aptos-labs/ts-sdk";
import { StreamdError, StreamdNetworkError, parseRetryAfter } from "../errors.js";
import { toStreamdError } from "./fullnode.js";

const PROCESSOR = "account_transactions_processor";
const DEFAULT_MAX_SCAN = 100;

const INDEXED_QUERY = `query IndexedVersion($processor: String!) {
  processor_status(where: { processor: { _eq: $processor } }) { last_success_version }
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

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
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
 * read from the fullnode.
 */
export class IndexerAccountAdapter {
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
      throw new StreamdError("Indexer returned a body that is not JSON");
    }
    if (isObject(body) && Array.isArray(body.errors) && body.errors.length > 0) {
      throw graphqlFailure(body.errors, retryAfter);
    }
    if (!isObject(body) || !isObject(body.data)) throw new StreamdError("Indexer returned no data");
    return body.data;
  }
}
