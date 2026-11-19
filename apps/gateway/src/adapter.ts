import { StreamdError, StreamdNetworkError, isObject, parsePage, parseRetryAfter } from "@streamd/core";
import type { EventAdapter, EventDescriptor, EventPage, FetchOptions } from "@streamd/core";
import { fromWireError, toJson } from "./wire.js";

type Fetch = (url: string, init: RequestInit) => Promise<Response>;

export interface GatewayAdapterOptions {
  /** The gateway's base URL, such as `http://127.0.0.1:8787`. */
  url: string;
  /** The gateway's token, sent as `Authorization: Bearer`. */
  token?: string;
  /** Defaults to the global `fetch`. */
  fetch?: Fetch;
}

/**
 * An `EventAdapter` over a streamd gateway, for clients that should share the
 * gateway's pollers and node rather than reach a node themselves. A walk with
 * no start follows the gateway's live topic from its newest event, as every
 * other client of that descriptor does; one with a start, an end, an offset
 * or newest-first order reads through the gateway's node.
 */
export class GatewayAdapter implements EventAdapter {
  private readonly base: string;
  private readonly token?: string;
  private readonly fetchFn: Fetch;

  constructor(options: GatewayAdapterOptions) {
    let url: URL;
    try {
      url = new URL(options.url);
    } catch {
      throw new StreamdError(`Not a gateway URL: ${options.url}`);
    }
    if (url.protocol !== "http:" && url.protocol !== "https:") throw new StreamdError(`Not an http(s) gateway URL: ${options.url}`);
    this.base = url.href.replace(/\/+$/, "");
    this.token = options.token;
    this.fetchFn = options.fetch ?? ((url, init) => fetch(url, init));
  }

  async fetchEvents(descriptor: EventDescriptor, options: FetchOptions): Promise<EventPage> {
    return parsePage(await this.call("fetch", { descriptor, options }));
  }

  async getHeadVersion(descriptor: EventDescriptor): Promise<bigint> {
    return version(await this.call("head", { descriptor }));
  }

  async getEarliestVersion(descriptor: EventDescriptor): Promise<bigint> {
    return version(await this.call("earliest", { descriptor }));
  }

  private async call(route: string, body: unknown): Promise<unknown> {
    const headers: Record<string, string> = { "content-type": "application/json" };
    if (this.token !== undefined) headers.authorization = `Bearer ${this.token}`;
    let response: Response;
    let text: string;
    try {
      response = await this.fetchFn(`${this.base}/v1/${route}`, { method: "POST", headers, body: toJson(body) });
      text = await response.text();
    } catch (err) {
      throw new StreamdNetworkError(`Gateway request failed: ${err instanceof Error ? err.message : String(err)}`, undefined, err);
    }
    let parsed: unknown;
    try {
      parsed = JSON.parse(text);
    } catch {
      parsed = undefined;
    }
    if (!response.ok) throw fromWireError(parsed, response.status, parseRetryAfter(response.headers.get("retry-after")));
    return parsed;
  }
}

function version(body: unknown): bigint {
  if (!isObject(body) || typeof body.version !== "string" || !/^\d+$/.test(body.version)) {
    throw new StreamdNetworkError("The gateway answered without a version");
  }
  return BigInt(body.version);
}
