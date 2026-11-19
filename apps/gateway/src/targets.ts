import { lookup as dnsLookup } from "node:dns/promises";
import { BlockList, isIP } from "node:net";
import { StreamdError } from "@streamd/core";

/** Resolves a host name to every address it has, as `dns.lookup` with `all: true` does. */
export type Lookup = (host: string) => Promise<readonly { address: string; family: number }[]>;

export interface TargetOptions {
  /** Allow loopback, private and other non-public addresses, for a gateway posting inside its own network. */
  allowPrivate?: boolean;
  lookup?: Lookup;
}

const MAX_URL = 2048;

const PRIVATE = new BlockList();
for (const [network, prefix] of [
  ["0.0.0.0", 8],
  ["10.0.0.0", 8],
  ["100.64.0.0", 10],
  ["127.0.0.0", 8],
  ["169.254.0.0", 16],
  ["172.16.0.0", 12],
  ["192.0.0.0", 24],
  ["192.0.2.0", 24],
  ["192.168.0.0", 16],
  ["198.18.0.0", 15],
  ["198.51.100.0", 24],
  ["203.0.113.0", 24],
  ["224.0.0.0", 4],
  ["240.0.0.0", 4],
] as const) {
  PRIVATE.addSubnet(network, prefix, "ipv4");
}
for (const [network, prefix] of [
  ["::", 128],
  ["::1", 128],
  ["64:ff9b::", 96],
  ["100::", 64],
  ["2001:db8::", 32],
  ["fc00::", 7],
  ["fe80::", 10],
  ["ff00::", 8],
] as const) {
  PRIVATE.addSubnet(network, prefix, "ipv6");
}

const systemLookup: Lookup = (host) => dnsLookup(host, { all: true, verbatim: true });

function refuse(message: string): never {
  throw new StreamdError(message);
}

/** Whether `address`, an IPv4 or IPv6 literal, is outside the public internet. */
export function isPrivateAddress(address: string): boolean {
  const family = isIP(address);
  if (family === 0) return true;
  // BlockList matches a v4-mapped IPv6 address against the IPv4 rules too.
  return PRIVATE.check(address, family === 4 ? "ipv4" : "ipv6");
}

/** `url` as a webhook target, refused unless it is http or https with no credentials. */
export function parseTarget(value: unknown): URL {
  if (typeof value !== "string" || value === "") refuse('"url" must be a non-empty string');
  if (value.length > MAX_URL) refuse(`"url" is limited to ${MAX_URL} characters`);
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    refuse('"url" must be an absolute URL');
  }
  if (url.protocol !== "http:" && url.protocol !== "https:") refuse('"url" must be http or https');
  if (url.username !== "" || url.password !== "") refuse('"url" must not carry credentials');
  return url;
}

/**
 * Throws a `StreamdError` unless every address `url`'s host resolves to is
 * public, or `allowPrivate` is set. A name re-pointed between this check and
 * the connection is not caught: the address is not pinned for the request.
 */
export async function checkTarget(url: URL, options: TargetOptions = {}): Promise<void> {
  if (options.allowPrivate) return;
  const host = url.hostname.startsWith("[") ? url.hostname.slice(1, -1) : url.hostname;
  let addresses: readonly string[];
  if (isIP(host) !== 0) {
    addresses = [host];
  } else {
    try {
      addresses = (await (options.lookup ?? systemLookup)(host)).map((answer) => answer.address);
    } catch {
      refuse(`cannot resolve ${host}`);
    }
  }
  if (addresses.length === 0) refuse(`cannot resolve ${host}`);
  if (addresses.some(isPrivateAddress)) refuse(`${host} is not a public address; the gateway refuses private targets`);
}
