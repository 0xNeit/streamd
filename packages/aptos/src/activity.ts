import { StreamdError, normalizeAddress } from "@streamd/core";
import type { AccountEventDescriptor, EventAdapter, EventPage, FetchOptions, StreamdEvent, Transfer } from "@streamd/core";
import { TRANSFER_DESCRIPTOR, transferSource } from "./transfers.js";
import type { Activity, BalanceDelta } from "./types.js";

/**
 * Every transaction that affected `account`, the address or name as given;
 * the source puts the resolved address in before any read.
 */
export function activityDescriptor(account: string): AccountEventDescriptor {
  return { kind: "account", accountAddress: account, received: true };
}

function push<K, V>(map: Map<K, V[]>, key: K, value: V): void {
  const list = map.get(key);
  if (list === undefined) map.set(key, [value]);
  else list.push(value);
}

const byIdentity = (a: StreamdEvent, b: StreamdEvent) =>
  a.version === b.version ? a.eventIndex - b.eventIndex : a.version < b.version ? -1 : 1;

function deltas(transfers: Transfer[], who: string): BalanceDelta[] {
  const sums = new Map<string, bigint>();
  for (const { asset, amount, from, to } of transfers) {
    const delta = (to === who ? amount : 0n) - (from === who ? amount : 0n);
    sums.set(asset, (sums.get(asset) ?? 0n) + delta);
  }
  return [...sums].filter(([, delta]) => delta !== 0n).map(([asset, delta]) => ({ asset, delta }));
}

/** One transaction's entry, keeping its first event's identity. */
function entry(events: StreamdEvent[], transfers: Transfer[], who: string): StreamdEvent<Activity> {
  const first = events[0]!;
  const sender = first.sender === undefined ? undefined : normalizeAddress(first.sender);
  return {
    ...first,
    data: {
      direction: sender === who ? "sent" : "received",
      transfers,
      balanceChanges: deltas(transfers, who),
      events,
    },
  };
}

async function withNames(
  record: StreamdEvent<Activity>,
  who: string,
  name: (address: string) => Promise<string | null>,
): Promise<StreamdEvent<Activity>> {
  const sender = record.sender === undefined ? undefined : normalizeAddress(record.sender);
  const addresses = new Set([who, sender, ...record.data.transfers.flatMap((t) => [t.from, t.to])]);
  addresses.delete(undefined);
  const names = await Promise.all([...addresses].map(async (a) => [a!, await name(a!)] as const));
  return { ...record, data: { ...record.data, names: Object.fromEntries(names) } };
}

/**
 * `adapter` as a source of activity records: each page of an `account`
 * descriptor with `received: true` becomes one record per transaction, its
 * transfers built by `transferSource` over the same page and kept when
 * `account` sent or received them. `name`, when given, attaches `names`.
 *
 * Grouping sees one page; `IndexerAccountAdapter` pages hold whole
 * transactions.
 */
export function activitySource(
  adapter: EventAdapter,
  account: () => Promise<string>,
  withAssetInfo?: boolean,
  requestTimeout?: number,
  name?: (address: string) => Promise<string | null>,
): EventAdapter {
  if (typeof adapter.view !== "function") {
    throw new StreamdError("activity needs an adapter that implements view, for its transfer reads");
  }
  // transferSource reads the page it is handed, keyed by the options object of each call.
  const pages = new WeakMap<FetchOptions, EventPage>();
  const feed: EventAdapter = {
    fetchEvents: async (_descriptor, options) => pages.get(options)!,
    view: adapter.view.bind(adapter),
  };
  const transfers = transferSource(feed, undefined, account, withAssetInfo, requestTimeout);

  const source: EventAdapter = {
    fetchEvents: async (descriptor, options) => {
      const who = await account();
      const page = await adapter.fetchEvents({ ...descriptor, accountAddress: who } as AccountEventDescriptor, options);
      const key: FetchOptions = {};
      pages.set(key, page);
      const moved = new Map<bigint, Transfer[]>();
      for (const { version, data } of (await transfers.fetchEvents(TRANSFER_DESCRIPTOR, key)).events) {
        push(moved, version, data as Transfer);
      }
      const transactions = new Map<bigint, StreamdEvent[]>();
      for (const event of [...page.events].sort(byIdentity)) {
        push(transactions, event.version, event);
      }
      let events = [...transactions].map(([version, txn]) => entry(txn, moved.get(version) ?? [], who));
      if (name !== undefined) {
        const named: StreamdEvent<Activity>[] = [];
        for (const record of events) named.push(await withNames(record, who, name));
        events = named;
      }
      if (options.orderBy === "desc") events.reverse();
      return { ...page, events };
    },
  };
  for (const method of ["getHeadVersion", "getEarliestVersion", "getBlock", "getBlockHeights", "view"] as const) {
    const fn = adapter[method];
    if (fn !== undefined) Object.assign(source, { [method]: fn.bind(adapter) });
  }
  return source;
}
