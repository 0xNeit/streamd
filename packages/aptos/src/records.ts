import { StreamdError, UNSIGNED_INTEGER, normalizeAddress } from "@streamd/core";
import type { EventAdapter, ModuleEventDescriptor, MoveEventType, StreamdEvent } from "@streamd/core";

export type Fields = Record<string, unknown>;

export const address = (field: unknown) => (typeof field === "string" ? normalizeAddress(field) : undefined);
/** An `Object<T>`, which the fullnode writes as `{ inner: address }`. */
export const object = (field: unknown) =>
  typeof field === "object" && field !== null ? address((field as Fields).inner) : undefined;
export const amount = (field: unknown) =>
  typeof field === "string" && UNSIGNED_INTEGER.test(field) ? BigInt(field) : undefined;

/**
 * `adapter` as a source of records: each page's events become `toRecord`'s
 * records, in the page's order, dropping those it returns `undefined` for.
 */
export function recordSource<T>(adapter: EventAdapter, toRecord: (event: StreamdEvent) => StreamdEvent<T> | undefined): EventAdapter {
  return pageSource(adapter, (events) => events.map(toRecord).filter((e): e is StreamdEvent<T> => e !== undefined));
}

/**
 * `adapter` with each page's events replaced by what `rewrite` makes of them.
 * The optional reads are passed through, so negative `fromVersion`,
 * `fromTime` and `withState` work.
 */
export function pageSource(adapter: EventAdapter, rewrite: (events: StreamdEvent[]) => StreamdEvent[] | Promise<StreamdEvent[]>): EventAdapter {
  const source: EventAdapter = {
    fetchEvents: async (descriptor, options) => {
      const page = await adapter.fetchEvents(descriptor, options);
      return { ...page, events: await rewrite(page.events) };
    },
  };
  for (const name of ["getHeadVersion", "getEarliestVersion", "getBlock", "getBlockHeights", "view"] as const) {
    const method = adapter[name];
    if (method !== undefined) Object.assign(source, { [name]: method.bind(adapter) });
  }
  return source;
}

/**
 * One scan over the event types of the entries `chosen` names (one name or a
 * list; every entry when omitted), refusing a name not among `names`.
 */
export function namedTypes<N extends string>(
  entries: readonly { name: N; type: MoveEventType }[],
  chosen: unknown,
  field: string,
  noun: string,
): ModuleEventDescriptor {
  const names = [...new Set(entries.map((e) => e.name))];
  const list: unknown = chosen === undefined ? names : typeof chosen === "string" ? [chosen] : chosen;
  if (!Array.isArray(list) || list.length === 0) {
    throw new StreamdError(`${field} must be one of ${names.join(", ")}, or a non-empty list of them`);
  }
  for (const name of list as unknown[]) {
    if (typeof name !== "string" || !names.includes(name as N)) {
      throw new StreamdError(`Unknown ${noun}: ${String(name)}; expected one of ${names.join(", ")}`);
    }
  }
  return { kind: "module", eventType: entries.filter((e) => list.includes(e.name)).map((e) => e.type) };
}
