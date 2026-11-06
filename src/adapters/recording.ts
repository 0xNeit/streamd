import type { EventAdapter, EventPage } from "../types.js";
import { serializeEvent } from "../serialize.js";
import { descriptorKey, fetchKey } from "../utils.js";
import { requireAdapter } from "./validate.js";

const FORMAT = "streamd-recording";

export type RecordedMethod =
  | "fetchEvents"
  | "getHeadVersion"
  | "getEarliestVersion"
  | "getBlock"
  | "getBlockHeights";

/**
 * One successful adapter call. `result` is JSON-safe: every `bigint` is a
 * decimal string and events are in the `serializeEvent` shape.
 */
export interface RecordedCall {
  method: RecordedMethod;
  key: string;
  result: unknown;
}

/** What `recordingAdapter` captured, ready for `JSON.stringify` and `replayAdapter`. */
export interface Recording {
  format: typeof FORMAT;
  version: 1;
  /** In the order the calls completed. */
  calls: RecordedCall[];
}

export interface RecordingAdapter extends EventAdapter {
  /** The calls recorded so far, so `JSON.stringify(recorder)` is the whole recording. */
  toJSON(): Recording;
}

function toJson(value: unknown): unknown {
  return JSON.parse(
    JSON.stringify(value, (_key, v: unknown) => (typeof v === "bigint" ? v.toString() : v)),
  );
}

function pageJson(page: EventPage): unknown {
  return toJson({
    events: page.events.map((event) => JSON.parse(serializeEvent(event)) as unknown),
    nextCursor: page.nextCursor,
    hasMore: page.hasMore,
    final: page.final,
    throughVersion: page.throughVersion,
  });
}

/**
 * Wraps `adapter` and records every call that succeeds, for `replayAdapter`
 * to answer offline later. Failures pass through unrecorded, and so does a
 * call whose descriptor is not one of the three kinds. `getHeadVersion`,
 * `getEarliestVersion`, `getBlock` and `getBlockHeights` are recorded only
 * when the wrapped adapter has them.
 */
export function recordingAdapter(adapter: EventAdapter): RecordingAdapter {
  const source = requireAdapter(adapter, "recordingAdapter");
  const calls: RecordedCall[] = [];
  // Serialised as each call settles, so a caller mutating the returned value cannot alter the record.
  async function tap<T>(
    method: RecordedMethod,
    key: string | undefined,
    call: Promise<T>,
    toResult: (value: T) => unknown = toJson,
  ): Promise<T> {
    const value = await call;
    if (key !== undefined) calls.push({ method, key, result: toResult(value) });
    return value;
  }

  const recorder: RecordingAdapter = {
    fetchEvents: (descriptor, options = {}) =>
      tap("fetchEvents", fetchKey(descriptor, options), source.fetchEvents(descriptor, options), pageJson),
    toJSON: () => ({ format: FORMAT, version: 1, calls: [...calls] }),
  };
  if (source.getHeadVersion !== undefined) {
    recorder.getHeadVersion = (descriptor) =>
      tap("getHeadVersion", descriptorKey(descriptor), source.getHeadVersion!(descriptor));
  }
  if (source.getEarliestVersion !== undefined) {
    recorder.getEarliestVersion = (descriptor) =>
      tap("getEarliestVersion", descriptorKey(descriptor), source.getEarliestVersion!(descriptor));
  }
  if (source.getBlock !== undefined) {
    recorder.getBlock = (descriptor, height) =>
      tap("getBlock", blockKey(descriptorKey(descriptor), height), source.getBlock!(descriptor, height));
  }
  if (source.getBlockHeights !== undefined) {
    recorder.getBlockHeights = (descriptor) =>
      tap("getBlockHeights", descriptorKey(descriptor), source.getBlockHeights!(descriptor));
  }
  return recorder;
}

function blockKey(selector: string | undefined, height: bigint): string | undefined {
  return selector === undefined ? undefined : `${selector}|${String(height)}`;
}
