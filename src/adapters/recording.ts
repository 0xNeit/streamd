import type { BlockHeights, BlockInfo, EventAdapter, EventPage } from "../types.js";
import { UNSIGNED_INTEGER } from "../decode.js";
import { StreamdError } from "../errors.js";
import { parseEvent, serializeEvent } from "../serialize.js";
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

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function decimal(value: unknown, field: string): bigint {
  if (typeof value !== "string" || !UNSIGNED_INTEGER.test(value)) {
    throw new StreamdError(`"${field}" must be an unsigned decimal string`);
  }
  return BigInt(value);
}

function decimals(value: unknown, fields: readonly string[]): Record<string, bigint> {
  if (!isObject(value)) throw new StreamdError("expected an object");
  return Object.fromEntries(fields.map((field) => [field, decimal(value[field], field)]));
}

function optional<T>(value: unknown, type: string, field: string): T | undefined {
  if (value !== undefined && typeof value !== type) {
    throw new StreamdError(`"${field}" must be a ${type}`);
  }
  return value as T | undefined;
}

function parsePage(value: unknown): EventPage {
  if (!isObject(value) || !Array.isArray(value.events)) {
    throw new StreamdError('"events" must be an array');
  }
  if (typeof value.hasMore !== "boolean") throw new StreamdError('"hasMore" must be a boolean');
  const page: EventPage = {
    events: value.events.map((event) => parseEvent(JSON.stringify(event) ?? "")),
    hasMore: value.hasMore,
  };
  const nextCursor = optional<string>(value.nextCursor, "string", "nextCursor");
  if (nextCursor !== undefined) page.nextCursor = nextCursor;
  const final = optional<boolean>(value.final, "boolean", "final");
  if (final !== undefined) page.final = final;
  if (value.throughVersion !== undefined) {
    page.throughVersion = decimal(value.throughVersion, "throughVersion");
  }
  return page;
}

const PARSERS: Record<RecordedMethod, (result: unknown) => unknown> = {
  fetchEvents: parsePage,
  getHeadVersion: (result) => decimal(result, "result"),
  getEarliestVersion: (result) => decimal(result, "result"),
  getBlock: (result) => decimals(result, ["height", "firstVersion", "lastVersion", "timestamp"]),
  getBlockHeights: (result) => decimals(result, ["oldest", "newest"]),
};

function invalid(message: string, cause?: unknown): never {
  throw new StreamdError(`Invalid recording: ${message}`, cause);
}

function parseCall(call: unknown, i: number): [string, unknown] {
  if (!isObject(call) || typeof call.key !== "string") invalid(`call ${i} needs a string "key"`);
  const method = call.method;
  if (typeof method !== "string" || !Object.hasOwn(PARSERS, method)) {
    invalid(`call ${i} has unknown method ${JSON.stringify(method) ?? "undefined"}`);
  }
  try {
    return [`${method}:${call.key}`, PARSERS[method as RecordedMethod](call.result)];
  } catch (err) {
    invalid(`call ${i} (${method}): ${(err as Error).message}`, err);
  }
}

/** Every recorded result, parsed back, grouped by method and request key in recorded order. */
function loadRecording(input: unknown): Map<string, unknown[]> {
  let value = input;
  if (typeof input === "string") {
    try {
      value = JSON.parse(input);
    } catch (err) {
      invalid("not valid JSON", err);
    }
  }
  if (!isObject(value) || value.format !== FORMAT) {
    invalid(`expected an object with format "${FORMAT}"`);
  }
  if (value.version !== 1) invalid(`unsupported version ${JSON.stringify(value.version)}`);
  if (!Array.isArray(value.calls)) invalid('"calls" must be an array');
  const answers = new Map<string, unknown[]>();
  value.calls.forEach((call, i) => {
    const [id, result] = parseCall(call, i);
    const list = answers.get(id);
    if (list === undefined) answers.set(id, [result]);
    else list.push(result);
  });
  return answers;
}

/**
 * An `EventAdapter` that answers from a `recordingAdapter` recording, given as
 * the object or its JSON text, with no network. Identical requests get their
 * recorded results in order, then the last one again. Any other request
 * rejects with a `StreamdError`. The optional methods exist only when the
 * recording holds calls to them. Malformed input throws a `StreamdError` here.
 */
export function replayAdapter(recording: Recording | string): EventAdapter {
  const answers = loadRecording(recording);
  const served = new Map<string, number>();
  function answer<T>(method: RecordedMethod, key: string | undefined): T {
    const id = `${method}:${key}`;
    const list = key === undefined ? undefined : answers.get(id);
    if (list === undefined) throw new StreamdError(`No recorded ${method} call matches this request`);
    const i = served.get(id) ?? 0;
    served.set(id, Math.min(i + 1, list.length - 1));
    return list[i] as T;
  }
  const has = (method: RecordedMethod) =>
    [...answers.keys()].some((id) => id.startsWith(`${method}:`));

  const adapter: EventAdapter = {
    async fetchEvents(descriptor, options = {}) {
      const page = answer<EventPage>("fetchEvents", fetchKey(descriptor, options));
      return { ...page, events: [...page.events] };
    },
  };
  if (has("getHeadVersion")) {
    adapter.getHeadVersion = async (descriptor) =>
      answer<bigint>("getHeadVersion", descriptorKey(descriptor));
  }
  if (has("getEarliestVersion")) {
    adapter.getEarliestVersion = async (descriptor) =>
      answer<bigint>("getEarliestVersion", descriptorKey(descriptor));
  }
  if (has("getBlock")) {
    adapter.getBlock = async (descriptor, height) =>
      ({ ...answer<BlockInfo>("getBlock", blockKey(descriptorKey(descriptor), height)) });
  }
  if (has("getBlockHeights")) {
    adapter.getBlockHeights = async (descriptor) =>
      ({ ...answer<BlockHeights>("getBlockHeights", descriptorKey(descriptor)) });
  }
  return adapter;
}
