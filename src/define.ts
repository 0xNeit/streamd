import type { ModuleEventDescriptor, MoveEventType } from "./types.js";

/**
 * A `module` descriptor typed with its payload, so `on`, `once`, `stream` and
 * `getEvents` infer `event.data` as `TData`. `TData` is the caller's claim
 * about the JSON-decoded payload (u64 values arrive as strings); nothing
 * checks it at runtime.
 */
export function defineEvent<TData = unknown>(
  eventType: MoveEventType | readonly MoveEventType[],
): ModuleEventDescriptor<TData> {
  return { kind: "module", eventType };
}
