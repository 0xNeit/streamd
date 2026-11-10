import { StreamdError } from "./errors.js";
import { requireEventType } from "./adapters/validate.js";
import type { ModuleEventDescriptor, MoveEventType, TaggedEvent } from "./types.js";

/**
 * A `module` descriptor typed with its payload, so `on`, `once`, `stream` and
 * `getEvents` infer `event.data` as `TData`. `TData` is the caller's claim
 * about the JSON-decoded payload (u64 values arrive as strings unless named
 * in the `decode` option); nothing checks it at runtime.
 */
export function defineEvent<TData = unknown>(
  eventType: MoveEventType | readonly MoveEventType[],
): ModuleEventDescriptor<TData> {
  return { kind: "module", eventType };
}

type PayloadOf<TDescriptor> = TDescriptor extends ModuleEventDescriptor<infer TData> ? TData : never;

/** The tagged union `defineEvents` carries: one member per Move type. */
export type TaggedEvents<TEvents> = {
  [TType in keyof TEvents & string]: TaggedEvent<TType, PayloadOf<TEvents[TType]>>;
}[keyof TEvents & string];

/**
 * A `module` descriptor over several event types whose events narrow on
 * `event.type`. Takes an object from Move type to its `defineEvent` descriptor,
 * so the literal types are inferred. As with `defineEvent`, payload types are
 * the caller's claim and nothing checks them at runtime.
 */
export function defineEvents<TEvents extends Record<string, ModuleEventDescriptor<any>>>(
  events: TEvents,
): ModuleEventDescriptor<TaggedEvents<TEvents>> {
  const entries = Object.entries(events ?? {});
  if (entries.length === 0) throw new StreamdError("defineEvents needs at least one event type");
  for (const [type, descriptor] of entries) {
    requireEventType(type);
    if (descriptor?.kind !== "module" || descriptor.eventType !== type) {
      throw new StreamdError(`defineEvents: "${type}" must map to the descriptor for that one type`);
    }
  }
  return { kind: "module", eventType: entries.map(([type]) => type as MoveEventType) };
}
