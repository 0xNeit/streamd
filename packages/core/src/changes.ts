import { sameValue } from "./compare.js";
import { eventId } from "./cursor.js";
import { MISSING, parseFieldPath, readField, type FieldPath } from "./filter.js";
import type { StreamdEvent, SyncEventPredicate } from "./types.js";

const REMEMBERED = 1000;
const DELETED: unique symbol = Symbol("deleted");

interface TypeState {
  last: StreamdEvent;
  value: unknown;
  verdicts: Map<string, boolean>;
}

/**
 * Drops a `resource` or `object` change whose value, or the value at `path`
 * (`whereField`'s syntax), deep-equals the previous change's. The first change
 * always passes.
 *
 * State is kept per `event.type`, since a change names no address: one
 * descriptor follows one resource. Each call is a fresh state, so never share
 * one between subscriptions. An event tested again, as after a failed poll,
 * gets its first answer; an earlier one it does not remember passes.
 */
export function changesOnly(path?: FieldPath): SyncEventPredicate {
  const segments = path === undefined ? undefined : parseFieldPath(path);
  const states = new Map<string, TypeState>();
  const valueOf = (event: StreamdEvent): unknown => {
    if (segments === undefined) return event.data;
    if (event.data === null) return DELETED;
    const value = readField(event.data, segments);
    return value === MISSING ? undefined : value;
  };
  return (event) => {
    const id = eventId(event);
    const state = states.get(event.type);
    const remembered = state?.verdicts.get(id);
    if (remembered !== undefined) return remembered;
    if (state !== undefined && !isAfter(event, state.last)) return true;

    const value = valueOf(event);
    const verdict = state === undefined || !sameValue(state.value, value);
    const verdicts = state?.verdicts ?? new Map<string, boolean>();
    verdicts.set(id, verdict);
    if (verdicts.size > REMEMBERED) verdicts.delete(verdicts.keys().next().value!);
    states.set(event.type, { last: event, value, verdicts });
    return verdict;
  };
}

function isAfter(a: StreamdEvent, b: StreamdEvent): boolean {
  return a.version > b.version || (a.version === b.version && a.eventIndex > b.eventIndex);
}
