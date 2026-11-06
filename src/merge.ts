import type { StreamdEvent } from "./types.js";
import { eventId } from "./cursor.js";

type Walk<TData> = AsyncIterator<StreamdEvent<TData>[], unknown>;

function before(a: StreamdEvent, b: StreamdEvent): boolean {
  return a.version < b.version || (a.version === b.version && a.eventIndex < b.eventIndex);
}

/**
 * The events of several ascending page walks as one ascending
 * `(version, eventIndex)` sequence, with an event that more than one walk
 * returns yielded once. Each walk is pulled only when the event it buffered
 * last has been taken, one walk at a time, so at most one page per walk is
 * held and a failing walk leaves no other request in flight. `limit` counts
 * what is yielded. Every walk is closed when this one ends.
 */
export async function* mergeEvents<TData>(
  walks: readonly Walk<TData>[],
  limit = Infinity,
): AsyncGenerator<StreamdEvent<TData>, void, unknown> {
  const pages: StreamdEvent<TData>[][] = walks.map(() => []);
  const at = walks.map(() => 0);
  const live = walks.map(() => true);
  const fill = async (i: number) => {
    while (live[i] && at[i]! >= pages[i]!.length) {
      const step = await walks[i]!.next();
      if (step.done) live[i] = false;
      else [pages[i], at[i]] = [step.value, 0];
    }
  };
  try {
    for (let i = 0; i < walks.length; i++) await fill(i);
    // Every walk ascends, so an event two walks share comes out adjacent.
    let last: string | undefined;
    let taken = -1;
    for (let yielded = 0; yielded < limit; ) {
      if (taken >= 0) await fill(taken);
      taken = -1;
      for (let i = 0; i < walks.length; i++) {
        const head = pages[i]![at[i]!];
        if (head !== undefined && (taken < 0 || before(head, pages[taken]![at[taken]!]!))) {
          taken = i;
        }
      }
      if (taken < 0) return;
      const event = pages[taken]![at[taken]!++]!;
      const id = eventId(event);
      if (id === last) continue;
      last = id;
      yielded++;
      yield event;
    }
  } finally {
    await Promise.all(walks.map((walk) => walk.return?.()));
  }
}
