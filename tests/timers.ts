import { vi, type Mock } from "bun:test";

// Captured before any test installs fake timers.
const realSetImmediate = globalThis.setImmediate;
const flush = () => new Promise<void>((r) => realSetImmediate(r));

/**
 * Async timer helpers missing from bun:test. They let promise callbacks
 * settle between timer firings, so a poll that awaits its fetch and then
 * schedules the next timer is picked up within the same advance.
 */
// ponytail: steps the fake clock 1ms at a time; fine for the sub-second windows these tests use.
export async function advanceTimersByTimeAsync(ms: number): Promise<void> {
  await flush();
  vi.advanceTimersByTime(0);
  await flush();
  for (let i = 0; i < ms; i++) {
    vi.advanceTimersByTime(1);
    await flush();
  }
}

export async function advanceTimersToNextTimerAsync(): Promise<void> {
  await flush();
  vi.advanceTimersToNextTimer();
  await flush();
}

export const mocked = <T extends (...args: any[]) => any>(fn: T) =>
  fn as unknown as Mock<T>;
