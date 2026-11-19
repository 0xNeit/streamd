/** The part of `Storage` the key needs. */
export type KeyStorage = Pick<Storage, "getItem" | "setItem" | "removeItem">;

export const API_KEY_ITEM = "streamd-playground:api-key";

/** `localStorage`, or undefined where reading it throws (blocked storage, some private windows). */
export function browserStorage(): KeyStorage | undefined {
  try {
    return globalThis.localStorage;
  } catch {
    return undefined;
  }
}

/** The key remembered on this device, or "" when none was. */
export function rememberedKey(storage: KeyStorage | undefined): string {
  try {
    return storage?.getItem(API_KEY_ITEM) ?? "";
  } catch {
    return "";
  }
}

/** Stores the key only while `remember` is on; turning it off, or a blank key, removes it. */
export function storeKey(storage: KeyStorage | undefined, key: string, remember: boolean): void {
  try {
    if (remember && key.trim() !== "") storage?.setItem(API_KEY_ITEM, key.trim());
    else storage?.removeItem(API_KEY_ITEM);
  } catch {
    // Storage that refuses writes leaves the key in memory only, which is the default anyway.
  }
}
