import { describe, expect, it } from "bun:test";
import { API_KEY_ITEM, rememberedKey, storeKey } from "../src/settings.js";
import type { KeyStorage } from "../src/settings.js";

function memory(): KeyStorage & { items: Map<string, string> } {
  const items = new Map<string, string>();
  return {
    items,
    getItem: (k) => items.get(k) ?? null,
    setItem: (k, v) => void items.set(k, v),
    removeItem: (k) => void items.delete(k),
  };
}

const broken: KeyStorage = {
  getItem: () => {
    throw new Error("SecurityError");
  },
  setItem: () => {
    throw new Error("QuotaExceededError");
  },
  removeItem: () => {
    throw new Error("SecurityError");
  },
};

describe("API key storage", () => {
  it("stores nothing unless remember is on", () => {
    const s = memory();
    storeKey(s, "key-1", false);
    expect(s.items.size).toBe(0);
    expect(rememberedKey(s)).toBe("");
  });

  it("stores a remembered key, trimmed, and removes it when remember is turned off", () => {
    const s = memory();
    storeKey(s, " key-1 ", true);
    expect(s.items.get(API_KEY_ITEM)).toBe("key-1");
    expect(rememberedKey(s)).toBe("key-1");
    storeKey(s, "key-1", false);
    expect(s.items.size).toBe(0);
  });

  it("removes a remembered key that is cleared", () => {
    const s = memory();
    storeKey(s, "key-1", true);
    storeKey(s, "  ", true);
    expect(s.items.size).toBe(0);
  });

  it("ignores storage that is missing or throws", () => {
    expect(rememberedKey(undefined)).toBe("");
    expect(rememberedKey(broken)).toBe("");
    expect(() => storeKey(broken, "key-1", true)).not.toThrow();
    expect(() => storeKey(undefined, "key-1", true)).not.toThrow();
  });
});
