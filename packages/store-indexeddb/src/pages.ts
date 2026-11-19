import { StreamdError, type PageStore } from "@streamd/core";

const DATABASE = "streamd-pages";
const VERSION = 1;
const STORE = "pages";
const KEY_INDEX = "key";

/** The parts of an IndexedDB request this store reads. */
export interface IdbRequestLike<T = unknown> {
  result: T;
  error: unknown;
  onsuccess: ((event: unknown) => void) | null;
  onerror: ((event: unknown) => void) | null;
}

export interface IdbOpenRequestLike extends IdbRequestLike<IdbDatabaseLike> {
  onupgradeneeded: ((event: unknown) => void) | null;
}

export interface IdbIndexLike {
  get(key: string): IdbRequestLike<unknown>;
  getKey(key: string): IdbRequestLike<unknown>;
}

export interface IdbObjectStoreLike {
  add(value: unknown): IdbRequestLike<unknown>;
  delete(key: unknown): IdbRequestLike<unknown>;
  count(): IdbRequestLike<number>;
  getAllKeys(query: null, count: number): IdbRequestLike<unknown[]>;
  index(name: string): IdbIndexLike;
  createIndex(name: string, keyPath: string, options: { unique: boolean }): unknown;
}

export interface IdbTransactionLike {
  error: unknown;
  objectStore(name: string): IdbObjectStoreLike;
  oncomplete: ((event: unknown) => void) | null;
  onabort: ((event: unknown) => void) | null;
  onerror: ((event: unknown) => void) | null;
}

export interface IdbDatabaseLike {
  objectStoreNames: { contains(name: string): boolean };
  createObjectStore(name: string, options: { autoIncrement: boolean }): IdbObjectStoreLike;
  transaction(store: string, mode: "readonly" | "readwrite"): IdbTransactionLike;
  close(): void;
  onversionchange: ((event: unknown) => void) | null;
  onclose: ((event: unknown) => void) | null;
}

/** The part of `IDBFactory` (the browser's `indexedDB`) this store uses. */
export interface IdbFactoryLike {
  open(name: string, version: number): IdbOpenRequestLike;
}

export interface IndexedDbPageStoreOptions {
  /**
   * Keeps one network's pages from answering another's, since page keys name
   * only the request. Use one per network or source, for example `"mainnet"`.
   */
  namespace: string;
  /** Drop the oldest written past this many pages, across the database (default: no limit). */
  maxEntries?: number;
  /** The IndexedDB factory (default: the global `indexedDB`). */
  indexedDB?: IdbFactoryLike;
}

function failure(what: string, error: unknown): StreamdError {
  const name = (error as { name?: unknown } | null)?.name;
  if (name === "QuotaExceededError") {
    return new StreamdError(`IndexedDB ${what} failed: the origin's storage quota is exhausted`, { cause: error });
  }
  return new StreamdError(`IndexedDB ${what} failed`, { cause: error });
}

/** A `PageStore` for `CachingAdapter`, over the browser's IndexedDB. */
export class IndexedDbPageStore implements PageStore {
  private readonly namespace: string;
  private readonly maxEntries?: number;
  private readonly factory?: IdbFactoryLike;
  private db?: Promise<IdbDatabaseLike>;

  constructor(options: IndexedDbPageStoreOptions) {
    const { namespace, maxEntries, indexedDB } = options ?? {};
    if (typeof namespace !== "string" || namespace === "") {
      throw new StreamdError("IndexedDbPageStore needs a namespace, one per network or source");
    }
    if (maxEntries !== undefined && (!Number.isSafeInteger(maxEntries) || maxEntries < 1)) {
      throw new StreamdError(`Invalid maxEntries: ${String(maxEntries)} (expected a positive integer)`);
    }
    if (indexedDB !== undefined && typeof indexedDB?.open !== "function") {
      throw new StreamdError("Expected an IndexedDB factory with open()");
    }
    this.namespace = namespace;
    this.maxEntries = maxEntries;
    // Resolved here but checked on use, so a store built during server rendering throws nothing.
    this.factory = indexedDB ?? (globalThis as { indexedDB?: IdbFactoryLike }).indexedDB;
  }

  async get(key: string): Promise<string | undefined> {
    const tx = (await this.open()).transaction(STORE, "readonly");
    const record = await request(tx.objectStore(STORE).index(KEY_INDEX).get(this.scoped(key)), "read");
    const page = (record as { page?: unknown } | undefined)?.page;
    return typeof page === "string" ? page : undefined;
  }

  async set(key: string, page: string): Promise<void> {
    const scoped = this.scoped(key);
    const tx = (await this.open()).transaction(STORE, "readwrite");
    // A quota failure arrives as the transaction's abort, not on any request.
    const done = new Promise<void>((resolve, reject) => {
      tx.oncomplete = () => resolve();
      tx.onabort = () => reject(failure("write", tx.error));
      tx.onerror = () => reject(failure("write", tx.error));
    });
    const store = tx.objectStore(STORE);
    const max = this.maxEntries;
    const lookup = store.index(KEY_INDEX).getKey(scoped);
    lookup.onsuccess = () => {
      // Delete then add gives the key a new auto-increment id, so id order is write order.
      if (lookup.result !== undefined) store.delete(lookup.result);
      store.add({ key: scoped, page });
      if (max === undefined) return;
      const counted = store.count();
      counted.onsuccess = () => {
        if (counted.result <= max) return;
        const oldest = store.getAllKeys(null, counted.result - max);
        oldest.onsuccess = () => {
          for (const id of oldest.result) store.delete(id);
        };
      };
    };
    await done;
  }

  private scoped(key: string): string {
    return `${this.namespace}\n${key}`;
  }

  private open(): Promise<IdbDatabaseLike> {
    this.db ??= openDatabase(this.factory).then(
      (db) => {
        // Another tab upgrading the database waits until every connection closes.
        db.onversionchange = () => {
          db.close();
          this.db = undefined;
        };
        db.onclose = () => {
          this.db = undefined;
        };
        return db;
      },
      (err: unknown) => {
        this.db = undefined;
        throw err;
      },
    );
    return this.db;
  }
}

function request<T>(req: IdbRequestLike<T>, what: string): Promise<T> {
  return new Promise((resolve, reject) => {
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(failure(what, req.error));
  });
}

function openDatabase(factory: IdbFactoryLike | undefined): Promise<IdbDatabaseLike> {
  if (factory === undefined) return Promise.reject(new StreamdError("IndexedDB is not available here"));
  return new Promise((resolve, reject) => {
    const req = factory.open(DATABASE, VERSION);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(STORE)) {
        db.createObjectStore(STORE, { autoIncrement: true }).createIndex(KEY_INDEX, KEY_INDEX, { unique: true });
      }
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(failure("open", req.error));
  });
}
