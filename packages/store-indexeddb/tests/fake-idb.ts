import type { IdbDatabaseLike, IdbFactoryLike, IdbObjectStoreLike, IdbRequestLike, IdbTransactionLike } from "../src/pages.js";

interface Row {
  key: string;
  page: unknown;
}

class FakeRequest<T> implements IdbRequestLike<T> {
  result!: T;
  error: unknown = null;
  onsuccess: ((event: unknown) => void) | null = null;
  onerror: ((event: unknown) => void) | null = null;
}

/** One object store with auto-increment ids and a unique `key` index, in memory. */
export class FakeIndexedDb implements IdbFactoryLike {
  rows = new Map<number, Row>();
  nextId = 1;
  opens = 0;
  failOpens = 0;
  abortWrites?: { name: string };
  created = false;

  open(): FakeRequest<IdbDatabaseLike> & { onupgradeneeded: ((event: unknown) => void) | null } {
    const req = Object.assign(new FakeRequest<IdbDatabaseLike>(), { onupgradeneeded: null as ((event: unknown) => void) | null });
    this.opens++;
    queueMicrotask(() => {
      if (this.failOpens > 0) {
        this.failOpens--;
        req.error = { name: "UnknownError" };
        req.onerror?.({});
        return;
      }
      req.result = this.database();
      if (!this.created) req.onupgradeneeded?.({});
      req.onsuccess?.({});
    });
    return req;
  }

  private database(): IdbDatabaseLike {
    const factory = this;
    return {
      objectStoreNames: { contains: () => factory.created },
      createObjectStore() {
        factory.created = true;
        return { createIndex: () => undefined } as unknown as IdbObjectStoreLike;
      },
      transaction: (_store, mode) => factory.transaction(mode),
      close() {},
      onversionchange: null,
      onclose: null,
    };
  }

  private transaction(mode: "readonly" | "readwrite"): IdbTransactionLike {
    const rows = new Map(this.rows);
    let nextId = this.nextId;
    const queue: (() => void)[] = [];
    const factory = this;
    const tx: IdbTransactionLike = {
      error: null,
      oncomplete: null,
      onabort: null,
      onerror: null,
      objectStore: () => store,
    };
    const run = <T>(op: () => T): FakeRequest<T> => {
      const req = new FakeRequest<T>();
      queue.push(() => {
        req.result = op();
        req.onsuccess?.({});
      });
      return req;
    };
    const findId = (key: string) => [...rows].find(([, row]) => row.key === key)?.[0];
    const writable = () => {
      if (mode !== "readwrite") throw new Error("ReadOnlyError");
    };
    const store: IdbObjectStoreLike = {
      add: (value) => run(() => {
        writable();
        const row = structuredClone(value) as Row;
        if (findId(row.key) !== undefined) throw new Error("ConstraintError");
        rows.set(nextId, row);
        return nextId++;
      }),
      delete: (id) => run(() => {
        writable();
        rows.delete(id as number);
      }),
      count: () => run(() => rows.size),
      getAllKeys: (_query, count) => run(() => [...rows.keys()].sort((a, b) => a - b).slice(0, count)),
      index: () => ({
        get: (key) => run(() => {
          const id = findId(key);
          return id === undefined ? undefined : structuredClone(rows.get(id));
        }),
        getKey: (key) => run(() => findId(key)),
      }),
      createIndex: () => undefined,
    };
    const step = () => {
      const next = queue.shift();
      if (next !== undefined) {
        next();
        queueMicrotask(step);
        return;
      }
      if (mode === "readwrite" && factory.abortWrites !== undefined) {
        tx.error = factory.abortWrites;
        tx.onabort?.({});
        return;
      }
      if (mode === "readwrite") {
        factory.rows = rows;
        factory.nextId = nextId;
      }
      tx.oncomplete?.({});
    };
    queueMicrotask(step);
    return tx;
  }

  /** Scoped keys in id order, which is write order. */
  keys(): string[] {
    return [...this.rows].sort(([a], [b]) => a - b).map(([, row]) => row.key);
  }
}
