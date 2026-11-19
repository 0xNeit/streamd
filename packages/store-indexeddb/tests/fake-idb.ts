import type { IdbDatabaseLike, IdbFactoryLike, IdbObjectStoreLike, IdbRequestLike, IdbTransactionLike } from "../src/index.js";

interface Row {
  key: string;
  page: unknown;
}

type Handler = ((event: never) => void) | null;

class FakeRequest<T> implements IdbRequestLike<T> {
  result!: T;
  error: unknown = null;
  onsuccess: Handler = null;
  onerror: Handler = null;
}

type FakeTransaction = IdbTransactionLike & { onerror: Handler };

function fire(handler: Handler): void {
  (handler as ((event: unknown) => void) | null)?.({});
}

/**
 * One database in memory, as IndexedDB behaves where this store relies on it:
 * requests in order within a transaction, read-write transactions one at a
 * time, a failed request aborting and rolling back its transaction, ids never
 * reused, and `getAllKeys` with a count of 0 meaning every key.
 */
export class FakeIndexedDb implements IdbFactoryLike {
  rows = new Map<number, Row>();
  /** Records under string keys, which sort after every numeric id. */
  meta = new Map<string, unknown>();
  nextId = 1;
  opens = 0;
  failOpens = 0;
  /** Abort every read-write transaction as it would commit, with this error. */
  abortWrites?: { name: string };
  /** Fail every request of this kind with an error. */
  failRequests?: "get" | "add";
  schema?: { store: string; autoIncrement: boolean; index: [string, string, { unique: boolean }] };
  connections: IdbDatabaseLike[] = [];
  private writer: Promise<void> = Promise.resolve();

  open(name: string, version: number): FakeRequest<IdbDatabaseLike> & { onupgradeneeded: Handler } {
    const req = Object.assign(new FakeRequest<IdbDatabaseLike>(), { onupgradeneeded: null as Handler });
    this.opens++;
    queueMicrotask(() => {
      if (this.failOpens > 0 || name !== "streamd-pages" || version !== 1) {
        this.failOpens = Math.max(0, this.failOpens - 1);
        req.error = { name: "UnknownError" };
        fire(req.onerror);
        return;
      }
      req.result = this.database();
      if (this.schema === undefined) fire(req.onupgradeneeded);
      if (this.schema === undefined) throw new Error("the upgrade created no store");
      fire(req.onsuccess);
    });
    return req;
  }

  /** What another tab upgrading or deleting the database does to open connections. */
  versionChange(): void {
    for (const db of this.connections) fire(db.onversionchange);
  }

  /** A row by its scoped key. */
  row(key: string): Row | undefined {
    return [...this.rows.values()].find((row) => row.key === key);
  }

  /** Scoped keys in id order, which is write order. */
  keys(): string[] {
    return [...this.rows].sort(([a], [b]) => a - b).map(([, row]) => row.key);
  }

  private database(): IdbDatabaseLike {
    const factory = this;
    const db: IdbDatabaseLike & { closed: boolean } = {
      closed: false,
      objectStoreNames: { contains: (name) => factory.schema?.store === name },
      createObjectStore(store, options) {
        return {
          createIndex(name: string, keyPath: string, indexOptions: { unique: boolean }) {
            factory.schema = { store, autoIncrement: options.autoIncrement, index: [name, keyPath, indexOptions] };
          },
        } as unknown as IdbObjectStoreLike;
      },
      transaction: (store, mode) => {
        if (db.closed) throw new Error("InvalidStateError");
        if (store !== factory.schema?.store) throw new Error("NotFoundError");
        return factory.transaction(mode);
      },
      close() {
        db.closed = true;
      },
      onversionchange: null,
      onclose: null,
    };
    this.connections.push(db);
    return db;
  }

  private transaction(mode: "readonly" | "readwrite"): FakeTransaction {
    const queue: (() => void)[] = [];
    const tx: FakeTransaction = {
      error: null,
      oncomplete: null,
      onabort: null,
      onerror: null,
      objectStore: (name) => {
        if (name !== this.schema?.store) throw new Error("NotFoundError");
        return store;
      },
    };
    let rows = new Map<number, Row>();
    let meta = new Map<string, unknown>();
    let nextId = 0;
    let failed: { name: string } | undefined;
    const run = <T>(kind: string, op: () => T): FakeRequest<T> => {
      const req = new FakeRequest<T>();
      queue.push(() => {
        if (failed !== undefined) return;
        try {
          if (this.failRequests === kind) throw { name: "UnknownError" };
          req.result = op();
        } catch (err) {
          req.error = err;
          failed = err as { name: string };
          fire(req.onerror);
          fire(tx.onerror);
          return;
        }
        fire(req.onsuccess);
      });
      return req;
    };
    const findId = (key: string) => [...rows].find(([, row]) => row.key === key)?.[0];
    const writable = () => {
      if (mode !== "readwrite") throw { name: "ReadOnlyError" };
    };
    const index = (name: string) => {
      if (name !== this.schema?.index[0]) throw new Error("NotFoundError");
      return {
        get: (key: string) => run("get", () => {
          const id = findId(key);
          return id === undefined ? undefined : structuredClone(rows.get(id));
        }),
        getKey: (key: string) => run("getKey", () => findId(key)),
      };
    };
    const store: IdbObjectStoreLike = {
      add: (value) => run("add", () => {
        writable();
        const row = structuredClone(value) as Row;
        if (findId(row.key) !== undefined) throw { name: "ConstraintError" };
        rows.set(nextId, row);
        return nextId++;
      }),
      delete: (id) => run("delete", () => {
        writable();
        if (typeof id === "string") meta.delete(id);
        else rows.delete(id as number);
      }),
      get: (key) => run("storeGet", () => structuredClone(meta.get(key))),
      put: (value, key) => run("put", () => {
        writable();
        meta.set(key, structuredClone(value));
        return key;
      }),
      getAllKeys: (_query, count) => run("getAllKeys", () => {
        const ids: unknown[] = [...rows.keys()].sort((a, b) => a - b);
        const all = [...ids, ...[...meta.keys()].sort()];
        return count === 0 ? all : all.slice(0, count);
      }),
      index,
      createIndex: () => undefined,
    };
    const finish = (): void => {
      const next = queue.shift();
      if (next !== undefined) {
        next();
        queueMicrotask(finish);
        return;
      }
      failed ??= mode === "readwrite" ? this.abortWrites : undefined;
      if (failed !== undefined) {
        tx.error = failed;
        fire(tx.onabort);
      } else {
        if (mode === "readwrite") {
          this.rows = rows;
          this.meta = meta;
          this.nextId = nextId;
        }
        fire(tx.oncomplete);
      }
      settle();
    };
    let settle: () => void = () => undefined;
    const start = () => {
      rows = new Map(this.rows);
      meta = new Map(this.meta);
      nextId = this.nextId;
      queueMicrotask(finish);
    };
    if (mode === "readwrite") {
      const previous = this.writer;
      this.writer = new Promise<void>((resolve) => {
        settle = resolve;
      });
      void previous.then(start);
    } else {
      start();
    }
    return tx;
  }
}
