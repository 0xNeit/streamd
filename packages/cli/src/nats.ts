import { randomBytes } from "node:crypto";
import { connect, type Socket } from "node:net";

/** The first message on a publish's reply inbox: `status` from a header-only reply such as 503 no responders. */
export interface NatsReply {
  status?: number;
  description?: string;
  data: string;
}

/** The one call the NATS sink makes, which `connectNats` gives over the NATS text protocol. */
export interface NatsLike {
  request(subject: string, headers: Record<string, string>, body: string, signal: AbortSignal): Promise<NatsReply>;
  close(): unknown;
}

interface Waiter<T> {
  resolve(value: T): void;
  reject(err: unknown): void;
}

const DEFAULT_PORT = 4222;
const STATUS = /^NATS\/1\.0[ \t]+(\d{3})[ \t]*(.*)$/;

/**
 * One connection: INFO, then CONNECT, SUB on a wildcard inbox and PING, ready
 * on the PONG. Any `-ERR`, socket error or close fails the handshake and every
 * publish waiting on it, and the connection is never reused after that.
 */
class Connection {
  closed = false;
  private readonly socket: Socket;
  private readonly ready: Promise<void>;
  private readonly inbox = `_INBOX.${randomBytes(11).toString("hex")}`;
  private readonly pending = new Map<string, Waiter<NatsReply>>();
  private handshake: Waiter<void> | undefined;
  private buffer: Buffer = Buffer.alloc(0);
  private greeted = false;
  private next = 0;

  constructor(private readonly url: URL) {
    this.ready = new Promise<void>((resolve, reject) => (this.handshake = { resolve, reject }));
    this.ready.catch(() => {});
    this.socket = connect(Number(url.port || DEFAULT_PORT), url.hostname.replace(/^\[(.*)\]$/, "$1"));
    this.socket.on("data", (chunk: Buffer) => this.read(chunk));
    this.socket.on("error", (err) => this.fail(err));
    this.socket.on("close", () => this.fail(new Error("NATS connection closed")));
  }

  publish(subject: string, headers: Record<string, string>, body: string, signal: AbortSignal): Promise<NatsReply> {
    return new Promise<NatsReply>((resolve, reject) => {
      const token = String(++this.next);
      const settle = () => {
        this.pending.delete(token);
        signal.removeEventListener("abort", onAbort);
      };
      const onAbort = () => {
        settle();
        reject(signal.reason);
      };
      if (signal.aborted) return onAbort();
      if (this.closed) return reject(new Error("NATS connection closed"));
      signal.addEventListener("abort", onAbort, { once: true });
      this.pending.set(token, {
        resolve: (reply) => (settle(), resolve(reply)),
        reject: (err) => (settle(), reject(err)),
      });
      let block = "NATS/1.0\r\n";
      for (const [name, value] of Object.entries(headers)) block += `${name}: ${value}\r\n`;
      block += "\r\n";
      const headerBytes = Buffer.byteLength(block);
      const frame = `HPUB ${subject} ${this.inbox}.${token} ${headerBytes} ${headerBytes + Buffer.byteLength(body)}\r\n${block}${body}\r\n`;
      this.ready.then(
        () => {
          if (this.pending.has(token) && !this.closed) this.socket.write(frame);
        },
        () => {},
      );
    });
  }

  fail(err: unknown): void {
    if (this.closed) return;
    this.closed = true;
    this.socket.destroy();
    this.handshake?.reject(err);
    for (const waiter of [...this.pending.values()]) waiter.reject(err);
  }

  private read(chunk: Buffer): void {
    this.buffer = this.buffer.length === 0 ? chunk : Buffer.concat([this.buffer, chunk]);
    while (!this.closed) {
      const end = this.buffer.indexOf("\r\n");
      if (end < 0) return;
      const line = this.buffer.subarray(0, end).toString();
      const [op = "", ...args] = line.split(/[ \t]+/);
      const verb = op.toUpperCase();
      if (verb === "MSG" || verb === "HMSG") {
        const total = Number(args.at(-1));
        const headerBytes = verb === "HMSG" ? Number(args.at(-2)) : 0;
        if (this.buffer.length < end + 2 + total + 2) return;
        const block = this.buffer.subarray(end + 2, end + 2 + total);
        this.buffer = this.buffer.subarray(end + 4 + total);
        this.reply(args[0] ?? "", block.subarray(0, headerBytes).toString(), block.subarray(headerBytes).toString());
        continue;
      }
      this.buffer = this.buffer.subarray(end + 2);
      if (verb === "PING") this.socket.write("PONG\r\n");
      else if (verb === "PONG") {
        this.handshake?.resolve();
        this.handshake = undefined;
      }
      else if (verb === "-ERR") this.fail(new Error(`NATS -ERR ${line.slice(4).trim()}`));
      else if (verb === "INFO" && !this.greeted) this.greet(line.slice(4));
    }
  }

  private greet(json: string): void {
    this.greeted = true;
    let info: { tls_required?: boolean; headers?: boolean };
    try {
      info = JSON.parse(json) as typeof info;
    } catch {
      return this.fail(new Error("NATS server sent an unreadable INFO"));
    }
    if (info.tls_required === true) return this.fail(new Error("the NATS server requires TLS, which this sink does not speak"));
    if (info.headers !== true) return this.fail(new Error("the NATS server does not support headers (NATS 2.2 or later)"));
    const user = decodeURIComponent(this.url.username);
    const pass = decodeURIComponent(this.url.password);
    const options = {
      verbose: false,
      pedantic: false,
      tls_required: false,
      name: "streamd forward",
      lang: "streamd",
      version: "1",
      protocol: 1,
      headers: true,
      no_responders: true,
      ...(pass !== "" ? { user, pass } : user !== "" && { auth_token: user }),
    };
    this.socket.write(`CONNECT ${JSON.stringify(options)}\r\nSUB ${this.inbox}.* 1\r\nPING\r\n`);
  }

  private reply(subject: string, headers: string, data: string): void {
    const waiter = subject.startsWith(`${this.inbox}.`) ? this.pending.get(subject.slice(this.inbox.length + 1)) : undefined;
    if (waiter === undefined) return;
    const status = STATUS.exec(headers.split("\r\n", 1)[0] ?? "");
    waiter.resolve(status === null ? { data } : { status: Number(status[1]), description: status[2]!, data });
  }
}

/**
 * A small NATS client over `node:net`, so the CLI ships no driver under Bun
 * or Node. It connects on the first publish, and again on the next publish
 * after a close, an `-ERR` or a dropped connection; it never reconnects or
 * queues on its own, so a lost connection fails the attempt and the retry
 * backoff decides when to try again.
 */
export function connectNats(url: string): NatsLike {
  const target = new URL(url);
  let current: Connection | undefined;
  return {
    request(subject, headers, body, signal) {
      if (current === undefined || current.closed) current = new Connection(target);
      return current.publish(subject, headers, body, signal);
    },
    close() {
      current?.fail(new Error("NATS connection closed"));
      current = undefined;
    },
  };
}
